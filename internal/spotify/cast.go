package spotify

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	SpotifyCastAppID    = "CC32E753"
	SpotifyCastClientID = "d7df0887fb71494ea994202cb473eae7"
	castMDNSService     = "_googlecast._tcp.local"
)

// CastSpeaker 代表在區域網路上透過 mDNS 探索到的 Google Cast 音響
type CastSpeaker struct {
	ID              string    `json:"id"`
	SpotifyDeviceID string    `json:"spotify_device_id,omitempty"`
	Name            string    `json:"name"`
	Model           string    `json:"model"`
	IP              string    `json:"ip"`
	Port            int       `json:"port"`
	ExpiresAt       time.Time `json:"expires_at"`
}

var (
	castMu       sync.RWMutex
	castSpeakers = make(map[string]*CastSpeaker)
	castStarted  bool
	castListener func()

	castDial = func(addr string) (net.Conn, error) {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		dialer := &net.Dialer{Timeout: 4 * time.Second}
		return tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	}
)

// OnCastSpeakerChange 註冊當探索到音響變動時的通知回調
func OnCastSpeakerChange(fn func()) {
	castMu.Lock()
	defer castMu.Unlock()
	castListener = fn
}

func notifyChange() {
	castMu.RLock()
	fn := castListener
	castMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// GetDiscoveredCastSpeakers 取得當前在線的 Google 音響列表快照（依名稱與 ID 穩定排序）
func GetDiscoveredCastSpeakers() []*CastSpeaker {
	castMu.RLock()
	defer castMu.RUnlock()
	now := time.Now()
	list := make([]*CastSpeaker, 0, len(castSpeakers))
	for _, s := range castSpeakers {
		if now.Before(s.ExpiresAt) {
			clone := *s
			list = append(list, &clone)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Name != list[j].Name {
			return list[i].Name < list[j].Name
		}
		return list[i].ID < list[j].ID
	})
	return list
}

// StartCastDiscovery 啟動背景 mDNS 多播監聽與定期掃描
func StartCastDiscovery() {
	castMu.Lock()
	if castStarted {
		castMu.Unlock()
		return
	}
	castStarted = true
	castMu.Unlock()

	go discoveryLoop()
}

func discoveryLoop() {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			_ = c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 0x0200 /* SO_REUSEPORT */, 1)
				if opErr == nil {
					opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				}
			})
			return opErr
		},
	}

	packetConn, err := lc.ListenPacket(context.Background(), "udp4", "0.0.0.0:5353")
	if err != nil {
		addr, _ := net.ResolveUDPAddr("udp4", "224.0.0.251:5353")
		packetConn, err = net.ListenMulticastUDP("udp4", nil, addr)
		if err != nil {
			packetConn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
			if err != nil {
				return
			}
		}
	}
	defer packetConn.Close()

	if udpConn, ok := packetConn.(*net.UDPConn); ok {
		if rawConn, err := udpConn.SyscallConn(); err == nil {
			_ = rawConn.Control(func(fd uintptr) {
				mreq := syscall.IPMreq{
					Multiaddr: [4]byte{224, 0, 0, 251},
					Interface: [4]byte{0, 0, 0, 0},
				}
				_ = syscall.SetsockoptIPMreq(int(fd), syscall.IPPROTO_IP, syscall.IP_ADD_MEMBERSHIP, &mreq)
			})
		}
	}

	destAddr := &net.UDPAddr{IP: net.ParseIP("224.0.0.251"), Port: 5353}
	query := buildPTRQuery(castMDNSService)

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			_, _ = packetConn.WriteTo(query, destAddr)
			<-ticker.C
		}
	}()

	_, _ = packetConn.WriteTo(query, destAddr)

	buf := make([]byte, 4096)
	for {
		_ = packetConn.SetReadDeadline(time.Now().Add(45 * time.Second))
		n, src, err := packetConn.ReadFrom(buf)
		if err != nil {
			cleanExpired()
			continue
		}

		parseMDNSResponse(buf[:n], src)
	}
}

func cleanExpired() {
	castMu.Lock()
	now := time.Now()
	changed := false
	for id, s := range castSpeakers {
		if now.After(s.ExpiresAt) {
			delete(castSpeakers, id)
			changed = true
		}
	}
	castMu.Unlock()
	if changed {
		notifyChange()
	}
}

func buildPTRQuery(service string) []byte {
	buf := new(bytes.Buffer)
	buf.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	for _, part := range strings.Split(service, ".") {
		buf.WriteByte(byte(len(part)))
		buf.WriteString(part)
	}
	buf.WriteByte(0x00)
	binary.Write(buf, binary.BigEndian, uint16(12)) // PTR = 12
	binary.Write(buf, binary.BigEndian, uint16(1))  // IN = 1
	return buf.Bytes()
}

type mdnsServiceRecord struct {
	id    string
	name  string
	model string
	port  int
}

func readDNSName(pkt []byte, off int) (string, int, bool) {
	var parts []string
	jumped := false
	nextOff := off
	for i := 0; i < 64 && off < len(pkt); i++ {
		l := int(pkt[off])
		if l == 0 {
			if !jumped {
				nextOff = off + 1
			}
			return strings.Join(parts, "."), nextOff, true
		}
		if l&0xC0 == 0xC0 {
			if off+1 >= len(pkt) {
				return "", len(pkt), false
			}
			ptr := ((l & 0x3F) << 8) | int(pkt[off+1])
			if !jumped {
				nextOff = off + 2
				jumped = true
			}
			off = ptr
			continue
		}
		off++
		if off+l > len(pkt) {
			return "", len(pkt), false
		}
		parts = append(parts, string(pkt[off:off+l]))
		off += l
	}
	return "", len(pkt), false
}

func extractMDNSRecords(pkt []byte) ([]mdnsServiceRecord, string) {
	if len(pkt) < 12 {
		return nil, ""
	}
	qdCount := int(binary.BigEndian.Uint16(pkt[4:6]))
	anCount := int(binary.BigEndian.Uint16(pkt[6:8]))
	nsCount := int(binary.BigEndian.Uint16(pkt[8:10]))
	arCount := int(binary.BigEndian.Uint16(pkt[10:12]))
	totalRR := anCount + nsCount + arCount
	if totalRR <= 0 || totalRR > 64 {
		return nil, ""
	}

	off := 12
	for i := 0; i < qdCount && off < len(pkt); i++ {
		var ok bool
		_, off, ok = readDNSName(pkt, off)
		if !ok || off+4 > len(pkt) {
			return nil, ""
		}
		off += 4
	}

	byOwner := make(map[string]*mdnsServiceRecord)
	var order []string
	getRec := func(owner string) *mdnsServiceRecord {
		if owner == "" {
			owner = "_default"
		}
		rec, ok := byOwner[owner]
		if !ok {
			rec = &mdnsServiceRecord{}
			byOwner[owner] = rec
			order = append(order, owner)
		}
		return rec
	}

	var ipv4 string
	var fallbackPort int
	for i := 0; i < totalRR && off < len(pkt); i++ {
		owner, nextOff, ok := readDNSName(pkt, off)
		if !ok || nextOff+10 > len(pkt) {
			break
		}
		off = nextOff
		rrType := binary.BigEndian.Uint16(pkt[off : off+2])
		rdLen := int(binary.BigEndian.Uint16(pkt[off+8 : off+10]))
		off += 10
		if off+rdLen > len(pkt) {
			break
		}
		rdata := pkt[off : off+rdLen]
		off += rdLen

		switch rrType {
		case 1: // A
			if rdLen == 4 && ipv4 == "" {
				ipv4 = net.IP(rdata).String()
			}
		case 33: // SRV
			if rdLen >= 6 {
				port := int(binary.BigEndian.Uint16(rdata[4:6]))
				if port > 0 {
					getRec(owner).port = port
					if fallbackPort == 0 {
						fallbackPort = port
					}
				}
			}
		case 16: // TXT
			rec := getRec(owner)
			for j := 0; j < len(rdata); {
				tl := int(rdata[j])
				j++
				if j+tl > len(rdata) {
					break
				}
				kv := string(rdata[j : j+tl])
				j += tl
				if strings.HasPrefix(kv, "fn=") {
					rec.name = strings.TrimSpace(kv[3:])
				} else if strings.HasPrefix(kv, "md=") {
					rec.model = strings.TrimSpace(kv[3:])
				} else if strings.HasPrefix(kv, "id=") {
					val := strings.TrimSpace(kv[3:])
					if cut := strings.IndexAny(val, "#'"); cut != -1 {
						val = val[:cut]
					}
					rec.id = val
				}
			}
		}
	}

	var out []mdnsServiceRecord
	for _, owner := range order {
		rec := byOwner[owner]
		if rec.id == "" || rec.name == "" {
			continue
		}
		if rec.port == 0 {
			rec.port = fallbackPort
		}
		out = append(out, *rec)
	}
	return out, ipv4
}

func parseMDNSResponse(data []byte, src net.Addr) {
	if len(data) < 12 {
		return
	}

	ipStr := ""
	if udpAddr, ok := src.(*net.UDPAddr); ok {
		ipStr = udpAddr.IP.String()
	}

	if !bytes.Contains(data, []byte("fn=")) || !bytes.Contains(data, []byte("id=")) {
		return
	}

	records, dnsIP := extractMDNSRecords(data)
	if ipStr == "" && dnsIP != "" {
		ipStr = dnsIP
	}
	if len(records) == 0 {
		name, model, id := extractCastFields(data)
		if id != "" && name != "" {
			records = []mdnsServiceRecord{{id: id, name: name, model: model, port: 8009}}
		}
	}
	if len(records) == 0 {
		return
	}

	changed := false
	now := time.Now()
	castMu.Lock()
	for _, rec := range records {
		model := rec.model
		if model == "" {
			model = "Google Cast Speaker"
		}
		port := rec.port
		if port <= 0 {
			port = 8009
		}
		existing, found := castSpeakers[rec.id]
		if !found || existing.Name != rec.name || existing.IP != ipStr || existing.Port != port || existing.Model != model {
			spotifyDevID := ""
			if found {
				spotifyDevID = existing.SpotifyDeviceID
			}
			castSpeakers[rec.id] = &CastSpeaker{
				ID:              rec.id,
				SpotifyDeviceID: spotifyDevID,
				Name:            rec.name,
				Model:           model,
				IP:              ipStr,
				Port:            port,
				ExpiresAt:       now.Add(90 * time.Second),
			}
			changed = true
		} else {
			existing.ExpiresAt = now.Add(90 * time.Second)
			existing.IP = ipStr
			existing.Port = port
		}
	}
	castMu.Unlock()
	if changed {
		notifyChange()
	}
}

func extractCastFields(data []byte) (name, model, id string) {
	knownKeys := []string{"id=", "fn=", "md="}
	for _, key := range knownKeys {
		idx := bytes.Index(data, []byte(key))
		if idx == -1 {
			continue
		}
		valStart := idx + len(key)
		valEnd := valStart

		for valEnd < len(data) {
			if data[valEnd] < 32 {
				break
			}
			if valEnd+3 < len(data) && (data[valEnd+2] == '=' || data[valEnd+3] == '=') {
				prevChar := data[valEnd-1]
				if prevChar < 32 || prevChar == ' ' {
					valEnd--
					break
				}
			}
			valEnd++
		}

		rawVal := strings.TrimSpace(string(data[valStart:valEnd]))
		if strings.HasPrefix(key, "fn") {
			name = rawVal
		} else if strings.HasPrefix(key, "md") {
			model = rawVal
		} else if strings.HasPrefix(key, "id") {
			if cut := strings.IndexAny(rawVal, "#'"); cut != -1 {
				rawVal = rawVal[:cut]
			}
			id = rawVal
		}
	}
	return
}

func encodeCastBytes(fieldNum int, val []byte) []byte {
	out := make([]byte, 0, 6+len(val))
	out = append(out, byte((fieldNum<<3)|2))
	n := len(val)
	for n >= 0x80 {
		out = append(out, byte(n)|0x80)
		n >>= 7
	}
	out = append(out, byte(n))
	return append(out, val...)
}

func encodeCastVarint(fieldNum int, val byte) []byte {
	return []byte{byte((fieldNum << 3) | 0), val}
}

func writeCastFrame(conn net.Conn, sourceID, destID, namespace string, payload []byte) error {
	body := make([]byte, 0, 128+len(payload))
	body = append(body, encodeCastVarint(1, 0)...)
	body = append(body, encodeCastBytes(2, []byte(sourceID))...)
	body = append(body, encodeCastBytes(3, []byte(destID))...)
	body = append(body, encodeCastBytes(4, []byte(namespace))...)
	body = append(body, encodeCastVarint(5, 0)...)
	body = append(body, encodeCastBytes(6, payload)...)

	packet := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(packet[:4], uint32(len(body)))
	copy(packet[4:], body)
	_, err := conn.Write(packet)
	return err
}

func readCastFrame(conn net.Conn) (string, error) {
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length > 1<<20 {
		return "", errors.New("cast frame too large")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func fetchCastDeviceAuthToken(clientID, deviceID string, renew bool) (string, error) {
	id, err := CurrentIdentity()
	if err != nil {
		return "", err
	}
	if clientID == "" {
		clientID = SpotifyCastClientID
	}
	rawBody, err := json.Marshal(map[string]string{
		"clientId": clientID,
		"deviceId": deviceID,
	})
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, spclientURL+"/device-auth/v1/refresh", bytes.NewReader(rawBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+id.Token)
	if id.ClientToken != "" {
		req.Header.Set("client-token", id.ClientToken)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Origin", "https://open.spotify.com")
	req.Header.Set("Referer", "https://open.spotify.com/")
	req.Header.Set("app-platform", "WebPlayer")

	res, err := Client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if res.StatusCode == http.StatusUnauthorized && renew {
		if _, err := RenewedIdentity(); err != nil {
			return "", err
		}
		return fetchCastDeviceAuthToken(clientID, deviceID, false)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return "", fmt.Errorf("device-auth HTTP %d", res.StatusCode)
	}
	var reply struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return "", err
	}
	if reply.AccessToken == "" {
		return "", errors.New("empty accessToken from device-auth")
	}
	return reply.AccessToken, nil
}

// ConnectCastSpeaker 喚醒目標 Google Cast 音響上的 Spotify Receiver (CC32E753)，
// 並透過 com.spotify.chromecast.secure.v1 完成 getInfo 與 addUser 認證，回傳 Spotify Connect Device ID。
func ConnectCastSpeaker(spk *CastSpeaker) (string, error) {
	if spk == nil {
		return "", errors.New("cast speaker is nil")
	}
	port := spk.Port
	if port <= 0 {
		port = 8009
	}
	addr := fmt.Sprintf("%s:%d", spk.IP, port)
	conn, err := castDial(addr)
	if err != nil {
		return "", err
	}
	keepOpen := false
	defer func() {
		if !keepOpen {
			conn.Close()
		}
	}()

	_ = conn.SetDeadline(time.Now().Add(12 * time.Second))

	const senderID = "sender-0"
	// Step 1: 建立與 receiver-0 的虛擬通道
	if err := writeCastFrame(conn, senderID, "receiver-0", "urn:x-cast:com.google.cast.tp.connection", []byte(`{"type":"CONNECT"}`)); err != nil {
		return "", err
	}
	// Step 2: 啟動 Spotify Cast Receiver (CC32E753)
	launchPayload, _ := json.Marshal(map[string]any{
		"type":      "LAUNCH",
		"appId":     SpotifyCastAppID,
		"requestId": 1,
	})
	if err := writeCastFrame(conn, senderID, "receiver-0", "urn:x-cast:com.google.cast.receiver", launchPayload); err != nil {
		return "", err
	}

	remoteName := spk.Name
	if remoteName == "" {
		remoteName = "Google Cast Speaker"
	}
	devID := spk.ID
	if devID == "" {
		devID = "myspotify-cast"
	}
	isGroup := strings.Contains(strings.ToLower(spk.Model), "group")

	var transportID string
	var spotifyDeviceID string
	sentAddUser := false

	for i := 0; i < 24; i++ {
		msg, err := readCastFrame(conn)
		if err != nil {
			return "", err
		}
		if strings.Contains(msg, `"PING"`) {
			_ = writeCastFrame(conn, senderID, "receiver-0", "urn:x-cast:com.google.cast.tp.heartbeat", []byte(`{"type":"PONG"}`))
		}
		if strings.Contains(msg, `"LAUNCH_ERROR"`) {
			return "", errors.New("cast receiver refused to launch Spotify app")
		}
		if transportID == "" && strings.Contains(msg, `"`+SpotifyCastAppID+`"`) {
			if idx := strings.Index(msg, `"transportId":"`); idx != -1 {
				sub := msg[idx+len(`"transportId":"`):]
				if end := strings.IndexByte(sub, '"'); end != -1 {
					transportID = sub[:end]
					if err := writeCastFrame(conn, senderID, transportID, "urn:x-cast:com.google.cast.tp.connection", []byte(`{"type":"CONNECT"}`)); err != nil {
						return "", err
					}
					getInfoBody, _ := json.Marshal(map[string]any{
						"type": "getInfo",
						"payload": map[string]any{
							"remoteName":        remoteName,
							"deviceID":          devID,
							"deviceAPI_isGroup": isGroup,
						},
					})
					if err := writeCastFrame(conn, senderID, transportID, "urn:x-cast:com.spotify.chromecast.secure.v1", getInfoBody); err != nil {
						return "", err
					}
				}
			}
		}
		if idx := strings.Index(msg, `{"type":"getInfoResponse"`); idx != -1 && !sentAddUser {
			sentAddUser = true
			var env struct {
				Payload struct {
					DeviceID string `json:"deviceID"`
					ClientID string `json:"clientID"`
				} `json:"payload"`
			}
			if jsonEnd := strings.LastIndexByte(msg[idx:], '}'); jsonEnd != -1 {
				_ = json.Unmarshal([]byte(msg[idx:idx+jsonEnd+1]), &env)
			}
			if env.Payload.DeviceID == "" {
				return "", errors.New("missing deviceID in getInfoResponse")
			}
			spotifyDeviceID = env.Payload.DeviceID
			castToken, err := fetchCastDeviceAuthToken(env.Payload.ClientID, spotifyDeviceID, true)
			if err != nil {
				return "", err
			}
			addUserBody, _ := json.Marshal(map[string]any{
				"type": "addUser",
				"payload": map[string]any{
					"tokenType": "accesstoken",
					"blob":      castToken,
				},
			})
			if err := writeCastFrame(conn, senderID, transportID, "urn:x-cast:com.spotify.chromecast.secure.v1", addUserBody); err != nil {
				return "", err
			}
		}
		if strings.Contains(msg, `"addUserError"`) || strings.Contains(msg, `"getInfoError"`) {
			return "", errors.New("spotify cast authentication rejected by speaker")
		}
		if strings.Contains(msg, `"addUserResponse"`) {
			if spk.ID != "" && spotifyDeviceID != "" {
				castMu.Lock()
				if existing, ok := castSpeakers[spk.ID]; ok {
					existing.SpotifyDeviceID = spotifyDeviceID
				}
				castMu.Unlock()
			}
			keepOpen = true
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
				for {
					m, err := readCastFrame(conn)
					if err != nil {
						return
					}
					if strings.Contains(m, `"PING"`) {
						_ = writeCastFrame(conn, senderID, "receiver-0", "urn:x-cast:com.google.cast.tp.heartbeat", []byte(`{"type":"PONG"}`))
					}
				}
			}()
			return spotifyDeviceID, nil
		}
	}
	return "", errors.New("cast handshake timed out")
}

// WakeupCastSpeaker 向目標音響發起 CastV2 喚醒與 Spotify 認證握手
func WakeupCastSpeaker(ip string, port int) error {
	_, err := ConnectCastSpeaker(&CastSpeaker{IP: ip, Port: port})
	return err
}

// ResetCastSpeakersForTest 供測試注入固定的探索音響列表
func ResetCastSpeakersForTest(list []*CastSpeaker) {
	castMu.Lock()
	defer castMu.Unlock()
	castSpeakers = make(map[string]*CastSpeaker, len(list))
	for _, s := range list {
		clone := *s
		castSpeakers[s.ID] = &clone
	}
}
