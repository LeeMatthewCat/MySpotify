package spotify

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func buildTestMDNSPacket(instance, host string, port uint16, txt []string) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00, 0x84, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00})

	writeName := func(b *bytes.Buffer, name string) {
		for _, part := range strings.Split(name, ".") {
			b.WriteByte(byte(len(part)))
			b.WriteString(part)
		}
		b.WriteByte(0)
	}

	writeName(&buf, instance)
	binary.Write(&buf, binary.BigEndian, uint16(33)) // SRV
	binary.Write(&buf, binary.BigEndian, uint16(1))  // IN
	binary.Write(&buf, binary.BigEndian, uint32(120))
	var srvData bytes.Buffer
	binary.Write(&srvData, binary.BigEndian, uint16(0))
	binary.Write(&srvData, binary.BigEndian, uint16(0))
	binary.Write(&srvData, binary.BigEndian, port)
	writeName(&srvData, host)
	binary.Write(&buf, binary.BigEndian, uint16(srvData.Len()))
	buf.Write(srvData.Bytes())

	writeName(&buf, instance)
	binary.Write(&buf, binary.BigEndian, uint16(16)) // TXT
	binary.Write(&buf, binary.BigEndian, uint16(1))  // IN
	binary.Write(&buf, binary.BigEndian, uint32(120))
	var txtData bytes.Buffer
	for _, entry := range txt {
		txtData.WriteByte(byte(len(entry)))
		txtData.WriteString(entry)
	}
	binary.Write(&buf, binary.BigEndian, uint16(txtData.Len()))
	buf.Write(txtData.Bytes())

	return buf.Bytes()
}

// Discovered Cast speakers keep a stable order across calls and read their SRV port (such as a Cast group's dynamic port) instead of randomizing map iteration on port 8009.
func TestCastSpeakersOrderAndSRVPort(t *testing.T) {
	castMu.Lock()
	castSpeakers = make(map[string]*CastSpeaker)
	castMu.Unlock()

	pktGrey := buildTestMDNSPacket(
		"Google-Home-Mini-25c0._googlecast._tcp.local",
		"grey.local",
		8009,
		[]string{"id=25c0340e9130a11426ab189531081c48", "fn=灰", "md=Google Home Mini"},
	)
	pktGroup := buildTestMDNSPacket(
		"Google-Cast-Group-D0D3._googlecast._tcp.local",
		"grey.local",
		32072,
		[]string{"id=D0D368CA-30E6-4429-B44C-7285A1803BB5", "fn=音響", "md=Google Cast Group"},
	)
	pktBlack := buildTestMDNSPacket(
		"Google-Nest-Mini-4c21._googlecast._tcp.local",
		"black.local",
		8009,
		[]string{"id=4c218e44fa2685c879581473af027", "fn=黑", "md=Google Nest Mini"},
	)

	parseMDNSResponse(pktGrey, &net.UDPAddr{IP: net.ParseIP("192.168.1.107"), Port: 5353})
	parseMDNSResponse(pktGroup, &net.UDPAddr{IP: net.ParseIP("192.168.1.107"), Port: 5353})
	parseMDNSResponse(pktBlack, &net.UDPAddr{IP: net.ParseIP("192.168.1.106"), Port: 5353})

	var baseline string
	for i := 0; i < 30; i++ {
		got := GetDiscoveredCastSpeakers()
		if len(got) != 3 {
			t.Fatalf("got %d speakers, want 3", len(got))
		}
		names := make([]string, len(got))
		for j, s := range got {
			names[j] = s.Name + ":" + strconv.Itoa(s.Port)
			if s.Name == "音響" && s.Port != 32072 {
				t.Fatalf("Cast group 音響 port = %d, want 32072 from SRV record", s.Port)
			}
		}
		order := strings.Join(names, "|")
		if i == 0 {
			baseline = order
		} else if order != baseline {
			t.Fatalf("speaker order jumped on call #%d: %q vs %q", i, order, baseline)
		}
	}
}

// Waking a Cast speaker opens receiver-0, launches CC32E753, exchanges getInfo for a device-auth token, and sends addUser with a multi-byte varint frame.
func TestWakeupCastSpeakerHandshake(t *testing.T) {
	oldClient := Client
	oldDial := castDial
	oldID, _ := CurrentIdentity()
	defer func() {
		Client = oldClient
		castDial = oldDial
		SetIdentity(oldID)
	}()
	SetIdentity(Identity{Token: "web-player-token", ClientToken: "client-tok", Expires: time.Now().Add(time.Hour)})

	longCastToken := strings.Repeat("cast-access-token-", 25) // 450 bytes to verify multi-byte protobuf varint
	Client = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/device-auth/v1/refresh" {
				body, _ := json.Marshal(map[string]string{"accessToken": longCastToken})
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(body)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			}
			return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
		}),
	}

	addUserReceived := make(chan string, 1)
	castDial = func(addr string) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			receiverConnected := false
			appConnected := false
			for {
				var length uint32
				if err := binary.Read(serverConn, binary.BigEndian, &length); err != nil {
					return
				}
				frame := make([]byte, length)
				if _, err := io.ReadFull(serverConn, frame); err != nil {
					return
				}
				s := string(frame)
				if !receiverConnected {
					if strings.Contains(s, "urn:x-cast:com.google.cast.tp.connection") && strings.Contains(s, `"CONNECT"`) {
						receiverConnected = true
						continue
					}
					_ = writeCastFrame(serverConn, "receiver-0", "sender-0", "urn:x-cast:com.google.cast.tp.connection", []byte(`{"type":"CLOSE"}`))
					return
				}
				if strings.Contains(s, "urn:x-cast:com.google.cast.receiver") && strings.Contains(s, `"LAUNCH"`) {
					if !strings.Contains(s, `"CC32E753"`) {
						_ = writeCastFrame(serverConn, "receiver-0", "sender-0", "urn:x-cast:com.google.cast.receiver", []byte(`{"type":"LAUNCH_ERROR","reason":"NOT_FOUND","requestId":1}`))
						return
					}
					_ = writeCastFrame(serverConn, "receiver-0", "sender-0", "urn:x-cast:com.google.cast.receiver", []byte(`{"type":"RECEIVER_STATUS","requestId":1,"status":{"applications":[{"appId":"CC32E753","transportId":"transport-spotify-1"}]}}`))
					continue
				}
				if strings.Contains(s, "transport-spotify-1") && strings.Contains(s, "urn:x-cast:com.google.cast.tp.connection") && strings.Contains(s, `"CONNECT"`) {
					appConnected = true
					continue
				}
				if appConnected && strings.Contains(s, "urn:x-cast:com.spotify.chromecast.secure.v1") && strings.Contains(s, `"getInfo"`) {
					_ = writeCastFrame(serverConn, "transport-spotify-1", "sender-0", "urn:x-cast:com.spotify.chromecast.secure.v1", []byte(`{"type":"getInfoResponse","payload":{"deviceID":"e59f3506ff31b29fd7ae7b5b7e445e4e","clientID":"d7df0887fb71494ea994202cb473eae7","tokenType":"accesstoken","status":101,"statusString":"OK"}}`))
					continue
				}
				if appConnected && strings.Contains(s, "urn:x-cast:com.spotify.chromecast.secure.v1") && strings.Contains(s, `"addUser"`) {
					if strings.Contains(s, longCastToken) {
						addUserReceived <- "e59f3506ff31b29fd7ae7b5b7e445e4e"
						_ = writeCastFrame(serverConn, "transport-spotify-1", "sender-0", "urn:x-cast:com.spotify.chromecast.secure.v1", []byte(`{"type":"addUserResponse","payload":{"status":101,"statusString":"OK","deviceId":"e59f3506ff31b29fd7ae7b5b7e445e4e"}}`))
					} else {
						_ = writeCastFrame(serverConn, "transport-spotify-1", "sender-0", "urn:x-cast:com.spotify.chromecast.secure.v1", []byte(`{"type":"addUserError","payload":{"status":202,"statusString":"ERROR-LOGIN-FAILED"}}`))
					}
					return
				}
			}
		}()
		return clientConn, nil
	}

	gotDevID, err := ConnectCastSpeaker(&CastSpeaker{
		ID:    "D0D368CA-30E6-4429-B44C-7285A1803BB5",
		Name:  "音響",
		Model: "Google Cast Group",
		IP:    "127.0.0.1",
		Port:  32072,
	})
	if err != nil {
		t.Fatalf("ConnectCastSpeaker failed: %v", err)
	}
	if gotDevID != "e59f3506ff31b29fd7ae7b5b7e445e4e" {
		t.Fatalf("ConnectCastSpeaker deviceID = %q, want e59f3506ff31b29fd7ae7b5b7e445e4e", gotDevID)
	}
	select {
	case gotID := <-addUserReceived:
		if gotID != "e59f3506ff31b29fd7ae7b5b7e445e4e" {
			t.Fatalf("got deviceID %q", gotID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectCastSpeaker returned without completing getInfo + device-auth + addUser handshake")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
