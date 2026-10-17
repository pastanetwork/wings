package websocket

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server"
)

// setupRecordingHandler returns a handler whose client sends every event it
// receives on the returned channel.
func setupRecordingHandler(t *testing.T) (*Handler, <-chan Message) {
	t.Helper()

	cfg, err := config.NewAtPath(filepath.Join(t.TempDir(), "config.yml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.AuthenticationToken = "test-token"
	config.Set(cfg)

	s, err := server.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	settings, _ := json.Marshal(map[string]string{"uuid": serverUuid})
	if err := s.SyncWithConfiguration(remote.ServerConfigurationResponse{Settings: settings, ProcessConfiguration: &remote.ProcessConfiguration{}}); err != nil {
		t.Fatal(err)
	}

	conns := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		conns <- c
	}))
	t.Cleanup(srv.Close)

	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	received := make(chan Message, 16)
	go func() {
		for {
			var m Message
			if err := client.ReadJSON(&m); err != nil {
				return
			}
			received <- m
		}
	}()

	return &Handler{Connection: <-conns, server: s, limiter: NewLimiter()}, received
}

// Backup and archive events are only sent to users allowed to see them.
func TestEventsRequirePermission(t *testing.T) {
	events := map[string]string{
		server.BackupStartedEvent + ":abc":  PermissionReceiveBackups,
		server.BackupDeletedEvent + ":abc":  PermissionReceiveBackups,
		server.BackupCompletedEvent + ":ab": PermissionReceiveBackups,
		server.ArchiveStartedEvent:          PermissionReceiveArchives,
		server.ArchiveCompletedEvent:        PermissionReceiveArchives,
		server.ArchiveFailedEvent:           PermissionReceiveArchives,
		server.ArchiveProgressEvent:         PermissionReceiveArchives,
	}

	for event, permission := range events {
		t.Run(event, func(t *testing.T) {
			h, received := setupRecordingHandler(t)

			_, denied := signToken(t, serverUuid, PermissionConnect)
			h.setJwt(denied)
			if err := h.SendJson(Message{Event: Event(event)}); err != nil {
				t.Fatal(err)
			}

			_, allowed := signToken(t, serverUuid, PermissionConnect, permission)
			h.setJwt(allowed)
			if err := h.SendJson(Message{Event: Event(event)}); err != nil {
				t.Fatal(err)
			}
			// A marker event that is always sent, so the test knows every earlier
			// message has been delivered.
			if err := h.SendJson(Message{Event: Event(server.StatusEvent)}); err != nil {
				t.Fatal(err)
			}

			var got []Event
			for {
				select {
				case m := <-received:
					if m.Event == Event(server.StatusEvent) {
						if len(got) != 1 {
							t.Fatalf("expected the event to be sent once, only with %q, got %d times", permission, len(got))
						}
						return
					}
					got = append(got, m.Event)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for events")
				}
			}
		})
	}
}
