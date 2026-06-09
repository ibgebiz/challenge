package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestSend_Accepts202(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m1","status":"accepted","timestamp":"2026-06-09T00:00:00Z"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client())
	resp, err := c.Send(context.Background(), domain.Notification{Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp.MessageID != "m1" {
		t.Fatalf("got %q", resp.MessageID)
	}
}

func TestSend_SendsCorrectBody(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m1","status":"accepted","timestamp":"t"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client())
	_, err := c.Send(context.Background(), domain.Notification{Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "Your message"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, want := range []string{`"to":"+905551234567"`, `"channel":"sms"`, `"content":"Your message"`} {
		if !strings.Contains(gotBody, want) {
			t.Fatalf("body %q missing %q", gotBody, want)
		}
	}
}

func TestSend_Non202IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, srv.Client())
	if _, err := c.Send(context.Background(), domain.Notification{Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi"}); err == nil {
		t.Fatal("expected error on 500")
	}
}
