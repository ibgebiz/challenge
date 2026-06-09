package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateNotification_Valid(t *testing.T) {
	n := Notification{Channel: ChannelSMS, Recipient: "+905551234567", Content: "hi", Priority: PriorityNormal}
	if err := ValidateNotification(n); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
}

func TestValidateNotification_SMSTooLong(t *testing.T) {
	n := Notification{Channel: ChannelSMS, Recipient: "+905551234567", Content: strings.Repeat("a", 1601), Priority: PriorityNormal}
	err := ValidateNotification(n)
	if err == nil {
		t.Fatal("expected sms length error")
	}
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestValidateNotification_BadChannel(t *testing.T) {
	n := Notification{Channel: "fax", Recipient: "x", Content: "hi", Priority: PriorityNormal}
	if err := ValidateNotification(n); err == nil {
		t.Fatal("expected channel error")
	}
}

func TestValidateNotification_MissingRecipient(t *testing.T) {
	n := Notification{Channel: ChannelSMS, Content: "hi", Priority: PriorityNormal}
	if err := ValidateNotification(n); err == nil {
		t.Fatal("expected recipient error")
	}
}

func TestValidateNotification_ContentOrTemplateRequired(t *testing.T) {
	n := Notification{Channel: ChannelSMS, Recipient: "+1", Priority: PriorityNormal}
	if err := ValidateNotification(n); err == nil {
		t.Fatal("expected content/template error")
	}
	tid := "tmpl-1"
	withTemplate := Notification{Channel: ChannelSMS, Recipient: "+1", Priority: PriorityNormal, TemplateID: &tid}
	if err := ValidateNotification(withTemplate); err != nil {
		t.Fatalf("template should satisfy content requirement: %v", err)
	}
}
