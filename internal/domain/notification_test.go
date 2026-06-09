package domain

import "testing"

func TestChannelValid(t *testing.T) {
	if !ChannelSMS.Valid() {
		t.Fatal("sms should be valid")
	}
	if Channel("fax").Valid() {
		t.Fatal("fax should be invalid")
	}
}

func TestPriorityValid(t *testing.T) {
	if !PriorityHigh.Valid() {
		t.Fatal("high should be valid")
	}
	if Priority("urgent").Valid() {
		t.Fatal("urgent should be invalid")
	}
}

func TestStatusCancellable(t *testing.T) {
	cases := map[Status]bool{
		StatusPending:   true,
		StatusQueued:    true,
		StatusSending:   false,
		StatusDelivered: false,
		StatusFailed:    false,
		StatusCancelled: false,
	}
	for s, want := range cases {
		if s.Cancellable() != want {
			t.Fatalf("%s cancellable=%v want %v", s, s.Cancellable(), want)
		}
	}
}
