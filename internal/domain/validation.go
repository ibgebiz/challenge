package domain

import "fmt"

// channelMaxLen is the per-channel content character limit.
var channelMaxLen = map[Channel]int{
	ChannelSMS:   1600,
	ChannelEmail: 100000,
	ChannelPush:  4000,
}

// ValidateNotification enforces required fields, valid enums, and content limits.
// Content may be empty only when a template will render it.
func ValidateNotification(n Notification) error {
	if !n.Channel.Valid() {
		return fmt.Errorf("%w: invalid channel %q", ErrValidation, n.Channel)
	}
	if !n.Priority.Valid() {
		return fmt.Errorf("%w: invalid priority %q", ErrValidation, n.Priority)
	}
	if n.Recipient == "" {
		return fmt.Errorf("%w: recipient required", ErrValidation)
	}
	if n.Content == "" && n.TemplateID == nil {
		return fmt.Errorf("%w: content or template required", ErrValidation)
	}
	if maxLen := channelMaxLen[n.Channel]; len(n.Content) > maxLen {
		return fmt.Errorf("%w: content exceeds %d chars for %s", ErrValidation, maxLen, n.Channel)
	}
	return nil
}
