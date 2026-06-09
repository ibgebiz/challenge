// Package webhook implements the Provider port by POSTing to a webhook.site URL
// that simulates the external notification provider.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// Client sends notifications to a configured provider URL.
type Client struct {
	url  string
	http *http.Client
}

// New constructs a Client. If hc is nil, http.DefaultClient is used.
func New(url string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{url: url, http: hc}
}

type reqBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Send POSTs the notification and parses the provider's 202 response.
func (c *Client) Send(ctx context.Context, n domain.Notification) (usecase.ProviderResponse, error) {
	body, err := json.Marshal(reqBody{To: n.Recipient, Channel: string(n.Channel), Content: n.Content})
	if err != nil {
		return usecase.ProviderResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return usecase.ProviderResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return usecase.ProviderResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		return usecase.ProviderResponse{}, fmt.Errorf("provider returned status %d", resp.StatusCode)
	}
	var out usecase.ProviderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return usecase.ProviderResponse{}, fmt.Errorf("decode provider response: %w", err)
	}
	return out, nil
}
