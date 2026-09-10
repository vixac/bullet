// Package rest implements Bullet's tenant-scoped client interfaces over HTTP.
package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vixac/bullet/client"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
)

type Logger interface{ Printf(string, ...any) }
type Option func(*Client)
type Client struct {
	baseURL    string
	space      model.TenancySpace
	httpClient *http.Client
	logger     Logger
}

var _ client.Client = (*Client)(nil)

// New binds all requests to space. The caller owns any supplied HTTP client.
func New(baseURL string, space model.TenancySpace, opts ...Option) *Client {
	c := &Client{baseURL: strings.TrimRight(baseURL, "/"), space: space, httpClient: &http.Client{Timeout: 30 * time.Second}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.httpClient = h } }
func WithLogger(l Logger) Option           { return func(c *Client) { c.logger = l } }

// HTTPError preserves server details and unwraps shared domain errors for errors.Is.
type HTTPError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *HTTPError) Error() string { return e.Message }
func (e *HTTPError) Unwrap() error { return protocol.DomainError(e.Code) }

func (c *Client) do(ctx context.Context, method, path string, body, result any, status int) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(protocol.AppIDHeader, strconv.FormatInt(int64(c.space.AppId), 10))
	req.Header.Set(protocol.TenancyIDHeader, strconv.FormatInt(c.space.TenancyId, 10))
	if c.httpClient == nil {
		return fmt.Errorf("HTTP client is nil")
	}
	if c.logger != nil {
		c.logger.Printf(">> %s %s", method, req.URL)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if c.logger != nil {
		c.logger.Printf("<< %d", resp.StatusCode)
	}
	if resp.StatusCode != status {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var failure protocol.ErrorResponse
		if json.Unmarshal(data, &failure) != nil || failure.Error == "" {
			failure.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, data)
		}
		return &HTTPError{StatusCode: resp.StatusCode, Code: failure.Code, Message: failure.Error}
	}
	if result == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return err
	}
	if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
		return fmt.Errorf("decode Bullet response: %w", err)
	}
	return nil
}
func (c *Client) call(method, path string, body, result any, status int) error {
	return c.do(context.Background(), method, path, body, result, status)
}
