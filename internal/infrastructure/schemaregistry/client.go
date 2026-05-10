package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) Register(ctx context.Context, subject, schema string) (int, error) {
	body, _ := json.Marshal(map[string]string{"schema": schema, "schemaType": "AVRO"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("register schema %s: status=%d body=%s", subject, resp.StatusCode, string(data))
	}
	var out struct {
		ID int `json:"id"`
	}
	return out.ID, json.Unmarshal(data, &out)
}

func (c *Client) SetCompatibility(ctx context.Context, subject, compatibility string) error {
	body, _ := json.Marshal(map[string]string{"compatibility": compatibility})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/config/"+subject, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set compatibility %s: status=%d body=%s", subject, resp.StatusCode, string(data))
	}
	return nil
}

func (c *Client) SchemaByID(ctx context.Context, id int) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("get schema id %d: status=%d body=%s", id, resp.StatusCode, string(data))
	}
	var out struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return "", err
	}
	return out.Schema, nil
}
