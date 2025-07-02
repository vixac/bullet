package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/vixac/bullet/model"
)

type BulletClient struct {
	BaseURL    string
	HTTPClient *http.Client
	AppID      string
}

func NewBulletClient(baseURL string, appId int) *BulletClient {
	return &BulletClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{},
		AppID:      strconv.Itoa(appId),
	}
}

func (c *BulletClient) GetMany(req model.GetManyRequest) (*model.GetManyResponse, error) {

	// marshal request body
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// create request
	httpReq, err := http.NewRequest("POST", c.BaseURL+"/get-many", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-App-ID", c.AppID)

	// execute
	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// read response
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	// unmarshal
	var result model.GetManyResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w, message body was '%s'", err, string(respBody))
	}

	return &result, nil
}

func (c *BulletClient) InsertOne(bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	reqBody := map[string]interface{}{
		"bucketId": bucketID,
		"key":      key,
		"value":    value,
	}
	if tag != nil {
		reqBody["tag"] = *tag
	}
	if metric != nil {
		reqBody["metric"] = *metric
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", c.BaseURL+"/insert-one", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-ID", c.AppID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bullet client: unexpected status code %d", resp.StatusCode)
	}
	return nil
}
