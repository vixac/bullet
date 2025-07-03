package client

import (
	"encoding/json"
	"fmt"

	"github.com/vixac/bullet/model"
)

func (c *BulletClient) WayFinderInsertOne(req model.WayFinderPutRequest) (int64, error) {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.postReq("/insert-one", bodyBytes)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	var result struct {
		ItemId int64 `json:"itemId"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return 0, fmt.Errorf("failed to unmarshal response: %w, message body was '%s'", err, string(resp))
	}

	return result.ItemId, nil
}

func (c *BulletClient) WayFinderQueryByPrefix(req model.WayFinderPrefixQueryRequest) ([]model.WayFinderQueryItem, error) {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.postReq("/query-by-prefix", bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	var result struct {
		Items []model.WayFinderQueryItem `json:"items"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w, message body was '%s'", err, string(resp))
	}
	return result.Items, nil
}
