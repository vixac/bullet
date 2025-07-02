package client

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/vixac/bullet/model"
)

func (c *BulletClient) TrackGetMany(req model.TrackGetManyRequest) (*model.TrackGetManyResponse, error) {

	// marshal request body
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// execute
	resp, err := c.postReq("/get-many", bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	// unmarshal
	var result model.TrackGetManyResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w, message body was '%s'", err, string(resp))
	}

	return &result, nil
}

func (c *BulletClient) TrackInsertOne(bucketID int32, key string, value int, tag *int64, metric *float64) error {
	reqBody := map[string]interface{}{
		"bucketId": bucketID,
		"key":      key,
		"value":    strconv.Itoa(value),
	}
	if tag != nil {
		reqBody["tag"] = *tag
	}
	if metric != nil {
		reqBody["metric"] = *metric
	}
	bodyBytes, _ := json.Marshal(reqBody)
	_, err := c.postReq("/insert-one", bodyBytes)
	if err != nil {
		return err
	}
	return nil
}
