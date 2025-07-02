package client

import (
	"encoding/json"
	"fmt"

	"github.com/vixac/bullet/model"
)

func (c *BulletClient) DepotInsertOne(req model.DepotRequest) error {
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}
	_, err = c.postReq("/insert-one", bodyBytes)
	if err != nil {
		return err
	}
	return nil
}

func (c *BulletClient) DepotGetMany(req model.DepotGetManyRequest) (*model.DepotGetManyResponse, error) {
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
	var result model.DepotGetManyResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w, message body was '%s'", err, string(resp))
	}
	return &result, nil
}
