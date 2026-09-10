package rest

import (
	"github.com/vixac/bullet/protocol"
	"net/http"
	"strconv"
)

func depotPath(id int64) string  { return "/depot/items/" + strconv.FormatInt(id, 10) }
func bucketPath(id int32) string { return "/depot/bucket/" + strconv.FormatInt(int64(id), 10) }
func (c *Client) DepotCreate(bucket int32, value string) (int64, error) {
	var r protocol.DepotCreateResponse
	err := c.call(http.MethodPost, "/depot/items", protocol.DepotCreateRequest{BucketID: bucket, Value: value}, &r, http.StatusCreated)
	if err != nil {
		return 0, err
	}
	return r.ID, nil
}
func (c *Client) DepotCreateMany(bucket int32, values []string) ([]int64, error) {
	var r protocol.DepotCreateManyResponse
	err := c.call(http.MethodPost, "/depot/items/batch", protocol.DepotCreateManyRequest{BucketID: bucket, Values: values}, &r, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	return r.IDs, nil
}
func (c *Client) DepotUpdate(id int64, value string) error {
	return c.call(http.MethodPut, depotPath(id), protocol.DepotUpdateRequest{Value: value}, nil, http.StatusOK)
}
func (c *Client) DepotGet(id int64) (string, error) {
	var r protocol.DepotGetResponse
	err := c.call(http.MethodGet, depotPath(id), nil, &r, http.StatusOK)
	if err != nil {
		return "", err
	}
	return r.Value, nil
}
func (c *Client) DepotGetMany(ids []int64) (map[int64]string, []int64, error) {
	var r protocol.DepotGetManyResponse
	err := c.call(http.MethodPost, "/depot/items/batch-get", protocol.DepotGetManyRequest{IDs: ids}, &r, http.StatusOK)
	if err != nil {
		return nil, nil, err
	}
	return r.Values, r.Missing, nil
}
func (c *Client) DepotDelete(id int64) error {
	return c.call(http.MethodDelete, depotPath(id), nil, nil, http.StatusNoContent)
}
func (c *Client) DepotDeleteByBucket(bucket int32) error {
	return c.call(http.MethodDelete, bucketPath(bucket), nil, nil, http.StatusNoContent)
}
func (c *Client) DepotGetAllByBucket(bucket int32) (map[int64]string, error) {
	var r protocol.DepotGetAllByBucketResponse
	err := c.call(http.MethodGet, bucketPath(bucket), nil, &r, http.StatusOK)
	if err != nil {
		return nil, err
	}
	return r.Values, nil
}
