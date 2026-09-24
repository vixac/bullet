package rest

import (
	"fmt"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	"net/http"
	"strconv"
)

func (c *Client) TrackPut(bucketID int32, key string, value model.TrackValue) error {
	return c.call(http.MethodPost, "/track/items", protocol.TrackRequest{BucketID: bucketID, Key: key, Value: value.Value, Tag: value.Tag, Metric: value.Metric, Payload: protocol.PayloadFromModel(value.Payload)}, nil, http.StatusOK)
}
func (c *Client) TrackGet(bucketID int32, key string, opts model.TrackReadOptions) (model.TrackValue, error) {
	var r protocol.TrackGetResponse
	err := c.call(http.MethodPost, "/track/items/get", protocol.TrackGetRequest{BucketID: bucketID, Key: key, IncludePayload: opts.IncludePayload}, &r, http.StatusOK)
	if err != nil {
		return model.TrackValue{}, err
	}
	return r.Value.Model(), nil
}
func (c *Client) TrackPutMany(items map[int32][]model.TrackKeyValueItem) error {
	r := protocol.TrackPutManyRequest{}
	for bucket, values := range items {
		r.Buckets = append(r.Buckets, protocol.TrackPutItems{BucketID: bucket, Items: protocol.TrackItemsFromModel(values, true)})
	}
	return c.call(http.MethodPost, "/track/items/batch", r, nil, http.StatusOK)
}
func (c *Client) TrackDeleteMany(keys []model.TrackKey) error {
	r := protocol.TrackDeleteManyRequest{Items: make([]protocol.TrackBucketKeyPair, len(keys))}
	for i, k := range keys {
		r.Items[i] = protocol.TrackBucketKeyPair{BucketID: k.BucketID, Key: k.Key}
	}
	return c.call(http.MethodDelete, "/track/items", r, nil, http.StatusOK)
}
func (c *Client) TrackMutate(req model.TrackMutation) (model.TrackMutationResult, error) {
	r := protocol.TrackMutateRequest{MutationID: string(req.MutationID)}
	for _, p := range req.Puts {
		r.Puts = append(r.Puts, protocol.TrackRequest{BucketID: p.BucketID, Key: p.Key, Value: p.Value.Value, Tag: p.Value.Tag, Metric: p.Value.Metric, Payload: protocol.PayloadFromModel(p.Value.Payload), IfAbsent: p.IfAbsent})
	}
	for _, k := range req.Deletes {
		r.Deletes = append(r.Deletes, protocol.TrackBucketKeyPair{BucketID: k.BucketID, Key: k.Key})
	}
	var result protocol.TrackMutateResponse
	if err := c.call(http.MethodPost, "/track/mutate", r, &result, http.StatusOK); err != nil {
		return model.TrackMutationResult{}, err
	}
	return model.TrackMutationResult{Applied: result.Applied}, nil
}
func (c *Client) TrackGetMany(keys map[int32][]string, opts model.TrackReadOptions) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {
	req := protocol.TrackGetManyRequest{IncludePayload: opts.IncludePayload}
	for bucket, ks := range keys {
		req.Buckets = append(req.Buckets, protocol.TrackGetKeys{BucketID: bucket, Keys: ks})
	}
	var r protocol.TrackGetManyResponse
	if err := c.call(http.MethodPost, "/track/items/batch-get", req, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	values := make(map[int32]map[string]model.TrackValue, len(r.Values))
	missing := make(map[int32][]string, len(r.Missing))
	for bucket, vs := range r.Values {
		b, err := strconv.ParseInt(bucket, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid bucket ID %q: %w", bucket, err)
		}
		values[int32(b)] = make(map[string]model.TrackValue, len(vs))
		for k, v := range vs {
			values[int32(b)][k] = v.Model()
		}
	}
	for bucket, ks := range r.Missing {
		b, err := strconv.ParseInt(bucket, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid bucket ID %q: %w", bucket, err)
		}
		missing[int32(b)] = ks
	}
	return values, missing, nil
}
func trackMetric(value *float64, gt bool) *protocol.MetricFilter {
	if value == nil {
		return nil
	}
	op := "lt"
	if gt {
		op = "gt"
	}
	return &protocol.MetricFilter{Operator: op, Value: *value}
}
func (c *Client) trackQuery(path string, req any) ([]model.TrackKeyValueItem, error) {
	var r protocol.TrackQueryResponse
	if err := c.call(http.MethodPost, path, req, &r, http.StatusOK); err != nil {
		return nil, err
	}
	if r.Items == nil {
		return nil, nil
	}
	items := make([]model.TrackKeyValueItem, len(r.Items))
	for i, v := range r.Items {
		items[i] = model.TrackKeyValueItem{Key: v.Key, Value: v.Value.Model()}
	}
	return items, nil
}
func (c *Client) GetItemsByKeyPrefix(bucketID int32, prefix string, tags []int64, metric *float64, gt bool) ([]model.TrackKeyValueItem, error) {
	return c.trackQuery("/track/query", protocol.TrackGetItemsByPrefixRequest{BucketID: bucketID, Prefix: prefix, Tags: tags, Metric: trackMetric(metric, gt)})
}
func (c *Client) GetItemsByKeyPrefixes(bucketID int32, prefixes []string, tags []int64, metric *float64, gt bool) ([]model.TrackKeyValueItem, error) {
	return c.trackQuery("/track/query/multi", protocol.TrackGetItemsByPrefixesRequest{BucketID: bucketID, Prefixes: prefixes, Tags: tags, Metric: trackMetric(metric, gt)})
}
