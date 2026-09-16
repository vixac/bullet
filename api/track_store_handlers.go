package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

type trackHandler struct {
	store store_interface.TrackStore
}

// SetupTrackRouter registers all track endpoints under the given prefix.
//
// Endpoints:
//
//	POST   {prefix}/mutate         — atomically apply idempotent puts and deletes
//	POST   {prefix}/items          — upsert one
//	POST   {prefix}/items/batch    — upsert many
//	POST   {prefix}/items/get      — get one (key in body to support arbitrary key strings)
//	POST   {prefix}/items/batch-get — get many
//	DELETE {prefix}/items          — delete many
//	POST   {prefix}/query          — prefix query
//	POST   {prefix}/query/multi    — multi-prefix query
func SetupTrackRouter(store store_interface.TrackStore, prefix string, engine *gin.Engine) *gin.Engine {
	h := &trackHandler{store: store}
	g := engine.Group(prefix)
	g.POST("/mutate", h.mutate)
	g.POST("/items", h.upsertOne)
	g.POST("/items/batch", h.upsertMany)
	g.POST("/items/get", h.getOne)
	g.POST("/items/batch-get", h.getMany)
	g.DELETE("/items", h.deleteMany)
	g.POST("/query", h.queryByPrefix)
	g.POST("/query/multi", h.queryByPrefixes)
	return engine
}

func (h *trackHandler) mutate(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackMutateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	mutation := model.TrackMutation{MutationID: model.MutationID(req.MutationID)}
	for _, put := range req.Puts {
		mutation.Puts = append(mutation.Puts, model.TrackPut{
			BucketID: put.BucketID, Key: put.Key,
			Value: put.Value, Tag: put.Tag, Metric: put.Metric, IfAbsent: put.IfAbsent,
		})
	}
	for _, key := range req.Deletes {
		mutation.Deletes = append(mutation.Deletes, model.TrackKey{
			BucketID: key.BucketID, Key: key.Key,
		})
	}
	result, err := h.store.TrackMutate(space, mutation)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, model.ErrTrackMutationUnsupported) {
			status = http.StatusNotImplemented
		} else if errors.Is(err, model.ErrTrackKeyAlreadyExists) {
			status = http.StatusConflict
		}
		c.JSON(status, protocol.ErrorResponseFrom(err))
		return
	}
	if result.Applied {
		incrementObjects(c, "track", "written", len(req.Puts))
	}
	c.JSON(http.StatusOK, protocol.TrackMutateResponse{Applied: result.Applied})
}

func (h *trackHandler) upsertOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	if req.IfAbsent {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "ifAbsent is supported only by /mutate"})
		return
	}
	if err := h.store.TrackPut(space, req.BucketID, req.Key, req.Value, req.Tag, req.Metric); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "track", "written", 1)
	c.Status(http.StatusOK)
}

func (h *trackHandler) upsertMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackPutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	items := req.Model()
	if err := h.store.TrackPutMany(space, items); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	count := 0
	for _, bucketItems := range items {
		count += len(bucketItems)
	}
	incrementObjects(c, "track", "written", count)
	c.Status(http.StatusOK)
}

func (h *trackHandler) getOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	value, err := h.store.TrackGet(space, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "track", "read", 1)
	c.JSON(http.StatusOK, protocol.TrackGetResponse{Value: value})
}

func (h *trackHandler) getMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	keys := make(map[int32][]string)
	for _, bucket := range req.Buckets {
		keys[bucket.BucketID] = append(keys[bucket.BucketID], bucket.Keys...)
	}
	values, missing, err := h.store.TrackGetMany(space, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	count := 0
	for _, bucketValues := range values {
		count += len(bucketValues)
	}
	incrementObjects(c, "track", "read", count)
	// Convert int32 bucket keys to strings for JSON serialization.
	strValues := make(map[string]map[string]protocol.TrackValue, len(values))
	for bucketID, vals := range values {
		wireValues := make(map[string]protocol.TrackValue, len(vals))
		for key, value := range vals {
			wireValues[key] = protocol.TrackValueFromModel(value)
		}
		strValues[strconv.Itoa(int(bucketID))] = wireValues
	}
	strMissing := make(map[string][]string, len(missing))
	for bucketID, ks := range missing {
		strMissing[strconv.Itoa(int(bucketID))] = ks
	}
	c.JSON(http.StatusOK, protocol.TrackGetManyResponse{Values: strValues, Missing: strMissing})
}

func (h *trackHandler) deleteMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackDeleteManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	if err := h.store.TrackDeleteMany(space, req.Model()); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	c.Status(http.StatusOK)
}

func (h *trackHandler) queryByPrefix(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackGetItemsByPrefixRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	var metricValue *float64
	var isGt bool
	if req.Metric != nil {
		metricValue = &req.Metric.Value
		isGt = req.Metric.Operator == "gt"
	}
	items, err := h.store.GetItemsByKeyPrefix(space, req.BucketID, req.Prefix, req.Tags, metricValue, isGt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "track", "read", len(items))
	c.JSON(http.StatusOK, protocol.TrackQueryResponse{Items: protocol.TrackItemsFromModel(items)})
}

func (h *trackHandler) queryByPrefixes(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.TrackGetItemsByPrefixesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	var metricValue *float64
	var isGt bool
	if req.Metric != nil {
		metricValue = &req.Metric.Value
		isGt = req.Metric.Operator == "gt"
	}
	items, err := h.store.GetItemsByKeyPrefixes(space, req.BucketID, req.Prefixes, req.Tags, metricValue, isGt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "track", "read", len(items))
	c.JSON(http.StatusOK, protocol.TrackQueryResponse{Items: protocol.TrackItemsFromModel(items)})
}
