package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

type trackHandler struct {
	store store_interface.TrackStore
}

// SetupTrackRouter registers all track endpoints under the given prefix.
//
// Endpoints:
//
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
	g.POST("/items", h.upsertOne)
	g.POST("/items/batch", h.upsertMany)
	g.POST("/items/get", h.getOne)
	g.POST("/items/batch-get", h.getMany)
	g.DELETE("/items", h.deleteMany)
	g.POST("/query", h.queryByPrefix)
	g.POST("/query/multi", h.queryByPrefixes)
	return engine
}

func (h *trackHandler) upsertOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.store.TrackPut(space, req.BucketID, req.Key, req.Value, req.Tag, req.Metric); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "track", "written", 1)
	c.Status(http.StatusOK)
}

func (h *trackHandler) upsertMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackPutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	items := make(map[int32][]model.TrackKeyValueItem)
	for _, bucket := range req.Buckets {
		items[bucket.BucketID] = append(items[bucket.BucketID], bucket.Items...)
	}
	if err := h.store.TrackPutMany(space, items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := h.store.TrackGet(space, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "track", "read", 1)
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func (h *trackHandler) getMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	keys := make(map[int32][]string)
	for _, bucket := range req.Buckets {
		keys[bucket.BucketID] = append(keys[bucket.BucketID], bucket.Keys...)
	}
	values, missing, err := h.store.TrackGetMany(space, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	count := 0
	for _, bucketValues := range values {
		count += len(bucketValues)
	}
	incrementObjects(c, "track", "read", count)
	// Convert int32 bucket keys to strings for JSON serialization.
	strValues := make(map[string]map[string]model.TrackValue, len(values))
	for bucketID, vals := range values {
		strValues[strconv.Itoa(int(bucketID))] = vals
	}
	strMissing := make(map[string][]string, len(missing))
	for bucketID, ks := range missing {
		strMissing[strconv.Itoa(int(bucketID))] = ks
	}
	c.JSON(http.StatusOK, model.TrackGetManyResponse{Values: strValues, Missing: strMissing})
}

func (h *trackHandler) deleteMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackDeleteManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.store.TrackDeleteMany(space, req.Items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func (h *trackHandler) queryByPrefix(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackGetItemsByPrefixRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "track", "read", len(items))
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (h *trackHandler) queryByPrefixes(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.TrackGetItemsByPrefixesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "track", "read", len(items))
	c.JSON(http.StatusOK, gin.H{"items": items})
}
