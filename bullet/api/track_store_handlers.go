package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"

	"github.com/gin-gonic/gin"
)

var trackStore store.TrackStore

// used by pigeon too
func extractAppIDFromHeader(c *gin.Context) (int32, error) {
	appIDStr := c.GetHeader("X-App-ID")
	if appIDStr == "" {
		return 0, errors.New("X-App-ID header missing")
	}

	appID64, err := strconv.ParseInt(appIDStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid X-App-ID header: %w", err)
	}

	return int32(appID64), nil
}

func SetupTrackRouter(store store.TrackStore, prefix string, engine *gin.Engine) *gin.Engine {
	trackStore = store
	engine.POST(prefix+"/insert-one", trackPutHandler)
	engine.POST(prefix+"/insert-many", trackPutManyHandler)
	engine.POST(prefix+"/get-many", trackGetManyHandler)

	engine.POST(prefix+"/get-one", trackGetHandler)
	engine.POST(prefix+"/delete-one", trackDeleteHandler)
	engine.POST(prefix+"/get-query", handleGetItemsByPrefix)
	return engine
}

func trackPutHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := trackStore.TrackPut(appId, req.BucketID, req.Key, req.Value, req.Tag, req.Metric); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func trackPutManyHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.PutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make(map[int32][]model.TrackKeyValueItem)
	for _, bucket := range req.Buckets {
		items[bucket.BucketID] = append(items[bucket.BucketID], bucket.Items...)
	}

	if err := trackStore.TrackPutMany(appId, items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func trackGetManyHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.GetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	keys := make(map[int32][]string)
	for _, bucket := range req.Buckets {
		keys[bucket.BucketID] = append(keys[bucket.BucketID], bucket.Keys...)
	}

	values, missing, err := trackStore.TrackGetMany(appId, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func trackGetHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := trackStore.TrackGet(appId, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func trackDeleteHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.TrackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := trackStore.TrackDelete(appId, req.BucketID, req.Key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func handleGetItemsByPrefix(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.GetItemsByPrefixRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	//Metric is the optional part, so we need to extract it.
	var metricValue *float64 = nil
	var isGt = false

	if req.Metric != nil {
		metricValue = &req.Metric.Value
		isGt = req.Metric.Operator == "gt"
	}
	items, err := trackStore.GetItemsByKeyPrefix(
		appId,
		req.BucketID,
		req.Prefix,
		req.Tags,
		metricValue,
		isGt,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "lookup failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": items,
	})
}
