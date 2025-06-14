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

var bucketStore store.BucketStore

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

func SetupBucketRouter(store store.BucketStore, prefix string, engine *gin.Engine) *gin.Engine {
	bucketStore = store
	engine.POST(prefix+"/insert-one", bucketPutHandler)
	engine.POST(prefix+"/insert-many", bucketPutManyHandler)
	engine.POST(prefix+"/get-many", bucketGetManyHandler)

	engine.POST(prefix+"/get-one", bucketGetHandler)
	engine.POST(prefix+"/delete-one", bucketDeleteHandler)
	engine.POST(prefix+"/get-query", handleGetItemsByPrefix)
	return engine
}

func bucketPutHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.BucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := bucketStore.BucketPut(appId, req.BucketID, req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func bucketPutManyHandler(c *gin.Context) {
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

	items := make(map[int32][]model.BucketKeyValueItem)
	for _, bucket := range req.Buckets {
		items[bucket.BucketID] = append(items[bucket.BucketID], bucket.Items...)
	}

	if err := bucketStore.BucketPutMany(appId, items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}
func bucketGetManyHandler(c *gin.Context) {
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

	values, missing, err := bucketStore.BucketGetMany(appId, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func bucketGetHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.BucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := bucketStore.BucketGet(appId, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func bucketDeleteHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.BucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := bucketStore.BucketDelete(appId, req.BucketID, req.Key); err != nil {
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

	items, err := bucketStore.GetItemsByKeyPrefix(
		appId,
		req.BucketID,
		req.Prefix,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "lookup failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items": items,
	})
}
