package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"
)

var wayFinderStore store.WayFinderStore

func SetupWayFinderRouter(store store.WayFinderStore, prefix string, engine *gin.Engine) *gin.Engine {
	wayFinderStore = store
	engine.POST(prefix+"/insert-one", wayFinderPutHandler)
	engine.POST(prefix+"/query-by-prefix", wayFinderQueryByPrefixHandler)
	engine.POST(prefix+"/get-one", wayFinderGetOneHandler)
	return engine
}

func wayFinderPutHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.WayFinderPutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	itemId, err := wayFinderStore.WayFinderPut(appId, req.BucketId, req.Key, req.Payload, req.Tag, req.Metric)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"itemId": itemId})
}

func wayFinderQueryByPrefixHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.WayFinderPrefixQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items, err := wayFinderStore.WayFinderGetByPrefix(
		appId, req.BucketId, req.Prefix, req.Tags, req.Metric, req.MetricIsGt,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}

func wayFinderGetOneHandler(c *gin.Context) {

	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.WayFinderGetOneRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("request is ", req)
	item, err := wayFinderStore.WayFinderGetOne(appId, req.BucketId, req.Key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	fmt.Printf("Wayfound found %+v \n", item)
	if item == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "item not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"item": item})
}
