package api

import (
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
		appId, req.BucketId, req.Prefix, req.Tags, req.MetricValue, req.MetricIsGt,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}
