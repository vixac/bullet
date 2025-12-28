package api

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

var wayFinderStore store_interface.WayFinderStore

func SetupWayFinderRouter(store store_interface.WayFinderStore, prefix string, engine *gin.Engine) *gin.Engine {
	wayFinderStore = store
	engine.POST(prefix+"/insert-one", wayFinderPutHandler)
	engine.POST(prefix+"/query-by-prefix", wayFinderQueryByPrefixHandler)
	engine.POST(prefix+"/get-one", wayFinderGetOneHandler)
	engine.POST(prefix+"/delete-many", wayFinderDeleteManyHandler)
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
	fmt.Println("with request request is", req)
	space := store_interface.TenancySpace{
		AppId:     appId,
		TenancyId: 0, //VX:TODO collect the tenancyId
	}
	itemId, err := wayFinderStore.WayFinderPut(space, req.BucketId, req.Key, req.Payload, req.Tag, req.Metric)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("created item ", itemId)

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

	space := store_interface.TenancySpace{
		AppId:     appId,
		TenancyId: 0, //VX:TODO collect the tenancyId
	}
	items, err := wayFinderStore.WayFinderGetByPrefix(
		space, req.BucketId, req.Prefix, req.Tags, req.Metric, req.MetricIsGt,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"items": items})
}

func wayFinderDeleteManyHandler(c *gin.Context) {

	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	//VX:TODO get this working for delete
	var req model.WayFinderGetOneRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	fmt.Println("get one called with appId", appId)
	fmt.Println("with request request is", req)

	space := store_interface.TenancySpace{
		AppId:     appId,
		TenancyId: 0, //VX:TODO collect the tenancyId
	}

	item, err := wayFinderStore.WayFinderGetOne(space, req.BucketId, req.Key)
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
	fmt.Println("get one called with appId", appId)
	fmt.Println("with request request is", req)

	space := store_interface.TenancySpace{
		AppId:     appId,
		TenancyId: 0, //VX:TODO collect the tenancyId
	}
	item, err := wayFinderStore.WayFinderGetOne(space, req.BucketId, req.Key)
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
