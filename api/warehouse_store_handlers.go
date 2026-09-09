package api

import (
	"github.com/gin-gonic/gin"
	si "github.com/vixac/bullet/store/store_interface"
	"net/http"
)

// SetupWarehouseRouter serves JSON blobs; byte values use standard base64 JSON encoding.
func SetupWarehouseRouter(store si.WarehouseStore, prefix string, engine *gin.Engine) *gin.Engine {
	g := engine.Group(prefix)
	g.POST("/blobs", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		var req si.PutBlobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		blob, err := store.WarehousePut(c.Request.Context(), space, req)
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "written", 1)
		c.JSON(http.StatusOK, blob)
	})
	g.GET("/blobs/:id", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		blob, err := store.WarehouseGet(c.Request.Context(), space, si.BlobID(c.Param("id")))
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", 1)
		c.JSON(http.StatusOK, blob)
	})
	g.POST("/blobs/batch-get", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		var req struct {
			IDs []si.BlobID `json:"ids"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		blobs, err := store.WarehouseGetMany(c.Request.Context(), space, req.IDs)
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", len(blobs))
		c.JSON(http.StatusOK, blobs)
	})
	return engine
}
