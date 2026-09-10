package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	si "github.com/vixac/bullet/store/store_interface"
)

// SetupWarehouseRouter serves JSON blobs; byte values use standard base64 JSON encoding.
func SetupWarehouseRouter(store si.WarehouseStore, prefix string, engine *gin.Engine) *gin.Engine {
	g := engine.Group(prefix)
	g.POST("/blobs", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponse{Error: err.Error()})
			return
		}
		var req protocol.PutBlobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: err.Error()})
			return
		}
		blob, err := store.WarehousePut(c.Request.Context(), space, req.Model())
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "written", 1)
		c.JSON(http.StatusOK, protocol.BlobFromModel(blob))
	})
	g.GET("/blobs/:id", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponse{Error: err.Error()})
			return
		}
		blob, err := store.WarehouseGet(c.Request.Context(), space, model.BlobID(c.Param("id")))
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", 1)
		c.JSON(http.StatusOK, protocol.BlobFromModel(blob))
	})
	g.POST("/blobs/batch-get", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponse{Error: err.Error()})
			return
		}
		var req protocol.WarehouseGetManyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: err.Error()})
			return
		}
		blobs, err := store.WarehouseGetMany(c.Request.Context(), space, req.IDs)
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", len(blobs))
		c.JSON(http.StatusOK, protocol.BlobsFromModel(blobs))
	})
	return engine
}
