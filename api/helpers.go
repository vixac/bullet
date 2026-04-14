package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

func extractSpace(c *gin.Context) (store_interface.TenancySpace, error) {
	appIDStr := c.GetHeader("X-App-Id")
	if appIDStr == "" {
		return store_interface.TenancySpace{}, errors.New("X-App-Id header missing")
	}
	appID64, err := strconv.ParseInt(appIDStr, 10, 64)
	if err != nil {
		return store_interface.TenancySpace{}, fmt.Errorf("invalid X-App-Id: %w", err)
	}

	tenancyIDStr := c.GetHeader("X-Tenancy-Id")
	if tenancyIDStr == "" {
		return store_interface.TenancySpace{}, errors.New("X-Tenancy-Id header missing")
	}
	tenancyID, err := strconv.ParseInt(tenancyIDStr, 10, 64)
	if err != nil {
		return store_interface.TenancySpace{}, fmt.Errorf("invalid X-Tenancy-Id: %w", err)
	}

	return store_interface.TenancySpace{
		AppId:     int32(appID64),
		TenancyId: tenancyID,
	}, nil
}

// respondError maps well-known grove store errors to appropriate HTTP status codes.
func respondError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store_interface.ErrNodeNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, store_interface.ErrNodeAlreadyExists):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case errors.Is(err, store_interface.ErrCycleDetected):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
	case errors.Is(err, store_interface.ErrMutationConflict):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}
