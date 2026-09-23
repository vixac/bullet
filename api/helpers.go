package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
)

func extractSpace(c *gin.Context) (model.TenancySpace, error) {
	appIDStr := c.GetHeader(protocol.AppIDHeader)
	if appIDStr == "" {
		return model.TenancySpace{}, errors.New("X-App-Id header missing")
	}
	appID64, err := strconv.ParseInt(appIDStr, 10, 64)
	if err != nil {
		return model.TenancySpace{}, fmt.Errorf("invalid X-App-Id: %w", err)
	}
	c.Set(appIDContextKey, strconv.FormatInt(appID64, 10))

	tenancyIDStr := c.GetHeader(protocol.TenancyIDHeader)
	if tenancyIDStr == "" {
		return model.TenancySpace{}, errors.New("X-Tenancy-Id header missing")
	}
	tenancyID, err := strconv.ParseInt(tenancyIDStr, 10, 64)
	if err != nil {
		return model.TenancySpace{}, fmt.Errorf("invalid X-Tenancy-Id: %w", err)
	}

	return model.TenancySpace{
		AppId:     int32(appID64),
		TenancyId: tenancyID,
	}, nil
}

// respondError maps well-known store errors to appropriate HTTP status codes.
func respondError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, model.ErrWarehouseUnsupported):
		c.JSON(http.StatusNotImplemented, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrWarehouseInvalidPutID):
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrWarehousePutConflict):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrBlobNotFound):
		c.JSON(http.StatusNotFound, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrCheckpointUnsupported):
		c.JSON(http.StatusNotImplemented, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrCheckpointInvalid):
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrCheckpointConflict),
		errors.Is(err, model.ErrCheckpointCorrupt):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrCheckpointNotFound):
		c.JSON(http.StatusNotFound, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrLedgerInvalidID),
		errors.Is(err, model.ErrLedgerInvalidAppendID),
		errors.Is(err, model.ErrLedgerPayloadTooLarge),
		errors.Is(err, model.ErrLedgerInvalidSelector),
		errors.Is(err, model.ErrLedgerInvalidPageSize),
		errors.Is(err, model.ErrLedgerInvalidCursor):
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrLedgerAppendConflict),
		errors.Is(err, model.ErrLedgerBatchConflict):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrLedgerUnsupported):
		c.JSON(http.StatusNotImplemented, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrNodeNotFound):
		c.JSON(http.StatusNotFound, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrNodeAlreadyExists):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrTrackKeyAlreadyExists):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrCycleDetected):
		c.JSON(http.StatusUnprocessableEntity, protocol.ErrorResponseFrom(err))
	case errors.Is(err, model.ErrMutationConflict):
		c.JSON(http.StatusConflict, protocol.ErrorResponseFrom(err))
	default:
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
	}
}
