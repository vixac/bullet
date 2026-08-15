package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/metrics"
)

const metricsContextKey = "bullet.metrics"
const appIDContextKey = "bullet.metrics.app_id"

// SetupObservationsRouter adds the server-lifetime observations endpoint and records one
// request for every route subsequently registered on engine.
func SetupObservationsRouter(m *metrics.Metrics, engine *gin.Engine) *gin.Engine {
	engine.Use(func(c *gin.Context) {
		c.Set(metricsContextKey, m)
		c.Next()
		if route := c.FullPath(); route != "" {
			key := "requests." + c.Request.Method + " " + route
			m.IncrementCounter(key)
			incrementAppCounter(c, key, 1)
		}
	})
	engine.GET("/observations", func(c *gin.Context) {
		c.JSON(http.StatusOK, m.Snapshot())
	})
	return engine
}

func incrementObjects(c *gin.Context, collection, operation string, count int) {
	if count <= 0 {
		return
	}
	if value, ok := c.Get(metricsContextKey); ok {
		m := value.(*metrics.Metrics)
		key := collection + ".objects_" + operation
		m.AddCounter(key, uint64(count))
		incrementAppCounter(c, key, uint64(count))
	}
}

func incrementAppCounter(c *gin.Context, key string, count uint64) {
	metricsValue, hasMetrics := c.Get(metricsContextKey)
	appIDValue, hasAppID := c.Get(appIDContextKey)
	if hasMetrics && hasAppID {
		metricsValue.(*metrics.Metrics).AddAppCounter(appIDValue.(string), key, count)
	}
}
