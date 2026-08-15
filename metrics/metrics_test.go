package metrics

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppCountersAreBoundedAndOverflowIntoOther(t *testing.T) {
	m := NewMetrics()
	for i := range MaxApps + 1 {
		m.AddAppCounter(fmt.Sprintf("%d", i), "requests.GET /track/items", 1)
	}

	require.Len(t, m.appCounters, MaxApps)
	require.Equal(t, uint64(1), m.appCounters["0"].snapshot()["requests.GET /track/items"])
	require.Equal(t, uint64(1), m.otherApps.snapshot()["requests.GET /track/items"])
}

func TestSnapshotSeparatesCountersAndGauges(t *testing.T) {
	m := NewMetrics()
	m.AddCounter("objects.read", 646)
	m.SetGauge("workers.busy", 1.5)

	bullet := m.Snapshot().Namespaces["bullet"]
	require.Equal(t, uint64(646), bullet.Counters["objects.read"])
	require.Equal(t, 1.5, bullet.Gauges["workers.busy"])
}
