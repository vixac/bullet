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

	snapshot := m.Snapshot()
	require.Len(t, snapshot.Apps, MaxApps+1)
	require.Equal(t, uint64(1), snapshot.Apps["0"]["requests.GET /track/items"])
	require.Equal(t, uint64(1), snapshot.Apps["other"]["requests.GET /track/items"])
}
