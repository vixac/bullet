package rest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/client"
	"github.com/vixac/bullet/model"
)

func TestLedgerPrefix(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			for _, id := range []string{"orders_", "orders_a", "ordersXa", "Orders_a", "orders_b", "other"} {
				_, err := pair.local.LedgerAppend(model.LedgerID(id), "one", id)
				require.NoError(t, err)
			}
			for name, client := range map[string]client.Client{"local": pair.local, "rest": pair.rest} {
				t.Run(name, func(t *testing.T) {
					selector := model.LedgerSelector{Prefix: "orders_"}
					page, err := client.LedgerReadBackward(selector, nil, 2)
					require.NoError(t, err)
					require.Len(t, page.Records, 2)
					assert.Equal(t, model.LedgerID("orders_b"), page.Records[0].LedgerID)
					assert.Equal(t, model.LedgerID("orders_a"), page.Records[1].LedgerID)
					require.NotNil(t, page.NextCursor)

					secondPage, err := client.LedgerReadBackward(selector, page.NextCursor, 2)
					require.NoError(t, err)
					require.Len(t, secondPage.Records, 1)
					assert.Equal(t, model.LedgerID("orders_"), secondPage.Records[0].LedgerID)
					assert.Nil(t, secondPage.NextCursor)

					_, err = client.LedgerReadBackward(model.LedgerSelector{Prefix: "orders"}, page.NextCursor, 2)
					require.Error(t, err)

					forward, err := client.LedgerReadForward(selector, 0, nil, 10)
					require.NoError(t, err)
					require.Len(t, forward, 3)
					assert.Equal(t, model.LedgerID("orders_"), forward[0].LedgerID)
					assert.Equal(t, model.LedgerID("orders_a"), forward[1].LedgerID)
					assert.Equal(t, model.LedgerID("orders_b"), forward[2].LedgerID)

					through := forward[1].Position
					bounded, err := client.LedgerReadForward(selector, forward[0].Position, &through, 10)
					require.NoError(t, err)
					assert.Equal(t, forward[1:2], bounded)
					limited, err := client.LedgerReadForward(selector, 0, nil, 1)
					require.NoError(t, err)
					assert.Equal(t, forward[:1], limited)

					missing := model.LedgerSelector{Prefix: "missing"}
					emptyForward, err := client.LedgerReadForward(missing, 0, nil, 10)
					require.NoError(t, err)
					assert.Empty(t, emptyForward)
					emptyBackward, err := client.LedgerReadBackward(missing, nil, 10)
					require.NoError(t, err)
					assert.Empty(t, emptyBackward.Records)
					assert.Nil(t, emptyBackward.NextCursor)

					for _, invalid := range []model.LedgerSelector{
						{Prefix: ""},
						{All: true, Prefix: "orders_"},
						{LedgerIDs: []model.LedgerID{"orders_"}, Prefix: "orders_"},
						{Prefix: "bad%prefix"},
					} {
						_, err := client.LedgerReadBackward(invalid, nil, 10)
						require.Error(t, err, "selector: %+v", invalid)
						_, err = client.LedgerReadForward(invalid, 0, nil, 10)
						require.Error(t, err, "selector: %+v", invalid)
					}
				})
			}
		})
	}
}
