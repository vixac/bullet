package client_test

import (
	"reflect"
	"testing"

	"github.com/vixac/bullet/client"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

// Keep the intentional tenancy difference from becoming an accidental second API.
func TestClientContractsMirrorStores(t *testing.T) {
	pairs := []struct {
		name          string
		client, store reflect.Type
	}{
		{"Track", reflect.TypeFor[client.Track](), reflect.TypeFor[store_interface.TrackStore]()},
		{"Depot", reflect.TypeFor[client.Depot](), reflect.TypeFor[store_interface.DepotStore]()},
		{"Grove", reflect.TypeFor[client.Grove](), reflect.TypeFor[store_interface.GroveStore]()},
		{"Ledger", reflect.TypeFor[client.Ledger](), reflect.TypeFor[store_interface.LedgerStore]()},
		{"Warehouse", reflect.TypeFor[client.Warehouse](), reflect.TypeFor[store_interface.WarehouseStore]()},
		{"Client", reflect.TypeFor[client.Client](), reflect.TypeFor[store_interface.Store]()},
	}
	space := reflect.TypeFor[model.TenancySpace]()
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			if pair.client.NumMethod() != pair.store.NumMethod() {
				t.Fatal("client and store have different method counts")
			}
			for i := 0; i < pair.store.NumMethod(); i++ {
				backend := pair.store.Method(i)
				scoped, ok := pair.client.MethodByName(backend.Name)
				if !ok {
					t.Fatalf("client missing %s", backend.Name)
				}
				var inputs []reflect.Type
				spaces := 0
				for j := 0; j < backend.Type.NumIn(); j++ {
					typ := backend.Type.In(j)
					if typ == space {
						spaces++
						continue
					}
					inputs = append(inputs, typ)
				}
				if spaces != 1 {
					t.Fatalf("%s must take tenancy exactly once", backend.Name)
				}
				if len(inputs) != scoped.Type.NumIn() || backend.Type.NumOut() != scoped.Type.NumOut() {
					t.Fatalf("%s signature differs beyond tenancy", backend.Name)
				}
				for j, typ := range inputs {
					if scoped.Type.In(j) != typ {
						t.Fatalf("%s argument %d differs", backend.Name, j)
					}
				}
				for j := 0; j < backend.Type.NumOut(); j++ {
					if scoped.Type.Out(j) != backend.Type.Out(j) {
						t.Fatalf("%s result %d differs", backend.Name, j)
					}
				}
			}
		})
	}
}
