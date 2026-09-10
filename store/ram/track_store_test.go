package ram

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
	si "github.com/vixac/bullet/store/store_interface"
)

func TestTrackBatchesHaveNoPartialVisibility(t *testing.T) {
	s := NewRamStore()
	space := si.TenancySpace{AppId: 1, TenancyId: 2}
	puts := map[int32][]model.TrackKeyValueItem{}
	keys := map[int32][]string{}
	var deletes []model.TrackBucketKeyPair
	const count = 2000
	for i := 0; i < count; i++ {
		bucket, key := int32(i%2), fmt.Sprint(i)
		puts[bucket] = append(puts[bucket], model.TrackKeyValueItem{Key: key, Value: model.TrackValue{Value: 42}})
		keys[bucket] = append(keys[bucket], key)
		deletes = append(deletes, model.TrackBucketKeyPair{BucketID: bucket, Key: key})
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			if err := s.TrackPutMany(space, puts); err != nil {
				done <- err
				return
			}
			if err := s.TrackDeleteMany(space, deletes); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	// Always join the writer, even if a read assertion fails.
	defer func() { require.NoError(t, <-done) }()
	for i := 0; i < 200; i++ {
		found, _, err := s.TrackGetMany(space, keys)
		require.NoError(t, err)
		n := 0
		for _, bucket := range found {
			n += len(bucket)
		}
		require.True(t, n == 0 || n == count, "observed partial batch of %d items", n)
	}
}
