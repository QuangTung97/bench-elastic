package main

import (
	"bytes"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildBulkRequestBody(t *testing.T) {
	var buf bytes.Buffer
	buildBulkRequestBody(&buf, []Shop{
		{
			ID: 11,
			Location: Location{
				Lat: 21.0,
				Lon: 101.0,
			},
		},
		{
			ID: 12,
			Location: Location{
				Lat: 22.3,
				Lon: 102.3,
			},
		},
	})
	assert.Equal(t, `{"index":{"_id":"11"}}
{"id":11,"lat":21,"lon":101}
{"index":{"_id":"12"}}
{"id":12,"lat":22.3,"lon":102.3}
`, buf.String())
}

func BenchmarkGetFromMemcache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		client, err := memcache.New("localhost:11211", 16)
		if err != nil {
			panic(err)
		}

		//writeDataToDB(db, shops)
		//writeDataToMemcached(db, client)

		//doSearchUsingDB(db)
		doSearchUsingMemcache(client)

		_ = client.Close()
	}
}
