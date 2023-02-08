package caching

import (
	"bench_elastic/pb"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRepo(t *testing.T) {
	db := NewDB()
	rp := NewRepository(db)

	err := rp.InsertProducts(context.Background(), []ProductContent{
		ProductContentFromProduct(Product{
			Product: &pb.Product{
				Sku:  "SKU001",
				Name: "SKU Name 1",
			},
		}),
	})
	assert.Equal(t, nil, err)
}
