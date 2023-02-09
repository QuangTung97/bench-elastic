package caching

import (
	"bench_elastic/pb"
	"context"
	"github.com/stretchr/testify/assert"
	"math/rand"
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

func TestRepo__Insert_Data(t *testing.T) {
	t.Run("setup database", func(t *testing.T) {
		rand.Seed(randSeed)

		db := NewDB()
		rp := NewRepository(db)

		for k := 0; k < numberOfProducts; {
			const batchSize = 1000

			batch := make([]Product, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				batch = append(batch, randomProduct(k))
				k++
			}

			err := rp.InsertProducts(context.Background(), mapSlice(batch, func(x Product) ProductContent {
				return ProductContentFromProduct(x)
			}))
			if err != nil {
				panic(err)
			}
		}
	})
}
