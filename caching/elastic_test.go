package caching

import (
	"context"
	"math/rand"
	"testing"
)

const numberOfProducts = 4000000
const randSeed = 12348888

func TestElasticClient_IndexProducts(t *testing.T) {
	t.Run("setup index", func(t *testing.T) {
		rand.Seed(randSeed)

		client := NewElasticClient()

		for k := 0; k < numberOfProducts; {
			const batchSize = 1000

			batch := make([]Product, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				batch = append(batch, randomProduct(k))
				k++
			}

			client.IndexProducts(context.Background(), batch)
		}
	})

	t.Run("setup simple index", func(t *testing.T) {
		rand.Seed(randSeed)

		client := NewElasticClient()

		for k := 0; k < numberOfProducts; {
			const batchSize = 1000

			batch := make([]SimpleProduct, 0, batchSize)
			for i := 0; i < batchSize; i++ {
				batch = append(batch, randomSimpleProduct(k))
				k++
			}

			client.IndexSimpleProducts(context.Background(), batch)
		}
	})
}
