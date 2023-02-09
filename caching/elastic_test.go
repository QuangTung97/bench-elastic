package caching

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

const numberOfProducts = 4000000
const randSeed = 12348888

func TestElasticClient_IndexProducts(t *testing.T) {
	t.Run("setup index", func(t *testing.T) {
		t.Skip()

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
		t.Skip()

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

const querySeed = 77778888

func TestElasticClient_Search(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	searchText := randomSentence(10, 20)

	t.Run("search full products", func(t *testing.T) {
		client := NewElasticClient()

		start := time.Now()
		client.Search(context.Background(), searchText, fullProductIndex)
		fmt.Println(time.Since(start))
	})

	t.Run("search simple products", func(t *testing.T) {
		client := NewElasticClient()

		start := time.Now()
		client.Search(context.Background(), searchText, productIndex)
		fmt.Println(time.Since(start))
	})
}

func TestElasticClient_Search_Alternate(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	client := NewElasticClient()

	const loops = 200

	var fullMut sync.Mutex
	fullDurations := make([]time.Duration, 0, loops)

	var simpleMut sync.Mutex
	simpleDurations := make([]time.Duration, 0, loops)

	const numThreads = 10

	var wg sync.WaitGroup
	wg.Add(numThreads * 2)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < loops; i++ {
				searchText := randomSentence(2, 3)

				start := time.Now()
				client.Search(context.Background(), searchText, productIndex)
				duration := time.Since(start)

				simpleMut.Lock()
				simpleDurations = append(simpleDurations, duration)
				simpleMut.Unlock()
			}
		}()
	}

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < loops; i++ {
				searchText := randomSentence(2, 4)

				start := time.Now()
				client.Search(context.Background(), searchText, fullProductIndex)
				duration := time.Since(start)

				fullMut.Lock()
				fullDurations = append(fullDurations, duration)
				fullMut.Unlock()
			}
		}()
	}

	wg.Wait()

	sort.Slice(fullDurations, func(i, j int) bool {
		return fullDurations[i] < fullDurations[j]
	})

	sort.Slice(simpleDurations, func(i, j int) bool {
		return simpleDurations[i] < simpleDurations[j]
	})

	printPercentile := func(name string, list []time.Duration, p float64) {
		index := int(p * float64(len(list)))
		fmt.Printf("%s: %f %v\n", name, p, list[index])
	}

	printPercentile("FULL", fullDurations, 0.5)
	printPercentile("SIMPLE", simpleDurations, 0.5)

	printPercentile("FULL", fullDurations, 0.9)
	printPercentile("SIMPLE", simpleDurations, 0.9)

	printPercentile("FULL", fullDurations, 0.95)
	printPercentile("SIMPLE", simpleDurations, 0.95)
}
