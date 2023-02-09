package caching

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestCacheRepo(t *testing.T) {
	f := NewCacheFactory("localhost:11211", NewDB())
	defer func() { _ = f.Close() }()

	repo := f.NewRepo()
	defer repo.Finish()

	start := time.Now()
	product, err := repo.GetProduct(context.Background(), "SKU001")()
	fmt.Println(product, err, time.Since(start))
}

func TestCacheRepo__Use_Only_Repo(t *testing.T) {
	repo := NewRepository(NewDB())

	var products []ProductContent
	var err error

	start := time.Now()
	for i := 0; i < 1000; i++ {
		products, err = repo.GetProducts(context.Background(), []string{"SKU001"})
	}
	fmt.Println(products, err, time.Since(start))
}

func TestCacheRepo__Multi_Get_Products(t *testing.T) {
	obj := make([]byte, 128<<20) // 128MB

	defer func() {
		runtime.KeepAlive(obj)
	}()

	rand.Seed(time.Now().UnixNano())

	f := NewCacheFactory("localhost:11211", NewDB())
	defer func() { _ = f.Close() }()

	const loops = 2000

	var mut sync.Mutex
	durations := make([]time.Duration, 0, loops)

	const numThreads = 10
	const batchSize = 40

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < loops; i++ {
				start := time.Now()

				repo := f.NewRepo()

				fnList := make([]func() (Product, error), 0, batchSize)
				for m := 0; m < batchSize; m++ {
					sku := fmt.Sprintf("SKU%08d", rand.Intn(3000))
					fn := repo.GetProduct(context.Background(), sku)
					fnList = append(fnList, fn)
				}

				for _, fn := range fnList {
					product, err := fn()
					if err != nil {
						panic(err)
					}
					if len(product.Field1) == 0 {
						panic("missing value")
					}
				}

				repo.Finish()

				duration := time.Since(start)

				mut.Lock()
				durations = append(durations, duration)
				mut.Unlock()
			}
		}()
	}

	wg.Wait()

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	printPercentile := func(name string, list []time.Duration, p float64) {
		index := int(p * float64(len(list)))
		fmt.Printf("%s: %f %v\n", name, p, list[index])
	}

	printPercentile("CACHE", durations, 0.5)
	printPercentile("CACHE", durations, 0.9)
	printPercentile("CACHE", durations, 0.95)
	printPercentile("CACHE", durations, 0.99)
}
