package caching

import (
	"context"
	"fmt"
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
