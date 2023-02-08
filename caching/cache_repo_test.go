package caching

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCacheRepo(t *testing.T) {
	f := NewCacheFactory("localhost:11211", NewDB())
	repo := f.NewRepo()
	defer repo.Finish()

	start := time.Now()
	product, err := repo.GetProduct(context.Background(), "SKU001")()
	fmt.Println(product, err, time.Since(start))
}
