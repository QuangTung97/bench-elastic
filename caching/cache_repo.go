package caching

import (
	"context"
	"github.com/QuangTung97/go-memcache/memcache"
	"github.com/QuangTung97/memproxy"
	"github.com/QuangTung97/memproxy/item"
	"github.com/jmoiron/sqlx"
)

type CacheRepoFactory struct {
	mc       memproxy.Memcache
	provider memproxy.SessionProvider

	repo *Repository
}

type CacheRepo struct {
	sess memproxy.Session
	pipe memproxy.Pipeline

	productItem *item.Item[Product, ProductKey]
}

func NewCacheFactory(memcacheAddr string, db *sqlx.DB) *CacheRepoFactory {
	client, err := memcache.New(memcacheAddr, 4)
	if err != nil {
		panic(err)
	}

	mc := memproxy.NewPlainMemcache(client, 3)

	return &CacheRepoFactory{
		mc:       mc,
		provider: memproxy.NewSessionProvider(),

		repo: NewRepository(db),
	}
}

func mapSlice[A any, B any](input []A, mapFunc func(x A) B) []B {
	result := make([]B, 0, len(input))
	for _, e := range input {
		result = append(result, mapFunc(e))
	}
	return result
}

func (f *CacheRepoFactory) NewRepo() *CacheRepo {
	sess := f.provider.New()
	pipe := f.mc.Pipeline(context.Background(), sess)

	productItem := item.New[Product, ProductKey](
		pipe,
		unmarshalProduct,
		item.NewMultiGetFiller[Product, ProductKey](
			func(ctx context.Context, keys []ProductKey) ([]Product, error) {
				contents, err := f.repo.GetProducts(ctx, mapSlice(keys, func(x ProductKey) string {
					return x.SKU
				}))
				if err != nil {
					return nil, err
				}

				return mapSlice(contents, func(x ProductContent) Product {
					product, err := unmarshalProduct(x.ContentData)
					if err != nil {
						panic(err)
					}
					return product
				}), nil
			}, func(v Product) ProductKey {
				return ProductKey{
					SKU: v.Sku,
				}
			},
		),
	)

	return &CacheRepo{
		sess:        sess,
		pipe:        pipe,
		productItem: productItem,
	}
}

func (r *CacheRepo) GetProduct(ctx context.Context, sku string) func() (Product, error) {
	return r.productItem.Get(ctx, ProductKey{
		SKU: sku,
	})
}

func (r *CacheRepo) Finish() {
	r.pipe.Finish()
}
