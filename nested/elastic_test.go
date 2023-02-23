package nested

import (
	"bench_elastic/util"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func getSku(i int) string {
	return fmt.Sprintf("SKU%07d", i)
}

func getAttr(i int) string {
	return fmt.Sprintf("ATTR%05d", i)
}

func randomAttr() string {
	return getAttr(rand.Intn(1000))
}

var globalSeed int64

func init() {
	globalSeed = time.Now().UnixNano()
	fmt.Println("SEED:", globalSeed)
}

func TestInsertSimple(t *testing.T) {
	c := NewElasticClient()

	util.CreateBatch[SimpleProduct](
		1000,
		500000,
		func(i int) SimpleProduct {
			return SimpleProduct{
				Sku:          getSku(i),
				AttributeIDs: util.RandomSlice[string](3, 8, randomAttr),
			}
		},
		c.InsertSimple,
	)
}

func TestInsertNested(t *testing.T) {
	c := NewElasticClient()

	util.CreateBatch[Product](
		1000,
		500000,
		func(i int) Product {
			return Product{
				Sku: getSku(i),
				Attributes: util.MapSlice(
					util.RandomSlice[string](3, 8, randomAttr),
					func(attr string) Attribute {
						return Attribute{
							ID: attr,
						}
					},
				),
			}
		},
		c.InsertNested,
	)
}

func TestSearch_Simple(t *testing.T) {
	rand.Seed(globalSeed)

	c := NewElasticClient()

	util.BenchConcurrent(
		200,
		100,
		func() {
			c.SearchSimple(randomAttr())
		},
	)
}

func TestSearch_Nested(t *testing.T) {
	rand.Seed(globalSeed)

	c := NewElasticClient()

	util.BenchConcurrent(
		200,
		100,
		func() {
			c.SearchNested(randomAttr())
		},
	)
}
