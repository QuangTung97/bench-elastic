package nested

import (
	"bench_elastic/util"
	"fmt"
	"math/rand"
	"testing"
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
