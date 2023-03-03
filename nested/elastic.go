package nested

import (
	"bench_elastic/util"
	"bytes"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"io"
	"net"
	"net/http"
	"time"
)

type SimpleProduct struct {
	Sku          string   `json:"sku"`
	AttributeIDs []string `json:"attribute_ids"`
}

// Product nested
type Product struct {
	Sku        string      `json:"sku"`
	Attributes []Attribute `json:"attributes"`
}

// Attribute ...
type Attribute struct {
	ID string `json:"id"`
}

type ElasticClient struct {
	client *elasticsearch.Client
}

const maxConnsPerHost = 20

func NewElasticClient() *ElasticClient {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxConnsPerHost:       maxConnsPerHost, // default = 2
		MaxIdleConnsPerHost:   maxConnsPerHost, // default = 2
	}

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9400"},
		Transport: transport,
	})
	if err != nil {
		panic(err)
	}
	return &ElasticClient{
		client: client,
	}
}

const simpleProductIndex = "simple_products"
const nestedProductIndex = "nested_products"

func (c *ElasticClient) InsertSimple(products []SimpleProduct) {
	util.InsertBulkElastic[SimpleProduct](
		c.client, simpleProductIndex, products,
		func(e SimpleProduct) string {
			return e.Sku
		},
	)
}

func (c *ElasticClient) InsertNested(products []Product) {
	util.InsertBulkElastic[Product](
		c.client, nestedProductIndex, products,
		func(e Product) string {
			return e.Sku
		},
	)
}

func (c *ElasticClient) doSearch(index string, query string) {
	var buf bytes.Buffer
	buf.WriteString(query)

	resp, err := c.client.Search(c.client.Search.WithBody(&buf), c.client.Search.WithIndex(index))
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if _, err := io.ReadAll(resp.Body); err != nil {
		panic(err)
	} else {
	}
}

func (c *ElasticClient) SearchSimple(
	attr string,
) {
	query := fmt.Sprintf(`
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "attribute_ids": %q
          }
        }
      ]
    }
  },
  "_source": false,
  "stored_fields": "_none_",
  "docvalue_fields": ["sku"],
  "size": 20
}
`, attr)
	c.doSearch(simpleProductIndex, query)
}

func (c *ElasticClient) SearchNested(
	attr string,
) {
	query := fmt.Sprintf(`
{
  "query": {
    "nested": {
      "path": "attributes",
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "attributes.id": %q
              }
            }
          ]
        }
      }
    }
  },
  "_source": false,
  "stored_fields": "_none_",
  "docvalue_fields": ["sku"],
  "size": 20
}
`, attr)

	c.doSearch(nestedProductIndex, query)
}

func (c *ElasticClient) AggregateSimple() {
	query := fmt.Sprintf(`
{
  "aggs": {
    "attrs": {
      "terms": {
        "field": "attribute_ids",
        "size": 20
      }
    }
  }
}
`)
	c.doSearch(simpleProductIndex, query)
}

func (c *ElasticClient) AggregateNested() {
	query := fmt.Sprintf(`
{
  "aggs": {
    "attrs": {
      "nested": {
        "path": "attributes"
      },
      "aggs": {
        "attr_id": {
          "terms": {
            "field": "attributes.id",
            "size": 20
          }
        }
      }
    }
  }
}
`)
	c.doSearch(nestedProductIndex, query)
}

const searchAndAggSimple = `
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "attribute_ids": "ATTR00009"
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "attrs": {
      "terms": {
        "field": "attribute_ids",
        "size": 20
      }
    }
  },
  "profile": true
}
`

const searchAndAggNested = `
`
