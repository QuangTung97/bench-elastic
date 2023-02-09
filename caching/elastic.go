package caching

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"io"
	"net"
	"net/http"
	"time"
)

type ElasticClient struct {
	client *elasticsearch.Client
}

const fullProductIndex = "bench_full_products"
const productIndex = "bench_products"

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

func (c *ElasticClient) IndexProducts(ctx context.Context, products []Product) {
	var buf bytes.Buffer

	type indexActionContent struct {
		ID string `json:"_id"`
	}
	type indexAction struct {
		Index indexActionContent `json:"index"`
	}

	enc := json.NewEncoder(&buf)

	for _, p := range products {
		err := enc.Encode(indexAction{
			Index: indexActionContent{
				ID: p.Sku,
			},
		})
		if err != nil {
			panic(err)
		}

		err = enc.Encode(p.Product)
		if err != nil {
			panic(err)
		}
	}

	resp, err := c.client.Bulk(&buf,
		c.client.Bulk.WithContext(ctx),
		c.client.Bulk.WithIndex(fullProductIndex),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	fmt.Println(string(body), err)
}

func (c *ElasticClient) IndexSimpleProducts(ctx context.Context, products []SimpleProduct) {
	var buf bytes.Buffer

	type indexActionContent struct {
		ID string `json:"_id"`
	}
	type indexAction struct {
		Index indexActionContent `json:"index"`
	}

	enc := json.NewEncoder(&buf)

	for _, p := range products {
		err := enc.Encode(indexAction{
			Index: indexActionContent{
				ID: p.SKU,
			},
		})
		if err != nil {
			panic(err)
		}

		err = enc.Encode(p)
		if err != nil {
			panic(err)
		}
	}

	resp, err := c.client.Bulk(&buf,
		c.client.Bulk.WithContext(ctx),
		c.client.Bulk.WithIndex(productIndex),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	fmt.Println(string(body), err)
}

func (c *ElasticClient) Search(ctx context.Context, searchText string, index string) {
	query := fmt.Sprintf(`
{
  "track_total_hits": false,
  "from": 0,
  "size": 30,
  "query": {
    "match": {
	  "search_text": %q
    }
  }
}
`, searchText)

	var buf bytes.Buffer
	buf.WriteString(query)
	resp, err := c.client.Search(
		c.client.Search.WithBody(&buf),
		c.client.Search.WithIndex(index),
		c.client.Search.WithContext(ctx),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	fmt.Println(resp.Status(), err, len(body))
}
