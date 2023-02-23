package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"io"
	"math/rand"
)

func CreateBatch[T any](
	batchSize int,
	limit int,
	createFn func(i int) T,
	handleFn func(batch []T),
) {
	index := 0
	for index < limit {
		batch := make([]T, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			batch = append(batch, createFn(index))
			index++
		}
		handleFn(batch)
	}
}

func RandomSlice[T any](
	min, max int,
	randElem func() T,
) []T {
	n := rand.Intn(max-min+1) + min
	result := make([]T, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, randElem())
	}
	return result
}

func MapSlice[A, B any](inputs []A, fn func(e A) B) []B {
	result := make([]B, 0, len(inputs))
	for _, e := range inputs {
		result = append(result, fn(e))
	}
	return result
}

type indexActionContent struct {
	ID string `json:"_id"`
}
type indexAction struct {
	Index indexActionContent `json:"index"`
}

func InsertBulkElastic[T any](
	client *elasticsearch.Client, index string,
	elems []T,
	getID func(e T) string,
) {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)

	mustEnc := func(v interface{}) {
		err := enc.Encode(v)
		if err != nil {
			panic(err)
		}
	}

	for _, e := range elems {
		mustEnc(indexAction{
			Index: indexActionContent{ID: getID(e)},
		})
		mustEnc(e)
	}

	resp, err := client.Bulk(&buf,
		client.Bulk.WithIndex(index),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	fmt.Println(len(body), err)
}
