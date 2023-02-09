package caching

import (
	"bench_elastic/pb"
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Product struct {
	*pb.Product
}

type SimpleProduct struct {
	SKU        string `json:"sku"`
	SearchText string `json:"search_text"`
}

type ProductKey struct {
	SKU string
}

func (p Product) GetKey() ProductKey {
	return ProductKey{
		SKU: p.Sku,
	}
}

func (p Product) ToRPC() *pb.Product {
	return p.Product
}

func (k ProductKey) String() string {
	return k.SKU
}

type ProductContent struct {
	SKU         string `db:"sku"`
	ContentData []byte `db:"content_data"`

	CreatedAt time.Time `db:"updated_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (p ProductContent) GetSKU() string {
	return p.SKU
}

func (p Product) Marshal() ([]byte, error) {
	return proto.Marshal(p.Product)
}

func unmarshalProduct(data []byte) (Product, error) {
	result := Product{
		Product: &pb.Product{},
	}

	err := proto.Unmarshal(data, result.Product)
	return result, err
}

func ProductContentFromProduct(p Product) ProductContent {
	data, err := p.Marshal()
	if err != nil {
		panic(err)
	}

	return ProductContent{
		SKU:         p.Sku,
		ContentData: data,
	}
}

func readAllWords() []string {
	file, err := os.Open("./all_words.txt")
	if err != nil {
		panic(err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)

	result := make([]string, 0, 200)
	for scanner.Scan() {
		line := scanner.Text()
		result = append(result, line)
	}

	return result
}

var allWords = readAllWords()

func randomWord() string {
	return allWords[rand.Intn(len(allWords))]
}

func randomSentence(from, to int) string {
	n := rand.Intn(to-from+1) + from
	var buf strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(randomWord())
	}
	return buf.String()
}

func randomProduct(i int) Product {
	return Product{
		Product: &pb.Product{
			Sku:        fmt.Sprintf("SKU%08d", i),
			Name:       randomSentence(10, 20),
			SearchText: randomSentence(20, 30),

			Field1: randomSentence(20, 30),
			Field2: randomSentence(20, 30),
			Field3: randomSentence(20, 30),
			Field4: randomSentence(20, 30),
			Field5: randomSentence(20, 30),
			Field6: randomSentence(20, 30),
			Field7: randomSentence(20, 30),
			Field8: randomSentence(20, 30),
			Field9: randomSentence(20, 30),
		},
	}
}

func randomSimpleProduct(i int) SimpleProduct {
	return SimpleProduct{
		SKU:        fmt.Sprintf("SKU%08d", i),
		SearchText: randomSentence(20, 30),
	}
}
