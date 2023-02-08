package caching

import (
	"bench_elastic/pb"
	"bufio"
	"github.com/golang/protobuf/proto"
	"os"
	"time"
)

type Product struct {
	*pb.Product
}

type ProductKey struct {
	SKU string
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
