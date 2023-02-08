package main

import (
	"bench_elastic/pb"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	data, err := protojson.Marshal(&pb.Product{
		Sku:  "SKU001",
		Name: "Quang Tung",
	})
	fmt.Println(string(data), err)
}
