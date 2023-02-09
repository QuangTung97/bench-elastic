package caching

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadAllWords(t *testing.T) {
	words := readAllWords()
	assert.Equal(t, 3000, len(words))
}

func TestRandomProduct(t *testing.T) {
	p := randomProduct(10)
	fmt.Println(p)
}
