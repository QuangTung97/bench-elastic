package caching

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadAllWords(t *testing.T) {
	words := readAllWords()
	assert.Equal(t, 3000, len(words))
}
