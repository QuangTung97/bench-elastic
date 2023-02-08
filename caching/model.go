package caching

import (
	"bufio"
	"os"
)

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
