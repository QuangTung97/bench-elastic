package util

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

func BenchConcurrent(
	requestsPerThread int,
	numThreads int,
	fn func(),
) {
	fmt.Println("REQUESTS PER THREAD:", requestsPerThread)
	fmt.Println("NUM THREADS:", numThreads)

	durations := make([]time.Duration, 0, requestsPerThread*numThreads)
	var mut sync.Mutex

	totalStart := time.Now()

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < requestsPerThread; i++ {
				start := time.Now()
				fn()
				d := time.Since(start)

				mut.Lock()
				durations = append(durations, d)
				mut.Unlock()
			}
		}()
	}

	wg.Wait()

	totalDuration := time.Since(totalStart)

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	fmt.Println("TOTAL TIME:", totalDuration)

	printPercentile := func(p float64) {
		index := int(p * float64(len(durations)) / 100.0)
		fmt.Printf("PERCENTILE P%.2f: %v\n", p, durations[index])
	}
	printPercentile(50)
	printPercentile(90)
	printPercentile(95)
	printPercentile(99)
	printPercentile(99.9)

	fmt.Printf("MAX DURATION: %v\n", durations[len(durations)-1])
	fmt.Println("QPS:", float64(numThreads*requestsPerThread)/totalDuration.Seconds())
}
