package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/QuangTung97/haversine"
	"github.com/elastic/go-elasticsearch/v7"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/QuangTung97/geohash"
	"github.com/QuangTung97/go-memcache/memcache"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Shop struct {
	ID       int64    `json:"id"`
	Location Location `json:"location"`
}

const precision = 6

func (s Shop) toModel() ShopModel {
	return ShopModel{
		ID:  s.ID,
		Lat: s.Location.Lat,
		Lon: s.Location.Lon,
		Geohash: geohash.ComputeGeohash(geohash.Pos{
			Lat: s.Location.Lat,
			Lon: s.Location.Lon,
		}, precision).String(),
	}
}

func shopsToModels(shops []Shop) []ShopModel {
	result := make([]ShopModel, 0, len(shops))
	for _, s := range shops {
		result = append(result, s.toModel())
	}
	return result
}

type ShopModel struct {
	ID      int64   `db:"id"`
	Lat     float64 `db:"lat"`
	Lon     float64 `db:"lon"`
	Geohash string  `db:"geohash"`
}

func loadShops(filename string) []Shop {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func() { _ = file.Close() }()

	reader := csv.NewReader(file)

	result := make([]Shop, 0, 10000)
	for index := 0; ; index++ {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		if index == 0 {
			continue
		}

		id, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			panic(err)
		}

		lat, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			panic(err)
		}

		lon, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			panic(err)
		}

		result = append(result, Shop{
			ID: id,
			Location: Location{
				Lat: lat,
				Lon: lon,
			},
		})
	}

	return result
}

func batchShops(shops []Shop, batchSize int, fn func(shops []Shop) error) error {
	for len(shops) > 0 {
		n := batchSize
		if n > len(shops) {
			n = len(shops)
		}

		if err := fn(shops[:n]); err != nil {
			return err
		}
		shops = shops[n:]
	}
	return nil
}

func buildBulkRequestBody(writer io.Writer, shops []Shop) {
	type indexAction struct {
		ID string `json:"_id"`
	}

	type action struct {
		Index indexAction `json:"index"`
	}

	enc := json.NewEncoder(writer)
	for _, s := range shops {
		err := enc.Encode(action{
			Index: indexAction{
				ID: strconv.FormatInt(s.ID, 10),
			},
		})

		if err != nil {
			panic(err)
		}

		if err := enc.Encode(s); err != nil {
			panic(err)
		}
	}
}

const indexName = "bench_shops"

func indexData(client *elasticsearch.Client, shops []Shop) {
	err := batchShops(shops, 1000, func(shops []Shop) error {
		start := time.Now()
		var buf bytes.Buffer
		buildBulkRequestBody(&buf, shops)

		resp, err := client.Bulk(&buf, client.Bulk.WithIndex(indexName))
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.IsError() || resp.StatusCode != http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			fmt.Println(string(data), err)
			panic(string(data))
		}

		fmt.Println("DURATION:", time.Since(start), len(shops))

		return nil
	})
	if err != nil {
		panic(err)
	}
}

func writeDataToDB(db *sqlx.DB, shops []Shop) {
	err := batchShops(shops, 1000, func(shops []Shop) error {
		start := time.Now()

		query := `
INSERT INTO shops (id, lat, lon, geohash)
VALUES (:id, :lat, :lon, :geohash)
`
		_, err := db.NamedExec(query, shopsToModels(shops))
		if err != nil {
			panic(err)
		}

		fmt.Println("DURATION:", time.Since(start), len(shops))
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func writeDataToMemcached(db *sqlx.DB, client *memcache.Client) {
	var result []ShopModel
	err := db.Select(&result, `SELECT id, lat, lon, geohash FROM shops`)
	if err != nil {
		panic(err)
	}

	shopMap := map[string][]ShopModel{}
	for _, s := range result {
		shopMap[s.Geohash] = append(shopMap[s.Geohash], s)
	}

	p := client.Pipeline()
	defer p.Finish()

	for k, v := range shopMap {
		data, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		p.MSet(k, data, memcache.MSetOptions{})
	}
}

var durationMut sync.Mutex
var durations []time.Duration

func searchWithES(client *elasticsearch.Client) {
	lat := randLat()

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(`
{
    "query": {
        "bool": {
            "filter": [
                {
                    "geo_distance": {
                        "distance": "0.5km",
                        "location": {
                            "lat": %v,
                            "lon": 105.827342
                        }
                    }
                }
            ]
        }
    },
    "size": 20
}
`, lat))

	start := time.Now()

	resp, err := client.Search(client.Search.WithIndex(indexName), client.Search.WithBody(&buf))
	if err != nil {
		fmt.Println("ERROR:", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.IsError() {
		data, _ := io.ReadAll(resp.Body)
		panic(string(data))
	}

	d := time.Since(start)
	durationMut.Lock()
	durations = append(durations, d)
	durationMut.Unlock()
}

func getESClient(maxConnsPerHost int) (*elasticsearch.Client, func()) {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxConnsPerHost:       maxConnsPerHost,
		MaxIdleConnsPerHost:   maxConnsPerHost,
	}

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9400"},
		Transport: transport,
	})
	if err != nil {
		panic(err)
	}

	return client, func() { transport.CloseIdleConnections() }
}

func doSearchESByLoc() {
	const maxConnsPerHost = 10

	client, deferFn := getESClient(maxConnsPerHost)
	defer deferFn()

	const numThreads = 100
	const numLoops = 100

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < numLoops; i++ {
				searchWithES(client)
			}
		}()
	}

	wg.Wait()

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	printPercentile := func(p float64) {
		index := int(p / 100 * float64(len(durations)))
		fmt.Printf("Percentile %f: %v\n", p, durations[index])
	}

	fmt.Println("=========================================")

	printPercentile(50)
	printPercentile(90)
	printPercentile(95)
	printPercentile(99)
	printPercentile(99.9)

	fmt.Println("MAX CONNS PER HOST:", maxConnsPerHost)
	fmt.Println("Num Threads:", numThreads)
	fmt.Println("TOTAL Requests:", numThreads*numLoops)
	d := time.Since(start)
	fmt.Println("TOTAL Duration:", d)
	fmt.Println("AVG QPS:", float64(numThreads*numLoops)/d.Seconds())
}

func searchWithDB(db *sqlx.DB) {
	const radius = 0.5

	start := time.Now()

	lat := randLat()

	hashList := geohash.NearbyGeohashList(geohash.Pos{
		Lat: lat,
		Lon: 105.827342,
	}, radius, precision)

	hashes := make([]string, 0, len(hashList))
	for _, h := range hashList {
		hashes = append(hashes, h.String())
	}

	query := `
SELECT id, lat, lon, geohash
FROM shops WHERE geohash IN (?)
`

	query, args, err := sqlx.In(query, hashes)
	if err != nil {
		panic(err)
	}

	var result []ShopModel
	err = db.Select(&result, query, args...)
	if err != nil {
		panic(err)
	}

	count := 0
	for _, s := range result {
		pos1 := haversine.Pos{
			Lat: lat,
			Lon: 105.827342,
		}
		pos2 := haversine.Pos{
			Lat: s.Lat,
			Lon: s.Lon,
		}
		d := haversine.DistanceEarth(pos1, pos2)
		if d <= radius {
			count++
		}
	}

	d := time.Since(start)
	durationMut.Lock()
	durations = append(durations, d)
	durationMut.Unlock()
}

func randFloat64(a, b float64) float64 {
	return rand.Float64()*(b-a) + a
}

func randLat() float64 {
	return randFloat64(20.920967, 21.020967)
}

func searchWithMemcache(client *memcache.Client) {
	const radius = 0.5

	start := time.Now()

	lat := randLat()

	hashList := geohash.NearbyGeohashList(geohash.Pos{
		Lat: lat,
		Lon: 105.827342,
	}, radius, precision)

	respList := make([]func() (memcache.MGetResponse, error), 0, len(hashList))

	p := client.Pipeline()
	defer p.Finish()

	for _, h := range hashList {
		fn := p.MGet(h.String(), memcache.MGetOptions{})
		respList = append(respList, fn)
	}

	result := make([]ShopModel, 0, 100)
	for _, fn := range respList {
		resp, err := fn()
		if err != nil {
			panic(err)
		}
		if resp.Type != memcache.MGetResponseTypeVA {
			continue
		}

		var modelList []ShopModel
		err = json.Unmarshal(resp.Data, &modelList)
		if err != nil {
			panic(err)
		}
		result = append(result, modelList...)
	}

	count := 0
	for _, s := range result {
		pos1 := haversine.Pos{
			Lat: lat,
			Lon: 105.827342,
		}
		pos2 := haversine.Pos{
			Lat: s.Lat,
			Lon: s.Lon,
		}
		d := haversine.DistanceEarth(pos1, pos2)
		if d <= radius {
			count++
		}
	}

	d := time.Since(start)
	durationMut.Lock()
	durations = append(durations, d)
	durationMut.Unlock()
}

func doSearchUsingDB(db *sqlx.DB) {
	const numConns = 100

	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)

	const numThreads = 100
	const numLoops = 100

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < numLoops; i++ {
				searchWithDB(db)
			}
		}()
	}

	wg.Wait()

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	printPercentile := func(p float64) {
		index := int(p / 100 * float64(len(durations)))
		if index >= len(durations) {
			index = len(durations) - 1
		}
		fmt.Printf("Percentile %f: %v\n", p, durations[index])
	}

	fmt.Println("=========================================")
	fmt.Println("Search With Database")

	printPercentile(50)
	printPercentile(90)
	printPercentile(95)
	printPercentile(99)
	printPercentile(99.9)

	fmt.Println("MAX CONNS:", numConns)
	fmt.Println("Num Threads:", numThreads)
	fmt.Println("TOTAL Requests:", numThreads*numLoops)
	d := time.Since(start)
	fmt.Println("TOTAL Duration:", d)
	fmt.Println("AVG QPS:", float64(numThreads*numLoops)/d.Seconds())
}

func doSearchUsingMemcache(client *memcache.Client) {
	const numThreads = 50
	const numLoops = 100

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numThreads)

	for th := 0; th < numThreads; th++ {
		go func() {
			defer wg.Done()

			for i := 0; i < numLoops; i++ {
				searchWithMemcache(client)
			}
		}()
	}

	wg.Wait()

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	printPercentile := func(p float64) {
		index := int(p / 100 * float64(len(durations)))
		if index >= len(durations) {
			index = len(durations) - 1
		}
		fmt.Printf("Percentile %f: %v\n", p, durations[index])
	}

	fmt.Println("=========================================")
	fmt.Println("Search With Memcache")

	printPercentile(50)
	printPercentile(90)
	printPercentile(95)
	printPercentile(99)
	printPercentile(99.9)

	fmt.Println("Num Threads:", numThreads)
	fmt.Println("TOTAL Requests:", numThreads*numLoops)
	d := time.Since(start)
	fmt.Println("TOTAL Duration:", d)
	fmt.Println("AVG QPS:", float64(numThreads*numLoops)/d.Seconds())
}

func main() {
	//shops := loadShops("shops.csv")
	//indexData(client, shops)
	//doSearchESByLoc()

	//db := sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")

	client, err := memcache.New("localhost:11211", 32)
	if err != nil {
		panic(err)
	}

	//writeDataToDB(db, shops)
	//writeDataToMemcached(db, client)

	//doSearchUsingDB(db)
	doSearchUsingMemcache(client)
}
