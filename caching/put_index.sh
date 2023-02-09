curl -X PUT -H "Content-type: application/json" localhost:9400/bench_full_products -d @./mappings.json
curl -X PUT -H "Content-type: application/json" localhost:9400/bench_products -d @./min_mappings.json
