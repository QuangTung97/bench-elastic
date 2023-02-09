curl localhost:9400/_cat/indices?format=json | jq -c ".[]" | grep bench | jq
