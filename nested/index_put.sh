curl -X PUT -H "Content-type: application/json" localhost:9400/simple_products -d @./simple_mappings.json
echo "Finish Create Simple Products"
curl -X PUT -H "Content-type: application/json" localhost:9400/nested_products -d @./nested_mappings.json
echo "Finish Create Nested Products"
