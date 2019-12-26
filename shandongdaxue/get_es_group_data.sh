curl -XGET 'http://localhost:9200/_search?pretty' -H 'Content-Type: application/json' -d '
{
  "size": 0,
  "aggs": {
    "thread": {
      "terms": {
        "size": 1000,
        "field": "family"
      },
      "aggs": {
        "name": {
          "terms": {
            "size": 1000,
            "field": "name"
          }
        }
      }
    }
  }
}'