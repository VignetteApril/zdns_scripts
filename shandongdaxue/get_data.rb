require 'byebug'
require 'json'

ES_SCRIPT = <<END_OF_SCRIPT
curl -XGET 'http://127.0.0.1:9200/_search?pretty' -H 'Content-Type: application/json' -d '
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
END_OF_SCRIPT

data = `#{ES_SCRIPT}`
json_data = JSON.parse(data)

lines = ''
json_data['aggregations']['thread']['buckets'].each do |thread|
	lines <<  "#{thread['key']}:\n"
	thread['name']['buckets'].each do |name|
		lines << "#{name['key']}\n" 
	end
	lines << "\n"
end

dir_path = ARGV[0]
file_path = File.join(dir_path, 'es_data.txt')

File.open(file_path, "w"){|f| f << lines}