from elasticsearch.helpers import bulk,BulkIndexError
from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['http://localhost:9200/'] )
documents_file='D:/Πτυχιακη/Datasets/trec-covid/corpus.jsonl'


index_name = "test"

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)


index_settings = {
    "index.analyze.max_token_count": 20000  # Set the desired limit
}

'''
try:
    es.indices.put_settings(index=index_name, body=index_settings)
    print(f"Updated index settings for '{index_name}'.")
except Exception as e:
    print(f"Failed to update index settings: {str(e)}")
    '''

index_mapping = {
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_" 
        },
        "english_stemmer": {
          "type":       "stemmer",
          "language":   "english"
        },
        "english_possessive_stemmer": {
          "type":       "stemmer",
          "language":   "possessive_english"
        }
      },
      "analyzer": {
        "rebuilt_english": {
          "tokenizer":  "standard",
          "filter": [ 
            "english_possessive_stemmer",
            "lowercase",
            "english_stop",
            "english_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
        "properties": {
            "text_field": {
                "type": "text",
                "analyzer": "rebuilt_english"  
          }
      }
  }
}
'''
index_mapping={
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type": "stop",
          "stopwords": "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "porter_stemmer": {
          "type": "stemmer",
          "name": "porter"
        }
      },
      "analyzer": {
        "rebuilt_english": {
          "tokenizer": "standard",
          "filter": [
            "porter_stemmer",
            "lowercase",
            "english_stop",
            "english_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "text_field": {
        "type": "text",
        "analyzer": "rebuilt_english" 
      },
      "fields": {
        "type": "keyword"
      }
    }
  }
}
'''
es.indices.create(index=index_name, body=index_mapping)




def index_documents(file_path, index_name):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        doc_count=0
        for line in file:
            try:
                doc = json.loads(line)
                doc_id = doc["_id"]
                title = doc.get("title", "")
                text = doc.get("text", "")
                metadata = doc.get("metadata", {})
                url = metadata.get("url", "")
                pubmed_id = metadata.get("pubmed_id", "")

                combined_text = f"{title}. {text}"

                doc_count+=1
                if doc_count % 10000==0:
                   print(f"Indexed {doc_count} documents") 
                # Define the document to be indexed
                document = {
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": {
                        "title": title,
                        "text_field": combined_text,
                        "url": url,
                        "pubmed_id": pubmed_id,
                        "only_text": text
                    }
                }
                
                yield document  # Yield the document for bulk indexing
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                continue
            
        
try:
    success, failed = bulk(es, index_documents(documents_file, index_name=index_name))
    if not failed:
        print("Indexing completed successfully.")
    else:
        print(f"Indexing failed for {len(failed)} documents.")
        for item in failed:
            print(f"Failed to index document with ID: {item['index']['_id']}")
except BulkIndexError as e:
    print(f"Bulk indexing failed with {len(e.errors)} errors.")
    for error in e.errors:
        print(error)

es.indices.refresh(index=index_name)