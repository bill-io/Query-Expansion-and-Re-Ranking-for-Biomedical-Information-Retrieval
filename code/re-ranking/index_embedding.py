from elasticsearch.helpers import bulk, BulkIndexError
from elasticsearch import Elasticsearch
import json
import pickle
import numpy as np

es = Elasticsearch(['http://localhost:9200/'], timeout=30)

documents_file = 'D:/Πτυχιακη/Datasets/trec-covid/corpus.jsonl'
#embedding_file = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_correct_test.pickle'
#embedding_file = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_all_mini.pickle'
#embedding_file = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_clinical.pickle'
#embedding_file = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_covidBert.pickle'
embedding_file = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_all_mega.pickle'

#index_name = "embedding_correct"
#index_name = "embedding_all_mini"
#index_name = "embedding_covidbert"
#index_name = "embedding_clinical"
index_name = "embedding_all_mega"

# Load the embeddings
with open(embedding_file, 'rb') as fd:
    all_embeddings = pickle.load(fd)

# Define the index mapping
index_mapping = {
    "mappings": {
        "properties": {
            "vector": {
                "type": "dense_vector",
                "dims": 768  #768 #384
            }
        }
    }
}

# Create the index with mapping
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

es.indices.create(index=index_name, body=index_mapping)

def index_documents(file_path, index_name, embeddings):
    doc_count=0
    for line in open(file_path, 'r', encoding='utf-8', errors='ignore'):
        try:
            doc = json.loads(line)
            doc_id = doc["_id"]
            title = doc.get("title", "")
            text = doc.get("text", "")
            metadata = doc.get("metadata", {})
            url = metadata.get("url", "")
            pubmed_id = metadata.get("pubmed_id", "")
            
            combined_text = f"{title}. {text}"

            # Get the corresponding embedding for the document
            doc_tuple = next(item for item in all_embeddings if item[0] == doc_id)
            doc_emb = doc_tuple[1]

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
                    "only_text": text,
                    "vector": doc_emb
                }
            }

            yield document  # Yield the document for bulk indexing

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            continue

# Index the documents along with their embeddings
try:
    success, failed = bulk(es, index_documents(documents_file, index_name=index_name, embeddings=all_embeddings))
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
