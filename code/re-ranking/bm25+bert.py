from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util as STutil
import tqdm
import concurrent.futures
import json
import pickle
import torch

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200/'])
index_name = "graph_title"

#index_embedding='embedding_all_mini'
#index_embedding='embedding_covidbert'
#index_embedding='embedding_clinical'
#index_embedding='embedding_correct'
index_embedding = "embedding_all_mega"

# Load BERT model
#stsb = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
#stsb = SentenceTransformer('allenai/biomed_roberta_base')
#stsb = SentenceTransformer("medicalai/ClinicalBERT")
#stsb = SentenceTransformer("gsarti/covidbert-nli")
stsb = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")



# Function to combine BM25 and BERT scores
def combine_scores(bm25_score, query_embedding, document_embedding, bm25_weight=1.0, bert_weight=30):
    bert_score = float(STutil.pytorch_cos_sim(query_embedding, document_embedding).numpy())
    #print(f"similarity score of BERT: {bert_score}")
    #print(f"similarity score of BM25: {bm25_score}")
    combined_score = bm25_weight * bm25_score + bert_weight * bert_score
    return combined_score

# Your constants
output_file_path = "trec_covid/Results/mega_30.txt"
queries_file = 'D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'


def get_bm25_scores(query, field_name="text_field"):
    # Define the function_score query
    function_score_query = {
        "query": {
            "function_score": {

                "query": {
                    "match": {
                        field_name: query
                    }
                },
                "field_value_factor": {
                    "field": "bm25_field",  # Replace with your actual BM25 field name
                    "modifier": "none",
                    "missing": 1  # Default value for missing fields
                }
            }
        }
    }

    # Execute the search with the function_score query
    search_results = es.search(index=index_name, body=function_score_query, size=1000)

    # Extract document IDs and corresponding BM25 scores
    bm25_scores = {}
    for hit in search_results['hits']['hits']:
        doc_id = hit['_id']
        score = hit['_score']
        bm25_scores[doc_id] = score

    return bm25_scores





def get_document_embeddings(doc_ids):
    # Define the multi GET request
    multi_get_request = []
    for doc_id in doc_ids:
        multi_get_request.append(
            {"_index": index_embedding, "_id": doc_id}
        )

    # Execute the multi GET request
    multi_get_results = es.mget(body={"docs": multi_get_request})

    # Extract document IDs and corresponding embeddings
    doc_embeddings = []
    for doc in multi_get_results["docs"]:
        if "found" in doc and doc["found"]:
            embedding = doc.get("_source", {}).get("vector")
            if embedding:
                doc_embeddings.append(torch.tensor(embedding))

    return doc_embeddings


# Process queries
queries = []
questions = []
combined = []

def build_queries(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            try:
                doc = json.loads(line)
                doc_id = doc["_id"]
                text = doc.get("text", "")
                metadata = doc.get("metadata", {})
                query = metadata.get("query", "")
                narrative = metadata.get("narrative", "")
                queries.append(query)

                combine=f"{query}. {narrative}"
                combined.append(combine)

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                continue

build_queries(queries_file)

# Perform combined BM25 and BERT retrieval
with open(output_file_path, "w") as output_file:
    for query_num, query in enumerate(queries, start=1):
        analyzed_query = es.indices.analyze(index=index_name, body={
            "analyzer": "rebuilt_english",
            "text": query
        })
        analyzed_terms = [token["token"] for token in analyzed_query["tokens"]]
        analyzed_query_text = " ".join(analyzed_terms)
        
        # Retrieve BM25 scores (replace this with your actual BM25 implementation)
        bm25_scores = get_bm25_scores(analyzed_query_text)
        #query_embedding = stsb.encode(analyzed_query_text, convert_to_tensor=True)


        combined_query=combined[query_num-1]
        '''
        combined_analyzed_query = es.indices.analyze(index=index_name, body={
            "analyzer": "rebuilt_english",
            "text": combined_query
        })
        combined_analyzed_terms = [token["token"] for token in combined_analyzed_query["tokens"]]
        analyzed_combined_text = " ".join(combined_analyzed_terms)
        combined_embedding = stsb.encode(analyzed_combined_text, convert_to_tensor=True)
        '''
        combined_embedding = stsb.encode(combined_query, convert_to_tensor=True)
        # Process search results
        search_results = es.search(index=index_name, body={
            "query": {
                "match": {
                    "text_field": analyzed_query_text  # Replace with your field name
                }
            },
            "size": 1000
        })
        
        results_to_sort = []
        for rank, hit in enumerate(search_results['hits']['hits'], start=1):
            doc_id = hit['_id']
            bm25_score = bm25_scores.get(doc_id, 0.0)  # Replace with your actual retrieval scores
            document_embeddings = get_document_embeddings([doc_id])
            document_embedding = document_embeddings[0] if document_embeddings else None
        
            if document_embedding is not None:
                bert_score = float(STutil.pytorch_cos_sim(combined_embedding, document_embedding).numpy())
                combined_score = combine_scores(bm25_score, combined_embedding, document_embedding)
        
                # Store results for sorting
                results_to_sort.append((doc_id, combined_score))
        
        # Sort the results in descending order based on the combined score
        results_to_sort.sort(key=lambda x: x[1], reverse=True)
        
        # Write the sorted results to the output file
        for rank, (doc_id, combined_score) in enumerate(results_to_sort, start=1):
            line = f"Q0{query_num}\t0\t{doc_id}\t0\t{combined_score}\tSearcherProject\n"
            if query_num >= 10:
                line = f"Q{query_num}\t0\t{doc_id}\t0\t{combined_score}\tSearcherProject\n"
            output_file.write(line)
        print(query_num)