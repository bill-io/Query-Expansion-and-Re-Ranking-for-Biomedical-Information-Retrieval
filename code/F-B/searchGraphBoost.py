from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['http://localhost:9200/'], timeout=50)
index_name = "graph_title"
output_file = "trec_covid/Results/graphBoost_bm25.txt"
queries_file = 'D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'

original_boost=3
queries = []
covid_list=["covid", "coronavirus", "corona","covid19","19","coronaviru","cov","sar" ,"coronoviru"]

def build_queries(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            try:
                doc = json.loads(line)
                doc_id = doc["_id"]
                text = doc.get("text", "")
                metadata = doc.get("metadata", {})
                query = metadata.get("query", "")
                queries.append(query)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                continue


build_queries(queries_file)

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

with open(output_file, "w") as file:
    query_number = 1
    for query in queries:
        print("=========================")
        print(query_number)
        words = query.split()
        expanded_keywords = []
        for word in words:

            aggregation_query = {
                "size": 0,
                "query": {
                    "match": {
                        "text_field": word
                    }
                },
                "aggs": {
                    "significant_terms": {
                        "significant_text": {
                            "field": "text_field",
                            "background_filter": {
                                "match_all": {}
                            }
                        }
                    }
                }
            }
            aggregation_result = es.search(index=index_name, body=aggregation_query)
            significant_terms = aggregation_result["aggregations"]["significant_terms"]["buckets"]

            for term in significant_terms:
                if term['score'] > 3:
                    if term in covid_list:
                        boost=term['score']/200.0
                    # Calculate the boost based on the relatedness score
                    #print(f"score of relavance between {word} and {term['key']} is  {term['score']} ")
                    boost = term['score'] / 80.0  # Adjust as needed
                    
                    expanded_keywords.append(f"{term['key']}^{boost}")
        
        
        # Combine the original query and expanded terms with boosts
        expanded_query = " ".join([f"{word}^{original_boost}" for word in words] + expanded_keywords)
        print(expanded_query)

        bm25_scores = get_bm25_scores(expanded_query)

        search_results = es.search(index=index_name, body={
            "query": {
                "query_string": {
                    "query": expanded_query,
                    "default_field": "text_field"
                }
            },
            "size": 1000  
        })
        
        
        for rank, hit in enumerate(search_results['hits']['hits'], start=1):
            doc_id = hit['_id']
            score = bm25_scores.get(doc_id, 0.0)
            #
            score = hit['_score']
            line = f"Q0{query_number}\t0\t{doc_id}\t{rank}\t{score}\tSearcherProject\n"
            if query_number >= 10:
                line = f"Q{query_number}\t0\t{doc_id}\t{rank}\t{score}\tSearcherProject\n"
            file.write(line)
        
        query_number += 1

print(f"Results written to {output_file}.")
