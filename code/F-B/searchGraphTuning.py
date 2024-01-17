from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['http://localhost:9200/'], timeout=50)
index_name = "graph_title"


queries_file = 'D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'
original_boost=2

simple_expansion="trec_covid/Results/simple_expansion.txt"
increase_conceptual_precision="trec_covid/Results/increase_conceptual_precision.txt"
increase_precision_reduce_recall="trec_covid/Results/increase_precision_reduce_recall.txt"
slightly_increased_recall="trec_covid/Results/slightly_increased_recall.txt"
same_results_better_ranking="trec_covid/Results/same_results_better_ranking.txt"

#file=simple_expansion
#file=increase_conceptual_precision
#file=increase_precision_reduce_recall
file=slightly_increased_recall
#file=same_results_better_ranking

covid_list=["covid", "coronavirus", "corona","covid19","19","coronaviru","cov","sar" ,"coronoviru"]
queries = []

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

with open(file, "w") as file:
    query_number = 1
    for query in queries:
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
                if term['score'] > 1:
                    if term in covid_list:
                        boost = term['score'] / 200.0
                    boost = term['score'] / 80.0  # Adjust as needed
                    expanded_keywords.append(f"{term['key']}^{boost}")
                
        
        
        # Combine the original query and expanded terms with boosts
        expanded_query = " ".join([f"{word}^{original_boost}" for word in words] + expanded_keywords)
        #print(expanded_query)

        words_expanded=words+expanded_keywords
        # Simple Query Expansion
        simple_expansion_query = {
            "query": {
                "query_string": {
                    "query": expanded_query,
                    "default_field": "text_field"
                }
            },
            "size": 1000
        }

        # Increased Precision, Reduced Recall Query
        increase_conceptual_precision_query = {
            "query": {
                "query_string": {
                    "query": expanded_query,
                    "default_field": "text_field",
                    "minimum_should_match": "35%"
                }
            },
            "size": 1000
        }

        # Increased Precision, No Reduction in Recall Query
        increase_precision_reduce_recall_query = {
            "query": {
                "bool": {
                    "should": [
                        {"query_string": {"query": word, "default_field": "text_field", "boost": 1.0}} for word in
                        words_expanded],
                    "minimum_should_match": 2
                }
            },
            "size": 1000
        }

        # Slightly Increased Recall Query
        '''
        slightly_increased_recall_query = {
            "query": {
                "bool": {
                    "should": [
                        {"query_string": {"query": expanded_query, "default_field": "text_field", "boost": 2.0}} for word in
                        words_expanded],
                    "minimum_should_match": "1%"
                }
            },
            "size": 1000
        }
        '''
        slightly_increased_recall_query = {
            "query": {
                "query_string": {
                    "query": expanded_query,
                    "default_field": "text_field",
                    "minimum_should_match": "1%"
                }
            },
            "size": 1000
        }
        

        # Same Results, Better Conceptual Ranking Query
        same_results_better_ranking_query = {
            "query": {
                "query_string": {
                    "query": expanded_query,
                    "default_field": "text_field",
                    "boost": 1.0
                }
            },
            "size": 1000
        }

        # Execute the queries
        #simple_expansion_results = es.search(index=index_name, body=simple_expansion_query)
        
        #increase_conceptual_precision_results = es.search(index=index_name,
        #                                                  body=increase_conceptual_precision_query)
        
        #increase_precision_reduce_recall_results = es.search(index=index_name,
        #                                                      body=increase_precision_reduce_recall_query)

        slightly_increased_recall_results = es.search(index=index_name, body=slightly_increased_recall_query)
        #same_results_better_ranking_results = es.search(index=index_name, body=same_results_better_ranking_query)
        
        
        for rank, hit in enumerate(slightly_increased_recall_results['hits']['hits'], start=1):
            doc_id = hit['_id']
            score = hit['_score']
            line = f"Q0{query_number}\t0\t{doc_id}\t{rank}\t{score}\tSearcherProject\n"
            if query_number >= 10:
                line = f"Q{query_number}\t0\t{doc_id}\t{rank}\t{score}\tSearcherProject\n"
            file.write(line)
        
        query_number += 1

print(f"Results written to {file}.")
