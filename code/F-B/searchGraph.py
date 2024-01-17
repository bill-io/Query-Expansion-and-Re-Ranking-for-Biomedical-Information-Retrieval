from elasticsearch import Elasticsearch
import json
from math import sqrt
import sys

es = Elasticsearch(['http://localhost:9200/'],timeout=50)
index_name = "graph_title"
output_file = "trec_covid/Results/graph.txt"
queries_file='D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'



queries=[]
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

'''# Background Query
background_query = {
    "query": {
        "match_all": {}
    },
    "size": 200000
}
background_result = es.search(index=index_name, body=background_query)
count_bg = background_result["hits"]["total"]["value"]
total_docs_bg = len(background_result["hits"]["hits"])

'''
with open(output_file, "w") as file:
    query_number=1
    for query in queries:
        words= query.split()
        expanded_keywords = []
        print("==============\n")
        print(f"query number    {query_number} query: {query}")
        for word in words:

            '''# Foreground Query
            foreground_query = {
                "query": {
                    "match": {
                    "text_field": word
                        }
                    },
                "size": 1000
            }
            foreground_result = es.search(index=index_name, body=foreground_query)
            count_fg = foreground_result["hits"]["total"]["value"]
            #print(f"The count of occurrences of the term {word} in the foreground set of documents is {count_fg}")
            total_docs_fg = len(foreground_result["hits"]["hits"])
            prob_bg = count_bg / total_docs_bg'''

            
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
                if term['score']>17:
                    expanded_keywords.append(term['key'])
                    print(word + "   adding : "  +term['key'])


        expanded_query = " ".join(words + expanded_keywords)
        analyzed_query = es.indices.analyze(index=index_name, body={
            "analyzer": "rebuilt_english",
            "text": expanded_query
        })
        

        analyzed_terms = [token["token"] for token in analyzed_query["tokens"]]
        analyzed_query_text = " ".join(analyzed_terms)
        

        unique_list=[]
        for word in analyzed_terms:
            if word not in unique_list:
                unique_list.append(word)

        analyzed_query_text=" ".join(unique_list)

        print("analyzed_query   :" + analyzed_query_text + "    Instead of : " + query)
        

        search_results = es.search(index=index_name, body={
            "query": {
                "match": {
                    "text_field": analyzed_query_text
                }
            },
            "size": 1000  # Retrieve the top n results
        })
        
        
        for rank, hit in enumerate(search_results['hits']['hits'], start=1):
            doc_id = hit['_id']
            score = hit['_score']
            line = f"Q0{query_number}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
            if query_number>=10:
                line = f"Q{query_number}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
            file.write(line)
        
        query_number+=1
        

print(f"Results written to {output_file}.")


