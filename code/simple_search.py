from elasticsearch import Elasticsearch
import json

es = Elasticsearch(['http://localhost:9200/'])
index_name = "graph_title"

output_file = open("trec_covid/Results/title_text.txt", "w")
test=open("trec_covid/Results/queries.txt", "w")

queries_file='D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'

queries=[]
questions=[]
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

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                continue

build_queries(queries_file)



query_num=1
for query in queries:
        
    analyzed_query = es.indices.analyze(index=index_name, body={
        "analyzer": "rebuilt_english",
        "text": query
    })
    analyzed_terms = [token["token"] for token in analyzed_query["tokens"]]
    analyzed_query_text = " ".join(analyzed_terms)
    print(analyzed_query_text)


    search_results = es.search(index=index_name, body={
        "query": {
            "match": {
                "text_field": analyzed_query_text #textfield-#text
            }
        },
        "size": 1000
    })

    for rank, hit in enumerate(search_results['hits']['hits'], start=1):
        doc_id = hit['_id']
        score = hit['_score']
        line = f"Q0{query_num}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
        if query_num>=10:
            line = f"Q{query_num}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
        output_file.write(line)
        
    query_num=+query_num+1  
      

output_file.close()
