from elasticsearch import Elasticsearch
from gensim.models import Word2Vec
import json

es = Elasticsearch(['http://localhost:9200/'])

index = "graph_title"
index_train = "index_trec_covid_only_lowercase"

word2vec_model = Word2Vec.load("w2v_models/200_5_skip/word2vec.model")

output_file = "trec_covid/Results/title_text_w2v_200_weights.txt"
queries_file = 'D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'

min_sim = 0.70
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


def analyze_w2v(query_text):
    analyzed_text = es.indices.analyze(index=index_train, body={
        "analyzer": "lowercase",
        "text": query_text
    })
    analyzed_words = [token["token"] for token in analyzed_text["tokens"]]
    return analyzed_words


def find_similar_words(word, word2vec_model):
    try:
        word_vector = word2vec_model.wv[word]
        similar_words = word2vec_model.wv.similar_by_vector(word_vector, topn=2)
        similar_words = [(token, score) for token, score in similar_words if score >= min_sim]
        similar_words = [(token, score) for token, score in similar_words if token != word]
        return similar_words

    except KeyError:
        return []

covid_list=["covid", "coronavirus", "corona","covid19","19","coronaviru","cov","sar"]

with open(output_file, "w") as file:
    query_number = 1

    for query in queries:
        keywords = analyze_w2v(query)
        expanded_keywords = []
        print("-------------------")
        print(query_number)
        for keyword in keywords:

            if keyword.lower() in covid_list:
                weight = 0.8  
            else:
                weight = 1.0
            expanded_keywords.append((keyword, weight))

            similar_words = find_similar_words(keyword, word2vec_model)
            for similar_word, score in similar_words:
                if similar_word not in keywords:
                   
                    if similar_word.lower() in covid_list:
                        weight = 0.5  
                    else:
                        weight = score
                    expanded_keywords.append((similar_word, weight))
                    print(f"{keyword} adding {similar_word} with weight {weight}")

        expanded_query_terms = [(term, weight) for term, weight in expanded_keywords]
        expanded_query = " ".join([f"{term}^{weight}" for term, weight in expanded_query_terms])

        analyzed_query = es.indices.analyze(index=index, body={
            "analyzer": "rebuilt_english",
            "text": expanded_query
        })

        analyzed_terms = [token["token"] for token in analyzed_query["tokens"]]
        analyzed_query_text = " ".join(analyzed_terms)

        print("analyzed_query   :" + analyzed_query_text + "    Instead of : " + query)

        search_results = es.search(index=index, body={
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
            score = hit['_score']
            line = f"Q0{query_number}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
            if query_number >= 10:
                line = f"Q{query_number}\t0\t{doc_id}\t0\t{score}\tSearcherProject\n"
            file.write(line)

        query_number += 1

print(f"Results written to {output_file}.")

