from elasticsearch import Elasticsearch
from gensim.models import Word2Vec
import json

es = Elasticsearch(['http://localhost:9200/'])


index = "graph_title"
index_train="index_trec_covid_only_lowercase"

#settings = es.indices.get_settings(index=index)
#print(settings)

word2vec_model =Word2Vec.load("w2v_models/400_6_cbow_stem/word2vec.model")

output_file = "trec_covid/Results/w2v_400_6_cbow.txt"
queries_file='D:/Πτυχιακη/Datasets/trec-covid/queries.jsonl'

min_sim=0.7

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


'''def covid(queries):
    words=["covid-19" , "COVID19" ,"covid" , "coronavirus" ,"COVID-19" ,"COVID"]
    queries_nocovid=[]
    for query in queries:
        query_correct=[]
        query_words=query.split()
        for word in query_words:
            if word not in words:
                query_correct.append(word)
        query_string=' '.join(query_correct)
        queries_nocovid.append(query_string)
    return queries_nocovid

queries=covid(queries)'''


def analyze_w2v(query_text):
    analyzed_text = es.indices.analyze(index=index, body={
        "analyzer": "rebuilt_english",
        "text":  query_text
    })
    analyzed_words = [token["token"] for token in analyzed_text["tokens"]]
    return analyzed_words



def find_similar_words(word, word2vec_model, top_max_n=2, max_similarity_difference=0.13):
    try:
        word_vector=word2vec_model.wv[word]
        similar_words = word2vec_model.wv.similar_by_vector(word_vector, topn=top_max_n)
        similar_words = [(token, score) for token, score in similar_words if score >= min_sim]
        similar_words = [token for token, _ in similar_words if token != word]
        return similar_words



        '''
        word_vector = word2vec_model.wv[word]
        similar_words = word2vec_model.wv.most_similar(word_vector, topn=top_max_n)
        similar_words = [(token, score) for token, score in similar_words if score >= min_sim]


        if len(similar_words) <= 1:
            return [token for token, _ in similar_words if token != word]


        
        filtered_similar_words = [similar_words[0]]
        for i in range(1, len(similar_words)):
            # Check the difference in similarity scores
            score_difference = filtered_similar_words[0][1] - similar_words[i][1]
            print(filtered_similar_words[0][0] + " has distance with " + similar_words[i][0] + " score: " + str(score_difference))
            if score_difference <= max_similarity_difference:
                filtered_similar_words.append(similar_words[i])
            else:
                break
        
        return [token for token, _ in filtered_similar_words if token != word]
        '''
    
    except KeyError:
        return []


with open(output_file, "w") as file:
    query_number=1

    for query in queries:
        keywords = analyze_w2v(query)
        expanded_keywords = []
        print("-------------------")
        print(query_number)
        for keyword in keywords:
            similar_words = find_similar_words(keyword, word2vec_model)
            expanded_keywords.extend(similar_words)
            print(keyword + "   adding : " + ", ".join(similar_words))

        expanded_query = " ".join(keywords + expanded_keywords)
        #expanded_query = " ".join(full_list)
        
        analyzed_query = es.indices.analyze(index=index, body={
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

        search_results = es.search(index=index, body={
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
