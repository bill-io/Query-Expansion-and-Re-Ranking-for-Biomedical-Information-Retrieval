from elasticsearch import Elasticsearch
from gensim.models import Word2Vec
import re
import json
import os 



es = Elasticsearch(['http://localhost:9200/'])
file_path='D:/Πτυχιακη/Datasets/trec-covid/corpus.jsonl'
index_name = "graph_title"

def analyze_words(words):
    analyzed_text = es.indices.analyze(index=index_name, body={
        "analyzer": "rebuilt_english",
        "text": " ".join(words)  
    })
    analyzed_words = [token["token"] for token in analyzed_text["tokens"]]
    return analyzed_words

    

def create_sentences(file):
    doc_count = 0
    sentences = []
    with open(file, 'r', encoding='utf-8', errors='ignore') as jsonl_file:
        for line in jsonl_file:
            data = json.loads(line)
            title = data.get("title", "")
            text = data.get("text", "")
            combined_text = f"{title}. {text}"
            if combined_text:
                words = re.findall(r'\b\w+\b', combined_text.lower())
                analyzed_sentence = analyze_words(words)
                sentences.append(analyzed_sentence)
                doc_count += 1
                if doc_count % 10000 == 0:
                    print(f"Analyzed {doc_count} documents")

    print(f"Finished analyzing {doc_count} documents")
    return sentences



sentences=create_sentences(file_path)


VECTOR_SIZE=400
WINDOW=6
SG=1
epochs=30
#1=skip-gram
#0=cbow
model = Word2Vec(sentences, 
                 vector_size=VECTOR_SIZE, 
                 window=WINDOW,
                 epochs=epochs,
                 min_count=10, 
                 sg=SG)
model.save("w2v_models/400_6_skip_stem/word2vec.model")
print("done training")

 

VECTOR_SIZE=400
WINDOW=6
epochs=30
SG=0
model = Word2Vec(sentences, 
                 vector_size=VECTOR_SIZE, 
                 window=WINDOW,
                 min_count=10, 
                 sg=SG)
model.save("w2v_models/400_6_cbow_stem/word2vec.model")
print("done training")



VECTOR_SIZE=700
WINDOW=6
epochs=30
SG=1
model = Word2Vec(sentences, 
                 vector_size=VECTOR_SIZE, 
                 window=WINDOW,
                 min_count=10, 
                 sg=SG)
model.save("w2v_models/700_6_skip_stem/word2vec.model")
print("done training")
