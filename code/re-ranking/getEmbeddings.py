import pickle
import json
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util as STutil
import tqdm
import os
import concurrent.futures
from transformers import AutoTokenizer, AutoModel

# Your constants
documents_file = 'D:/Πτυχιακη/Datasets/trec-covid/corpus.jsonl'
#EMBEDDING_FILE_PATH = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_correct_test.pickle'
#EMBEDDING_FILE_PATH = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_all_mini.pickle'
#EMBEDDING_FILE_PATH = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_clinical.pickle'
#EMBEDDING_FILE_PATH = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_covidBert.pickle'
EMBEDDING_FILE_PATH = 'D:/Πτυχιακη/ElasticSearch/code_correct/Graph_Book/embeddings_all_mega.pickle'


# Load BioBERT model from SentenceTransformer
#stsb = SentenceTransformer('allenai/biomed_roberta_base')
#stsb = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
#stsb = SentenceTransformer("medicalai/ClinicalBERT")
#stsb = SentenceTransformer("gsarti/covidbert-nli")
stsb = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")


# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200/'])

# Function to get all documents
def get_all_documents(file_path):
    documents = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            try:
                doc = json.loads(line)
                doc_id = doc["_id"]
                title = doc.get("title", "")
                text = doc.get("text", "")
                combined_text = f"{title}. {text}"
                documents.append((doc_id, combined_text))

            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                continue

    return documents

# Get all documents from the dataset
all_documents = get_all_documents(documents_file)

# Function to get embeddings in parallel with progress bar
def get_embeddings_parallel(documents, num_workers=4):
    embeddings_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(stsb.encode, doc_text, convert_to_tensor=True) for _, doc_text in documents]

        for i, future in enumerate(tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures))):
            doc_id, embeddings = documents[i][0], future.result().tolist()
            embeddings_list.append((doc_id, embeddings))

    return embeddings_list

# Call the function with all documents
all_embeddings = get_embeddings_parallel(all_documents)

# Save embeddings to a pickle file
with open(EMBEDDING_FILE_PATH, 'wb') as fd:
    pickle.dump(all_embeddings, fd)

print('All embeddings saved to:', EMBEDDING_FILE_PATH)
