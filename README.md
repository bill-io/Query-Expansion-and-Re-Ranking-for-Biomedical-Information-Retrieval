
**Query Expansion & Re-Ranking for Biomedical Information Retrieval**  

**Overview**  
This project explores advanced **query expansion** and **document re-ranking** techniques to improve biomedical information retrieval. 
It was developed as part of a **bachelor thesis** and focuses on enhancing search accuracy in large-scale biomedical datasets, particularly the **TREC-COVID** dataset.  

**Key Features**  
- **Query Expansion**: Implements synonym-based query expansion using **Word2Vec** and **Semantic Knowledge Graphs (SKG)**.  
- **Re-Ranking with Transformers**: Utilizes **BERT-based sentence embeddings** to refine search results.  
- **Evaluation & Benchmarking**: Compares methods against the **BM25** baseline, showing improved precision and recall.  
- **Scalable Search Implementation**: Uses **Elasticsearch** for document indexing and retrieval.  

**Thesis Contribution**  
- Proposes novel **query expansion** methods that dynamically enhance search queries.  
- Integrates **neural ranking models** for effective **document re-ranking**.  
- Demonstrates **state-of-the-art** results on the **TREC-COVID** dataset, outperforming standard retrieval methods.  

## **Installation & Setup**  
1. Clone the repository:  
   ```bash
   git clone https://github.com/bill-io/Thesis.git
   cd Thesis
   ```
2. Install dependencies:  
   ```bash
   pip install -r requirements.txt
   ```
3. Run the pipeline:  
   ```bash
   python main.py
   ```

## **Folder Structure**  
```
📂 Thesis
 ├── 📜 code/                   # Code implementation
 ├── 📜 results/               # Experimental results & benchmarks
 ├── 📜 README.md              # Project documentation (this file)
 └── 📜 thesis.pdf             # Full thesis document
```
## **The Whole TREC_COVID Dataset Can Be Found Through The Official Site Of "National Institute of Standards and Technology" **
https://ir.nist.gov/trec-covid/data.html

## **Citation**  
If you use this project in your work, please cite:  
**Ioannidis, V. (2024). "Query Expansion Techniques and Document Re-Ranking for Biomedical Information Retrieval."**  

---


