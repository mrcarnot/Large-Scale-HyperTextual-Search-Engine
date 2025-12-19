// semantic_searcher.cpp
// Fast semantic search using cosine similarity
// Compile: g++ -std=c++17 -O2 semantic_searcher.cpp -o semantic_searcher

#include <bits/stdc++.h>
#include <filesystem>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <random>

namespace fs = std::filesystem;
using namespace std;

// ==================== Data Structures ====================
struct SemanticResult {
    string docid;
    float similarity;
    size_t doc_index;
};

// ==================== Semantic Index ====================
class SemanticIndex {
private:
    vector<string> docids;
    vector<float> embeddings;  // Flattened: [doc0_dim0, doc0_dim1, ..., doc1_dim0, ...]
    int embedding_dim;
    int num_docs;
    bool loaded;
    
public:
    SemanticIndex() : embedding_dim(0), num_docs(0), loaded(false) {}
    
    // Load semantic index from binary file
    bool load(const string& index_path) {
        ifstream ifs(index_path, ios::binary);
        if (!ifs) {
            cerr << "ERROR: Cannot open semantic index: " << index_path << "\n";
            return false;
        }
        
        auto start = chrono::high_resolution_clock::now();
        
        // Read header
        uint32_t magic, version, dim, num;
        ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        
        if (magic != 0x53454D49) {
            cerr << "ERROR: Invalid semantic index file (bad magic)\n";
            return false;
        }
        
        ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
        ifs.read(reinterpret_cast<char*>(&dim), sizeof(dim));
        ifs.read(reinterpret_cast<char*>(&num), sizeof(num));
        
        embedding_dim = dim;
        num_docs = num;
        
        cerr << "Loading semantic index...\n";
        cerr << "  Dimension: " << embedding_dim << "\n";
        cerr << "  Documents: " << num_docs << "\n";
        
        // Read docids
        docids.reserve(num_docs);
        for (int i = 0; i < num_docs; i++) {
            uint32_t docid_len;
            ifs.read(reinterpret_cast<char*>(&docid_len), sizeof(docid_len));
            
            string docid(docid_len, '\0');
            ifs.read(&docid[0], docid_len);
            docids.push_back(docid);
        }
        
        // Read embeddings
        size_t total_floats = (size_t)num_docs * embedding_dim;
        embeddings.resize(total_floats);
        
        ifs.read(reinterpret_cast<char*>(embeddings.data()), 
                 total_floats * sizeof(float));
        
        ifs.close();
        loaded = true;
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cerr << "Loaded in " << duration.count() << " ms\n";
        cerr << "Memory usage: ~" << (total_floats * sizeof(float) / (1024.0 * 1024.0))
             << " MB\n";
        
        return true;
    }
    
    // Compute cosine similarity between two vectors
    float cosine_similarity(const float* vec1, const float* vec2, int dim) const {
        float dot = 0.0f;
        for (int i = 0; i < dim; i++) {
            dot += vec1[i] * vec2[i];
        }
        return dot;  // Assuming normalized vectors
    }
    
    // Search with query embedding
    vector<SemanticResult> search(const vector<float>& query_embedding, 
                                  int top_k = 10) {
        if (!loaded) {
            cerr << "ERROR: Index not loaded\n";
            return {};
        }
        
        if ((int)query_embedding.size() != embedding_dim) {
            cerr << "ERROR: Query embedding dimension mismatch: "
                 << query_embedding.size() << " vs " << embedding_dim << "\n";
            return {};
        }
        
        // Normalize query embedding
        vector<float> normalized_query = query_embedding;
        float norm = 0.0f;
        for (float val : normalized_query) {
            norm += val * val;
        }
        norm = sqrt(norm);
        
        if (norm > 1e-8) {
            for (float& val : normalized_query) {
                val /= norm;
            }
        }
        
        // Compute similarities for all documents
        vector<SemanticResult> results;
        results.reserve(num_docs);
        
        for (int i = 0; i < num_docs; i++) {
            const float* doc_emb = &embeddings[i * embedding_dim];
            float sim = cosine_similarity(normalized_query.data(), doc_emb, embedding_dim);
            
            SemanticResult res;
            res.docid = docids[i];
            res.similarity = sim;
            res.doc_index = i;
            results.push_back(res);
        }
        
        // Partial sort to get top-K
        if ((int)results.size() > top_k) {
            partial_sort(results.begin(), results.begin() + top_k, results.end(),
                        [](const SemanticResult& a, const SemanticResult& b) {
                            return a.similarity > b.similarity;
                        });
            results.resize(top_k);
        } else {
            sort(results.begin(), results.end(),
                 [](const SemanticResult& a, const SemanticResult& b) {
                     return a.similarity > b.similarity;
                 });
        }
        
        return results;
    }
    
    // Get statistics
    void print_stats() const {
        cerr << "\n=== Semantic Index Statistics ===\n";
        cerr << "Documents: " << num_docs << "\n";
        cerr << "Embedding dimension: " << embedding_dim << "\n";
        cerr << "Memory: ~" << (embeddings.size() * sizeof(float) / (1024.0 * 1024.0))
             << " MB\n";
    }
    
    int get_num_docs() const { return num_docs; }
    int get_embedding_dim() const { return embedding_dim; }
};

// ==================== Query Embedding Generator ====================
// For demo: Generate random embedding or parse from input
vector<float> generate_query_embedding(const string& query_text, int dim) {
    // PLACEHOLDER: In real system, this would call a neural network
    // For now, generate a simple embedding based on query hash
    
    vector<float> embedding(dim);
    hash<string> hasher;
    size_t seed = hasher(query_text);
    
    mt19937 rng(seed);
    normal_distribution<float> dist(0.0f, 1.0f);
    
    for (int i = 0; i < dim; i++) {
        embedding[i] = dist(rng);
    }
    
    // Normalize
    float norm = 0.0f;
    for (float val : embedding) {
        norm += val * val;
    }
    norm = sqrt(norm);
    
    if (norm > 1e-8) {
        for (float& val : embedding) {
            val /= norm;
        }
    }
    
    return embedding;
}

// Parse embedding from comma-separated string
vector<float> parse_embedding_string(const string& emb_str) {
    vector<float> embedding;
    istringstream iss(emb_str);
    string token;
    
    while (getline(iss, token, ',')) {
        try {
            embedding.push_back(stof(token));
        } catch (...) {
            cerr << "Warning: Invalid float in embedding string\n";
        }
    }
    
    return embedding;
}

// ==================== Performance Benchmark ====================
void run_benchmark(SemanticIndex& index) {
    cerr << "\n=== Running Performance Benchmark ===\n";
    
    int dim = index.get_embedding_dim();
    vector<long long> timings;
    
    // Generate test queries
    vector<string> test_queries = {
        "covid symptoms treatment",
        "vaccine effectiveness",
        "transmission rates",
        "clinical trials results",
        "public health response"
    };
    
    cout << "\nRunning " << test_queries.size() << " test queries...\n\n";
    
    for (const auto& query : test_queries) {
        auto query_emb = generate_query_embedding(query, dim);
        
        auto start = chrono::high_resolution_clock::now();
        auto results = index.search(query_emb, 10);
        auto end = chrono::high_resolution_clock::now();
        
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        timings.push_back(duration.count());
        
        cout << "Query: \"" << query << "\"\n";
        cout << "  Time: " << (duration.count() / 1000.0) << " ms\n";
        cout << "  Results: " << results.size() << "\n";
        if (!results.empty()) {
            cout << "  Top result: " << results[0].docid 
                 << " (sim: " << fixed << setprecision(4) << results[0].similarity << ")\n";
        }
        cout << "\n";
    }
    
    // Calculate statistics
    sort(timings.begin(), timings.end());
    long long min_time = timings.front();
    long long max_time = timings.back();
    long long median = timings[timings.size() / 2];
    double avg = accumulate(timings.begin(), timings.end(), 0.0) / timings.size();
    
    cerr << "=== Performance Statistics ===\n";
    cerr << "Min: " << (min_time / 1000.0) << " ms\n";
    cerr << "Avg: " << (avg / 1000.0) << " ms\n";
    cerr << "Median: " << (median / 1000.0) << " ms\n";
    cerr << "Max: " << (max_time / 1000.0) << " ms\n";
}

// ==================== Main ====================
int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " -d index_dir [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -d DIR           : index directory (required)\n";
        cerr << "  -q \"QUERY\"       : query text (generates embedding)\n";
        cerr << "  -e \"EMB_VECTOR\"  : query embedding (comma-separated floats)\n";
        cerr << "  -k N             : number of results (default: 10)\n";
        cerr << "  --benchmark      : run performance benchmark\n";
        cerr << "  --stats          : show index statistics\n";
        cerr << "\nExamples:\n";
        cerr << "  Query text:  " << argv[0] << " -d index_out -q \"covid symptoms\"\n";
        cerr << "  Query vector: " << argv[0] << " -d index_out -e \"0.1,0.2,...\"\n";
        cerr << "  Benchmark:   " << argv[0] << " -d index_out --benchmark\n";
        cerr << "\nNOTE: Query text uses placeholder embedding generation.\n";
        cerr << "      For real semantic search, provide actual embeddings with -e\n";
        return 1;
    }
    
    string index_dir;
    string query_text;
    string embedding_str;
    int top_k = 10;
    bool run_bench = false;
    bool show_stats = false;
    bool interactive = false;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            index_dir = argv[++i];
        } else if (arg == "-q" && i+1 < argc) {
            query_text = argv[++i];
        } else if (arg == "-e" && i+1 < argc) {
            embedding_str = argv[++i];
        } else if (arg == "-k" && i+1 < argc) {
            top_k = stoi(argv[++i]);
        } else if (arg == "--benchmark") {
            run_bench = true;
        } else if (arg == "--stats") {
            show_stats = true;
        } else if (arg == "--interactive") {
            interactive = true;
        }
    }
    
    if (index_dir.empty()) {
        cerr << "ERROR: Index directory (-d) required\n";
        return 1;
    }
    
    string index_path = index_dir + "/semantic.idx";
    
    if (!fs::exists(index_path)) {
        cerr << "ERROR: Semantic index not found: " << index_path << "\n";
        cerr << "Run semantic_indexer first to build the index\n";
        return 1;
    }
    
    // Load index
    SemanticIndex index;
    if (!index.load(index_path)) {
        return 1;
    }
    
    if (show_stats) {
        index.print_stats();
    }
    
    if (run_bench) {
        run_benchmark(index);
        return 0;
    }
    
    if (interactive) {
        cout << "\n=== Semantic Search Interactive Mode ===\n";
        cout << "Enter query text (or 'quit' to exit)\n";
        cout << "NOTE: Using placeholder embedding generation\n\n";
        
        string line;
        while (true) {
            cout << "Query> ";
            if (!getline(cin, line)) break;
            
            if (line == "quit" || line == "exit") break;
            if (line.empty()) continue;
            
            auto query_emb = generate_query_embedding(line, index.get_embedding_dim());
            
            auto start = chrono::high_resolution_clock::now();
            auto results = index.search(query_emb, top_k);
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
            
            cout << "\nFound " << results.size() << " results in " 
                 << duration.count() << " ms\n\n";
            
            for (size_t i = 0; i < results.size(); i++) {
                cout << (i+1) << ". " << results[i].docid 
                     << " (similarity: " << fixed << setprecision(4) 
                     << results[i].similarity << ")\n";
            }
            cout << "\n";
        }
        
        return 0;
    }
    
    // Single query mode
    vector<float> query_emb;
    
    if (!embedding_str.empty()) {
        // Parse provided embedding
        query_emb = parse_embedding_string(embedding_str);
        
        if (query_emb.empty() || (int)query_emb.size() != index.get_embedding_dim()) {
            cerr << "ERROR: Invalid embedding dimension\n";
            cerr << "Expected: " << index.get_embedding_dim() << "\n";
            cerr << "Got: " << query_emb.size() << "\n";
            return 1;
        }
        
        cerr << "Using provided embedding vector\n";
        
    } else if (!query_text.empty()) {
        // Generate embedding from query text (placeholder)
        cerr << "WARNING: Using placeholder embedding generation\n";
        cerr << "For real semantic search, provide actual embeddings with -e\n";
        query_emb = generate_query_embedding(query_text, index.get_embedding_dim());
        
    } else {
        cerr << "ERROR: Either -q or -e required\n";
        return 1;
    }
    
    // Execute search
    auto start = chrono::high_resolution_clock::now();
    auto results = index.search(query_emb, top_k);
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    // Display results
    cout << "\nSemantic Search Results (" << duration.count() << " ms):\n\n";
    
    for (size_t i = 0; i < results.size(); i++) {
        cout << (i+1) << ". " << results[i].docid 
             << " (similarity: " << fixed << setprecision(4) 
             << results[i].similarity << ")\n";
    }
    
    if (results.empty()) {
        cout << "No results found\n";
    }
    
    return 0;
}