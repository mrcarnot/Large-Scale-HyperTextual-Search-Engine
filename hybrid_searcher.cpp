// hybrid_searcher.cpp - FIXED for Windows
// Combines keyword (BM25) and semantic search
// Compile: g++ -std=c++17 -O2 hybrid_searcher.cpp -o hybrid_searcher

#include <bits/stdc++.h>
#include <filesystem>
#include <algorithm>
#include <cmath>
#include <random>

namespace fs = std::filesystem;
using namespace std;

// ==================== Data Structures ====================
struct HybridResult {
    string docid;
    double bm25_score;
    float semantic_score;
    double final_score;
    int keyword_rank;
    int semantic_rank;
};

// ==================== Semantic Index ====================
class SemanticIndex {
private:
    vector<string> docids;
    vector<float> embeddings;
    int embedding_dim;
    int num_docs;
    bool loaded;
    
public:
    SemanticIndex() : embedding_dim(0), num_docs(0), loaded(false) {}
    
    bool load(const string& index_path) {
        ifstream ifs(index_path, ios::binary);
        if (!ifs) {
            cerr << "ERROR: Cannot open semantic index: " << index_path << "\n";
            return false;
        }
        
        uint32_t magic, version, dim, num;
        ifs.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        
        if (magic != 0x53454D49) {
            cerr << "ERROR: Invalid semantic index (bad magic number)\n";
            return false;
        }
        
        ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
        ifs.read(reinterpret_cast<char*>(&dim), sizeof(dim));
        ifs.read(reinterpret_cast<char*>(&num), sizeof(num));
        
        embedding_dim = dim;
        num_docs = num;
        
        cerr << "Loading semantic index (dim: " << embedding_dim 
             << ", docs: " << num_docs << ")...\n";
        
        docids.reserve(num_docs);
        for (int i = 0; i < num_docs; i++) {
            uint32_t len;
            ifs.read(reinterpret_cast<char*>(&len), sizeof(len));
            string docid(len, '\0');
            ifs.read(&docid[0], len);
            docids.push_back(docid);
        }
        
        size_t total = (size_t)num_docs * embedding_dim;
        embeddings.resize(total);
        ifs.read(reinterpret_cast<char*>(embeddings.data()), total * sizeof(float));
        
        if (!ifs.good()) {
            cerr << "ERROR: Failed to read semantic index completely\n";
            return false;
        }
        
        loaded = true;
        cerr << "Semantic index loaded successfully\n";
        return true;
    }
    
    vector<pair<string, float>> search(const vector<float>& query_emb, int top_k) {
        if (!loaded) {
            cerr << "ERROR: Semantic index not loaded\n";
            return {};
        }
        
        if ((int)query_emb.size() != embedding_dim) {
            cerr << "ERROR: Query embedding dimension mismatch: " 
                 << query_emb.size() << " vs " << embedding_dim << "\n";
            return {};
        }
        
        // Normalize query
        vector<float> norm_query = query_emb;
        float norm = 0.0f;
        for (float v : norm_query) norm += v * v;
        norm = sqrt(norm);
        if (norm > 1e-8) {
            for (float& v : norm_query) v /= norm;
        }
        
        // Compute similarities
        vector<pair<float, string>> results;
        results.reserve(num_docs);
        
        for (int i = 0; i < num_docs; i++) {
            const float* doc = &embeddings[i * embedding_dim];
            float sim = 0.0f;
            for (int j = 0; j < embedding_dim; j++) {
                sim += norm_query[j] * doc[j];
            }
            results.emplace_back(sim, docids[i]);
        }
        
        // Sort and get top-K
        int n = min(top_k, (int)results.size());
        partial_sort(results.begin(), results.begin() + n, results.end(),
                    [](auto& a, auto& b) { return a.first > b.first; });
        
        vector<pair<string, float>> output;
        for (int i = 0; i < n; i++) {
            output.emplace_back(results[i].second, results[i].first);
        }
        
        return output;
    }
    
    int get_dim() const { return embedding_dim; }
    bool is_loaded() const { return loaded; }
};

// ==================== BM25 Searcher Integration (Windows) ====================
vector<pair<string, double>> call_bm25_searcher(const string& query, int top_k, 
                                                const string& index_dir) {
    vector<pair<string, double>> results;
    
    // Check if searcher exists
    if (!fs::exists("searcher.exe")) {
        cerr << "ERROR: searcher.exe not found in current directory\n";
        cerr << "Please compile searcher.cpp first:\n";
        cerr << "  g++ -std=c++17 -O2 searcher.cpp -o searcher\n";
        return results;
    }
    
    // Create unique temp file
    string temp_file = "temp_bm25_" + to_string(time(nullptr)) + ".txt";
    
    // Build command - use proper escaping for Windows
    string safe_query = query;
    for (char& c : safe_query) {
        if (c == '"') c = '\'';  // Replace quotes
    }
    
    string cmd = "searcher.exe -d \"" + index_dir + "\" -q \"" + 
                 safe_query + "\" -k " + to_string(top_k) + 
                 " > \"" + temp_file + "\" 2>&1";
    
    cerr << "  Executing BM25 search...\n";
    int ret = system(cmd.c_str());
    
    if (ret != 0) {
        cerr << "  Warning: BM25 searcher returned error code " << ret << "\n";
    }
    
    // Read results
    ifstream ifs(temp_file);
    if (!ifs) {
        cerr << "  Warning: Could not read BM25 results from " << temp_file << "\n";
        remove(temp_file.c_str());
        return results;
    }
    
    string line;
    int parsed = 0;
    
    while (getline(ifs, line)) {
        // Parse: "1. [Final: 45.67 | BM25: 40.23 | Recency: 0.85] doc123 - Title"
        // or:    "1. [Final: 45.67 | BM25: 40.23 | Recency: 0.85]"
        
        size_t final_pos = line.find("[Final:");
        if (final_pos == string::npos) continue;
        
        size_t pipe_pos = line.find("|", final_pos);
        size_t bracket_close = line.find("]", final_pos);
        
        if (pipe_pos == string::npos || bracket_close == string::npos) continue;
        
        try {
            // Extract final score
            string score_str = line.substr(final_pos + 8, pipe_pos - final_pos - 8);
            double score = stod(score_str);
            
            // Extract docid (after the bracket)
            size_t docid_start = bracket_close + 1;
            while (docid_start < line.size() && isspace(line[docid_start])) {
                docid_start++;
            }
            
            if (docid_start >= line.size()) continue;
            
            string remaining = line.substr(docid_start);
            
            // Find first word (docid) - stop at space or dash
            string docid;
            for (char c : remaining) {
                if (isspace(c) || c == '-') break;
                docid += c;
            }
            
            if (!docid.empty()) {
                results.emplace_back(docid, score);
                parsed++;
            }
        } catch (const exception& e) {
            // Skip malformed lines
            continue;
        }
    }
    ifs.close();
    
    // Clean up temp file
    remove(temp_file.c_str());
    
    cerr << "  Parsed " << parsed << " BM25 results\n";
    return results;
}

// ==================== Hybrid Ranking ====================
vector<HybridResult> hybrid_rank(const vector<pair<string, double>>& bm25_results,
                                 const vector<pair<string, float>>& semantic_results,
                                 double keyword_weight = 0.6,
                                 double semantic_weight = 0.4) {
    
    // Build maps
    unordered_map<string, pair<double, int>> bm25_map;
    for (size_t i = 0; i < bm25_results.size(); i++) {
        bm25_map[bm25_results[i].first] = {bm25_results[i].second, (int)i};
    }
    
    unordered_map<string, pair<float, int>> semantic_map;
    for (size_t i = 0; i < semantic_results.size(); i++) {
        semantic_map[semantic_results[i].first] = {semantic_results[i].second, (int)i};
    }
    
    // All unique documents
    unordered_set<string> all_docs;
    for (const auto& r : bm25_results) all_docs.insert(r.first);
    for (const auto& r : semantic_results) all_docs.insert(r.first);
    
    // Normalize scores
    double max_bm25 = 0.0;
    for (const auto& r : bm25_results) {
        max_bm25 = max(max_bm25, r.second);
    }
    if (max_bm25 < 1e-8) max_bm25 = 1.0;
    
    float max_semantic = 0.0f;
    for (const auto& r : semantic_results) {
        max_semantic = max(max_semantic, r.second);
    }
    if (max_semantic < 1e-8) max_semantic = 1.0f;
    
    // Calculate hybrid scores
    vector<HybridResult> hybrid_results;
    hybrid_results.reserve(all_docs.size());
    
    for (const string& docid : all_docs) {
        HybridResult hr;
        hr.docid = docid;
        
        // Get BM25 score
        if (bm25_map.find(docid) != bm25_map.end()) {
            hr.bm25_score = bm25_map[docid].first / max_bm25;
            hr.keyword_rank = bm25_map[docid].second + 1;
        } else {
            hr.bm25_score = 0.0;
            hr.keyword_rank = -1;
        }
        
        // Get semantic score
        if (semantic_map.find(docid) != semantic_map.end()) {
            hr.semantic_score = semantic_map[docid].first / max_semantic;
            hr.semantic_rank = semantic_map[docid].second + 1;
        } else {
            hr.semantic_score = 0.0f;
            hr.semantic_rank = -1;
        }
        
        // Compute weighted hybrid score
        hr.final_score = keyword_weight * hr.bm25_score + 
                        semantic_weight * hr.semantic_score;
        
        hybrid_results.push_back(hr);
    }
    
    // Sort by final score
    sort(hybrid_results.begin(), hybrid_results.end(),
         [](const HybridResult& a, const HybridResult& b) {
             return a.final_score > b.final_score;
         });
    
    return hybrid_results;
}

// ==================== Query Embedding Generator (Placeholder) ====================
vector<float> generate_query_embedding(const string& query, int dim) {
    cerr << "  WARNING: Using placeholder embedding (hash-based)\n";
    cerr << "  For real semantic search, use query_embedder.py\n";
    
    vector<float> emb(dim);
    hash<string> hasher;
    mt19937 rng(hasher(query));
    normal_distribution<float> dist(0.0f, 1.0f);
    
    for (int i = 0; i < dim; i++) {
        emb[i] = dist(rng);
    }
    
    float norm = 0.0f;
    for (float v : emb) norm += v * v;
    norm = sqrt(norm);
    if (norm > 1e-8) {
        for (float& v : emb) v /= norm;
    }
    
    return emb;
}

// ==================== Main ====================
int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " -d index_dir [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -d DIR        : index directory (required)\n";
        cerr << "  -q \"QUERY\"    : query text\n";
        cerr << "  -k N          : results per method (default: 20)\n";
        cerr << "  -t N          : top final results (default: 10)\n";
        cerr << "  -w WEIGHT     : keyword weight 0.0-1.0 (default: 0.6)\n";
        cerr << "  --interactive : interactive mode\n";
        cerr << "\nExample:\n";
        cerr << "  " << argv[0] << " -d index_out -q \"covid symptoms\" -w 0.5\n";
        cerr << "\nNote: Requires searcher.exe in current directory\n";
        return 1;
    }
    
    string index_dir;
    string query;
    int top_k_each = 20;
    int top_final = 10;
    double keyword_weight = 0.6;
    bool interactive = false;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            index_dir = argv[++i];
        } else if (arg == "-q" && i+1 < argc) {
            query = argv[++i];
        } else if (arg == "-k" && i+1 < argc) {
            top_k_each = stoi(argv[++i]);
        } else if (arg == "-t" && i+1 < argc) {
            top_final = stoi(argv[++i]);
        } else if (arg == "-w" && i+1 < argc) {
            keyword_weight = stod(argv[++i]);
            if (keyword_weight < 0.0 || keyword_weight > 1.0) {
                cerr << "ERROR: Weight must be between 0.0 and 1.0\n";
                return 1;
            }
        } else if (arg == "--interactive") {
            interactive = true;
        } else if (arg == "-h" || arg == "--help") {
            // Show help (already shown above)
            return 0;
        }
    }
    
    if (index_dir.empty()) {
        cerr << "ERROR: Index directory (-d) required\n";
        return 1;
    }
    
    if (!fs::exists(index_dir)) {
        cerr << "ERROR: Index directory does not exist: " << index_dir << "\n";
        return 1;
    }
    
    double semantic_weight = 1.0 - keyword_weight;
    
    // Load semantic index
    SemanticIndex semantic_idx;
    string semantic_path = index_dir + "/semantic.idx";
    
    if (!fs::exists(semantic_path)) {
        cerr << "ERROR: Semantic index not found: " << semantic_path << "\n";
        cerr << "Please run semantic_indexer first:\n";
        cerr << "  semantic_indexer -i embeddings.csv -o " << index_dir << "\n";
        return 1;
    }
    
    cerr << "\n=== Loading Semantic Index ===\n";
    if (!semantic_idx.load(semantic_path)) {
        cerr << "ERROR: Failed to load semantic index\n";
        return 1;
    }
    
    cerr << "\n=== Hybrid Search Ready ===\n";
    cerr << "Keyword weight: " << fixed << setprecision(1) 
         << (keyword_weight * 100) << "%\n";
    cerr << "Semantic weight: " << (semantic_weight * 100) << "%\n";
    cerr << "Index directory: " << index_dir << "\n\n";
    
    auto process_query = [&](const string& q) {
        cout << "\n" << string(70, '=') << "\n";
        cout << "Query: \"" << q << "\"\n";
        cout << string(70, '=') << "\n";
        
        auto start_total = chrono::high_resolution_clock::now();
        
        // Step 1: BM25 search
        cerr << "\n[1/3] Running BM25 keyword search...\n";
        auto start_bm25 = chrono::high_resolution_clock::now();
        auto bm25_results = call_bm25_searcher(q, top_k_each, index_dir);
        auto end_bm25 = chrono::high_resolution_clock::now();
        auto dur_bm25 = chrono::duration_cast<chrono::milliseconds>(end_bm25 - start_bm25);
        
        if (bm25_results.empty()) {
            cerr << "  WARNING: No BM25 results\n";
        }
        
        // Step 2: Semantic search
        cerr << "\n[2/3] Running semantic search...\n";
        auto query_emb = generate_query_embedding(q, semantic_idx.get_dim());
        
        auto start_sem = chrono::high_resolution_clock::now();
        auto semantic_results = semantic_idx.search(query_emb, top_k_each);
        auto end_sem = chrono::high_resolution_clock::now();
        auto dur_sem = chrono::duration_cast<chrono::milliseconds>(end_sem - start_sem);
        
        cerr << "  Found " << semantic_results.size() << " semantic results\n";
        
        // Step 3: Hybrid ranking
        cerr << "\n[3/3] Computing hybrid rankings...\n";
        auto start_hybrid = chrono::high_resolution_clock::now();
        auto hybrid_results = hybrid_rank(bm25_results, semantic_results, 
                                         keyword_weight, semantic_weight);
        auto end_hybrid = chrono::high_resolution_clock::now();
        auto dur_hybrid = chrono::duration_cast<chrono::milliseconds>(end_hybrid - start_hybrid);
        
        auto end_total = chrono::high_resolution_clock::now();
        auto dur_total = chrono::duration_cast<chrono::milliseconds>(end_total - start_total);
        
        // Display results
        cout << "\n" << string(70, '-') << "\n";
        cout << "PERFORMANCE\n";
        cout << string(70, '-') << "\n";
        cout << "BM25 search:     " << setw(6) << dur_bm25.count() << " ms\n";
        cout << "Semantic search: " << setw(6) << dur_sem.count() << " ms\n";
        cout << "Hybrid ranking:  " << setw(6) << dur_hybrid.count() << " ms\n";
        cout << "Total time:      " << setw(6) << dur_total.count() << " ms\n";
        
        cout << "\n" << string(70, '-') << "\n";
        cout << "TOP " << min(top_final, (int)hybrid_results.size()) << " RESULTS\n";
        cout << string(70, '-') << "\n\n";
        
        for (int i = 0; i < min(top_final, (int)hybrid_results.size()); i++) {
            const auto& hr = hybrid_results[i];
            
            cout << setw(2) << (i+1) << ". " << hr.docid << "\n";
            cout << "    Final Score: " << fixed << setprecision(4) << hr.final_score << "\n";
            cout << "    BM25:        " << setprecision(4) << hr.bm25_score;
            if (hr.keyword_rank > 0) {
                cout << " (ranked #" << hr.keyword_rank << " in keyword search)";
            } else {
                cout << " (not in keyword results)";
            }
            cout << "\n";
            cout << "    Semantic:    " << setprecision(4) << hr.semantic_score;
            if (hr.semantic_rank > 0) {
                cout << " (ranked #" << hr.semantic_rank << " in semantic search)";
            } else {
                cout << " (not in semantic results)";
            }
            cout << "\n\n";
        }
        
        cout << string(70, '=') << "\n";
    };
    
    if (interactive) {
        cout << "\n=== HYBRID SEARCH - INTERACTIVE MODE ===\n";
        cout << "Configuration:\n";
        cout << "  Keyword weight:  " << (keyword_weight*100) << "%\n";
        cout << "  Semantic weight: " << (semantic_weight*100) << "%\n";
        cout << "\nEnter query (or 'quit' to exit)\n";
        
        string line;
        while (true) {
            cout << "\nQuery> ";
            cout.flush();
            
            if (!getline(cin, line)) break;
            
            if (line == "quit" || line == "exit" || line == "q") break;
            if (line.empty()) continue;
            
            try {
                process_query(line);
            } catch (const exception& e) {
                cerr << "ERROR: " << e.what() << "\n";
            }
        }
        
        cout << "\nGoodbye!\n";
        
    } else {
        if (query.empty()) {
            cerr << "ERROR: Query required (-q) for non-interactive mode\n";
            return 1;
        }
        
        try {
            process_query(query);
        } catch (const exception& e) {
            cerr << "ERROR: " << e.what() << "\n";
            return 1;
        }
    }
    
    return 0;
}
