// searcher.cpp
// Compile: g++ -std=c++17 -O2 searcher.cpp -o searcher
// 
// Query engine with BM25 ranking for the search system
// Reads: lexicon.txt, postings.bin, forward_index.jsonl, docid_map.txt
// Supports: single-word, multi-word, phrase queries

#include <bits/stdc++.h>
#include <filesystem>

namespace fs = std::filesystem;
using namespace std;

// -------------------- VByte Decoding --------------------
static uint32_t vbyte_decode_uint32(const uint8_t* data, size_t& offset) {
    uint32_t result = 0;
    int shift = 0;
    while (true) {
        uint8_t byte = data[offset++];
        if (byte & 0x80) {
            result |= ((byte & 0x7F) << shift);
            break;
        }
        result |= (byte << shift);
        shift += 7;
    }
    return result;
}

// -------------------- Data Structures --------------------
struct LexiconEntry {
    uint32_t wordID;
    string term;
    uint32_t doc_freq;
    uint64_t term_freq;
    uint64_t offset;
    uint64_t bytes;
};

struct PostingEntry {
    uint32_t docid;
    uint32_t term_freq;
    vector<uint32_t> positions;
};

struct DocMetadata {
    string orig_docid;
    uint32_t doc_length;
    string title;
    string authors;
    string pub_date;
};

// -------------------- Global State --------------------
unordered_map<string, LexiconEntry> lexicon;
unordered_map<uint32_t, string> int_to_docid;
unordered_map<uint32_t, DocMetadata> doc_metadata;
vector<uint8_t> postings_data;
uint32_t total_docs = 0;
double avg_doc_length = 0.0;

// BM25 parameters
const double k1 = 1.2;
const double b = 0.75;

// -------------------- Load Index --------------------
static void load_lexicon(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "Cannot open lexicon: " << path << "\n";
        exit(1);
    }
    string line;
    while (getline(ifs, line)) {
        istringstream iss(line);
        LexiconEntry entry;
        string skip_meta;
        if (iss >> entry.wordID >> entry.term >> entry.doc_freq >> 
                   entry.term_freq >> entry.offset >> entry.bytes) {
            getline(iss, skip_meta); // read rest as skip_meta
            lexicon[entry.term] = entry;
        }
    }
    cerr << "Loaded " << lexicon.size() << " terms from lexicon\n";
}

static void load_postings(const string& path) {
    ifstream ifs(path, ios::binary);
    if (!ifs) {
        cerr << "Cannot open postings: " << path << "\n";
        exit(1);
    }
    ifs.seekg(0, ios::end);
    size_t size = ifs.tellg();
    ifs.seekg(0, ios::beg);
    postings_data.resize(size);
    ifs.read(reinterpret_cast<char*>(postings_data.data()), size);
    cerr << "Loaded postings.bin (" << size << " bytes)\n";
}

static void load_docid_map(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "Cannot open docid_map: " << path << "\n";
        exit(1);
    }
    string line;
    while (getline(ifs, line)) {
        istringstream iss(line);
        string orig;
        uint32_t internal;
        if (iss >> orig >> internal) {
            int_to_docid[internal] = orig;
            total_docs++;
        }
    }
    cerr << "Loaded " << total_docs << " documents from docid_map\n";
}

static void load_forward_index(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "Warning: Cannot open forward_index: " << path << "\n";
        return;
    }
    
    uint64_t total_length = 0;
    string line;
    while (getline(ifs, line)) {
        // Simple JSON parsing for: {"docid":"...","postings":[...]}
        size_t docid_start = line.find("\"docid\":\"") + 9;
        size_t docid_end = line.find("\"", docid_start);
        string orig_docid = line.substr(docid_start, docid_end - docid_start);
        
        // Count postings for doc length
        uint32_t doc_len = 0;
        size_t pos = line.find("\"freq\":");
        while (pos != string::npos) {
            size_t num_start = pos + 7;
            size_t num_end = line.find_first_of(",}", num_start);
            uint32_t freq = stoul(line.substr(num_start, num_end - num_start));
            doc_len += freq;
            pos = line.find("\"freq\":", num_end);
        }
        
        // Find internal docid
        uint32_t internal_id = 0;
        for (const auto& p : int_to_docid) {
            if (p.second == orig_docid) {
                internal_id = p.first;
                break;
            }
        }
        
        if (internal_id > 0) {
            DocMetadata meta;
            meta.orig_docid = orig_docid;
            meta.doc_length = doc_len;
            doc_metadata[internal_id] = meta;
            total_length += doc_len;
        }
    }
    
    if (total_docs > 0) {
        avg_doc_length = (double)total_length / total_docs;
    }
    cerr << "Loaded forward index. Avg doc length: " << avg_doc_length << "\n";
}

// -------------------- Decode Postings --------------------
static vector<PostingEntry> decode_postings_list(const LexiconEntry& entry) {
    vector<PostingEntry> result;
    if (entry.offset + entry.bytes > postings_data.size()) {
        cerr << "Invalid postings offset for term: " << entry.term << "\n";
        return result;
    }
    
    size_t offset = entry.offset;
    uint32_t doc_count = vbyte_decode_uint32(postings_data.data(), offset);
    
    uint32_t last_docid = 0;
    for (uint32_t i = 0; i < doc_count; i++) {
        PostingEntry pe;
        uint32_t doc_delta = vbyte_decode_uint32(postings_data.data(), offset);
        pe.docid = last_docid + doc_delta;
        last_docid = pe.docid;
        
        pe.term_freq = vbyte_decode_uint32(postings_data.data(), offset);
        
        uint32_t last_pos = 0;
        for (uint32_t j = 0; j < pe.term_freq; j++) {
            uint32_t pos_delta = vbyte_decode_uint32(postings_data.data(), offset);
            uint32_t pos = last_pos + pos_delta;
            pe.positions.push_back(pos);
            last_pos = pos;
        }
        
        result.push_back(pe);
    }
    
    return result;
}

// -------------------- BM25 Scoring --------------------
static double compute_bm25(uint32_t tf, uint32_t doc_len, uint32_t df) {
    double idf = log((total_docs - df + 0.5) / (df + 0.5) + 1.0);
    double norm = doc_len / avg_doc_length;
    double score = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * norm));
    return score;
}

// -------------------- Query Processing --------------------
struct SearchResult {
    uint32_t docid;
    string orig_docid;
    double score;
    map<string, uint32_t> term_freqs;
};

static string normalize_term(string term) {
    transform(term.begin(), term.end(), term.begin(), 
              [](unsigned char c){ return tolower(c); });
    return term;
}

static vector<SearchResult> search_query(const vector<string>& query_terms, size_t top_k = 10) {
    // Normalize query terms
    vector<string> normalized_terms;
    vector<LexiconEntry> entries;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        if (lexicon.find(norm) != lexicon.end()) {
            normalized_terms.push_back(norm);
            entries.push_back(lexicon[norm]);
        } else {
            cerr << "Term not found in lexicon: " << term << "\n";
        }
    }
    
    if (entries.empty()) {
        cerr << "No valid query terms found\n";
        return {};
    }
    
    // Get postings for each term
    vector<vector<PostingEntry>> all_postings;
    for (const auto& entry : entries) {
        all_postings.push_back(decode_postings_list(entry));
    }
    
    // Accumulate scores
    unordered_map<uint32_t, SearchResult> scores;
    
    for (size_t i = 0; i < normalized_terms.size(); i++) {
        const string& term = normalized_terms[i];
        const LexiconEntry& entry = entries[i];
        const auto& postings = all_postings[i];
        
        for (const auto& posting : postings) {
            uint32_t docid = posting.docid;
            uint32_t tf = posting.term_freq;
            uint32_t df = entry.doc_freq;
            
            // Get document length
            uint32_t doc_len = 1000; // default
            if (doc_metadata.find(docid) != doc_metadata.end()) {
                doc_len = doc_metadata[docid].doc_length;
            }
            
            double bm25_score = compute_bm25(tf, doc_len, df);
            
            if (scores.find(docid) == scores.end()) {
                SearchResult sr;
                sr.docid = docid;
                sr.orig_docid = int_to_docid[docid];
                sr.score = 0.0;
                scores[docid] = sr;
            }
            
            scores[docid].score += bm25_score;
            scores[docid].term_freqs[term] = tf;
        }
    }
    
    // Convert to vector and sort
    vector<SearchResult> results;
    for (auto& p : scores) {
        results.push_back(p.second);
    }
    
    sort(results.begin(), results.end(), 
         [](const SearchResult& a, const SearchResult& b) {
             return a.score > b.score;
         });
    
    if (results.size() > top_k) {
        results.resize(top_k);
    }
    
    return results;
}

// -------------------- Phrase Query --------------------
static vector<SearchResult> search_phrase(const vector<string>& phrase_terms, size_t top_k = 10) {
    if (phrase_terms.empty()) return {};
    
    // Get postings for first term
    string first_term = normalize_term(phrase_terms[0]);
    if (lexicon.find(first_term) == lexicon.end()) {
        return {};
    }
    
    auto first_postings = decode_postings_list(lexicon[first_term]);
    vector<SearchResult> results;
    
    // For each document containing first term
    for (const auto& first_post : first_postings) {
        uint32_t docid = first_post.docid;
        bool phrase_found = false;
        
        // Check each position of first term
        for (uint32_t start_pos : first_post.positions) {
            bool match = true;
            
            // Check if subsequent terms appear at consecutive positions
            for (size_t i = 1; i < phrase_terms.size(); i++) {
                string term = normalize_term(phrase_terms[i]);
                if (lexicon.find(term) == lexicon.end()) {
                    match = false;
                    break;
                }
                
                auto postings = decode_postings_list(lexicon[term]);
                bool found_at_pos = false;
                
                for (const auto& post : postings) {
                    if (post.docid == docid) {
                        uint32_t expected_pos = start_pos + i;
                        if (find(post.positions.begin(), post.positions.end(), 
                                expected_pos) != post.positions.end()) {
                            found_at_pos = true;
                            break;
                        }
                    }
                }
                
                if (!found_at_pos) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                phrase_found = true;
                break;
            }
        }
        
        if (phrase_found) {
            SearchResult sr;
            sr.docid = docid;
            sr.orig_docid = int_to_docid[docid];
            sr.score = 100.0; // High score for exact phrase match
            results.push_back(sr);
        }
    }
    
    if (results.size() > top_k) {
        results.resize(top_k);
    }
    
    return results;
}

// -------------------- Main --------------------
int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " -d index_dir [-q \"query terms\"] [-p \"phrase query\"] [-k top_k]\n";
        cerr << "  -d: directory containing index files\n";
        cerr << "  -q: query terms (space-separated)\n";
        cerr << "  -p: phrase query (exact match)\n";
        cerr << "  -k: number of results (default 10)\n";
        cerr << "\nInteractive mode: run without -q or -p\n";
        return 1;
    }
    
    string index_dir;
    string query_str;
    string phrase_str;
    size_t top_k = 10;
    bool interactive = true;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            index_dir = argv[++i];
        } else if (arg == "-q" && i+1 < argc) {
            query_str = argv[++i];
            interactive = false;
        } else if (arg == "-p" && i+1 < argc) {
            phrase_str = argv[++i];
            interactive = false;
        } else if (arg == "-k" && i+1 < argc) {
            top_k = stoul(argv[++i]);
        }
    }
    
    if (index_dir.empty()) {
        cerr << "Index directory (-d) required\n";
        return 1;
    }
    
    // Load index files
    load_lexicon(index_dir + "/lexicon.txt");
    load_postings(index_dir + "/postings.bin");
    load_docid_map(index_dir + "/docid_map.txt");
    load_forward_index(index_dir + "/forward_index.jsonl");
    
    cerr << "\n=== Search Engine Ready ===\n";
    cerr << "Total terms: " << lexicon.size() << "\n";
    cerr << "Total docs: " << total_docs << "\n";
    cerr << "Avg doc length: " << avg_doc_length << "\n\n";
    
    if (interactive) {
        // Interactive mode
        cout << "Enter queries (or 'quit' to exit):\n";
        cout << "  - Regular search: machine learning\n";
        cout << "  - Phrase search: \"deep learning\"\n\n";
        
        string line;
        while (true) {
            cout << "Query> ";
            if (!getline(cin, line)) break;
            
            if (line == "quit" || line == "exit") break;
            if (line.empty()) continue;
            
            // Check if phrase query (surrounded by quotes)
            bool is_phrase = false;
            if (line.size() >= 2 && line.front() == '"' && line.back() == '"') {
                is_phrase = true;
                line = line.substr(1, line.size() - 2);
            }
            
            // Parse query terms
            vector<string> terms;
            istringstream iss(line);
            string term;
            while (iss >> term) {
                terms.push_back(term);
            }
            
            if (terms.empty()) continue;
            
            // Execute search
            auto start = chrono::high_resolution_clock::now();
            vector<SearchResult> results;
            
            if (is_phrase) {
                results = search_phrase(terms, top_k);
            } else {
                results = search_query(terms, top_k);
            }
            
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
            
            // Display results
            cout << "\nFound " << results.size() << " results in " 
                 << duration.count() << " ms\n\n";
            
            for (size_t i = 0; i < results.size(); i++) {
                cout << (i+1) << ". [Score: " << fixed << setprecision(2) 
                     << results[i].score << "] " << results[i].orig_docid << "\n";
                cout << "   Terms: ";
                for (const auto& tf : results[i].term_freqs) {
                    cout << tf.first << "(" << tf.second << ") ";
                }
                cout << "\n\n";
            }
        }
    } else {
        // Command-line mode
        vector<SearchResult> results;
        auto start = chrono::high_resolution_clock::now();
        
        if (!phrase_str.empty()) {
            vector<string> terms;
            istringstream iss(phrase_str);
            string term;
            while (iss >> term) terms.push_back(term);
            results = search_phrase(terms, top_k);
        } else {
            vector<string> terms;
            istringstream iss(query_str);
            string term;
            while (iss >> term) terms.push_back(term);
            results = search_query(terms, top_k);
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cout << "Results (" << duration.count() << " ms):\n\n";
        for (size_t i = 0; i < results.size(); i++) {
            cout << (i+1) << ". [" << fixed << setprecision(2) 
                 << results[i].score << "] " << results[i].orig_docid << "\n";
        }
    }
    
    return 0;
}