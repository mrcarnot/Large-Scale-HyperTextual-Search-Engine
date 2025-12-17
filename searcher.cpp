// searcher.cpp
// Compile: g++ -std=c++17 -O2 searcher.cpp -o searcher
// 
// Improved query engine with BM25 ranking
// Key improvements:
// - Better forward index parsing
// - Optimized phrase query processing
// - Enhanced error handling
// - Support for conjunctive queries (AND semantics)

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
unordered_map<string, uint32_t> docid_to_int; // Reverse mapping
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
            getline(iss, skip_meta);
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
            docid_to_int[orig] = internal;
            total_docs++;
        }
    }
    cerr << "Loaded " << total_docs << " documents from docid_map\n";
}

// Improved JSON parsing for forward index
static string extract_json_string(const string& json, const string& key) {
    string search_key = "\"" + key + "\":\"";
    size_t start = json.find(search_key);
    if (start == string::npos) return "";
    start += search_key.length();
    size_t end = json.find("\"", start);
    if (end == string::npos) return "";
    return json.substr(start, end - start);
}

static void load_forward_index(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "Warning: Cannot open forward_index: " << path << "\n";
        return;
    }
    
    uint64_t total_length = 0;
    string line;
    int line_count = 0;
    
    while (getline(ifs, line)) {
        line_count++;
        
        // Extract docid
        string orig_docid = extract_json_string(line, "docid");
        if (orig_docid.empty()) continue;
        
        // Count term frequencies for document length
        uint32_t doc_len = 0;
        size_t pos = 0;
        while ((pos = line.find("\"freq\":", pos)) != string::npos) {
            pos += 7;
            size_t num_end = line.find_first_of(",}", pos);
            if (num_end == string::npos) break;
            try {
                uint32_t freq = stoul(line.substr(pos, num_end - pos));
                doc_len += freq;
            } catch (...) {
                break;
            }
            pos = num_end;
        }
        
        // Find internal docid
        if (docid_to_int.find(orig_docid) != docid_to_int.end()) {
            uint32_t internal_id = docid_to_int[orig_docid];
            
            DocMetadata meta;
            meta.orig_docid = orig_docid;
            meta.doc_length = (doc_len > 0) ? doc_len : 100; // Fallback
            meta.title = extract_json_string(line, "title");
            meta.authors = extract_json_string(line, "authors");
            meta.pub_date = extract_json_string(line, "pub_date");
            
            doc_metadata[internal_id] = meta;
            total_length += meta.doc_length;
        }
    }
    
    if (total_docs > 0) {
        avg_doc_length = (double)total_length / total_docs;
    }
    
    cerr << "Loaded forward index (" << line_count << " lines). Avg doc length: " 
         << fixed << setprecision(2) << avg_doc_length << "\n";
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
    if (total_docs == 0 || df == 0) return 0.0;
    
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
    string title;
};

static string normalize_term(string term) {
    transform(term.begin(), term.end(), term.begin(), 
              [](unsigned char c){ return tolower(c); });
    return term;
}

// Regular OR query (disjunctive)
static vector<SearchResult> search_query(const vector<string>& query_terms, size_t top_k = 10) {
    vector<string> normalized_terms;
    vector<LexiconEntry> entries;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        if (lexicon.find(norm) != lexicon.end()) {
            normalized_terms.push_back(norm);
            entries.push_back(lexicon[norm]);
        } else {
            cerr << "  [Warning] Term not found: '" << term << "'\n";
        }
    }
    
    if (entries.empty()) {
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
                if (doc_metadata.find(docid) != doc_metadata.end()) {
                    sr.title = doc_metadata[docid].title;
                }
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

// Conjunctive query (AND - all terms must appear)
static vector<SearchResult> search_query_and(const vector<string>& query_terms, size_t top_k = 10) {
    vector<string> normalized_terms;
    vector<LexiconEntry> entries;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        if (lexicon.find(norm) != lexicon.end()) {
            normalized_terms.push_back(norm);
            entries.push_back(lexicon[norm]);
        }
    }
    
    if (entries.empty() || entries.size() != query_terms.size()) {
        return {};
    }
    
    // Get postings and create document sets
    vector<vector<PostingEntry>> all_postings;
    vector<unordered_set<uint32_t>> doc_sets;
    
    for (const auto& entry : entries) {
        auto postings = decode_postings_list(entry);
        all_postings.push_back(postings);
        
        unordered_set<uint32_t> doc_set;
        for (const auto& p : postings) {
            doc_set.insert(p.docid);
        }
        doc_sets.push_back(doc_set);
    }
    
    // Find intersection of all document sets
    unordered_set<uint32_t> result_docs = doc_sets[0];
    for (size_t i = 1; i < doc_sets.size(); i++) {
        unordered_set<uint32_t> intersection;
        for (uint32_t docid : result_docs) {
            if (doc_sets[i].find(docid) != doc_sets[i].end()) {
                intersection.insert(docid);
            }
        }
        result_docs = intersection;
    }
    
    // Score documents in intersection
    unordered_map<uint32_t, SearchResult> scores;
    
    for (uint32_t docid : result_docs) {
        SearchResult sr;
        sr.docid = docid;
        sr.orig_docid = int_to_docid[docid];
        sr.score = 0.0;
        if (doc_metadata.find(docid) != doc_metadata.end()) {
            sr.title = doc_metadata[docid].title;
        }
        
        uint32_t doc_len = 1000;
        if (doc_metadata.find(docid) != doc_metadata.end()) {
            doc_len = doc_metadata[docid].doc_length;
        }
        
        for (size_t i = 0; i < normalized_terms.size(); i++) {
            const string& term = normalized_terms[i];
            const LexiconEntry& entry = entries[i];
            const auto& postings = all_postings[i];
            
            for (const auto& posting : postings) {
                if (posting.docid == docid) {
                    double bm25_score = compute_bm25(posting.term_freq, doc_len, entry.doc_freq);
                    sr.score += bm25_score;
                    sr.term_freqs[term] = posting.term_freq;
                    break;
                }
            }
        }
        
        scores[docid] = sr;
    }
    
    // Convert and sort
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
    
    // Normalize all terms and check existence
    vector<string> normalized;
    for (const auto& term : phrase_terms) {
        string norm = normalize_term(term);
        if (lexicon.find(norm) == lexicon.end()) {
            cerr << "  [Warning] Phrase term not found: '" << term << "'\n";
            return {};
        }
        normalized.push_back(norm);
    }
    
    // Get postings for all terms
    vector<vector<PostingEntry>> all_postings;
    for (const auto& term : normalized) {
        all_postings.push_back(decode_postings_list(lexicon[term]));
    }
    
    // Create document sets for intersection
    unordered_set<uint32_t> candidate_docs;
    for (const auto& posting : all_postings[0]) {
        candidate_docs.insert(posting.docid);
    }
    
    // Keep only docs containing all terms
    for (size_t i = 1; i < all_postings.size(); i++) {
        unordered_set<uint32_t> term_docs;
        for (const auto& posting : all_postings[i]) {
            term_docs.insert(posting.docid);
        }
        
        unordered_set<uint32_t> intersection;
        for (uint32_t docid : candidate_docs) {
            if (term_docs.find(docid) != term_docs.end()) {
                intersection.insert(docid);
            }
        }
        candidate_docs = intersection;
    }
    
    // Check phrase match in candidate documents
    vector<SearchResult> results;
    
    for (uint32_t docid : candidate_docs) {
        // Get positions of first term in this doc
        vector<uint32_t> first_positions;
        for (const auto& posting : all_postings[0]) {
            if (posting.docid == docid) {
                first_positions = posting.positions;
                break;
            }
        }
        
        // Check each starting position
        bool phrase_found = false;
        for (uint32_t start_pos : first_positions) {
            bool match = true;
            
            // Verify consecutive positions for remaining terms
            for (size_t i = 1; i < normalized.size(); i++) {
                uint32_t expected_pos = start_pos + i;
                bool found = false;
                
                for (const auto& posting : all_postings[i]) {
                    if (posting.docid == docid) {
                        if (find(posting.positions.begin(), posting.positions.end(), 
                                expected_pos) != posting.positions.end()) {
                            found = true;
                            break;
                        }
                    }
                }
                
                if (!found) {
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
            if (doc_metadata.find(docid) != doc_metadata.end()) {
                sr.title = doc_metadata[docid].title;
            }
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
        cerr << "Usage: " << argv[0] << " -d index_dir [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -d DIR      : directory containing index files (required)\n";
        cerr << "  -q \"QUERY\"  : query terms (OR semantics)\n";
        cerr << "  -a \"QUERY\"  : query terms (AND semantics - all terms required)\n";
        cerr << "  -p \"PHRASE\" : phrase query (exact match)\n";
        cerr << "  -k N        : number of results (default 10)\n";
        cerr << "\nInteractive mode: run without -q, -a, or -p\n";
        cerr << "\nExamples:\n";
        cerr << "  " << argv[0] << " -d ./index\n";
        cerr << "  " << argv[0] << " -d ./index -q \"machine learning\"\n";
        cerr << "  " << argv[0] << " -d ./index -a \"neural network\"\n";
        cerr << "  " << argv[0] << " -d ./index -p \"deep learning\" -k 5\n";
        return 1;
    }
    
    string index_dir;
    string query_str;
    string and_query_str;
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
        } else if (arg == "-a" && i+1 < argc) {
            and_query_str = argv[++i];
            interactive = false;
        } else if (arg == "-p" && i+1 < argc) {
            phrase_str = argv[++i];
            interactive = false;
        } else if (arg == "-k" && i+1 < argc) {
            top_k = stoul(argv[++i]);
        }
    }
    
    if (index_dir.empty()) {
        cerr << "Error: Index directory (-d) required\n";
        return 1;
    }
    
    // Load index files
    cerr << "\n=== Loading Index ===\n";
    load_lexicon(index_dir + "/lexicon.txt");
    load_postings(index_dir + "/postings.bin");
    load_docid_map(index_dir + "/docid_map.txt");
    load_forward_index(index_dir + "/forward_index.jsonl");
    
    cerr << "\n=== Search Engine Ready ===\n";
    cerr << "Total terms: " << lexicon.size() << "\n";
    cerr << "Total docs: " << total_docs << "\n";
    cerr << "Avg doc length: " << fixed << setprecision(2) << avg_doc_length << "\n\n";
    
    if (interactive) {
        // Interactive mode
        cout << "Enter queries (or 'quit' to exit):\n";
        cout << "  Regular search (OR):  machine learning\n";
        cout << "  AND search:           +neural network\n";
        cout << "  Phrase search:        \"deep learning\"\n\n";
        
        string line;
        while (true) {
            cout << "Query> ";
            if (!getline(cin, line)) break;
            
            if (line == "quit" || line == "exit") break;
            if (line.empty()) continue;
            
            // Parse query type
            bool is_phrase = false;
            bool is_and = false;
            
            if (line.size() >= 2 && line.front() == '"' && line.back() == '"') {
                is_phrase = true;
                line = line.substr(1, line.size() - 2);
            } else if (!line.empty() && line[0] == '+') {
                is_and = true;
                line = line.substr(1);
            }
            
            // Parse terms
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
                cerr << "  [Phrase query: " << line << "]\n";
                results = search_phrase(terms, top_k);
            } else if (is_and) {
                cerr << "  [AND query: " << line << "]\n";
                results = search_query_and(terms, top_k);
            } else {
                cerr << "  [OR query: " << line << "]\n";
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
                if (!results[i].title.empty()) {
                    cout << "   Title: " << results[i].title << "\n";
                }
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
            cerr << "Executing phrase query...\n";
            results = search_phrase(terms, top_k);
        } else if (!and_query_str.empty()) {
            vector<string> terms;
            istringstream iss(and_query_str);
            string term;
            while (iss >> term) terms.push_back(term);
            cerr << "Executing AND query...\n";
            results = search_query_and(terms, top_k);
        } else {
            vector<string> terms;
            istringstream iss(query_str);
            string term;
            while (iss >> term) terms.push_back(term);
            cerr << "Executing OR query...\n";
            results = search_query(terms, top_k);
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cout << "\nResults (" << duration.count() << " ms):\n\n";
        for (size_t i = 0; i < results.size(); i++) {
            cout << (i+1) << ". [" << fixed << setprecision(2) 
                 << results[i].score << "] " << results[i].orig_docid;
            if (!results[i].title.empty()) {
                cout << " - " << results[i].title;
            }
            cout << "\n";
        }
    }
    
    return 0;
}
