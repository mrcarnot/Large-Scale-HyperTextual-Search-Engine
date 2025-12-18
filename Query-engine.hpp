// query_engine.hpp
// Complete search engine with BM25 ranking, barrel support, and multi-word queries

#ifndef QUERY_ENGINE_HPP
#define QUERY_ENGINE_HPP

#include <bits/stdc++.h>
#include <cmath>
using namespace std;

// Forward declarations
class BarrelManager;
class AutocompleteEngine;
class SemanticExpander;

// Search result structure
struct SearchResult {
    string docid;
    double score;
    string title;
    string snippet;
    unordered_map<string, uint32_t> term_frequencies;
    
    bool operator<(const SearchResult& other) const {
        return score > other.score; // Descending order
    }
};

// Lexicon entry
struct LexiconEntry {
    uint32_t wordID;
    string term;
    uint32_t doc_freq;
    uint64_t term_freq;
    uint64_t offset;
    uint64_t bytes;
    uint32_t barrel_id;
};

// VByte decoder
class VByteDecoder {
public:
    static uint32_t decode_uint32(const uint8_t* data, size_t& pos) {
        uint32_t result = 0;
        uint32_t shift = 0;
        
        while (true) {
            uint8_t byte = data[pos++];
            result |= ((byte & 0x7F) << shift);
            
            if (byte & 0x80) break;
            shift += 7;
        }
        
        return result;
    }
    
    static vector<uint32_t> decode_posting_list(const uint8_t* data, size_t offset, size_t bytes) {
        vector<uint32_t> docids;
        size_t pos = 0;
        
        if (bytes == 0) return docids;
        
        uint32_t num_docs = decode_uint32(data + offset, pos);
        uint32_t last_docid = 0;
        
        for (uint32_t i = 0; i < num_docs; i++) {
            uint32_t delta = decode_uint32(data + offset, pos);
            last_docid += delta;
            docids.push_back(last_docid);
            
            // Skip positions for now (just read frequency)
            uint32_t freq = decode_uint32(data + offset, pos);
            for (uint32_t j = 0; j < freq; j++) {
                decode_uint32(data + offset, pos); // Skip position
            }
        }
        
        return docids;
    }
};

class QueryEngine {
private:
    string index_dir;
    
    // Index structures
    unordered_map<string, LexiconEntry> lexicon; // term -> entry
    unordered_map<uint32_t, string> docid_map;   // internal_id -> original_id
    unordered_map<string, uint32_t> reverse_docid_map; // original_id -> internal_id
    
    // Document lengths for BM25
    unordered_map<uint32_t, uint32_t> doc_lengths;
    double avg_doc_length = 0.0;
    uint32_t total_docs = 0;
    
    // BM25 parameters
    const double k1 = 1.5;
    const double b = 0.75;
    
    // Barrel data cache
    unordered_map<uint32_t, vector<uint8_t>> barrel_cache;
    
    // Helper: Load barrel
    bool load_barrel(uint32_t barrel_id) {
        if (barrel_cache.find(barrel_id) != barrel_cache.end()) {
            return true; // Already loaded
        }
        
        string path = index_dir + "/barrel_" + to_string(barrel_id) + ".bin";
        ifstream ifs(path, ios::binary);
        if (!ifs) return false;
        
        ifs.seekg(0, ios::end);
        size_t size = ifs.tellg();
        ifs.seekg(0, ios::beg);
        
        vector<uint8_t> data(size);
        ifs.read(reinterpret_cast<char*>(data.data()), size);
        
        barrel_cache[barrel_id] = move(data);
        return true;
    }
    
    // Helper: Get posting list for a term
    vector<uint32_t> get_posting_list(const string& term) {
        auto it = lexicon.find(term);
        if (it == lexicon.end()) return {};
        
        LexiconEntry& entry = it->second;
        
        // Load barrel if needed
        if (!load_barrel(entry.barrel_id)) {
            cerr << "[QueryEngine] Failed to load barrel " << entry.barrel_id << "\n";
            return {};
        }
        
        const auto& barrel_data = barrel_cache[entry.barrel_id];
        return VByteDecoder::decode_posting_list(
            barrel_data.data(), entry.offset, entry.bytes
        );
    }
    
    // Helper: Calculate BM25 score
    double calculate_bm25(uint32_t doc_freq, uint32_t term_freq, 
                          uint32_t doc_length, uint32_t total_docs) {
        // IDF component
        double idf = log((total_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0);
        
        // TF component
        double tf = ((k1 + 1.0) * term_freq) / 
                    (k1 * (1.0 - b + b * (doc_length / avg_doc_length)) + term_freq);
        
        return idf * tf;
    }
    
    // Helper: Tokenize query
    vector<string> tokenize_query(const string& query) {
        vector<string> tokens;
        string current;
        
        for (char c : query) {
            if (isalnum(c) || c == '\'') {
                current += tolower(c);
            } else if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
        }
        
        if (!current.empty()) {
            tokens.push_back(current);
        }
        
        return tokens;
    }

public:
    QueryEngine(const string& dir) : index_dir(dir) {}
    
    // Load index structures
    bool load_index() {
        auto start = chrono::high_resolution_clock::now();
        
        // Load lexicon
        string lexicon_path = index_dir + "/lexicon.txt";
        ifstream lex_ifs(lexicon_path);
        if (!lex_ifs) {
            cerr << "[QueryEngine] Cannot open lexicon\n";
            return false;
        }
        
        string line;
        while (getline(lex_ifs, line)) {
            istringstream iss(line);
            LexiconEntry entry;
            string skip_meta;
            
            if (iss >> entry.wordID >> entry.term >> entry.doc_freq >> 
                      entry.term_freq >> entry.offset >> entry.bytes >> 
                      entry.barrel_id) {
                getline(iss, skip_meta); // Skip metadata
                lexicon[entry.term] = entry;
            }
        }
        
        cerr << "[QueryEngine] Loaded " << lexicon.size() << " terms\n";
        
        // Load docid mapping
        string docid_path = index_dir + "/docid_map.txt";
        ifstream doc_ifs(docid_path);
        if (!doc_ifs) {
            cerr << "[QueryEngine] Cannot open docid map\n";
            return false;
        }
        
        while (getline(doc_ifs, line)) {
            istringstream iss(line);
            string orig_id;
            uint32_t internal_id;
            
            if (iss >> orig_id >> internal_id) {
                docid_map[internal_id] = orig_id;
                reverse_docid_map[orig_id] = internal_id;
                total_docs++;
            }
        }
        
        cerr << "[QueryEngine] Loaded " << total_docs << " documents\n";
        
        // Load document lengths from forward index
        string fwd_path = index_dir + "/forward_index.jsonl";
        ifstream fwd_ifs(fwd_path);
        if (fwd_ifs) {
            uint64_t total_length = 0;
            
            while (getline(fwd_ifs, line)) {
                // Simple JSON parsing
                size_t docid_pos = line.find("\"docid\":\"");
                if (docid_pos == string::npos) continue;
                
                docid_pos += 9;
                size_t docid_end = line.find("\"", docid_pos);
                string docid = line.substr(docid_pos, docid_end - docid_pos);
                
                // Count term frequencies
                uint32_t doc_len = 0;
                size_t pos = 0;
                while ((pos = line.find("\"freq\":", pos)) != string::npos) {
                    pos += 7;
                    size_t end = line.find_first_of(",}", pos);
                    doc_len += stoul(line.substr(pos, end - pos));
                    pos = end;
                }
                
                auto it = reverse_docid_map.find(docid);
                if (it != reverse_docid_map.end()) {
                    doc_lengths[it->second] = doc_len;
                    total_length += doc_len;
                }
            }
            
            avg_doc_length = (double)total_length / total_docs;
            cerr << "[QueryEngine] Average doc length: " << avg_doc_length << "\n";
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cerr << "[QueryEngine] Index loaded in " << duration.count() << "ms\n";
        return true;
    }
    
    // Single-word search
    vector<SearchResult> search_single(const string& query, size_t top_k = 10) {
        vector<string> tokens = tokenize_query(query);
        if (tokens.empty()) return {};
        
        string term = tokens[0];
        auto postings = get_posting_list(term);
        
        if (postings.empty()) return {};
        
        auto it = lexicon.find(term);
        uint32_t doc_freq = it->second.doc_freq;
        
        vector<SearchResult> results;
        
        for (uint32_t docid : postings) {
            SearchResult result;
            result.docid = docid_map[docid];
            result.term_frequencies[term] = 1; // Simplified
            
            uint32_t doc_len = doc_lengths.count(docid) ? doc_lengths[docid] : avg_doc_length;
            result.score = calculate_bm25(doc_freq, 1, doc_len, total_docs);
            
            results.push_back(result);
        }
        
        sort(results.begin(), results.end());
        
        if (results.size() > top_k) {
            results.resize(top_k);
        }
        
        return results;
    }
    
    // Multi-word search (AND semantics)
    vector<SearchResult> search_multi(const string& query, size_t top_k = 10) {
        vector<string> tokens = tokenize_query(query);
        if (tokens.empty()) return {};
        
        if (tokens.size() == 1) {
            return search_single(query, top_k);
        }
        
        // Get posting lists for all terms
        vector<pair<string, vector<uint32_t>>> term_postings;
        
        for (const auto& term : tokens) {
            auto postings = get_posting_list(term);
            if (!postings.empty()) {
                term_postings.emplace_back(term, move(postings));
            }
        }
        
        if (term_postings.empty()) return {};
        
        // Find intersection (documents containing all terms)
        set<uint32_t> common_docs(term_postings[0].second.begin(), 
                                   term_postings[0].second.end());
        
        for (size_t i = 1; i < term_postings.size(); i++) {
            set<uint32_t> current(term_postings[i].second.begin(), 
                                 term_postings[i].second.end());
            set<uint32_t> intersection;
            set_intersection(common_docs.begin(), common_docs.end(),
                           current.begin(), current.end(),
                           inserter(intersection, intersection.begin()));
            common_docs = move(intersection);
        }
        
        // Calculate BM25 scores for common documents
        vector<SearchResult> results;
        
        for (uint32_t docid : common_docs) {
            SearchResult result;
            result.docid = docid_map[docid];
            result.score = 0.0;
            
            uint32_t doc_len = doc_lengths.count(docid) ? doc_lengths[docid] : avg_doc_length;
            
            // Sum BM25 scores for all query terms
            for (const auto& tp : term_postings) {
                auto it = lexicon.find(tp.first);
                if (it != lexicon.end()) {
                    uint32_t doc_freq = it->second.doc_freq;
                    result.score += calculate_bm25(doc_freq, 1, doc_len, total_docs);
                    result.term_frequencies[tp.first] = 1;
                }
            }
            
            results.push_back(result);
        }
        
        sort(results.begin(), results.end());
        
        if (results.size() > top_k) {
            results.resize(top_k);
        }
        
        return results;
    }
    
    // Search with semantic expansion (optional)
    vector<SearchResult> search_semantic(const string& query, 
                                         SemanticExpander* expander,
                                         size_t top_k = 10) {
        if (!expander) return search_multi(query, top_k);
        
        vector<string> tokens = tokenize_query(query);
        vector<string> expanded = expander->expand_query(tokens, 2);
        
        // Build expanded query
        string expanded_query;
        for (size_t i = 0; i < expanded.size(); i++) {
            if (i > 0) expanded_query += " ";
            expanded_query += expanded[i];
        }
        
        cerr << "[QueryEngine] Expanded query: " << expanded_query << "\n";
        
        return search_multi(expanded_query, top_k);
    }
    
    // Get statistics
    void print_stats() {
        cout << "\n=== INDEX STATISTICS ===\n";
        cout << "Total terms: " << lexicon.size() << "\n";
        cout << "Total documents: " << total_docs << "\n";
        cout << "Avg document length: " << avg_doc_length << "\n";
        cout << "Barrels loaded: " << barrel_cache.size() << "\n";
        cout << "========================\n\n";
    }
};

#endif // QUERY_ENGINE_HPP
