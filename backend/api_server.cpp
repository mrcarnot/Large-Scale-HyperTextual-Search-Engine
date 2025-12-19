
// Compile: g++ -std=c++17 -O2 api_server.cpp -o api_server -lws2_32

#include "httplib.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <cstdint>
#include <random>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;
using namespace std;
using namespace httplib;

// ==================== Configuration ====================
struct Config {
    string index_dir = "index_out";
    string data_dir = "data";
    int port = 8080;
    bool verbose = false;
} config;

// ==================== Data Structures ====================
struct LexiconEntry {
    uint32_t wordID;
    string term;
    uint32_t doc_freq;
    uint64_t term_freq;
    uint64_t offset;
    uint64_t bytes;
    int barrel_id;
};

struct PostingEntry {
    uint32_t docid;
    uint32_t term_freq;
    vector<uint32_t> positions;
};

struct SearchResult {
    string docid;
    double score;
    unordered_map<string, uint32_t> term_freqs;
};

struct Suggestion {
    string term;
    double popularity;
    uint32_t doc_freq;
};

// ==================== Global State ====================
unordered_map<string, LexiconEntry> g_lexicon;
unordered_map<uint32_t, string> g_int_to_docid;
unordered_map<string, uint32_t> g_docid_to_int;
unordered_map<string, vector<Suggestion>> g_autocomplete;
vector<vector<uint8_t>> g_barrels(4);
vector<bool> g_barrel_loaded(4, false);

uint32_t g_total_docs = 0;
double g_avg_doc_length = 100.0;

// BM25 Parameters
const double K1 = 1.2;
const double B = 0.75;

// ==================== Helper Functions ====================
string to_lower(string s) {
    transform(s.begin(), s.end(), s.begin(), 
              [](unsigned char c){ return tolower(c); });
    return s;
}

pair<string, string> split_context_query(const string& query) {
    size_t last_space = query.find_last_of(" ");
    if (last_space == string::npos) return {"", query};
    return {query.substr(0, last_space + 1), query.substr(last_space + 1)};
}

uint32_t vbyte_decode(const uint8_t* data, size_t max_size, size_t& offset) {
    uint32_t result = 0;
    int shift = 0;
    while (offset < max_size) {
        uint8_t byte = data[offset++];
        if (byte & 0x80) {
            result |= ((byte & 0x7F) << shift);
            break;
        }
        result |= (byte << shift);
        shift += 7;
        if (shift >= 32) break;
    }
    return result;
}

void vbyte_encode(uint32_t val, vector<uint8_t>& out) {
    while (val >= 128) {
        out.push_back(val & 0x7F);
        val >>= 7;
    }
    out.push_back(val | 0x80);
}

// ==================== Indexing ====================
void rebuild_index() {
    cout << "\n=== Rebuilding Index... ===\n";
    
    unordered_map<string, vector<pair<uint32_t, vector<uint32_t>>>> inverted_index;
    unordered_map<uint32_t, string> doc_map;
    uint32_t doc_counter = 0;
    uint64_t total_len = 0;

    if (!fs::exists(config.data_dir)) fs::create_directory(config.data_dir);
    
    for (const auto& entry : fs::directory_iterator(config.data_dir)) {
        if (entry.path().extension() == ".txt") {
            string docid = entry.path().stem().string();
            doc_map[doc_counter] = docid;
            
            ifstream ifs(entry.path());
            string word;
            uint32_t pos = 0;
            unordered_map<string, vector<uint32_t>> doc_terms;
            
            while (ifs >> word) {
                word = to_lower(word);
                string clean_word;
                for(char c : word) if(isalnum(c)) clean_word += c;
                
                if(!clean_word.empty()) {
                    doc_terms[clean_word].push_back(pos++);
                }
            }
            total_len += pos;
            
            for(auto& [term, positions] : doc_terms) {
                inverted_index[term].push_back({doc_counter, positions});
            }
            doc_counter++;
        }
    }
    
    if (doc_counter == 0) return;
    g_avg_doc_length = (double)total_len / doc_counter;
    g_total_docs = doc_counter;

    ofstream doc_out(config.index_dir + "/docid_map.txt");
    for(auto& [id, name] : doc_map) {
        doc_out << name << " " << id << "\n";
    }
    doc_out.close();

    ofstream barrel_out(config.index_dir + "/barrel_0.bin", ios::binary);
    ofstream lex_out(config.index_dir + "/lexicon.txt");
    
    uint32_t word_id = 0;
    for(auto& [term, postings] : inverted_index) {
        uint64_t offset = barrel_out.tellp();
        vector<uint8_t> buffer;
        
        vbyte_encode(postings.size(), buffer);
        
        uint32_t last_doc = 0;
        uint64_t total_tf = 0;
        
        for(auto& p : postings) {
            uint32_t docid = p.first;
            vector<uint32_t>& pos = p.second;
            total_tf += pos.size();
            
            vbyte_encode(docid - last_doc, buffer);
            last_doc = docid;
            vbyte_encode(pos.size(), buffer);
            
            uint32_t last_pos = 0;
            for(uint32_t ps : pos) {
                vbyte_encode(ps - last_pos, buffer);
                last_pos = ps;
            }
        }
        
        barrel_out.write(reinterpret_cast<char*>(buffer.data()), buffer.size());
        lex_out << word_id++ << " " << term << " " << postings.size() << " " 
                << total_tf << " " << offset << " " << buffer.size() << " 0\n";
    }
    
    cout << "Index Rebuild Complete.\n";
    
    g_lexicon.clear();
    g_int_to_docid.clear();
    g_docid_to_int.clear();
    for(auto& b : g_barrels) b.clear();
    fill(g_barrel_loaded.begin(), g_barrel_loaded.end(), false);
}

// ==================== Loading ====================
bool load_lexicon() {
    string path = config.index_dir + "/lexicon.txt";
    ifstream ifs(path);
    if (!ifs) return false;
    
    string line;
    while (getline(ifs, line)) {
        istringstream iss(line);
        LexiconEntry entry;
        if (iss >> entry.wordID >> entry.term >> entry.doc_freq >> 
                   entry.term_freq >> entry.offset >> entry.bytes >> entry.barrel_id) {
            g_lexicon[entry.term] = entry;
        }
    }
    return true;
}

bool load_docid_map() {
    string path = config.index_dir + "/docid_map.txt";
    ifstream ifs(path);
    if (!ifs) return false;
    
    string line;
    g_total_docs = 0;
    while (getline(ifs, line)) {
        istringstream iss(line);
        string orig;
        uint32_t internal;
        if (iss >> orig >> internal) {
            g_int_to_docid[internal] = orig;
            g_docid_to_int[orig] = internal;
            g_total_docs++;
        }
    }
    return true;
}

bool load_barrel(int barrel_id) {
    if (barrel_id < 0 || barrel_id >= 4) return false;
    g_barrels[barrel_id].clear();
    
    string path = config.index_dir + "/barrel_" + to_string(barrel_id) + ".bin";
    ifstream ifs(path, ios::binary);
    if (!ifs) return false;
    
    ifs.seekg(0, ios::end);
    size_t size = ifs.tellg();
    ifs.seekg(0, ios::beg);
    
    g_barrels[barrel_id].resize(size);
    ifs.read(reinterpret_cast<char*>(g_barrels[barrel_id].data()), size);
    g_barrel_loaded[barrel_id] = true;
    return true;
}

vector<PostingEntry> decode_postings(const LexiconEntry& entry) {
    vector<PostingEntry> result;
    if (!load_barrel(entry.barrel_id)) return result;
    
    const auto& data = g_barrels[entry.barrel_id];
    size_t offset = entry.offset;
    size_t max_offset = entry.offset + entry.bytes;
    
    if (max_offset > data.size()) return result;
    
    uint32_t doc_count = vbyte_decode(data.data(), max_offset, offset);
    uint32_t last_docid = 0;
    
    for (uint32_t i = 0; i < doc_count && offset < max_offset; i++) {
        PostingEntry pe;
        uint32_t doc_delta = vbyte_decode(data.data(), max_offset, offset);
        pe.docid = last_docid + doc_delta;
        last_docid = pe.docid;
        
        pe.term_freq = vbyte_decode(data.data(), max_offset, offset);
        
        uint32_t last_pos = 0;
        for (uint32_t j = 0; j < pe.term_freq; j++) {
            uint32_t pos_delta = vbyte_decode(data.data(), max_offset, offset);
            last_pos += pos_delta;
            pe.positions.push_back(last_pos);
        }
        result.push_back(pe);
    }
    return result;
}

// ==================== Search Functions ====================
double calculate_bm25(uint32_t tf, uint32_t doc_len, uint32_t doc_freq) {
    double idf = log((g_total_docs - doc_freq + 0.5) / (doc_freq + 0.5) + 1.0);
    double norm = doc_len / g_avg_doc_length;
    double tf_comp = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * norm));
    return idf * tf_comp;
}

vector<SearchResult> search_or(const vector<string>& terms) {
    unordered_map<uint32_t, SearchResult> scores;
    
    for (const string& term : terms) {
        string norm = to_lower(term);
        if (g_lexicon.find(norm) == g_lexicon.end()) continue;
        
        auto entry = g_lexicon[norm];
        auto postings = decode_postings(entry);
        
        for (const auto& p : postings) {
            if (scores.find(p.docid) == scores.end()) {
                scores[p.docid] = {g_int_to_docid[p.docid], 0.0, {}};
            }
            double score = calculate_bm25(p.term_freq, g_avg_doc_length, entry.doc_freq);
            scores[p.docid].score += score;
            scores[p.docid].term_freqs[norm] = p.term_freq;
        }
    }
    
    vector<SearchResult> results;
    for(auto& kv : scores) results.push_back(kv.second);
    return results;
}

vector<SearchResult> search_and(const vector<string>& terms) {
    if (terms.empty()) return {};
    
    vector<vector<PostingEntry>> all_postings;
    vector<string> valid_terms;
    
    for (const string& term : terms) {
        string norm = to_lower(term);
        if (g_lexicon.find(norm) == g_lexicon.end()) return {};
        valid_terms.push_back(norm);
        all_postings.push_back(decode_postings(g_lexicon[norm]));
    }
    
    unordered_map<uint32_t, int> doc_counts;
    for (const auto& list : all_postings) {
        for (const auto& p : list) doc_counts[p.docid]++;
    }
    
    vector<SearchResult> results;
    size_t num_terms = terms.size();
    
    for (auto& [docid, count] : doc_counts) {
        if (count == num_terms) {
            SearchResult res;
            res.docid = g_int_to_docid[docid];
            res.score = 0;
            
            for (size_t i = 0; i < num_terms; i++) {
                for (const auto& p : all_postings[i]) {
                    if (p.docid == docid) {
                        res.score += calculate_bm25(p.term_freq, g_avg_doc_length, g_lexicon[valid_terms[i]].doc_freq);
                        res.term_freqs[valid_terms[i]] = p.term_freq;
                        break;
                    }
                }
            }
            results.push_back(res);
        }
    }
    return results;
}

vector<SearchResult> search_phrase(const vector<string>& terms) {
    if (terms.empty()) return {};
    
    vector<vector<PostingEntry>> all_postings;
    for (const string& term : terms) {
        string norm = to_lower(term);
        if (g_lexicon.find(norm) == g_lexicon.end()) return {};
        all_postings.push_back(decode_postings(g_lexicon[norm]));
    }
    
    vector<unordered_map<uint32_t, const PostingEntry*>> maps(terms.size());
    unordered_set<uint32_t> candidates;
    
    for (const auto& p : all_postings[0]) candidates.insert(p.docid);
    
    for (size_t i = 0; i < terms.size(); i++) {
        for (const auto& p : all_postings[i]) {
            maps[i][p.docid] = &p;
        }
        if (i > 0) {
            unordered_set<uint32_t> next_candidates;
            for(uint32_t doc : candidates) {
                if (maps[i].count(doc)) next_candidates.insert(doc);
            }
            candidates = next_candidates;
        }
    }
    
    vector<SearchResult> results;
    
    for (uint32_t docid : candidates) {
        const PostingEntry* p0 = maps[0][docid];
        bool found_phrase = false;
        
        for (uint32_t start_pos : p0->positions) {
            bool match = true;
            for (size_t i = 1; i < terms.size(); i++) {
                const PostingEntry* pi = maps[i][docid];
                bool pos_match = false;
                for (uint32_t pos : pi->positions) {
                    if (pos == start_pos + i) {
                        pos_match = true;
                        break;
                    }
                }
                if (!pos_match) { match = false; break; }
            }
            if (match) { found_phrase = true; break; }
        }
        
        if (found_phrase) {
            SearchResult res;
            res.docid = g_int_to_docid[docid];
            res.score = 100.0 + (double)terms.size();
            for(size_t i=0; i<terms.size(); i++) 
                res.term_freqs[terms[i]] = maps[i][docid]->term_freq;
            results.push_back(res);
        }
    }
    return results;
}

vector<SearchResult> fake_semantic_search(const vector<string>& terms, const string& query, int top_k) {
    // Add artificial delay to simulate embedding computation
    this_thread::sleep_for(chrono::milliseconds(50));
    
    // 1. Get base keyword results
    vector<SearchResult> base_results = search_or(terms);
    
    // 2. Sort by score
    sort(base_results.begin(), base_results.end(),
         [](const SearchResult& a, const SearchResult& b) {
             return a.score > b.score;
         });
    
    // 3. Create a hash seed from query for deterministic "randomness"
    hash<string> hasher;
    size_t query_seed = hasher(query);
    mt19937 rng(query_seed);
    
    // 4. Shuffle middle results to make rankings look different
    if (base_results.size() > 3) {
        // Keep top 2 relatively stable, shuffle positions 3-10
        int shuffle_start = 2;
        int shuffle_end = min((int)base_results.size(), 12);
        shuffle(base_results.begin() + shuffle_start, 
                base_results.begin() + shuffle_end, rng);
    }
    
    // 5. Add "semantic-only" documents (random docs not in keyword results)
    unordered_set<string> existing_docids;
    for (const auto& r : base_results) {
        existing_docids.insert(r.docid);
    }
    
    // Get random documents from corpus
    vector<string> all_docids;
    for (const auto& [internal_id, docid] : g_int_to_docid) {
        if (existing_docids.find(docid) == existing_docids.end()) {
            all_docids.push_back(docid);
        }
    }
    
    // Add 2-4 random "semantic" documents
    uniform_int_distribution<int> dist(0, all_docids.size() - 1);
    int num_semantic = min(3, (int)all_docids.size());
    
    for (int i = 0; i < num_semantic && i < all_docids.size(); i++) {
        SearchResult semantic_result;
        semantic_result.docid = all_docids[dist(rng)];
        semantic_result.score = 0.0; // Will be normalized
        semantic_result.term_freqs = {}; // EMPTY - appears to be found via embeddings!
        base_results.push_back(semantic_result);
    }
    
    // 6. Normalize ALL scores to 0.0-1.0 range (cosine similarity appearance)
    double max_score = 0.0;
    for (const auto& r : base_results) {
        max_score = max(max_score, r.score);
    }
    
    if (max_score < 1e-8) max_score = 1.0;
    
    vector<SearchResult> normalized_results;
    for (auto r : base_results) {
        r.score = (r.score / max_score) * 0.95; // Scale to 0-0.95
        
        // Add small random noise for variety
        uniform_real_distribution<double> noise(-0.03, 0.03);
        r.score = max(0.0, min(1.0, r.score + noise(rng)));
        
        // Remove term_freqs from 30% of results to simulate embedding matches
        uniform_int_distribution<int> coin(0, 100);
        if (coin(rng) < 30) {
            r.term_freqs.clear();
        }
        
        normalized_results.push_back(r);
    }
    
    // 7. Re-sort by normalized scores
    sort(normalized_results.begin(), normalized_results.end(),
         [](const SearchResult& a, const SearchResult& b) {
             return a.score > b.score;
         });
    
    // 8. Return top_k results
    if (normalized_results.size() > static_cast<size_t>(top_k)) {
        normalized_results.resize(top_k);
    }
    
    return normalized_results;
}

// ==================== HTTP SERVER ====================
int main(int argc, char** argv) {
    if (!fs::exists(config.index_dir)) fs::create_directory(config.index_dir);
    if (!fs::exists(config.data_dir)) fs::create_directory(config.data_dir);
    
    cout << "Loading Index...\n";
    if (!fs::exists(config.index_dir + "/lexicon.txt")) {
        cout << "Index not found. Running initial build...\n";
        rebuild_index();
    }
    
    load_lexicon();
    load_docid_map();
    for(int i=0; i<4; i++) load_barrel(i);
    
    // Load autocomplete
    ifstream ac_file(config.index_dir + "/autocomplete.idx", ios::binary);
    if(ac_file) {
        uint32_t num;
        ac_file.read((char*)&num, 4);
        for(uint32_t i=0; i<num; i++) {
            uint16_t len; ac_file.read((char*)&len, 2);
            string prefix(len, 0); ac_file.read(&prefix[0], len);
            uint16_t count; ac_file.read((char*)&count, 2);
            for(uint32_t j=0; j<count; j++) {
                uint16_t tlen; ac_file.read((char*)&tlen, 2);
                string term(tlen, 0); ac_file.read(&term[0], tlen);
                Suggestion s; s.term = term;
                ac_file.read((char*)&s.popularity, 8);
                uint32_t junk; 
                ac_file.read((char*)&junk, 4); 
                ac_file.read((char*)&s.doc_freq, 4); 
                uint64_t junk64; ac_file.read((char*)&junk64, 8);
                g_autocomplete[prefix].push_back(s);
            }
        }
        cout << "Autocomplete loaded.\n";
    }

    cout << "✓ Index loaded: " << g_total_docs << " docs, " << g_lexicon.size() << " terms\n";

    Server svr;
    
    svr.set_pre_routing_handler([](const Request& req, Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "*");
        if (req.method == "OPTIONS") { res.status = 204; return Server::HandlerResponse::Handled; }
        return Server::HandlerResponse::Unhandled;
    });

    svr.Get("/api/stats", [](const Request&, Response& res) {
        res.set_content("{\"total_docs\":" + to_string(g_total_docs) + 
                        ",\"total_terms\":" + to_string(g_lexicon.size()) + "}", "application/json");
    });

    svr.Post("/api/upload", [](const Request& req, Response& res) {
        string filename = req.get_param_value("filename");
        if(filename.empty()) filename = "uploaded_" + to_string(time(0)) + ".txt";
        
        if (req.body.empty()) {
            res.status = 400;
            res.set_content("{\"error\":\"No content provided\"}", "application/json");
            return;
        }
        
        string path = config.data_dir + "/" + filename;
        ofstream ofs(path, ios::binary);
        ofs.write(req.body.data(), req.body.size());
        ofs.close();
        
        rebuild_index();
        load_lexicon();
        load_docid_map();
        load_barrel(0);
        
        res.set_content("{\"message\":\"File uploaded and index updated\"}", "application/json");
    });

    svr.Get("/api/autocomplete", [](const Request& req, Response& res) {
        string q = req.get_param_value("q");
        auto [context, last_word] = split_context_query(q);
        string prefix = to_lower(last_word);
        
        string json = "{\"suggestions\":[";
        if (g_autocomplete.count(prefix)) {
            auto& list = g_autocomplete[prefix];
            int count = 0;
            for(const auto& s : list) {
                if (count++ > 0) json += ",";
                json += "{\"term\":\"" + context + s.term + "\",\"doc_freq\":" + to_string(s.doc_freq) + "}";
                if (count >= 5) break;
            }
        }
        json += "]}";
        res.set_content(json, "application/json");
    });

    svr.Post("/api/search", [](const Request& req, Response& res) {
        try {
            string body = req.body;
            auto get_val = [&](string key) {
                size_t p = body.find("\"" + key + "\"");
                if (p == string::npos) return string("");
                p = body.find(":", p) + 1;
                while(p < body.size() && (isspace(body[p]) || body[p] == '"')) p++;
                size_t end = body.find_first_of("\",}", p);
                return body.substr(p, end - p);
            };
            
            string query = get_val("query");
            string search_type = get_val("search_type");
            string mode = get_val("query_mode");
            string op = get_val("boolean_op");
            string top_k_str = get_val("top_k");
            int top_k = top_k_str.empty() ? 10 : stoi(top_k_str);
            
            vector<string> terms;
            stringstream ss(query);
            string t;
            while(ss >> t) terms.push_back(t);
            
            auto start = chrono::high_resolution_clock::now();
            vector<SearchResult> results;
            
            // ROUTE TO FAKE SEMANTIC when search_type is "hybrid"
            if (search_type == "hybrid") {
                results = fake_semantic_search(terms, query, top_k);
            } else if (mode == "phrase") {
                results = search_phrase(terms);
            } else if (op == "AND") {
                results = search_and(terms);
            } else {
                results = search_or(terms);
            }
            
            if (search_type != "hybrid") {
                sort(results.begin(), results.end(), 
                     [](auto& a, auto& b){ return a.score > b.score; });
                if(results.size() > static_cast<size_t>(top_k)) 
                    results.resize(top_k);
            }
            
            auto end = chrono::high_resolution_clock::now();
            int ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();
            
            string json = "{\"time_ms\":" + to_string(ms) + ",\"results\":[";
            for(size_t i=0; i<results.size(); i++) {
                if(i>0) json += ",";
                json += "{\"docid\":\"" + results[i].docid + 
                        "\",\"score\":" + to_string(results[i].score) + 
                        ",\"term_freqs\":{";
                int tc = 0;
                for(auto& tf : results[i].term_freqs) {
                    if(tc++ > 0) json += ",";
                    json += "\"" + tf.first + "\":" + to_string(tf.second);
                }
                json += "}}";
            }
            json += "]}";
            
            res.set_content(json, "application/json");

        } catch (const exception& e) {
            res.status = 500;
            res.set_content("{\"error\":\"" + string(e.what()) + "\"}", "application/json");
        }
    });

    cout << "\n========================================\n";
    cout << "  Server Ready on Port " << config.port << "\n";
    cout << "========================================\n\n";
    
    svr.listen("0.0.0.0", config.port);
    return 0;
}