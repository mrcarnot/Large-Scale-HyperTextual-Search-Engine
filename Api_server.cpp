// api_server.cpp - REST API Server for Search Engine
// Download httplib.h from: https://github.com/yhirose/cpp-httplib
// Compile: g++ -std=c++17 -O2 api_server.cpp -o api_server -lws2_32

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <bits/stdc++.h>
#include <filesystem>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

namespace fs = std::filesystem;
using namespace std;
using namespace httplib;
using namespace rapidjson;

// ==================== CONFIGURATION ====================
struct Config {
    string index_dir = "index_out";
    int port = 8080;
    bool enable_cors = true;
};

Config config;

// ==================== VByte Decoding ====================
static uint32_t vbyte_decode_uint32(const uint8_t* data, size_t max_size, size_t& offset) {
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
    }
    return result;
}

// ==================== LEXICON & INDEX LOADER ====================
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

unordered_map<string, LexiconEntry> lexicon;
unordered_map<uint32_t, string> int_to_docid;
vector<vector<uint8_t>> barrel_data(4);
vector<bool> barrel_loaded(4, false);

// BM25 parameters
const double k1 = 1.2;
const double b = 0.75;
const double TITLE_BOOST = 3.0;
const double ABSTRACT_BOOST = 2.0;
const double BODY_BOOST = 1.0;
const double RECENCY_WEIGHT = 0.1;
const int CURRENT_YEAR = 2024;

uint32_t total_docs = 0;
double avg_doc_length = 100.0;

// ==================== AUTOCOMPLETE INDEX ====================
struct Suggestion {
    string term;
    double popularity;
    uint32_t doc_freq;
};

unordered_map<string, vector<Suggestion>> autocomplete_index;

// ==================== LOAD FUNCTIONS ====================
void load_lexicon() {
    string path = config.index_dir + "/lexicon.txt";
    ifstream ifs(path);
    if (!ifs) {
        cerr << "ERROR: Cannot load lexicon from " << path << "\n";
        exit(1);
    }
    
    string line;
    while (getline(ifs, line)) {
        istringstream iss(line);
        LexiconEntry entry;
        if (iss >> entry.wordID >> entry.term >> entry.doc_freq >> 
                   entry.term_freq >> entry.offset >> entry.bytes >> entry.barrel_id) {
            lexicon[entry.term] = entry;
        }
    }
    cout << "Loaded " << lexicon.size() << " terms\n";
}

void load_docid_map() {
    string path = config.index_dir + "/docid_map.txt";
    ifstream ifs(path);
    if (!ifs) {
        cerr << "ERROR: Cannot load docid_map from " << path << "\n";
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
    cout << "Loaded " << total_docs << " documents\n";
}

void load_barrel(int barrel_id) {
    if (barrel_id < 0 || barrel_id >= 4 || barrel_loaded[barrel_id]) return;
    
    string path = config.index_dir + "/barrel_" + to_string(barrel_id) + ".bin";
    ifstream ifs(path, ios::binary);
    if (!ifs) return;
    
    ifs.seekg(0, ios::end);
    size_t size = ifs.tellg();
    ifs.seekg(0, ios::beg);
    
    barrel_data[barrel_id].resize(size);
    ifs.read(reinterpret_cast<char*>(barrel_data[barrel_id].data()), size);
    barrel_loaded[barrel_id] = true;
}

void load_autocomplete() {
    string path = config.index_dir + "/autocomplete.idx";
    ifstream ifs(path, ios::binary);
    if (!ifs) {
        cerr << "Warning: Autocomplete index not found\n";
        return;
    }
    
    uint32_t num_prefixes;
    ifs.read(reinterpret_cast<char*>(&num_prefixes), sizeof(num_prefixes));
    
    for (uint32_t i = 0; i < num_prefixes; i++) {
        uint16_t prefix_len;
        ifs.read(reinterpret_cast<char*>(&prefix_len), sizeof(prefix_len));
        
        string prefix(prefix_len, '\0');
        ifs.read(&prefix[0], prefix_len);
        
        uint16_t num_terms;
        ifs.read(reinterpret_cast<char*>(&num_terms), sizeof(num_terms));
        
        vector<Suggestion> suggestions;
        for (uint16_t j = 0; j < num_terms; j++) {
            Suggestion sug;
            uint16_t term_len;
            ifs.read(reinterpret_cast<char*>(&term_len), sizeof(term_len));
            sug.term.resize(term_len);
            ifs.read(&sug.term[0], term_len);
            
            uint32_t word_id;
            uint64_t term_freq;
            ifs.read(reinterpret_cast<char*>(&sug.popularity), sizeof(sug.popularity));
            ifs.read(reinterpret_cast<char*>(&word_id), sizeof(word_id));
            ifs.read(reinterpret_cast<char*>(&sug.doc_freq), sizeof(sug.doc_freq));
            ifs.read(reinterpret_cast<char*>(&term_freq), sizeof(term_freq));
            
            suggestions.push_back(sug);
        }
        autocomplete_index[prefix] = suggestions;
    }
    cout << "Loaded autocomplete index with " << autocomplete_index.size() << " prefixes\n";
}

// ==================== DECODE POSTINGS ====================
vector<PostingEntry> decode_postings(const LexiconEntry& entry) {
    vector<PostingEntry> result;
    
    load_barrel(entry.barrel_id);
    if (!barrel_loaded[entry.barrel_id]) return result;
    
    const auto& data = barrel_data[entry.barrel_id];
    size_t offset = entry.offset;
    size_t max_offset = entry.offset + entry.bytes;
    
    uint32_t doc_count = vbyte_decode_uint32(data.data(), max_offset, offset);
    uint32_t last_docid = 0;
    
    for (uint32_t i = 0; i < doc_count && offset < max_offset; i++) {
        PostingEntry pe;
        uint32_t doc_delta = vbyte_decode_uint32(data.data(), max_offset, offset);
        pe.docid = last_docid + doc_delta;
        last_docid = pe.docid;
        pe.term_freq = vbyte_decode_uint32(data.data(), max_offset, offset);
        
        uint32_t last_pos = 0;
        for (uint32_t j = 0; j < pe.term_freq && offset < max_offset; j++) {
            uint32_t pos_delta = vbyte_decode_uint32(data.data(), max_offset, offset);
            pe.positions.push_back(last_pos + pos_delta);
            last_pos += pos_delta;
        }
        result.push_back(pe);
    }
    return result;
}

// ==================== SEARCH LOGIC ====================
struct SearchResult {
    string docid;
    double score;
    map<string, uint32_t> term_freqs;
};

string normalize_term(string term) {
    transform(term.begin(), term.end(), term.begin(), 
              [](unsigned char c){ return tolower(c); });
    return term;
}

vector<SearchResult> search_keywords(const vector<string>& query_terms, int top_k) {
    unordered_map<uint32_t, SearchResult> scores;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        auto it = lexicon.find(norm);
        if (it == lexicon.end()) continue;
        
        auto postings = decode_postings(it->second);
        
        for (const auto& posting : postings) {
            if (scores.find(posting.docid) == scores.end()) {
                SearchResult sr;
                sr.docid = int_to_docid[posting.docid];
                sr.score = 0.0;
                scores[posting.docid] = sr;
            }
            
            double idf = log((total_docs - it->second.doc_freq + 0.5) / (it->second.doc_freq + 0.5) + 1.0);
            double tf_score = (posting.term_freq * (k1 + 1.0)) / (posting.term_freq + k1);
            scores[posting.docid].score += idf * tf_score;
            scores[posting.docid].term_freqs[norm] = posting.term_freq;
        }
    }
    
    vector<SearchResult> results;
    for (auto& p : scores) results.push_back(p.second);
    
    sort(results.begin(), results.end(), 
         [](const SearchResult& a, const SearchResult& b) {
             return a.score > b.score;
         });
    
    if (results.size() > top_k) results.resize(top_k);
    return results;
}

// ==================== API ENDPOINTS ====================
void setup_routes(Server& svr) {
    // CORS middleware
    svr.set_pre_routing_handler([](const Request& req, Response& res) {
        if (config.enable_cors) {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        }
        if (req.method == "OPTIONS") {
            res.status = 200;
            return Server::HandlerResponse::Handled;
        }
        return Server::HandlerResponse::Unhandled;
    });
    
    // Health check
    svr.Get("/api/health", [](const Request&, Response& res) {
        res.set_content(R"({"status":"ok","message":"Search engine API is running"})", "application/json");
    });
    
    // Search endpoint
    svr.Post("/api/search", [](const Request& req, Response& res) {
        Document doc;
        doc.Parse(req.body.c_str());
        
        if (doc.HasParseError() || !doc.HasMember("query")) {
            res.status = 400;
            res.set_content(R"({"error":"Invalid request"})", "application/json");
            return;
        }
        
        string query = doc["query"].GetString();
        int top_k = doc.HasMember("top_k") ? doc["top_k"].GetInt() : 10;
        
        // Parse query into terms
        vector<string> terms;
        istringstream iss(query);
        string term;
        while (iss >> term) terms.push_back(term);
        
        auto start = chrono::high_resolution_clock::now();
        auto results = search_keywords(terms, top_k);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        // Build JSON response
        StringBuffer sb;
        Writer<StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("query"); writer.String(query.c_str());
        writer.Key("time_ms"); writer.Int(duration.count());
        writer.Key("total_results"); writer.Int(results.size());
        writer.Key("results"); writer.StartArray();
        
        for (const auto& r : results) {
            writer.StartObject();
            writer.Key("docid"); writer.String(r.docid.c_str());
            writer.Key("score"); writer.Double(r.score);
            writer.Key("term_freqs"); writer.StartObject();
            for (const auto& tf : r.term_freqs) {
                writer.Key(tf.first.c_str());
                writer.Uint(tf.second);
            }
            writer.EndObject();
            writer.EndObject();
        }
        
        writer.EndArray();
        writer.EndObject();
        
        res.set_content(sb.GetString(), "application/json");
    });
    
    // Autocomplete endpoint
    svr.Get("/api/autocomplete", [](const Request& req, Response& res) {
        if (!req.has_param("q")) {
            res.status = 400;
            res.set_content(R"({"error":"Missing query parameter"})", "application/json");
            return;
        }
        
        string prefix = req.get_param_value("q");
        transform(prefix.begin(), prefix.end(), prefix.begin(), 
                 [](unsigned char c){ return tolower(c); });
        
        int max_results = req.has_param("limit") ? stoi(req.get_param_value("limit")) : 10;
        
        vector<Suggestion> suggestions;
        auto it = autocomplete_index.find(prefix);
        if (it != autocomplete_index.end()) {
            suggestions = it->second;
            if (suggestions.size() > max_results) {
                suggestions.resize(max_results);
            }
        }
        
        // Build JSON response
        StringBuffer sb;
        Writer<StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("prefix"); writer.String(prefix.c_str());
        writer.Key("suggestions"); writer.StartArray();
        
        for (const auto& sug : suggestions) {
            writer.StartObject();
            writer.Key("term"); writer.String(sug.term.c_str());
            writer.Key("doc_freq"); writer.Uint(sug.doc_freq);
            writer.Key("popularity"); writer.Double(sug.popularity);
            writer.EndObject();
        }
        
        writer.EndArray();
        writer.EndObject();
        
        res.set_content(sb.GetString(), "application/json");
    });
    
    // Stats endpoint
    svr.Get("/api/stats", [](const Request&, Response& res) {
        StringBuffer sb;
        Writer<StringBuffer> writer(sb);
        writer.StartObject();
        writer.Key("total_terms"); writer.Uint(lexicon.size());
        writer.Key("total_docs"); writer.Uint(total_docs);
        writer.Key("avg_doc_length"); writer.Double(avg_doc_length);
        writer.Key("autocomplete_prefixes"); writer.Uint(autocomplete_index.size());
        writer.EndObject();
        
        res.set_content(sb.GetString(), "application/json");
    });
}

// ==================== MAIN ====================
int main(int argc, char** argv) {
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            config.index_dir = argv[++i];
        } else if (arg == "-p" && i+1 < argc) {
            config.port = stoi(argv[++i]);
        } else if (arg == "--no-cors") {
            config.enable_cors = false;
        }
    }
    
    cout << "\n=== Search Engine REST API Server ===\n";
    cout << "Index directory: " << config.index_dir << "\n";
    cout << "Port: " << config.port << "\n";
    cout << "CORS: " << (config.enable_cors ? "enabled" : "disabled") << "\n\n";
    
    // Load all indexes
    cout << "Loading indexes...\n";
    load_lexicon();
    load_docid_map();
    load_autocomplete();
    
    // Preload barrels
    cout << "\nPreloading barrels...\n";
    for (int i = 0; i < 4; i++) {
        load_barrel(i);
        cout << "  Barrel " << i << ": " 
             << (barrel_data[i].size() / 1024.0 / 1024.0) << " MB\n";
    }
    
    cout << "\n=== Server Ready ===\n";
    cout << "Listening on http://localhost:" << config.port << "\n";
    cout << "Endpoints:\n";
    cout << "  POST /api/search - Search query\n";
    cout << "  GET  /api/autocomplete?q=PREFIX - Autocomplete\n";
    cout << "  GET  /api/stats - Index statistics\n";
    cout << "  GET  /api/health - Health check\n";
    cout << "\nPress Ctrl+C to stop\n\n";
    
    Server svr;
    setup_routes(svr);
    
    if (!svr.listen("0.0.0.0", config.port)) {
        cerr << "ERROR: Failed to start server on port " << config.port << "\n";
        return 1;
    }
    
    return 0;
}
