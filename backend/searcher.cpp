// searcher.cpp - FIXED VERSION with barrel loading and performance benchmarks
// Compile: g++ -std=c++17 -O2 searcher.cpp -o searcher

#include <bits/stdc++.h>
#include <filesystem>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <algorithm>
#include <numeric>
#include <regex>

namespace fs = std::filesystem;
using namespace std;
using namespace rapidjson;

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
        if (shift >= 32) {
            cerr << "VByte decode error: shift overflow\n";
            return 0;
        }
    }
    return result;
}

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

struct DocMetadata {
    string orig_docid;
    uint32_t doc_length;
    string title;
    string authors;
    string pub_date;
};

// ==================== Global State ====================
unordered_map<string, LexiconEntry> lexicon;
unordered_map<uint32_t, string> int_to_docid;
unordered_map<string, uint32_t> docid_to_int;
unordered_map<uint32_t, DocMetadata> doc_metadata;

// BARREL SYSTEM: Load barrels on-demand
const int NUM_BARRELS = 4;
vector<vector<uint8_t>> barrel_data(NUM_BARRELS);
vector<bool> barrel_loaded(NUM_BARRELS, false);
string index_directory;

uint32_t total_docs = 0;
double avg_doc_length = 0.0;

// BM25 parameters
const double k1 = 1.2;
const double b = 0.75;

// Field boost weights
const double TITLE_BOOST = 3.0;
const double ABSTRACT_BOOST = 2.0;
const double BODY_BOOST = 1.0;

// Recency parameters
const int CURRENT_YEAR = 2024;
const double RECENCY_WEIGHT = 0.1;

// PERFORMANCE TRACKING
struct PerformanceStats {
    size_t queries_executed = 0;
    vector<long long> query_times; // microseconds
    unordered_map<string, size_t> query_type_counts;
    
    void record(const string& type, long long time_us) {
        queries_executed++;
        query_times.push_back(time_us);
        query_type_counts[type]++;
    }
    
    void report() {
        if (query_times.empty()) return;
        
        sort(query_times.begin(), query_times.end());
        long long min_time = query_times.front();
        long long max_time = query_times.back();
        long long median = query_times[query_times.size() / 2];
        long long p95 = query_times[(query_times.size() * 95) / 100];
        long long p99 = query_times[(query_times.size() * 99) / 100];
        double avg = accumulate(query_times.begin(), query_times.end(), 0.0) / query_times.size();
        
        cerr << "\n=== Performance Statistics ===\n";
        cerr << "Total queries: " << queries_executed << "\n";
        cerr << "Min: " << (min_time / 1000.0) << " ms\n";
        cerr << "Avg: " << (avg / 1000.0) << " ms\n";
        cerr << "Median: " << (median / 1000.0) << " ms\n";
        cerr << "P95: " << (p95 / 1000.0) << " ms\n";
        cerr << "P99: " << (p99 / 1000.0) << " ms\n";
        cerr << "Max: " << (max_time / 1000.0) << " ms\n";
        
        cerr << "\nQuery types:\n";
        for (const auto& p : query_type_counts) {
            cerr << "  " << p.first << ": " << p.second << "\n";
        }
        
        // Check targets
        cerr << "\nTarget compliance:\n";
        if (p95 / 1000.0 < 200) cerr << "  ✅ P95 < 200ms\n";
        else cerr << "  ❌ P95 >= 200ms\n";
        
        if (p99 / 1000.0 < 300) cerr << "  ✅ P99 < 300ms\n";
        else cerr << "  ❌ P99 >= 300ms\n";
    }
};

PerformanceStats perf_stats;

// ==================== Barrel Loading ====================
static void load_barrel_if_needed(int barrel_id) {
    if (barrel_id < 0 || barrel_id >= NUM_BARRELS) return;
    if (barrel_loaded[barrel_id]) return;
    
    string barrel_path = index_directory + "/barrel_" + to_string(barrel_id) + ".bin";
    
    if (!fs::exists(barrel_path)) {
        cerr << "Warning: Barrel file does not exist: " << barrel_path << "\n";
        return;
    }
    
    ifstream ifs(barrel_path, ios::binary);
    if (!ifs) {
        cerr << "Warning: Cannot open barrel: " << barrel_path << "\n";
        return;
    }
    
    ifs.seekg(0, ios::end);
    size_t size = ifs.tellg();
    ifs.seekg(0, ios::beg);
    
    barrel_data[barrel_id].resize(size);
    ifs.read(reinterpret_cast<char*>(barrel_data[barrel_id].data()), size);
    
    barrel_loaded[barrel_id] = true;
    cerr << "  Loaded barrel " << barrel_id << " (" << (size / 1024.0) << " KB)\n";
}

// ==================== Load Index ====================
static void load_lexicon(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "ERROR: Cannot open lexicon: " << path << "\n";
        exit(1);
    }
    string line;
    size_t line_num = 0;
    while (getline(ifs, line)) {
        line_num++;
        istringstream iss(line);
        LexiconEntry entry;
        if (iss >> entry.wordID >> entry.term >> entry.doc_freq >> 
                   entry.term_freq >> entry.offset >> entry.bytes >> entry.barrel_id) {
            lexicon[entry.term] = entry;
        } else {
            cerr << "Warning: Malformed lexicon line " << line_num << "\n";
        }
    }
    cerr << "Loaded " << lexicon.size() << " terms from lexicon\n";
}

static void load_docid_map(const string& path) {
    ifstream ifs(path);
    if (!ifs) {
        cerr << "ERROR: Cannot open docid_map: " << path << "\n";
        exit(1);
    }
    string line;
    size_t line_num = 0;
    while (getline(ifs, line)) {
        line_num++;
        istringstream iss(line);
        string orig;
        uint32_t internal;
        if (iss >> orig >> internal) {
            int_to_docid[internal] = orig;
            docid_to_int[orig] = internal;
            total_docs++;
        } else {
            cerr << "Warning: Malformed docid_map line " << line_num << "\n";
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
    size_t line_num = 0;
    size_t parse_errors = 0;
    size_t docs_loaded = 0;
    
    while (getline(ifs, line)) {
        line_num++;
        
        Document doc;
        doc.Parse(line.c_str());
        
        if (doc.HasParseError()) {
            parse_errors++;
            if (parse_errors <= 5) {
                cerr << "Warning: JSON parse error at line " << line_num << "\n";
            }
            continue;
        }
        
        if (!doc.HasMember("docid") || !doc["docid"].IsString()) {
            continue;
        }
        
        string orig_docid = doc["docid"].GetString();
        
        uint32_t doc_len = 0;
        
        if (doc.HasMember("postings") && doc["postings"].IsArray()) {
            for (auto& p : doc["postings"].GetArray()) {
                if (p.IsObject() && p.HasMember("freq")) {
                    uint32_t freq = 0;
                    if (p["freq"].IsUint()) {
                        freq = p["freq"].GetUint();
                    } else if (p["freq"].IsInt()) {
                        freq = (uint32_t)p["freq"].GetInt();
                    }
                    doc_len += freq;
                }
            }
        }
        
        auto it = docid_to_int.find(orig_docid);
        if (it == docid_to_int.end()) {
            continue;
        }
        
        uint32_t internal_id = it->second;
        
        DocMetadata meta;
        meta.orig_docid = orig_docid;
        meta.doc_length = (doc_len > 0) ? doc_len : 100;
        
        if (doc.HasMember("title") && doc["title"].IsString()) {
            meta.title = doc["title"].GetString();
        }
        if (doc.HasMember("authors") && doc["authors"].IsString()) {
            meta.authors = doc["authors"].GetString();
        }
        if (doc.HasMember("pub_date") && doc["pub_date"].IsString()) {
            meta.pub_date = doc["pub_date"].GetString();
        }
        
        doc_metadata[internal_id] = meta;
        total_length += meta.doc_length;
        docs_loaded++;
    }
    
    if (parse_errors > 5) {
        cerr << "Warning: " << (parse_errors - 5) << " more parse errors...\n";
    }
    
    if (total_docs > 0 && docs_loaded > 0) {
        avg_doc_length = (double)total_length / docs_loaded;
    } else {
        avg_doc_length = 100.0;
    }
    
    cerr << "Loaded forward index (" << docs_loaded << "/" << line_num << " docs). "
         << "Avg doc length: " << fixed << setprecision(2) << avg_doc_length << "\n";
}

// ==================== Decode Postings ====================
static vector<PostingEntry> decode_postings_list(const LexiconEntry& entry) {
    vector<PostingEntry> result;
    
    int barrel_id = entry.barrel_id;
    load_barrel_if_needed(barrel_id);
    
    if (!barrel_loaded[barrel_id]) {
        cerr << "ERROR: Barrel " << barrel_id << " not loaded\n";
        return result;
    }
    
    const auto& data = barrel_data[barrel_id];
    
    if (entry.offset >= data.size()) {
        cerr << "ERROR: Invalid offset for term '" << entry.term << "'\n";
        return result;
    }
    
    if (entry.offset + entry.bytes > data.size()) {
        cerr << "ERROR: Postings extend beyond barrel for term '" << entry.term << "'\n";
        return result;
    }
    
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
            uint32_t pos = last_pos + pos_delta;
            pe.positions.push_back(pos);
            last_pos = pos;
        }
        
        result.push_back(pe);
    }
    
    return result;
}

// ==================== Ranking Functions ====================
static int extract_year(const string& pub_date) {
    regex year_regex(R"(\b(19|20)\d{2}\b)");
    smatch match;
    if (regex_search(pub_date, match, year_regex)) {
        return stoi(match.str());
    }
    return 0;
}

static double compute_recency_score(int pub_year) {
    if (pub_year == 0) return 0.5;
    int age = CURRENT_YEAR - pub_year;
    if (age < 0) age = 0;
    double decay_rate = 0.1;
    return exp(-decay_rate * age);
}

static string guess_field_from_position(uint32_t position, uint32_t doc_length) {
    double ratio = (double)position / doc_length;
    if (ratio < 0.10) return "title";
    if (ratio < 0.30) return "abstract";
    return "body";
}

static double get_field_boost(const string& field) {
    if (field == "title") return TITLE_BOOST;
    if (field == "abstract") return ABSTRACT_BOOST;
    return BODY_BOOST;
}

static double compute_bm25_fielded(uint32_t tf, uint32_t doc_len, uint32_t df, 
                                    const string& field) {
    if (total_docs == 0 || df == 0) return 0.0;
    
    double idf = log((total_docs - df + 0.5) / (df + 0.5) + 1.0);
    double norm = doc_len / avg_doc_length;
    double base_score = idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * norm));
    
    double field_boost = get_field_boost(field);
    return base_score * field_boost;
}

static string normalize_term(string term) {
    transform(term.begin(), term.end(), term.begin(), 
              [](unsigned char c){ return tolower(c); });
    return term;
}

static uint32_t get_doc_length(uint32_t docid) {
    auto it = doc_metadata.find(docid);
    if (it != doc_metadata.end()) {
        return it->second.doc_length;
    }
    return (uint32_t)avg_doc_length;
}

static string get_orig_docid(uint32_t docid) {
    auto it = int_to_docid.find(docid);
    if (it != int_to_docid.end()) {
        return it->second;
    }
    return "UNKNOWN_" + to_string(docid);
}

// ==================== Search Results ====================
struct SearchResult {
    uint32_t docid;
    string orig_docid;
    double bm25_score;
    double recency_score;
    double final_score;
    map<string, uint32_t> term_freqs;
    string title;
    string pub_date;
};

// ==================== Query Processing ====================
static vector<SearchResult> search_query_ranked(const vector<string>& query_terms, 
                                                 size_t top_k = 10) {
    vector<string> normalized_terms;
    vector<LexiconEntry> entries;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        auto it = lexicon.find(norm);
        if (it != lexicon.end()) {
            normalized_terms.push_back(norm);
            entries.push_back(it->second);
        } else {
            cerr << "  [Warning] Term not in index: '" << term << "'\n";
        }
    }
    
    if (entries.empty()) {
        cerr << "  [Error] No valid terms found in query\n";
        return {};
    }
    
    vector<vector<PostingEntry>> all_postings;
    for (const auto& entry : entries) {
        all_postings.push_back(decode_postings_list(entry));
    }
    
    unordered_map<uint32_t, SearchResult> scores;
    
    for (size_t i = 0; i < normalized_terms.size(); i++) {
        const string& term = normalized_terms[i];
        const LexiconEntry& entry = entries[i];
        const auto& postings = all_postings[i];
        
        for (const auto& posting : postings) {
            uint32_t docid = posting.docid;
            uint32_t tf = posting.term_freq;
            uint32_t df = entry.doc_freq;
            uint32_t doc_len = get_doc_length(docid);
            
            string field = "body";
            if (!posting.positions.empty()) {
                field = guess_field_from_position(posting.positions[0], doc_len);
            }
            
            double bm25_score = compute_bm25_fielded(tf, doc_len, df, field);
            
            if (scores.find(docid) == scores.end()) {
                SearchResult sr;
                sr.docid = docid;
                sr.orig_docid = get_orig_docid(docid);
                sr.bm25_score = 0.0;
                sr.recency_score = 0.0;
                sr.final_score = 0.0;
                
                auto meta_it = doc_metadata.find(docid);
                if (meta_it != doc_metadata.end()) {
                    sr.title = meta_it->second.title;
                    sr.pub_date = meta_it->second.pub_date;
                    int pub_year = extract_year(meta_it->second.pub_date);
                    sr.recency_score = compute_recency_score(pub_year);
                }
                
                scores[docid] = sr;
            }
            
            scores[docid].bm25_score += bm25_score;
            scores[docid].term_freqs[term] = tf;
        }
    }
    
    for (auto& p : scores) {
        SearchResult& sr = p.second;
        sr.final_score = (1.0 - RECENCY_WEIGHT) * sr.bm25_score + 
                         RECENCY_WEIGHT * sr.recency_score * 10.0;
    }
    
    vector<SearchResult> results;
    results.reserve(scores.size());
    for (auto& p : scores) {
        results.push_back(p.second);
    }
    
    sort(results.begin(), results.end(), 
         [](const SearchResult& a, const SearchResult& b) {
             return a.final_score > b.final_score;
         });
    
    if (results.size() > top_k) {
        results.resize(top_k);
    }
    
    return results;
}

static vector<SearchResult> search_query_and_ranked(const vector<string>& query_terms, 
                                                     size_t top_k = 10) {
    vector<string> normalized_terms;
    vector<LexiconEntry> entries;
    
    for (const auto& term : query_terms) {
        string norm = normalize_term(term);
        auto it = lexicon.find(norm);
        if (it != lexicon.end()) {
            normalized_terms.push_back(norm);
            entries.push_back(it->second);
        } else {
            cerr << "  [Warning] Term not in index: '" << term << "'\n";
        }
    }
    
    if (entries.empty() || entries.size() != query_terms.size()) {
        cerr << "  [Error] Not all query terms found\n";
        return {};
    }
    
    vector<size_t> indices(entries.size());
    iota(indices.begin(), indices.end(), 0);
    sort(indices.begin(), indices.end(), 
         [&entries](size_t a, size_t b) {
             return entries[a].doc_freq < entries[b].doc_freq;
         });
    
    vector<string> ordered_terms;
    vector<LexiconEntry> ordered_entries;
    for (size_t idx : indices) {
        ordered_terms.push_back(normalized_terms[idx]);
        ordered_entries.push_back(entries[idx]);
    }
    
    vector<vector<PostingEntry>> all_postings;
    vector<unordered_set<uint32_t>> doc_sets;
    
    for (const auto& entry : ordered_entries) {
        auto postings = decode_postings_list(entry);
        all_postings.push_back(postings);
        
        unordered_set<uint32_t> doc_set;
        for (const auto& p : postings) {
            doc_set.insert(p.docid);
        }
        doc_sets.push_back(doc_set);
    }
    
    unordered_set<uint32_t> result_docs = doc_sets[0];
    for (size_t i = 1; i < doc_sets.size(); i++) {
        unordered_set<uint32_t> intersection;
        for (uint32_t docid : result_docs) {
            if (doc_sets[i].find(docid) != doc_sets[i].end()) {
                intersection.insert(docid);
            }
        }
        result_docs = intersection;
        
        if (result_docs.empty()) {
            cerr << "  [Info] No documents contain all terms\n";
            return {};
        }
    }
    
    unordered_map<uint32_t, SearchResult> scores;
    
    for (uint32_t docid : result_docs) {
        SearchResult sr;
        sr.docid = docid;
        sr.orig_docid = get_orig_docid(docid);
        sr.bm25_score = 0.0;
        sr.recency_score = 0.0;
        
        auto meta_it = doc_metadata.find(docid);
        if (meta_it != doc_metadata.end()) {
            sr.title = meta_it->second.title;
            sr.pub_date = meta_it->second.pub_date;
            int pub_year = extract_year(meta_it->second.pub_date);
            sr.recency_score = compute_recency_score(pub_year);
        }
        
        uint32_t doc_len = get_doc_length(docid);
        
        for (size_t i = 0; i < ordered_terms.size(); i++) {
            const string& term = ordered_terms[i];
            const LexiconEntry& entry = ordered_entries[i];
            const auto& postings = all_postings[i];
            
            for (const auto& posting : postings) {
                if (posting.docid == docid) {
                    string field = "body";
                    if (!posting.positions.empty()) {
                        field = guess_field_from_position(posting.positions[0], doc_len);
                    }
                    double bm25_score = compute_bm25_fielded(posting.term_freq, doc_len, 
                                                            entry.doc_freq, field);
                    sr.bm25_score += bm25_score;
                    sr.term_freqs[term] = posting.term_freq;
                    break;
                }
            }
        }
        
        sr.final_score = (1.0 - RECENCY_WEIGHT) * sr.bm25_score + 
                         RECENCY_WEIGHT * sr.recency_score * 10.0;
        scores[docid] = sr;
    }
    
    vector<SearchResult> results;
    results.reserve(scores.size());
    for (auto& p : scores) {
        results.push_back(p.second);
    }
    
    sort(results.begin(), results.end(), 
         [](const SearchResult& a, const SearchResult& b) {
             return a.final_score > b.final_score;
         });
    
    if (results.size() > top_k) {
        results.resize(top_k);
    }
    
    return results;
}

static vector<SearchResult> search_phrase_ranked(const vector<string>& phrase_terms, 
                                                 size_t top_k = 10) {
    if (phrase_terms.empty()) return {};
    
    vector<string> normalized;
    vector<LexiconEntry> entries;
    
    for (const auto& term : phrase_terms) {
        string norm = normalize_term(term);
        auto it = lexicon.find(norm);
        if (it == lexicon.end()) {
            cerr << "  [Warning] Phrase term not in index: '" << term << "'\n";
            return {};
        }
        normalized.push_back(norm);
        entries.push_back(it->second);
    }
    
    vector<unordered_map<uint32_t, const PostingEntry*>> posting_maps;
    vector<vector<PostingEntry>> all_postings;
    
    for (const auto& entry : entries) {
        all_postings.push_back(decode_postings_list(entry));
        
        unordered_map<uint32_t, const PostingEntry*> posting_map;
        for (const auto& posting : all_postings.back()) {
            posting_map[posting.docid] = &posting;
        }
        posting_maps.push_back(posting_map);
    }
    
    unordered_set<uint32_t> candidates;
    for (const auto& posting : all_postings[0]) {
        candidates.insert(posting.docid);
    }
    
    for (size_t i = 1; i < posting_maps.size(); i++) {
        unordered_set<uint32_t> intersection;
        for (uint32_t docid : candidates) {
            if (posting_maps[i].find(docid) != posting_maps[i].end()) {
                intersection.insert(docid);
            }
        }
        candidates = intersection;
    }
    
    vector<SearchResult> results;
    
    for (uint32_t docid : candidates) {
        const PostingEntry* first_posting = posting_maps[0][docid];
        
        bool phrase_found = false;
        uint32_t phrase_position = 0;
        
        for (uint32_t start_pos : first_posting->positions) {
            bool match = true;
            
            for (size_t i = 1; i < normalized.size(); i++) {
                uint32_t expected_pos = start_pos + i;
                
                const PostingEntry* posting = posting_maps[i][docid];
                bool found = (find(posting->positions.begin(), posting->positions.end(), 
                                  expected_pos) != posting->positions.end());
                
                if (!found) {
                    match = false;
                    break;
                }
            }
            
            if (match) {
                phrase_found = true;
                phrase_position = start_pos;
                break;
            }
        }
        
        if (phrase_found) {
            SearchResult sr;
            sr.docid = docid;
            sr.orig_docid = get_orig_docid(docid);
            
            uint32_t doc_len = get_doc_length(docid);
            string field = guess_field_from_position(phrase_position, doc_len);
            double field_boost = get_field_boost(field);
            
            sr.bm25_score = 100.0 * field_boost;
            
            auto meta_it = doc_metadata.find(docid);
            if (meta_it != doc_metadata.end()) {
                sr.title = meta_it->second.title;
                sr.pub_date = meta_it->second.pub_date;
                int pub_year = extract_year(meta_it->second.pub_date);
                sr.recency_score = compute_recency_score(pub_year);
            }
            
            sr.final_score = (1.0 - RECENCY_WEIGHT) * sr.bm25_score + 
                             RECENCY_WEIGHT * sr.recency_score * 10.0;
            
            results.push_back(sr);
        }
    }
    
    sort(results.begin(), results.end(), 
         [](const SearchResult& a, const SearchResult& b) {
             return a.final_score > b.final_score;
         });
    
    if (results.size() > top_k) {
        results.resize(top_k);
    }
    
    return results;
}

// ==================== Main ====================
int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " -d index_dir [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -d DIR      : directory containing index files (required)\n";
        cerr << "  -q \"QUERY\"  : query terms (OR semantics)\n";
        cerr << "  -a \"QUERY\"  : query terms (AND semantics - all required)\n";
        cerr << "  -p \"PHRASE\" : phrase query (exact match)\n";
        cerr << "  -k N        : number of results (default 10)\n";
        cerr << "  --benchmark : run performance benchmark\n";
        cerr << "\nInteractive mode queries:\n";
        cerr << "  Regular (OR):  machine learning\n";
        cerr << "  AND query:     +neural network\n";
        cerr << "  Phrase:        \"deep learning\"\n";
        return 1;
    }
    
    string query_str;
    string and_query_str;
    string phrase_str;
    size_t top_k = 10;
    bool interactive = true;
    bool run_benchmark = false;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            index_directory = argv[++i];
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
        } else if (arg == "--benchmark") {
            run_benchmark = true;
            interactive = false;
        }
    }
    
    if (index_directory.empty()) {
        cerr << "ERROR: Index directory (-d) required\n";
        return 1;
    }
    
    if (!fs::exists(index_directory)) {
        cerr << "ERROR: Index directory does not exist: " << index_directory << "\n";
        return 1;
    }
    
    cerr << "\n=== Loading Index ===\n";
    load_lexicon(index_directory + "/lexicon.txt");
    load_docid_map(index_directory + "/docid_map.txt");
    load_forward_index(index_directory + "/forward_index.jsonl");
    
    cerr << "\n=== Enhanced Search Engine Ready ===\n";
    cerr << "Total terms: " << lexicon.size() << "\n";
    cerr << "Total docs: " << total_docs << "\n";
    cerr << "Docs with metadata: " << doc_metadata.size() << "\n";
    cerr << "Avg doc length: " << fixed << setprecision(2) << avg_doc_length << "\n";
    cerr << "Barrels: " << NUM_BARRELS << " (lazy loading enabled)\n";
    cerr << "Field boosting: Title=" << TITLE_BOOST << "x, Abstract=" 
         << ABSTRACT_BOOST << "x, Body=" << BODY_BOOST << "x\n";
    cerr << "Recency weight: " << (RECENCY_WEIGHT * 100) << "%\n\n";
    
    if (run_benchmark) {
        cerr << "\n=== Running Performance Benchmark ===\n";
        
        vector<tuple<string, string, string>> test_queries = {
            {"Single term", "machine", "OR"},
            {"Two terms OR", "machine learning", "OR"},
            {"Three terms OR", "deep neural network", "OR"},
            {"Two terms AND", "+machine +learning", "AND"},
            {"Three terms AND", "+deep +neural +network", "AND"},
            {"Phrase 2 words", "\"machine learning\"", "PHRASE"},
            {"Phrase 3 words", "\"deep neural network\"", "PHRASE"},
            {"Common term", "data", "OR"},
            {"Rare term", "immunotherapy", "OR"}
        };
        
        cout << "\nRunning " << test_queries.size() << " test queries...\n\n";
        
        for (const auto& [name, query, type] : test_queries) {
            auto start = chrono::high_resolution_clock::now();
            
            vector<SearchResult> results;
            if (type == "PHRASE") {
                string clean = query.substr(1, query.size() - 2);
                vector<string> terms;
                istringstream iss(clean);
                string term;
                while (iss >> term) terms.push_back(term);
                results = search_phrase_ranked(terms, 10);
            } else if (type == "AND") {
                string clean = query;
                size_t pos = 0;
                while ((pos = clean.find('+')) != string::npos) {
                    clean.replace(pos, 1, "");
                }
                vector<string> terms;
                istringstream iss(clean);
                string term;
                while (iss >> term) terms.push_back(term);
                results = search_query_and_ranked(terms, 10);
            } else {
                vector<string> terms;
                istringstream iss(query);
                string term;
                while (iss >> term) terms.push_back(term);
                results = search_query_ranked(terms, 10);
            }
            
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
            
            perf_stats.record(type, duration.count());
            
            cout << name << " [" << type << "]: " 
                 << (duration.count() / 1000.0) << " ms";
            cout << " (" << results.size() << " results)";
            
            // Check targets
            double ms = duration.count() / 1000.0;
            if (type == "OR" && ms < 100) cout << " ✅";
            else if (type == "AND" && ms < 200) cout << " ✅";
            else if (type == "PHRASE" && ms < 300) cout << " ✅";
            else cout << " ⚠️";
            
            cout << "\n";
        }
        
        perf_stats.report();
        
        // Memory usage report
        size_t loaded_barrels = 0;
        size_t total_barrel_memory = 0;
        for (int i = 0; i < NUM_BARRELS; i++) {
            if (barrel_loaded[i]) {
                loaded_barrels++;
                total_barrel_memory += barrel_data[i].size();
            }
        }
        
        cerr << "\n=== Memory Usage ===\n";
        cerr << "Barrels loaded: " << loaded_barrels << "/" << NUM_BARRELS << "\n";
        cerr << "Barrel memory: " << (total_barrel_memory / (1024.0 * 1024.0)) << " MB\n";
        
        return 0;
    }
    
    if (interactive) {
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
            
            bool is_phrase = false;
            bool is_and = false;
            
            if (line.size() >= 2 && line.front() == '"' && line.back() == '"') {
                is_phrase = true;
                line = line.substr(1, line.size() - 2);
            } else if (!line.empty() && line[0] == '+') {
                is_and = true;
                line = line.substr(1);
            }
            
            vector<string> terms;
            istringstream iss(line);
            string term;
            while (iss >> term) {
                terms.push_back(term);
            }
            
            if (terms.empty()) continue;
            
            auto start = chrono::high_resolution_clock::now();
            vector<SearchResult> results;
            string query_type;
            
            if (is_phrase) {
                cerr << "  [Phrase query]\n";
                results = search_phrase_ranked(terms, top_k);
                query_type = "PHRASE";
            } else if (is_and) {
                cerr << "  [AND query]\n";
                results = search_query_and_ranked(terms, top_k);
                query_type = "AND";
            } else {
                cerr << "  [OR query]\n";
                results = search_query_ranked(terms, top_k);
                query_type = "OR";
            }
            
            auto end = chrono::high_resolution_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
            
            perf_stats.record(query_type, duration.count() * 1000); // convert to microseconds
            
            cout << "\nFound " << results.size() << " results in " 
                 << duration.count() << " ms\n\n";
            
            for (size_t i = 0; i < results.size(); i++) {
                cout << (i+1) << ". [Final: " << fixed << setprecision(2) 
                     << results[i].final_score 
                     << " | BM25: " << results[i].bm25_score
                     << " | Recency: " << results[i].recency_score
                     << "]" << "\n";
                cout << "   Doc: " << results[i].orig_docid << "\n";
                if (!results[i].title.empty()) {
                    cout << "   Title: " << results[i].title << "\n";
                }
                if (!results[i].pub_date.empty()) {
                    cout << "   Date: " << results[i].pub_date << "\n";
                }
                if (!results[i].term_freqs.empty()) {
                    cout << "   Terms: ";
                    for (const auto& tf : results[i].term_freqs) {
                        cout << tf.first << "(" << tf.second << ") ";
                    }
                    cout << "\n";
                }
                cout << "\n";
            }
        }
        
        cout << "\n";
        perf_stats.report();
        
    } else {
        vector<SearchResult> results;
        auto start = chrono::high_resolution_clock::now();
        
        if (!phrase_str.empty()) {
            vector<string> terms;
            istringstream iss(phrase_str);
            string term;
            while (iss >> term) terms.push_back(term);
            cerr << "Executing phrase query...\n";
            results = search_phrase_ranked(terms, top_k);
        } else if (!and_query_str.empty()) {
            vector<string> terms;
            istringstream iss(and_query_str);
            string term;
            while (iss >> term) terms.push_back(term);
            cerr << "Executing AND query...\n";
            results = search_query_and_ranked(terms, top_k);
        } else {
            vector<string> terms;
            istringstream iss(query_str);
            string term;
            while (iss >> term) terms.push_back(term);
            cerr << "Executing OR query...\n";
            results = search_query_ranked(terms, top_k);
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cout << "\nResults (" << duration.count() << " ms):\n\n";
        for (size_t i = 0; i < results.size(); i++) {
            cout << (i+1) << ". [Final: " << fixed << setprecision(2) 
                 << results[i].final_score 
                 << " | BM25: " << results[i].bm25_score
                 << " | Recency: " << results[i].recency_score
                 << "] " << results[i].orig_docid;
            if (!results[i].title.empty()) {
                cout << " - " << results[i].title;
            }
            cout << "\n";
        }
    }
    
    return 0;
}