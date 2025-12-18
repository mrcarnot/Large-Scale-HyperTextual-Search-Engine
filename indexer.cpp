// indexer.cpp - FIXED VERSION with barrels and memory tracking
// Compile: g++ -std=c++17 -O2 indexer.cpp -o indexer

#include <bits/stdc++.h>
#include <filesystem>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

namespace fs = std::filesystem;
using namespace std;
using namespace rapidjson;

// ==================== MEMORY TRACKER ====================
struct MemoryTracker {
    size_t current_bytes = 0;
    size_t peak_bytes = 0;
    size_t block_limit = 256 * 1024 * 1024; // 256 MB per block
    size_t total_processed = 0;
    
    void add(size_t bytes) {
        current_bytes += bytes;
        peak_bytes = max(peak_bytes, current_bytes);
        total_processed += bytes;
    }
    
    void reset() {
        current_bytes = 0;
    }
    
    bool should_flush() const {
        return current_bytes >= block_limit;
    }
    
    void report() const {
        cerr << "Memory Stats:\n";
        cerr << "  Peak: " << (peak_bytes / (1024.0 * 1024.0)) << " MB\n";
        cerr << "  Total processed: " << (total_processed / (1024.0 * 1024.0)) << " MB\n";
    }
    
    size_t estimate_posting_size(const vector<uint32_t>& positions) const {
        return sizeof(uint32_t) + positions.size() * sizeof(uint32_t) + 64; // padding
    }
};

// ==================== BARREL CONFIGURATION ====================
const int NUM_BARRELS = 4; // Split index into 4 barrels

// Hash function to assign terms to barrels
int get_barrel_id(const string& term) {
    return abs((int)(hash<string>{}(term))) % NUM_BARRELS;
}

// ==================== VByte Encoding ====================
static void vbyte_encode_uint32(uint32_t v, vector<uint8_t> &out) {
    while (true) {
        uint8_t byte = v & 0x7F;
        v >>= 7;
        if (v == 0) {
            byte |= 0x80;
            out.push_back(byte);
            break;
        } else {
            out.push_back(byte);
        }
    }
}

static string lower_copy(const string &s) {
    string t = s;
    for (char &c : t) {
        c = (char)tolower((unsigned char)c);
    }
    return t;
}

static void ensure_dir(const string &d) {
    if (!fs::exists(d)) fs::create_directories(d);
}

// ==================== Data Structures ====================
struct Posting {
    uint32_t docid;
    vector<uint32_t> positions;
};

using BlockDict = unordered_map<string, vector<Posting>>;
using BlockForward = unordered_map<string, vector<pair<string, vector<uint32_t>>>>;

// ==================== Docid Mapping ====================
static unordered_map<string, uint32_t> docid_to_int;
static vector<string> int_to_docid;
static uint32_t next_internal_docid = 1;

static uint32_t get_or_assign_docint(const string &orig) {
    auto it = docid_to_int.find(orig);
    if (it != docid_to_int.end()) return it->second;
    uint32_t id = next_internal_docid++;
    docid_to_int[orig] = id;
    if (int_to_docid.size() <= id) int_to_docid.resize(id + 1);
    int_to_docid[id] = orig;
    return id;
}

// ==================== Parse Cleaned JSONL ====================
static bool parse_cleaned_line(const string &line, string &docid_out, 
                               vector<pair<string, vector<uint32_t>>> &doc_terms) {
    doc_terms.clear();
    Document doc;
    doc.Parse(line.c_str());
    if (doc.HasParseError()) return false;
    if (!doc.HasMember("docid")) return false;
    
    if (doc["docid"].IsString()) docid_out = doc["docid"].GetString();
    else if (doc["docid"].IsUint64()) docid_out = to_string(doc["docid"].GetUint64());
    else if (doc["docid"].IsInt64()) docid_out = to_string(doc["docid"].GetInt64());
    else if (doc["docid"].IsInt()) docid_out = to_string(doc["docid"].GetInt());
    else if (doc["docid"].IsUint()) docid_out = to_string(doc["docid"].GetUint());
    else return false;

    unordered_map<string, vector<uint32_t>> agg;
    if (doc.HasMember("fields") && doc["fields"].IsArray()) {
        for (auto &f : doc["fields"].GetArray()) {
            if (!f.IsObject()) continue;
            if (!f.HasMember("tokens") || !f["tokens"].IsArray()) continue;
            for (auto &t : f["tokens"].GetArray()) {
                if (!t.IsObject()) continue;
                if (!t.HasMember("term") || !t.HasMember("pos")) continue;
                string term = t["term"].GetString();
                term = lower_copy(term);
                uint32_t pos = 0;
                if (t["pos"].IsUint()) pos = t["pos"].GetUint();
                else if (t["pos"].IsUint64()) pos = (uint32_t)t["pos"].GetUint64();
                else if (t["pos"].IsInt()) pos = (uint32_t)t["pos"].GetInt();
                else pos = 0;
                agg[term].push_back(pos);
            }
        }
    }
    doc_terms.reserve(agg.size());
    for (auto &p : agg) {
        auto vec = p.second;
        sort(vec.begin(), vec.end());
        doc_terms.emplace_back(p.first, std::move(vec));
    }
    return true;
}

// ==================== Flush Block to Disk ====================
static void flush_block_to_disk(const BlockDict &dict, const BlockForward &forward, 
                                const string &outdir, size_t block_id) {
    string invname = outdir + "/block_" + to_string(block_id) + ".inv";
    string fwdname = outdir + "/block_" + to_string(block_id) + ".fwd.jsonl";

    ofstream invofs(invname);
    if (!invofs) { cerr << "Cannot open " << invname << " for writing\n"; exit(1); }
    for (const auto &it : dict) {
        const string &term = it.first;
        const vector<Posting> &plist = it.second;
        invofs << term << '\t';
        bool firstDoc = true;
        for (const auto &p : plist) {
            if (!firstDoc) invofs << ';';
            firstDoc = false;
            invofs << p.docid << ':';
            for (size_t i = 0; i < p.positions.size(); ++i) {
                if (i) invofs << ',';
                invofs << p.positions[i];
            }
        }
        invofs << '\n';
    }
    invofs.close();

    ofstream fw(fwdname);
    if (!fw) { cerr << "Cannot open " << fwdname << " for writing\n"; exit(1); }
    for (const auto &d : forward) {
        fw << "{\"docid\":\"";
        for (char c : d.first) {
            if (c == '"' || c == '\\') fw << '\\' << c;
            else fw << c;
        }
        fw << "\",\"postings\":[";
        bool first = true;
        for (const auto &tp : d.second) {
            if (!first) fw << ",";
            first = false;
            string term = tp.first;
            fw << "{\"term\":\"";
            for (char c : term) {
                if (c == '"' || c == '\\') fw << '\\' << c;
                else fw << c;
            }
            fw << "\",\"positions\":[";
            for (size_t i = 0; i < tp.second.size(); ++i) {
                if (i) fw << ",";
                fw << tp.second[i];
            }
            fw << "]}";
        }
        fw << "]}\n";
    }
    fw.close();
}

// ==================== Merge Blocks into Barrels ====================
struct LexiconEntry {
    uint32_t wordID;
    string term;
    uint32_t doc_freq;
    uint64_t term_freq;
    uint64_t offset;
    uint64_t bytes;
    int barrel_id;
};

static void merge_blocks_into_barrels(const string &outdir, size_t num_blocks) {
    struct ReaderState {
        ifstream ifs;
        string current_line;
        string term;
        bool valid;
    };
    
    vector<ReaderState> readers;
    readers.reserve(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
        string name = outdir + "/block_" + to_string(i) + ".inv";
        ReaderState st;
        st.ifs.open(name);
        if (!st.ifs.is_open()) {
            cerr << "Cannot open block file " << name << "\n";
            exit(1);
        }
        st.valid = false;
        readers.push_back(move(st));
    }

    auto read_line_state = [&](ReaderState &rs)->bool {
        if (!rs.ifs.good()) { rs.valid = false; return false; }
        string line;
        if (!getline(rs.ifs, line)) { rs.valid = false; return false; }
        rs.current_line = line;
        size_t pos = line.find('\t');
        if (pos == string::npos) rs.term = line;
        else rs.term = line.substr(0, pos);
        rs.valid = true;
        return true;
    };

    for (auto &r : readers) read_line_state(r);

    // Open barrel files
    vector<ofstream> barrel_files(NUM_BARRELS);
    for (int i = 0; i < NUM_BARRELS; i++) {
        string barrel_path = outdir + "/barrel_" + to_string(i) + ".bin";
        barrel_files[i].open(barrel_path, ios::binary);
        if (!barrel_files[i]) {
            cerr << "Cannot open barrel " << barrel_path << "\n";
            exit(1);
        }
    }

    vector<LexiconEntry> lexicon;
    lexicon.reserve(1000000);
    uint32_t global_wordid = 0;

    while (true) {
        string min_term;
        bool any = false;
        for (auto &r : readers) {
            if (!r.valid) continue;
            if (!any || r.term < min_term) { min_term = r.term; any = true; }
        }
        if (!any) break;

        vector<string> block_lines;
        for (auto &r : readers) {
            if (!r.valid) continue;
            if (r.term == min_term) {
                block_lines.push_back(r.current_line);
                read_line_state(r);
            }
        }

        // Merge postings
        unordered_map<uint32_t, vector<uint32_t>> merged;
        uint64_t term_freq = 0;
        for (auto &ln : block_lines) {
            size_t tab = ln.find('\t');
            string rest = (tab == string::npos) ? string() : ln.substr(tab + 1);
            size_t idx = 0;
            while (idx < rest.size()) {
                size_t colon = rest.find(':', idx);
                if (colon == string::npos) break;
                string docid_s = rest.substr(idx, colon - idx);
                uint32_t docid = (uint32_t)stoul(docid_s);
                idx = colon + 1;
                size_t semi = rest.find(';', idx);
                string poslist = (semi == string::npos) ? rest.substr(idx) : rest.substr(idx, semi - idx);
                size_t p = 0;
                vector<uint32_t> posvec;
                while (p < poslist.size()) {
                    size_t comma = poslist.find(',', p);
                    string ps = (comma == string::npos) ? poslist.substr(p) : poslist.substr(p, comma - p);
                    if (!ps.empty()) {
                        uint32_t pv = (uint32_t)stoul(ps);
                        posvec.push_back(pv);
                        term_freq++;
                    }
                    if (comma == string::npos) break;
                    p = comma + 1;
                }
                auto &v = merged[docid];
                v.insert(v.end(), posvec.begin(), posvec.end());
                if (semi == string::npos) break;
                idx = semi + 1;
            }
        }

        vector<pair<uint32_t, vector<uint32_t>>> postings;
        postings.reserve(merged.size());
        for (auto &p : merged) {
            auto vec = p.second;
            sort(vec.begin(), vec.end());
            postings.emplace_back(p.first, std::move(vec));
        }
        sort(postings.begin(), postings.end(), [](auto &a, auto &b){ return a.first < b.first; });

        // Encode postings
        vector<uint8_t> encoded;
        uint32_t last_docid = 0;
        uint32_t doc_count = (uint32_t)postings.size();
        vbyte_encode_uint32(doc_count, encoded);
        uint32_t df = doc_count;
        for (auto &pd : postings) {
            uint32_t docid = pd.first;
            uint32_t doc_delta = docid - last_docid;
            last_docid = docid;
            vbyte_encode_uint32(doc_delta, encoded);
            uint32_t tf = (uint32_t)pd.second.size();
            vbyte_encode_uint32(tf, encoded);
            uint32_t last_pos = 0;
            for (uint32_t pos : pd.second) {
                uint32_t pos_delta = pos - last_pos;
                last_pos = pos;
                vbyte_encode_uint32(pos_delta, encoded);
            }
        }

        // Determine barrel and write
        int barrel_id = get_barrel_id(min_term);
        uint64_t offset = (uint64_t)barrel_files[barrel_id].tellp();
        barrel_files[barrel_id].write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        uint64_t bytes_written = encoded.size();

        ++global_wordid;
        LexiconEntry ent;
        ent.wordID = global_wordid;
        ent.term = min_term;
        ent.doc_freq = df;
        ent.term_freq = term_freq;
        ent.offset = offset;
        ent.bytes = bytes_written;
        ent.barrel_id = barrel_id;
        lexicon.push_back(move(ent));
    }

    for (auto &f : barrel_files) f.close();

    // Write lexicon with barrel info
    string lexpath = outdir + "/lexicon.txt";
    ofstream lexofs(lexpath);
    if (!lexofs) { cerr << "Cannot write lexicon\n"; exit(1); }
    for (auto &le : lexicon) {
        string t = le.term;
        for (char &c : t) if (c=='\t' || c=='\n' || c=='\r') c = ' ';
        lexofs << le.wordID << '\t' << t << '\t' << le.doc_freq << '\t' 
               << le.term_freq << '\t' << le.offset << '\t' << le.bytes << '\t'
               << le.barrel_id << '\n';
    }
    lexofs.close();

    string tlist = outdir + "/terms_list.txt";
    ofstream tl(tlist);
    if (!tl) { cerr << "Cannot write terms_list\n"; exit(1); }
    for (auto &le : lexicon) {
        tl << le.term << '\t' << le.wordID << '\n';
    }
    tl.close();

    cerr << "Merge done. Total terms: " << lexicon.size() << "\n";
    cerr << "Barrels created: " << NUM_BARRELS << "\n";
}

// ==================== Remap Forward Indices ====================
static unordered_map<string, uint32_t> load_term_to_id(const string &lex_terms_path) {
    unordered_map<string,uint32_t> map;
    ifstream ifs(lex_terms_path);
    if (!ifs) { cerr << "Cannot read " << lex_terms_path << "\n"; exit(1); }
    string line;
    while (getline(ifs, line)) {
        size_t tab = line.find('\t');
        if (tab==string::npos) continue;
        string term = line.substr(0, tab);
        uint32_t wid = (uint32_t)stoul(line.substr(tab+1));
        map[term] = wid;
    }
    return map;
}

static void remap_forward_indices(const string &outdir, size_t num_blocks) {
    auto t2id = load_term_to_id(outdir + "/terms_list.txt");
    string fout = outdir + "/forward_index.jsonl";
    ofstream ofs(fout);
    if (!ofs) { cerr << "Cannot write forward index\n"; exit(1); }

    for (size_t b = 0; b < num_blocks; ++b) {
        string fname = outdir + "/block_" + to_string(b) + ".fwd.jsonl";
        ifstream ifs(fname);
        if (!ifs) { cerr << "Warning: cannot open block forward " << fname << " (skipping)\n"; continue; }
        string line;
        while (getline(ifs, line)) {
            Document doc;
            doc.Parse(line.c_str());
            if (doc.HasParseError() || !doc.HasMember("docid")) continue;
            string orig_docid = doc["docid"].GetString();
            vector<tuple<uint32_t,uint32_t,vector<uint32_t>>> outpost;

            if (doc.HasMember("postings") && doc["postings"].IsArray()) {
                for (auto &p : doc["postings"].GetArray()) {
                    if (!p.IsObject()) continue;
                    if (!p.HasMember("term") || !p.HasMember("positions")) continue;
                    string term = p["term"].GetString();
                    term = lower_copy(term);
                    vector<uint32_t> positions;
                    for (auto &vp : p["positions"].GetArray()) {
                        if (vp.IsUint()) positions.push_back(vp.GetUint());
                        else if (vp.IsUint64()) positions.push_back((uint32_t)vp.GetUint64());
                        else if (vp.IsInt()) positions.push_back((uint32_t)vp.GetInt());
                    }
                    auto it = t2id.find(term);
                    if (it != t2id.end()) {
                        uint32_t wid = it->second;
                        outpost.emplace_back(wid, (uint32_t)positions.size(), positions);
                    }
                }
            }

            ofs << "{\"docid\":\"" << orig_docid << "\",\"postings\":[";
            for (size_t i = 0; i < outpost.size(); ++i) {
                auto &tp = outpost[i];
                uint32_t wid, freq; vector<uint32_t> pos;
                tie(wid, freq, pos) = tp;
                if (i) ofs << ",";
                ofs << "{\"wordid\":" << wid << ",\"freq\":" << freq << ",\"positions\":[";
                for (size_t k = 0; k < pos.size(); ++k) {
                    if (k) ofs << ",";
                    ofs << pos[k];
                }
                ofs << "]}";
            }
            ofs << "]}\n";
        }
    }
    ofs.close();
    cerr << "Forward index remapped & written to " << fout << "\n";
}

// ==================== Main Driver ====================
static void usage(const char *prog) {
    cerr << "Usage: " << prog << " -i cleaned.jsonl -o outdir [--block-memory MB]\n";
}

int main(int argc, char **argv) {
    string input;
    string outdir = "index_out";
    size_t block_memory_mb = 256;
    
    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-i" && i+1 < argc) input = argv[++i];
        else if (a == "-o" && i+1 < argc) outdir = argv[++i];
        else if (a == "--block-memory" && i+1 < argc) block_memory_mb = stoul(argv[++i]);
        else { usage(argv[0]); return 1; }
    }
    if (input.empty()) { usage(argv[0]); return 1; }

    ensure_dir(outdir);

    ifstream ifs(input);
    if (!ifs) { cerr << "Cannot open input " << input << "\n"; return 1; }

    BlockDict dict;
    BlockForward fwd;
    MemoryTracker mem_tracker;
    mem_tracker.block_limit = block_memory_mb * 1024 * 1024;
    
    size_t docs_in_block = 0;
    size_t block_id = 0;
    size_t total_docs = 0;

    cerr << "\n=== Indexing with Memory Tracking ===\n";
    cerr << "Block memory limit: " << block_memory_mb << " MB\n";
    cerr << "Barrels: " << NUM_BARRELS << "\n\n";

    string line;
    while (getline(ifs, line)) {
        ++total_docs;
        vector<pair<string, vector<uint32_t>>> doc_terms;
        string orig_docid;
        if (!parse_cleaned_line(line, orig_docid, doc_terms)) {
            cerr << "Warning: skipping malformed line " << total_docs << "\n";
            continue;
        }

        uint32_t doc_int = get_or_assign_docint(orig_docid);

        vector<pair<string, vector<uint32_t>>> tmp;
        tmp.swap(doc_terms);
        fwd[orig_docid] = tmp;

        for (auto &tp : fwd[orig_docid]) {
            const string &term = tp.first;
            auto &positions = tp.second;
            
            // Track memory
            mem_tracker.add(mem_tracker.estimate_posting_size(positions));
            
            auto &plist = dict[term];
            if (plist.empty() || plist.back().docid != doc_int) {
                Posting p;
                p.docid = doc_int;
                p.positions = positions;
                plist.push_back(move(p));
            } else {
                auto &lastp = plist.back();
                lastp.positions.insert(lastp.positions.end(), positions.begin(), positions.end());
            }
        }

        docs_in_block++;
        
        if (mem_tracker.should_flush()) {
            cerr << "Flushing block " << block_id << " (memory: " 
                 << (mem_tracker.current_bytes / (1024.0 * 1024.0)) << " MB, docs: " 
                 << docs_in_block << ")\n";
            flush_block_to_disk(dict, fwd, outdir, block_id);
            dict.clear();
            fwd.clear();
            mem_tracker.reset();
            docs_in_block = 0;
            ++block_id;
        }
    }

    if (!dict.empty() || !fwd.empty()) {
        cerr << "Flushing final block " << block_id << " (memory: " 
             << (mem_tracker.current_bytes / (1024.0 * 1024.0)) << " MB, docs: " 
             << docs_in_block << ")\n";
        flush_block_to_disk(dict, fwd, outdir, block_id);
        dict.clear();
        fwd.clear();
        ++block_id;
    }
    ifs.close();

    size_t num_blocks = block_id;
    cerr << "\nTotal documents processed: " << total_docs << ", blocks: " << num_blocks << "\n\n";
    mem_tracker.report();

    cerr << "\n=== Merging blocks into barrels ===\n";
    merge_blocks_into_barrels(outdir, num_blocks);

    cerr << "\n=== Remapping forward indices ===\n";
    remap_forward_indices(outdir, num_blocks);

    string docmap = outdir + "/docid_map.txt";
    ofstream dm(docmap);
    if (!dm) { cerr << "Cannot write docid_map\n"; }
    else {
        for (auto &p : docid_to_int) dm << p.first << '\t' << p.second << '\n';
        dm.close();
    }

    cerr << "\n=== Indexing Complete ===\n";
    cerr << "Output directory: " << outdir << "\n";
    cerr << "Barrels: " << NUM_BARRELS << " files (barrel_0.bin to barrel_" 
         << (NUM_BARRELS-1) << ".bin)\n";
    cerr << "Lexicon: lexicon.txt (includes barrel_id column)\n";
    
    return 0;
}
