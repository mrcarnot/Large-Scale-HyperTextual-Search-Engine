// indexer.cpp
// Compile:
//   g++ -std=c++17 -O2 indexer.cpp -Irapidjson-master/include -o indexer
//
// SPIMI-style blocked indexer (fixed):
// - supports string docids (PMC...) by assigning stable internal numeric IDs
// - ensures output directory exists
// - consistent lowercasing of terms
// - robust forward-block parsing using RapidJSON
// - outputs: block_*.inv, block_*.fwd.jsonl, postings.bin, lexicon.txt, terms_list.txt, forward_index.jsonl, docid_map.txt

#include <bits/stdc++.h>
#include <filesystem>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

namespace fs = std::filesystem;
using namespace std;
using namespace rapidjson;

// -------------------- utilities --------------------
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

// static string lower_copy(const string &s) {
//     string t = s;
//     for (unsigned char &c : *(unsigned char*)&t[0]) c = (unsigned char)tolower(c);
//     // above trick to modify bytes directly; works for ascii/stemmed tokens
//     return t;
// }
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

// -------------------- data structures --------------------
struct Posting {
    uint32_t docid;               // internal numeric doc id
    vector<uint32_t> positions;
};

using BlockDict = unordered_map<string, vector<Posting>>; // term -> postings (per block)
using BlockForward = unordered_map<string, vector<pair<string, vector<uint32_t>>>>; // original_docid -> [(term, positions)]

// -------------------- global config --------------------
size_t BLOCK_SIZE_DOCS = 10000;
size_t SKIP_INTERVAL = 128;

// -------------------- docid mapping --------------------
static unordered_map<string, uint32_t> docid_to_int;
static vector<string> int_to_docid; // 1-based: int_to_docid[internal_id] = original
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

// -------------------- parsing cleaned.jsonl --------------------
static bool parse_cleaned_line(const string &line, string &docid_out, vector<pair<string, vector<uint32_t>>> &doc_terms) {
    doc_terms.clear();
    Document doc;
    doc.Parse(line.c_str());
    if (doc.HasParseError()) return false;
    if (!doc.HasMember("docid")) return false;
    // Accept string or numeric docid
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
                // normalize term to lowercase to ensure consistency across blocks/forward mapping
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

// -------------------- flush block to disk --------------------
static void flush_block_to_disk(const BlockDict &dict, const BlockForward &forward, const string &outdir, size_t block_id) {
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
        // d.first is original docid string
        fw << "{\"docid\":\"";
        // escape quotes/backslashes in docid if any
        for (char c : d.first) {
            if (c == '"' || c == '\\') fw << '\\' << c;
            else fw << c;
        }
        fw << "\",\"postings\":[";
        bool first = true;
        for (const auto &tp : d.second) {
            if (!first) fw << ",";
            first = false;
            // term is already lowercased in dict building
            string term = tp.first;
            // escape term
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

// -------------------- merge blocks into final postings --------------------
struct LexiconEntry {
    uint32_t wordID;
    string term;
    uint32_t doc_freq;
    uint64_t term_freq;
    uint64_t offset;
    uint64_t bytes;
    string skip_meta;
};

static void merge_blocks(const string &outdir, size_t num_blocks) {
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

    string postings_path = outdir + "/postings.bin";
    ofstream pout(postings_path, ios::binary);
    if (!pout) { cerr << "Cannot open " << postings_path << "\n"; exit(1); }

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

        // merged: internal_docid -> positions
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
                uint32_t docid = (uint32_t)stoul(docid_s); // docid here is internal numeric id we wrote earlier
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
        uint64_t offset = (uint64_t)pout.tellp();
        pout.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        uint64_t bytes_written = encoded.size();

        string skip_meta = "{\"df\":" + to_string(df) + ",\"skip_interval\":" + to_string(SKIP_INTERVAL) + "}";

        ++global_wordid;
        LexiconEntry ent;
        ent.wordID = global_wordid;
        ent.term = min_term;
        ent.doc_freq = df;
        ent.term_freq = term_freq;
        ent.offset = offset;
        ent.bytes = bytes_written;
        ent.skip_meta = skip_meta;
        lexicon.push_back(move(ent));
    }

    pout.close();

    // write lexicon and terms list
    string lexpath = outdir + "/lexicon.txt";
    ofstream lexofs(lexpath);
    if (!lexofs) { cerr << "Cannot write lexicon\n"; exit(1); }
    for (auto &le : lexicon) {
        string t = le.term;
        for (char &c : t) if (c=='\t' || c=='\n' || c=='\r') c = ' ';
        lexofs << le.wordID << '\t' << t << '\t' << le.doc_freq << '\t' << le.term_freq << '\t' << le.offset << '\t' << le.bytes << '\t' << le.skip_meta << '\n';
    }
    lexofs.close();

    string tlist = outdir + "/terms_list.txt";
    ofstream tl(tlist);
    if (!tl) { cerr << "Cannot write terms_list\n"; exit(1); }
    for (auto &le : lexicon) {
        tl << le.term << '\t' << le.wordID << '\n';
    }
    tl.close();

    cerr << "Merge done. Total terms: " << lexicon.size() << ", postings.bin written.\n";
}

// -------------------- remap forward indices (use RapidJSON) --------------------
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
            vector<tuple<uint32_t,uint32_t,vector<uint32_t>>> outpost; // (wordID, freq, positions)

            if (doc.HasMember("postings") && doc["postings"].IsArray()) {
                for (auto &p : doc["postings"].GetArray()) {
                    if (!p.IsObject()) continue;
                    if (!p.HasMember("term") || !p.HasMember("positions")) continue;
                    string term = p["term"].GetString();
                    // term already lowercased when blocks were written, but normalize here too
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
                    } else {
                        // log missing terms for debugging
                        cerr << "[MISSING_TERM] " << term << " (doc " << orig_docid << ")\n";
                    }
                }
            }

            // write remapped forward for this doc (original docid kept)
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

// -------------------- main driver --------------------
static void usage(const char *prog) {
    cerr << "Usage: " << prog << " -i cleaned.jsonl -o outdir [--block-size N] [--skip-interval N]\n";
}

int main(int argc, char **argv) {
    string input;
    string outdir = "index_out";
    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-i" && i+1 < argc) input = argv[++i];
        else if (a == "-o" && i+1 < argc) outdir = argv[++i];
        else if (a == "--block-size" && i+1 < argc) BLOCK_SIZE_DOCS = stoul(argv[++i]);
        else if (a == "--skip-interval" && i+1 < argc) SKIP_INTERVAL = stoul(argv[++i]);
        else { usage(argv[0]); return 1; }
    }
    if (input.empty()) { usage(argv[0]); return 1; }

    ensure_dir(outdir);

    ifstream ifs(input);
    if (!ifs) { cerr << "Cannot open input " << input << "\n"; return 1; }

    BlockDict dict;
    BlockForward fwd;
    size_t docs_in_block = 0;
    size_t block_id = 0;
    size_t total_docs = 0;

    string line;
    while (getline(ifs, line)) {
        ++total_docs;
        vector<pair<string, vector<uint32_t>>> doc_terms;
        string orig_docid;
        if (!parse_cleaned_line(line, orig_docid, doc_terms)) {
            cerr << "Warning: skipping malformed line " << total_docs << "\n";
            continue;
        }

        // assign internal numeric id (stable)
        uint32_t doc_int = get_or_assign_docint(orig_docid);

        // add to block forward (store original docid string)
        vector<pair<string, vector<uint32_t>>> tmp;
        tmp.swap(doc_terms);
        fwd[orig_docid] = tmp;

        // update block dict using internal numeric id
        for (auto &tp : fwd[orig_docid]) {
            const string &term = tp.first; // already lowercased in parse_cleaned_line
            auto &positions = tp.second;
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
        if (docs_in_block >= BLOCK_SIZE_DOCS) {
            cerr << "Flushing block " << block_id << " containing " << docs_in_block << " docs\n";
            flush_block_to_disk(dict, fwd, outdir, block_id);
            dict.clear();
            fwd.clear();
            docs_in_block = 0;
            ++block_id;
        }
    }

    if (!dict.empty() || !fwd.empty()) {
        cerr << "Flushing final block " << block_id << " containing " << docs_in_block << " docs\n";
        flush_block_to_disk(dict, fwd, outdir, block_id);
        dict.clear();
        fwd.clear();
        ++block_id;
    }
    ifs.close();

    size_t num_blocks = block_id;
    cerr << "Total documents processed: " << total_docs << ", blocks: " << num_blocks << "\n";

    // merge blocks -> postings.bin & lexicon
    merge_blocks(outdir, num_blocks);

    // remap forward block files using lexicon -> forward_index.jsonl
    remap_forward_indices(outdir, num_blocks);

    // write docid_map (original -> internal)
    string docmap = outdir + "/docid_map.txt";
    ofstream dm(docmap);
    if (!dm) { cerr << "Cannot write docid_map\n"; }
    else {
        // write original_docid \t internal_id
        for (auto &p : docid_to_int) dm << p.first << '\t' << p.second << '\n';
        dm.close();
    }

    cerr << "Indexing complete. Files written to: " << outdir << "\n";
    return 0;
}
