#include <bits/stdc++.h>
using namespace std;

// ------------------- Variable Byte Encoding -------------------
vector<uint8_t> vbEncodeNumber(uint32_t n) {
    vector<uint8_t> tmp;
    do {
        tmp.push_back(n & 0x7F); // store 7-bit chunk
        n >>= 7;
    } while (n > 0);

    // set continuation bit for all but the last byte
    for (size_t i = 1; i < tmp.size(); ++i) {
        tmp[i] |= 0x80;
    }

    reverse(tmp.begin(), tmp.end());
    return tmp;
}

void vbEncodeList(const vector<uint32_t>& numbers, vector<uint8_t>& out) {
    for (auto n : numbers) {
        auto bytes = vbEncodeNumber(n);
        out.insert(out.end(), bytes.begin(), bytes.end());
    }
}

// ------------------- Inverted Index Merge -------------------
struct Posting {
    uint32_t docID;
    vector<uint32_t> positions;
};

// helper: trim leading/trailing whitespace
static inline string trim(const string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// read a single block into memory
unordered_map<string, vector<Posting>> readBlock(const string &filename) {
    unordered_map<string, vector<Posting>> blockIndex;
    ifstream fin(filename);
    if (!fin) {
        cerr << "Warning: cannot open block file '" << filename << "'. Skipping.\n";
        return blockIndex;
    }

    string line;
    while (getline(fin, line)) {
        line = trim(line);
        if (line.empty()) continue;

        stringstream ss(line);
        string term;
        if (!(ss >> term)) continue;

        string postings_str;
        getline(ss, postings_str); 
        postings_str = trim(postings_str);
        if (postings_str.empty()) continue;

        vector<Posting> postings;
        stringstream pss(postings_str);
        string doc_post;
        while (getline(pss, doc_post, ';')) {
            doc_post = trim(doc_post);
            if (doc_post.empty()) continue;

            size_t colon = doc_post.find(':');
            if (colon == string::npos) continue;

            string docid_str = trim(doc_post.substr(0, colon));
            string pos_str = trim(doc_post.substr(colon + 1));
            if (docid_str.empty()) continue; // removed redundant pos_str check

            uint32_t docID = 0;
            try {
                docID = static_cast<uint32_t>(stoul(docid_str));
            } catch (...) { continue; }

            vector<uint32_t> positions;
            if (!pos_str.empty()) { // only parse positions if not empty
                stringstream pos_ss(pos_str);
                string pos;
                while (getline(pos_ss, pos, ',')) {
                    pos = trim(pos);
                    if (pos.empty()) continue;
                    try { positions.push_back(static_cast<uint32_t>(stoul(pos))); } catch (...) {}
                }
            }

            postings.push_back({docID, positions}); // simplified: always push Posting
        }

        if (!postings.empty()) {
            blockIndex[term].insert(blockIndex[term].end(), postings.begin(), postings.end());
        }
    }
    return blockIndex;
}

int main(int argc, char* argv[]) {
    vector<string> blocks;
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) blocks.push_back(argv[i]);
    } else {
        blocks = {"block_0.inv", "block_1.inv"};
    }

    unordered_map<string, vector<Posting>> finalIndex;
    for (auto &blk : blocks) {
        auto blockIndex = readBlock(blk);
        for (auto &kv : blockIndex) {
            finalIndex[kv.first].insert(finalIndex[kv.first].end(),
                                        kv.second.begin(), kv.second.end());
        }
    }

    // Sort postings per term by docID and merge duplicates
    for (auto &kv : finalIndex) {
        auto &postings = kv.second;
        sort(postings.begin(), postings.end(), [](const Posting &a, const Posting &b) {
            return a.docID < b.docID;
        });

        vector<Posting> merged;
        for (const auto &p : postings) {
            if (!merged.empty() && merged.back().docID == p.docID) {
                merged.back().positions.insert(merged.back().positions.end(),
                                              p.positions.begin(), p.positions.end());
            } else {
                merged.push_back(p);
            }
        }

        for (auto &p : merged) {
            sort(p.positions.begin(), p.positions.end());
            p.positions.erase(unique(p.positions.begin(), p.positions.end()), p.positions.end());
        }

        postings.swap(merged);
    }

    ofstream lexicon("lexicon.txt");
    ofstream postings_out("postings.bin", ios::binary);
    if (!lexicon || !postings_out) {
        cerr << "Error: cannot open output files.\n";
        return 1;
    }

    uint64_t offset = 0;
    vector<string> terms;
    for (auto &kv : finalIndex) terms.push_back(kv.first);
    sort(terms.begin(), terms.end());

    for (const auto &term : terms) {
        const auto &postings_list = finalIndex[term];
        lexicon << term << " " << postings_list.size() << " " << offset << "\n";

        vector<uint32_t> numbers;
        uint32_t last_doc = 0;
        for (const auto &p : postings_list) {
            numbers.push_back(p.docID - last_doc);
            last_doc = p.docID;

            uint32_t tf = static_cast<uint32_t>(p.positions.size());
            numbers.push_back(tf);

            uint32_t last_pos = 0;
            for (auto pos : p.positions) {
                numbers.push_back(pos - last_pos);
                last_pos = pos;
            }
        }

        vector<uint8_t> encoded;
        vbEncodeList(numbers, encoded);
        postings_out.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        offset += encoded.size();
    }

    lexicon.close();
    postings_out.close();

    cout << "Merged " << blocks.size() << " block(s) into final inverted index.\n";
    cout << "Wrote lexicon.txt and postings.bin (total bytes: " << offset << ").\n";
    return 0;
}
