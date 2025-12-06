#include <bits/stdc++.h>
using namespace std;

// ------------------- Variable Byte Encoding -------------------
// Standard VB: split number into 7-bit chunks, LSB first, set continuation bit (0x80)
// on all bytes except the last. We return bytes in the final write order (big-endian),
// i.e., most-significant 7-bit chunk first.
vector<uint8_t> vbEncodeNumber(uint32_t n) {
    // collect LSB-first
    vector<uint8_t> tmp;
    do {
        uint8_t b = n & 0x7F;
        tmp.push_back(b);
        n >>= 7;
    } while (n > 0);

    // set continuation bit on all but the last (which is currently tmp[0] = least significant chunk)
    for (size_t i = 1; i < tmp.size(); ++i) {
        tmp[i] |= 0x80; // mark continuation for less significant chunks when reversed
    }

    // reverse to get most-significant first
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

        // Example expected format (one possible): "covid 1:4,9;3:5,8"
        // We'll split first token as term, rest as postings string.
        stringstream ss(line);
        string term;
        if (!(ss >> term)) continue;

        string postings_str;
        getline(ss, postings_str); // remainder of line
        postings_str = trim(postings_str);
        if (postings_str.empty()) {
            // no postings found; skip
            continue;
        }

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
            if (docid_str.empty() || pos_str.empty()) continue;

            uint32_t docID = 0;
            try {
                docID = static_cast<uint32_t>(stoul(docid_str));
            } catch (...) {
                continue;
            }

            vector<uint32_t> positions;
            stringstream pos_ss(pos_str);
            string pos;
            while (getline(pos_ss, pos, ',')) {
                pos = trim(pos);
                if (pos.empty()) continue;
                try {
                    uint32_t p = static_cast<uint32_t>(stoul(pos));
                    positions.push_back(p);
                } catch (...) {
                    // skip invalid position token
                }
            }

            if (!positions.empty()) {
                postings.push_back({docID, positions});
            } else {
                // If there are no positions but docID exists, push an empty positions vector
                postings.push_back({docID, {}});
            }
        }

        if (!postings.empty()) {
            // If the same term appears multiple times in the block file, append postings rather than overwrite
            auto &vec = blockIndex[term];
            vec.insert(vec.end(), postings.begin(), postings.end());
        }
    }
    return blockIndex;
}

int main(int argc, char* argv[]) {
    // Accept block files from command line; if none provided, use defaults
    vector<string> blocks;
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) blocks.push_back(argv[i]);
    } else {
        blocks = {"block_0.inv", "block_1.inv"}; // fallback defaults
    }

    unordered_map<string, vector<Posting>> finalIndex;

    // Merge all blocks
    for (auto &blk : blocks) {
        auto blockIndex = readBlock(blk);
        for (auto &kv : blockIndex) {
            const string &term = kv.first;
            const vector<Posting> &postings = kv.second;
            auto &dest = finalIndex[term];
            dest.insert(dest.end(), postings.begin(), postings.end());
        }
    }

    // Sort postings per term by docID and remove duplicates for same doc (if any) by merging positions
    for (auto &kv : finalIndex) {
        auto &postings = kv.second;
        sort(postings.begin(), postings.end(), [](const Posting &a, const Posting &b) {
            return a.docID < b.docID;
        });

        // merge postings for same docID (if duplicate entries exist across blocks)
        vector<Posting> merged;
        for (const auto &p : postings) {
            if (!merged.empty() && merged.back().docID == p.docID) {
                // append positions and later sort/unique them
                merged.back().positions.insert(merged.back().positions.end(), p.positions.begin(), p.positions.end());
            } else {
                merged.push_back(p);
            }
        }
        // sort & unique positions for each posting
        for (auto &p : merged) {
            if (!p.positions.empty()) {
                sort(p.positions.begin(), p.positions.end());
                p.positions.erase(unique(p.positions.begin(), p.positions.end()), p.positions.end());
            }
        }
        postings.swap(merged);
    }

    // Prepare lexicon and postings.bin
    ofstream lexicon("lexicon.txt");
    if (!lexicon) {
        cerr << "Error: cannot open lexicon.txt for writing.\n";
        return 1;
    }
    ofstream postings_out("postings.bin", ios::binary);
    if (!postings_out) {
        cerr << "Error: cannot open postings.bin for writing.\n";
        return 1;
    }

    uint64_t offset = 0;

    // To give deterministic output, iterate over sorted terms
    vector<string> terms;
    terms.reserve(finalIndex.size());
    for (auto &kv : finalIndex) terms.push_back(kv.first);
    sort(terms.begin(), terms.end());

    for (const auto &term : terms) {
        const auto &postings_list = finalIndex[term];
        // write lexicon entry: term docFreq offset
        lexicon << term << " " << postings_list.size() << " " << offset << "\n";

        vector<uint32_t> numbers; // sequence to encode with VB
        numbers.reserve(postings_list.size() * 4);

        uint32_t last_doc = 0;
        for (const auto &p : postings_list) {
            // docID delta
            numbers.push_back(p.docID - last_doc);
            last_doc = p.docID;

            // term frequency (number of positions)
            uint32_t tf = static_cast<uint32_t>(p.positions.size());
            numbers.push_back(tf);

            // positions as gaps (pos1, pos2 - pos1, ...)
            uint32_t last_pos = 0;
            for (auto pos : p.positions) {
                numbers.push_back(pos - last_pos);
                last_pos = pos;
            }
        }

        // encode numbers with variable-byte
        vector<uint8_t> encoded;
        vbEncodeList(numbers, encoded);

        // write to file and update offset
        if (!encoded.empty()) {
            postings_out.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
            offset += encoded.size();
        }
    }

    lexicon.close();
    postings_out.close();

    cout << "Merged " << blocks.size() << " block(s) into final inverted index.\n";
    cout << "Wrote lexicon.txt and postings.bin (total bytes: " << offset << ").\n";
    return 0;
}
