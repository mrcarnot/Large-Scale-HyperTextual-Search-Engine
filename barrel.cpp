#include <bits/stdc++.h>
using namespace std;

// ------------------- Variable Byte Encoding -------------------
vector<uint8_t> vbEncodeNumber(uint32_t n) {
    vector<uint8_t> bytes;
    while (true) {
        uint8_t b = n % 128;
        if (!bytes.empty()) b |= 0x80; // set continuation bit for all but last
        bytes.insert(bytes.begin(), b);
        if (n < 128) break;
        n /= 128;
    }
    return bytes;
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

// read a single block into memory
unordered_map<string, vector<Posting>> readBlock(const string &filename) {
    unordered_map<string, vector<Posting>> blockIndex;
    ifstream fin(filename);
    string line;
    while (getline(fin, line)) {
        // Example format: covid 1:4,9;3:5,8
        stringstream ss(line);
        string term;
        ss >> term;
        string postings_str;
        getline(ss, postings_str);
        vector<Posting> postings;
        stringstream pss(postings_str);
        string doc_post;
        while (getline(pss, doc_post, ';')) {
            size_t colon = doc_post.find(':');
            if (colon == string::npos) continue;
            uint32_t docID = stoi(doc_post.substr(0, colon));
            string pos_str = doc_post.substr(colon + 1);
            vector<uint32_t> positions;
            stringstream pos_ss(pos_str);
            string pos;
            while (getline(pos_ss, pos, ',')) positions.push_back(stoi(pos));
            postings.push_back({docID, positions});
        }
        blockIndex[term] = postings;
    }
    return blockIndex;
}

int main() {
    vector<string> blocks = {"block_0.inv", "block_1.inv"}; // add all your blocks
    unordered_map<string, vector<Posting>> finalIndex;

    // Merge all blocks
    for (auto &blk : blocks) {
        auto blockIndex = readBlock(blk);
        for (auto &[term, postings] : blockIndex) {
            finalIndex[term].insert(finalIndex[term].end(), postings.begin(), postings.end());
        }
    }

    // Sort postings per term by docID
    for (auto &[term, postings] : finalIndex) {
        sort(postings.begin(), postings.end(), [](const Posting &a, const Posting &b) {
            return a.docID < b.docID;
        });
    }

    // Write lexicon and postings.bin
    ofstream lexicon("lexicon.txt");
    ofstream postings("postings.bin", ios::binary);

    uint64_t offset = 0;
    for (auto &[term, postings_list] : finalIndex) {
        lexicon << term << " " << postings_list.size() << " " << offset << "\n";

        vector<uint32_t> doc_deltas;
        uint32_t last_doc = 0;

        for (auto &p : postings_list) {
            doc_deltas.push_back(p.docID - last_doc);
            last_doc = p.docID;

            for (auto &pos : p.positions) doc_deltas.push_back(pos); // positions
        }

        vector<uint8_t> encoded;
        vbEncodeList(doc_deltas, encoded);

        postings.write((char*)encoded.data(), encoded.size());
        offset += encoded.size();
    }

    lexicon.close();
    postings.close();

    cout << "Merged " << blocks.size() << " blocks into final inverted index.\n";
    return 0;
}
