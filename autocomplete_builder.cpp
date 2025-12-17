// autocomplete_builder.cpp
// Builds autocomplete index from terms_list.txt and lexicon.txt
// Compile: g++ -std=c++17 -O2 autocomplete_builder.cpp -o autocomplete_builder

#include <bits/stdc++.h>
#include <fstream>
#include <algorithm>

using namespace std;

struct TermInfo {
    string term;
    uint32_t word_id;
    uint32_t doc_freq;
    uint64_t term_freq;
    double popularity_score;
};

// Load terms with their frequencies from lexicon
static vector<TermInfo> load_terms_with_freq(const string& lexicon_path) {
    vector<TermInfo> terms;
    ifstream ifs(lexicon_path);
    
    if (!ifs) {
        cerr << "ERROR: Cannot open lexicon: " << lexicon_path << "\n";
        exit(1);
    }
    
    string line;
    size_t line_num = 0;
    while (getline(ifs, line)) {
        line_num++;
        istringstream iss(line);
        
        TermInfo info;
        string skip_meta;
        uint64_t offset, bytes;
        
        if (iss >> info.word_id >> info.term >> info.doc_freq >> 
                   info.term_freq >> offset >> bytes) {
            // Popularity score: combination of doc_freq and term_freq
            // More documents = more important
            // More occurrences = more common
            info.popularity_score = log(1 + info.doc_freq) * log(1 + info.term_freq);
            terms.push_back(info);
        } else {
            if (line_num <= 5) {
                cerr << "Warning: Malformed lexicon line " << line_num << "\n";
            }
        }
    }
    
    cerr << "Loaded " << terms.size() << " terms from lexicon\n";
    return terms;
}

// Build prefix map: prefix -> list of terms
static void build_autocomplete_index(const vector<TermInfo>& terms, 
                                      const string& output_path,
                                      int max_prefix_len = 15,
                                      int top_k_per_prefix = 20) {
    
    // Map: prefix -> vector of (popularity_score, term, word_id, doc_freq, term_freq)
    unordered_map<string, vector<tuple<double, string, uint32_t, uint32_t, uint64_t>>> prefix_map;
    
    cerr << "Building prefix index...\n";
    size_t total_prefixes = 0;
    
    for (const auto& info : terms) {
        const string& term = info.term;
        
        // Skip very short terms (less than 2 chars)
        if (term.length() < 2) continue;
        
        // Generate all prefixes (minimum 2 chars, maximum max_prefix_len)
        int max_len = min((int)term.length(), max_prefix_len);
        for (int len = 2; len <= max_len; len++) {
            string prefix = term.substr(0, len);
            
            // Convert to lowercase for case-insensitive matching
            transform(prefix.begin(), prefix.end(), prefix.begin(), 
                     [](unsigned char c){ return tolower(c); });
            
            prefix_map[prefix].emplace_back(
                info.popularity_score,
                info.term,
                info.word_id,
                info.doc_freq,
                info.term_freq
            );
            total_prefixes++;
        }
    }
    
    cerr << "Generated " << total_prefixes << " prefix entries for " 
         << prefix_map.size() << " unique prefixes\n";
    
    // Sort each prefix's term list by popularity (descending) and keep top-K
    cerr << "Sorting and pruning to top-" << top_k_per_prefix << " per prefix...\n";
    size_t total_kept = 0;
    
    for (auto& p : prefix_map) {
        auto& term_list = p.second;
        
        // Sort by popularity (descending)
        sort(term_list.begin(), term_list.end(),
             [](const auto& a, const auto& b) {
                 return get<0>(a) > get<0>(b); // Compare popularity scores
             });
        
        // Keep only top-K
        if (term_list.size() > top_k_per_prefix) {
            term_list.resize(top_k_per_prefix);
        }
        
        total_kept += term_list.size();
    }
    
    cerr << "Kept " << total_kept << " total suggestions (avg " 
         << (double)total_kept / prefix_map.size() << " per prefix)\n";
    
    // Write to binary file for fast loading
    cerr << "Writing autocomplete index to " << output_path << "...\n";
    ofstream ofs(output_path, ios::binary);
    
    if (!ofs) {
        cerr << "ERROR: Cannot write to " << output_path << "\n";
        exit(1);
    }
    
    // Write header: number of prefixes
    uint32_t num_prefixes = prefix_map.size();
    ofs.write(reinterpret_cast<const char*>(&num_prefixes), sizeof(num_prefixes));
    
    // Write each prefix entry
    for (const auto& p : prefix_map) {
        const string& prefix = p.first;
        const auto& term_list = p.second;
        
        // Write prefix length and prefix string
        uint16_t prefix_len = prefix.length();
        ofs.write(reinterpret_cast<const char*>(&prefix_len), sizeof(prefix_len));
        ofs.write(prefix.c_str(), prefix_len);
        
        // Write number of terms for this prefix
        uint16_t num_terms = term_list.size();
        ofs.write(reinterpret_cast<const char*>(&num_terms), sizeof(num_terms));
        
        // Write each term
        for (const auto& t : term_list) {
            double popularity = get<0>(t);
            const string& term = get<1>(t);
            uint32_t word_id = get<2>(t);
            uint32_t doc_freq = get<3>(t);
            uint64_t term_freq = get<4>(t);
            
            // Write term length and term string
            uint16_t term_len = term.length();
            ofs.write(reinterpret_cast<const char*>(&term_len), sizeof(term_len));
            ofs.write(term.c_str(), term_len);
            
            // Write metadata
            ofs.write(reinterpret_cast<const char*>(&popularity), sizeof(popularity));
            ofs.write(reinterpret_cast<const char*>(&word_id), sizeof(word_id));
            ofs.write(reinterpret_cast<const char*>(&doc_freq), sizeof(doc_freq));
            ofs.write(reinterpret_cast<const char*>(&term_freq), sizeof(term_freq));
        }
    }
    
    ofs.close();
    
    cerr << "Autocomplete index written successfully!\n";
    
    // Write human-readable version for inspection
    string txt_path = output_path + ".txt";
    ofstream txt(txt_path);
    
    // Sort prefixes alphabetically for readability
    vector<string> sorted_prefixes;
    for (const auto& p : prefix_map) {
        sorted_prefixes.push_back(p.first);
    }
    sort(sorted_prefixes.begin(), sorted_prefixes.end());
    
    for (const auto& prefix : sorted_prefixes) {
        txt << "PREFIX: " << prefix << " (" << prefix_map[prefix].size() << " suggestions)\n";
        for (size_t i = 0; i < min((size_t)5, prefix_map[prefix].size()); i++) {
            const auto& t = prefix_map[prefix][i];
            txt << "  " << (i+1) << ". " << get<1>(t) 
                << " (pop=" << fixed << setprecision(2) << get<0>(t)
                << ", df=" << get<3>(t) << ", tf=" << get<4>(t) << ")\n";
        }
        if (prefix_map[prefix].size() > 5) {
            txt << "  ... and " << (prefix_map[prefix].size() - 5) << " more\n";
        }
        txt << "\n";
    }
    
    txt.close();
    cerr << "Human-readable index written to " << txt_path << "\n";
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " -d index_dir [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -d DIR             : index directory (required)\n";
        cerr << "  -o FILE            : output autocomplete index (default: autocomplete.idx)\n";
        cerr << "  --max-prefix N     : maximum prefix length (default: 15)\n";
        cerr << "  --top-k N          : top suggestions per prefix (default: 20)\n";
        cerr << "\nExample:\n";
        cerr << "  " << argv[0] << " -d index_out -o autocomplete.idx\n";
        return 1;
    }
    
    string index_dir;
    string output = "autocomplete.idx";
    int max_prefix_len = 15;
    int top_k_per_prefix = 20;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-d" && i+1 < argc) {
            index_dir = argv[++i];
        } else if (arg == "-o" && i+1 < argc) {
            output = argv[++i];
        } else if (arg == "--max-prefix" && i+1 < argc) {
            max_prefix_len = stoi(argv[++i]);
        } else if (arg == "--top-k" && i+1 < argc) {
            top_k_per_prefix = stoi(argv[++i]);
        }
    }
    
    if (index_dir.empty()) {
        cerr << "ERROR: Index directory (-d) required\n";
        return 1;
    }
    
    string lexicon_path = index_dir + "/lexicon.txt";
    
    cerr << "\n=== Autocomplete Index Builder ===\n";
    cerr << "Input: " << lexicon_path << "\n";
    cerr << "Output: " << output << "\n";
    cerr << "Max prefix length: " << max_prefix_len << "\n";
    cerr << "Top-K per prefix: " << top_k_per_prefix << "\n\n";
    
    auto start = chrono::high_resolution_clock::now();
    
    // Load terms with frequencies
    vector<TermInfo> terms = load_terms_with_freq(lexicon_path);
    
    // Build autocomplete index
    build_autocomplete_index(terms, output, max_prefix_len, top_k_per_prefix);
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    cerr << "\n=== Build Complete ===\n";
    cerr << "Time taken: " << duration.count() << " ms\n";
    cerr << "Index file: " << output << "\n";
    cerr << "Text file: " << output << ".txt\n";
    
    return 0;
}
