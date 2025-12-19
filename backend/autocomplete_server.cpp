// autocomplete_server.cpp
// Fast autocomplete query service (<100ms)
// Compile: g++ -std=c++17 -O2 autocomplete_server.cpp -o autocomplete_server

#include <bits/stdc++.h>
#include <chrono>

using namespace std;

struct Suggestion {
    string term;
    double popularity;
    uint32_t word_id;
    uint32_t doc_freq;
    uint64_t term_freq;
};

class AutocompleteIndex {
private:
    // Map: prefix -> list of suggestions (already sorted by popularity)
    unordered_map<string, vector<Suggestion>> prefix_map;
    bool loaded = false;
    
public:
    // Load index from binary file
    bool load(const string& index_path) {
        ifstream ifs(index_path, ios::binary);
        
        if (!ifs) {
            cerr << "ERROR: Cannot open autocomplete index: " << index_path << "\n";
            return false;
        }
        
        auto start = chrono::high_resolution_clock::now();
        
        // Read header
        uint32_t num_prefixes;
        ifs.read(reinterpret_cast<char*>(&num_prefixes), sizeof(num_prefixes));
        
        prefix_map.reserve(num_prefixes);
        
        // Read each prefix entry
        for (uint32_t i = 0; i < num_prefixes; i++) {
            // Read prefix
            uint16_t prefix_len;
            ifs.read(reinterpret_cast<char*>(&prefix_len), sizeof(prefix_len));
            
            string prefix(prefix_len, '\0');
            ifs.read(&prefix[0], prefix_len);
            
            // Read number of terms
            uint16_t num_terms;
            ifs.read(reinterpret_cast<char*>(&num_terms), sizeof(num_terms));
            
            vector<Suggestion> suggestions;
            suggestions.reserve(num_terms);
            
            // Read each term
            for (uint16_t j = 0; j < num_terms; j++) {
                Suggestion sug;
                
                // Read term
                uint16_t term_len;
                ifs.read(reinterpret_cast<char*>(&term_len), sizeof(term_len));
                
                sug.term.resize(term_len);
                ifs.read(&sug.term[0], term_len);
                
                // Read metadata
                ifs.read(reinterpret_cast<char*>(&sug.popularity), sizeof(sug.popularity));
                ifs.read(reinterpret_cast<char*>(&sug.word_id), sizeof(sug.word_id));
                ifs.read(reinterpret_cast<char*>(&sug.doc_freq), sizeof(sug.doc_freq));
                ifs.read(reinterpret_cast<char*>(&sug.term_freq), sizeof(sug.term_freq));
                
                suggestions.push_back(sug);
            }
            
            prefix_map[prefix] = move(suggestions);
        }
        
        ifs.close();
        loaded = true;
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cerr << "Loaded autocomplete index: " << prefix_map.size() 
             << " prefixes in " << duration.count() << " ms\n";
        
        return true;
    }
    
    // Get suggestions for a query prefix
    vector<Suggestion> get_suggestions(const string& query, int max_results = 10) {
        if (!loaded) return {};
        
        // Normalize query to lowercase
        string normalized_query = query;
        transform(normalized_query.begin(), normalized_query.end(), 
                 normalized_query.begin(),
                 [](unsigned char c){ return tolower(c); });
        
        // Minimum 2 characters required
        if (normalized_query.length() < 2) {
            return {};
        }
        
        // Truncate if too long
        if (normalized_query.length() > 15) {
            normalized_query = normalized_query.substr(0, 15);
        }
        
        // Look up in prefix map
        auto it = prefix_map.find(normalized_query);
        if (it == prefix_map.end()) {
            return {}; // No suggestions
        }
        
        // Return top-K suggestions (already sorted by popularity)
        const vector<Suggestion>& all_suggestions = it->second;
        
        int n = min(max_results, (int)all_suggestions.size());
        vector<Suggestion> result(all_suggestions.begin(), all_suggestions.begin() + n);
        
        return result;
    }
    
    // Get statistics
    void print_stats() const {
        if (!loaded) {
            cerr << "Index not loaded\n";
            return;
        }
        
        size_t total_suggestions = 0;
        size_t min_suggestions = UINT_MAX;
        size_t max_suggestions = 0;
        
        for (const auto& p : prefix_map) {
            size_t count = p.second.size();
            total_suggestions += count;
            min_suggestions = min(min_suggestions, count);
            max_suggestions = max(max_suggestions, count);
        }
        
        double avg_suggestions = (double)total_suggestions / prefix_map.size();
        
        cerr << "\n=== Autocomplete Index Statistics ===\n";
        cerr << "Unique prefixes: " << prefix_map.size() << "\n";
        cerr << "Total suggestions: " << total_suggestions << "\n";
        cerr << "Avg suggestions per prefix: " << fixed << setprecision(2) 
             << avg_suggestions << "\n";
        cerr << "Min/Max suggestions: " << min_suggestions << " / " << max_suggestions << "\n";
    }
};

// Interactive mode
void interactive_mode(AutocompleteIndex& index, int max_results) {
    cout << "\n=== Autocomplete Interactive Mode ===\n";
    cout << "Type a prefix to get suggestions (or 'quit' to exit)\n";
    cout << "Minimum 2 characters required\n\n";
    
    string query;
    while (true) {
        cout << "Query> ";
        if (!getline(cin, query)) break;
        
        if (query == "quit" || query == "exit") break;
        if (query.empty()) continue;
        
        auto start = chrono::high_resolution_clock::now();
        vector<Suggestion> suggestions = index.get_suggestions(query, max_results);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        
        if (suggestions.empty()) {
            cout << "No suggestions found for '" << query << "'\n";
        } else {
            cout << "\nFound " << suggestions.size() << " suggestions in " 
                 << duration.count() << " μs (" 
                 << fixed << setprecision(2) << (duration.count() / 1000.0) << " ms):\n\n";
            
            for (size_t i = 0; i < suggestions.size(); i++) {
                const auto& sug = suggestions[i];
                cout << (i+1) << ". " << sug.term 
                     << " (docs: " << sug.doc_freq 
                     << ", freq: " << sug.term_freq << ")\n";
            }
        }
        cout << "\n";
    }
}

// Batch test mode
void batch_test_mode(AutocompleteIndex& index, const vector<string>& test_queries, 
                     int max_results) {
    cout << "\n=== Batch Test Mode ===\n";
    cout << "Testing " << test_queries.size() << " queries...\n\n";
    
    vector<long long> timings;
    timings.reserve(test_queries.size());
    
    for (const auto& query : test_queries) {
        auto start = chrono::high_resolution_clock::now();
        vector<Suggestion> suggestions = index.get_suggestions(query, max_results);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        
        timings.push_back(duration.count());
        
        cout << "Query: '" << query << "' -> " << suggestions.size() 
             << " suggestions in " << duration.count() << " μs";
        
        if (!suggestions.empty()) {
            cout << " [" << suggestions[0].term;
            if (suggestions.size() > 1) cout << ", " << suggestions[1].term;
            if (suggestions.size() > 2) cout << ", ...";
            cout << "]";
        }
        cout << "\n";
    }
    
    // Calculate statistics
    sort(timings.begin(), timings.end());
    long long min_time = timings.front();
    long long max_time = timings.back();
    long long median_time = timings[timings.size() / 2];
    long long p95_time = timings[(timings.size() * 95) / 100];
    long long p99_time = timings[(timings.size() * 99) / 100];
    double avg_time = accumulate(timings.begin(), timings.end(), 0.0) / timings.size();
    
    cout << "\n=== Performance Statistics ===\n";
    cout << "Min: " << min_time << " μs (" << (min_time / 1000.0) << " ms)\n";
    cout << "Avg: " << fixed << setprecision(2) << avg_time 
         << " μs (" << (avg_time / 1000.0) << " ms)\n";
    cout << "Median: " << median_time << " μs (" << (median_time / 1000.0) << " ms)\n";
    cout << "P95: " << p95_time << " μs (" << (p95_time / 1000.0) << " ms)\n";
    cout << "P99: " << p99_time << " μs (" << (p99_time / 1000.0) << " ms)\n";
    cout << "Max: " << max_time << " μs (" << (max_time / 1000.0) << " ms)\n";
    
    if (p99_time < 100000) { // 100ms = 100,000 μs
        cout << "\n✅ Performance target MET: P99 < 100ms\n";
    } else {
        cout << "\n⚠️  Performance target MISSED: P99 >= 100ms\n";
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " -i autocomplete.idx [OPTIONS]\n";
        cerr << "\nOptions:\n";
        cerr << "  -i FILE      : autocomplete index file (required)\n";
        cerr << "  -q QUERY     : single query (non-interactive)\n";
        cerr << "  -k N         : max results (default: 10)\n";
        cerr << "  --test       : run batch performance test\n";
        cerr << "  --stats      : show index statistics\n";
        cerr << "\nExamples:\n";
        cerr << "  Interactive:  " << argv[0] << " -i autocomplete.idx\n";
        cerr << "  Single query: " << argv[0] << " -i autocomplete.idx -q \"mach\"\n";
        cerr << "  Batch test:   " << argv[0] << " -i autocomplete.idx --test\n";
        return 1;
    }
    
    string index_file;
    string query;
    int max_results = 10;
    bool interactive = true;
    bool batch_test = false;
    bool show_stats = false;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-i" && i+1 < argc) {
            index_file = argv[++i];
        } else if (arg == "-q" && i+1 < argc) {
            query = argv[++i];
            interactive = false;
        } else if (arg == "-k" && i+1 < argc) {
            max_results = stoi(argv[++i]);
        } else if (arg == "--test") {
            batch_test = true;
            interactive = false;
        } else if (arg == "--stats") {
            show_stats = true;
        }
    }
    
    if (index_file.empty()) {
        cerr << "ERROR: Index file (-i) required\n";
        return 1;
    }
    
    // Load autocomplete index
    AutocompleteIndex index;
    if (!index.load(index_file)) {
        return 1;
    }
    
    if (show_stats) {
        index.print_stats();
    }
    
    if (batch_test) {
        // Predefined test queries
        vector<string> test_queries = {
            "ma", "mac", "mach", "machi", "machin", "machine",
            "ne", "neu", "neur", "neura", "neural",
            "de", "dee", "deep",
            "le", "lea", "lear", "learn", "learni", "learnin", "learning",
            "co", "com", "comp", "compu", "comput", "compute", "computer",
            "al", "alg", "algo", "algor", "algori", "algorit", "algorith", "algorithm",
            "da", "dat", "data",
            "mo", "mod", "mode", "model",
            "tr", "tra", "trai", "train", "traini", "trainin", "training",
            "op", "opt", "opti", "optim", "optimi", "optimiz", "optimize"
        };
        
        batch_test_mode(index, test_queries, max_results);
        
    } else if (interactive) {
        interactive_mode(index, max_results);
        
    } else {
        // Single query mode
        auto start = chrono::high_resolution_clock::now();
        vector<Suggestion> suggestions = index.get_suggestions(query, max_results);
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        
        cout << "\nQuery: '" << query << "'\n";
        cout << "Time: " << duration.count() << " μs (" 
             << fixed << setprecision(2) << (duration.count() / 1000.0) << " ms)\n\n";
        
        if (suggestions.empty()) {
            cout << "No suggestions found\n";
        } else {
            cout << "Suggestions (" << suggestions.size() << "):\n";
            for (size_t i = 0; i < suggestions.size(); i++) {
                const auto& sug = suggestions[i];
                cout << (i+1) << ". " << sug.term 
                     << " (docs: " << sug.doc_freq 
                     << ", freq: " << sug.term_freq << ")\n";
            }
        }
    }
    
    return 0;
}
