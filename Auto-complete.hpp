// autocomplete.hpp
// Fast prefix-based autocomplete using Trie data structure
// Target: < 100ms response time

#ifndef AUTOCOMPLETE_HPP
#define AUTOCOMPLETE_HPP

#include <bits/stdc++.h>
using namespace std;

struct TrieNode {
    unordered_map<char, unique_ptr<TrieNode>> children;
    bool is_end = false;
    uint32_t frequency = 0;  // Term frequency for ranking
    string term;             // Store complete term at leaf nodes
};

class AutocompleteEngine {
private:
    unique_ptr<TrieNode> root;
    size_t max_suggestions;
    
    // Helper: collect all terms from a node
    void collect_terms(TrieNode* node, vector<pair<string, uint32_t>>& results) {
        if (!node) return;
        
        if (node->is_end) {
            results.emplace_back(node->term, node->frequency);
        }
        
        for (auto& child : node->children) {
            collect_terms(child.second.get(), results);
        }
    }
    
    // Find node for prefix
    TrieNode* find_prefix_node(const string& prefix) {
        TrieNode* current = root.get();
        
        for (char c : prefix) {
            if (current->children.find(c) == current->children.end()) {
                return nullptr;
            }
            current = current->children[c].get();
        }
        
        return current;
    }

public:
    AutocompleteEngine(size_t max_sugg = 10) : max_suggestions(max_sugg) {
        root = make_unique<TrieNode>();
    }
    
    // Build autocomplete index from lexicon
    void build_from_lexicon(const string& lexicon_path) {
        auto start = chrono::high_resolution_clock::now();
        
        ifstream ifs(lexicon_path);
        if (!ifs) {
            cerr << "[Autocomplete] Cannot open lexicon\n";
            return;
        }
        
        string line;
        size_t count = 0;
        
        while (getline(ifs, line)) {
            istringstream iss(line);
            uint32_t wordID, doc_freq;
            uint64_t term_freq;
            string term;
            
            if (iss >> wordID >> term >> doc_freq >> term_freq) {
                insert(term, term_freq);
                count++;
            }
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
        
        cerr << "[Autocomplete] Built index with " << count 
             << " terms in " << duration.count() << "ms\n";
    }
    
    // Insert a term into the trie
    void insert(const string& term, uint32_t frequency = 1) {
        if (term.empty()) return;
        
        TrieNode* current = root.get();
        
        for (char c : term) {
            if (current->children.find(c) == current->children.end()) {
                current->children[c] = make_unique<TrieNode>();
            }
            current = current->children[c].get();
        }
        
        current->is_end = true;
        current->term = term;
        current->frequency = frequency;
    }
    
    // Get autocomplete suggestions for a prefix
    vector<string> suggest(const string& prefix) {
        auto start = chrono::high_resolution_clock::now();
        
        vector<string> suggestions;
        
        if (prefix.empty()) return suggestions;
        
        // Convert to lowercase
        string lower_prefix = prefix;
        transform(lower_prefix.begin(), lower_prefix.end(), 
                 lower_prefix.begin(), ::tolower);
        
        // Find prefix node
        TrieNode* prefix_node = find_prefix_node(lower_prefix);
        
        if (!prefix_node) {
            return suggestions;  // No matches
        }
        
        // Collect all terms with this prefix
        vector<pair<string, uint32_t>> candidates;
        collect_terms(prefix_node, candidates);
        
        // Sort by frequency (descending)
        sort(candidates.begin(), candidates.end(),
             [](const auto& a, const auto& b) {
                 return a.second > b.second;
             });
        
        // Take top N
        size_t limit = min(max_suggestions, candidates.size());
        for (size_t i = 0; i < limit; i++) {
            suggestions.push_back(candidates[i].first);
        }
        
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
        
        // Performance check (should be < 100ms = 100,000 microseconds)
        if (duration.count() > 100000) {
            cerr << "[Autocomplete] WARNING: Slow query (" 
                 << duration.count() / 1000.0 << "ms)\n";
        }
        
        return suggestions;
    }
    
    // Suggest with frequency information
    vector<pair<string, uint32_t>> suggest_with_freq(const string& prefix) {
        vector<pair<string, uint32_t>> results;
        
        if (prefix.empty()) return results;
        
        string lower_prefix = prefix;
        transform(lower_prefix.begin(), lower_prefix.end(), 
                 lower_prefix.begin(), ::tolower);
        
        TrieNode* prefix_node = find_prefix_node(lower_prefix);
        if (!prefix_node) return results;
        
        collect_terms(prefix_node, results);
        
        sort(results.begin(), results.end(),
             [](const auto& a, const auto& b) {
                 return a.second > b.second;
             });
        
        if (results.size() > max_suggestions) {
            results.resize(max_suggestions);
        }
        
        return results;
    }
    
    // Check if term exists
    bool contains(const string& term) {
        string lower_term = term;
        transform(lower_term.begin(), lower_term.end(), 
                 lower_term.begin(), ::tolower);
        
        TrieNode* node = find_prefix_node(lower_term);
        return node && node->is_end;
    }
    
    // Memory usage estimation
    size_t estimate_memory() const {
        size_t total = 0;
        function<void(const TrieNode*)> count = [&](const TrieNode* node) {
            if (!node) return;
            total += sizeof(TrieNode);
            total += node->term.size();
            for (const auto& child : node->children) {
                count(child.second.get());
            }
        };
        count(root.get());
        return total;
    }
};

#endif // AUTOCOMPLETE_HPP
