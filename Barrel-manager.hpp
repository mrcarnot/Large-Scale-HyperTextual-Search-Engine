// barrel_manager.hpp
// Add this new file to manage barrel-based index loading
// This allows efficient memory usage by loading only needed barrels

#ifndef BARREL_MANAGER_HPP
#define BARREL_MANAGER_HPP

#include <bits/stdc++.h>
#include <filesystem>

namespace fs = std::filesystem;
using namespace std;

// Barrel configuration
struct BarrelConfig {
    static const size_t NUM_BARRELS = 10;  // Number of barrels to split index into
    static const size_t MAX_BARRELS_IN_MEMORY = 3;  // Max barrels loaded at once
};

// Barrel metadata
struct BarrelInfo {
    uint32_t barrel_id;
    string start_term;
    string end_term;
    uint64_t offset;
    uint64_t size;
    size_t term_count;
};

class BarrelManager {
private:
    string index_dir;
    vector<BarrelInfo> barrel_metadata;
    unordered_map<uint32_t, vector<uint8_t>> loaded_barrels;  // barrel_id -> data
    list<uint32_t> lru_order;  // LRU cache order
    
    // Determine which barrel a term belongs to
    uint32_t get_barrel_id(const string& term) {
        // Simple hash-based distribution
        hash<string> hasher;
        size_t h = hasher(term);
        return h % BarrelConfig::NUM_BARRELS;
    }
    
    // Load barrel from disk
    bool load_barrel(uint32_t barrel_id) {
        if (loaded_barrels.find(barrel_id) != loaded_barrels.end()) {
            // Already loaded, update LRU
            lru_order.remove(barrel_id);
            lru_order.push_front(barrel_id);
            return true;
        }
        
        // Check if we need to evict
        if (loaded_barrels.size() >= BarrelConfig::MAX_BARRELS_IN_MEMORY) {
            uint32_t evict_id = lru_order.back();
            lru_order.pop_back();
            loaded_barrels.erase(evict_id);
            cerr << "[BarrelManager] Evicted barrel " << evict_id << "\n";
        }
        
        // Load barrel file
        string barrel_path = index_dir + "/barrel_" + to_string(barrel_id) + ".bin";
        ifstream ifs(barrel_path, ios::binary);
        if (!ifs) {
            cerr << "[BarrelManager] Cannot open barrel " << barrel_id << "\n";
            return false;
        }
        
        ifs.seekg(0, ios::end);
        size_t size = ifs.tellg();
        ifs.seekg(0, ios::beg);
        
        vector<uint8_t> data(size);
        ifs.read(reinterpret_cast<char*>(data.data()), size);
        
        loaded_barrels[barrel_id] = move(data);
        lru_order.push_front(barrel_id);
        
        cerr << "[BarrelManager] Loaded barrel " << barrel_id << " (" << size << " bytes)\n";
        return true;
    }

public:
    BarrelManager(const string& dir) : index_dir(dir) {
        load_metadata();
    }
    
    void load_metadata() {
        string meta_path = index_dir + "/barrel_metadata.txt";
        ifstream ifs(meta_path);
        if (!ifs) {
            cerr << "[BarrelManager] Warning: No barrel metadata found\n";
            return;
        }
        
        string line;
        while (getline(ifs, line)) {
            istringstream iss(line);
            BarrelInfo info;
            if (iss >> info.barrel_id >> info.start_term >> info.end_term >> 
                       info.offset >> info.size >> info.term_count) {
                barrel_metadata.push_back(info);
            }
        }
        
        cerr << "[BarrelManager] Loaded metadata for " << barrel_metadata.size() << " barrels\n";
    }
    
    // Get barrel data for a term (loads if needed)
    const vector<uint8_t>* get_barrel_for_term(const string& term) {
        uint32_t barrel_id = get_barrel_id(term);
        
        if (!load_barrel(barrel_id)) {
            return nullptr;
        }
        
        return &loaded_barrels[barrel_id];
    }
    
    // Preload specific barrels
    void preload_barrels(const vector<string>& terms) {
        set<uint32_t> needed_barrels;
        for (const auto& term : terms) {
            needed_barrels.insert(get_barrel_id(term));
        }
        
        for (uint32_t barrel_id : needed_barrels) {
            load_barrel(barrel_id);
        }
    }
    
    size_t memory_usage() const {
        size_t total = 0;
        for (const auto& p : loaded_barrels) {
            total += p.second.size();
        }
        return total;
    }
    
    void clear_cache() {
        loaded_barrels.clear();
        lru_order.clear();
        cerr << "[BarrelManager] Cache cleared\n";
    }
};

#endif // BARREL_MANAGER_HPP
