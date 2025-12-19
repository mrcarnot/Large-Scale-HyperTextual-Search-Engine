// semantic_indexer.cpp
// Builds semantic search index from embeddings.csv
// Compile: g++ -std=c++17 -O2 semantic_indexer.cpp -o semantic_indexer

#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <algorithm>
#include <cmath>

namespace fs = std::filesystem;
using namespace std;

// ==================== Configuration ====================
const int EMBEDDING_DIM = 768;  // Common for BERT/SciBERT models
const int MAX_DOCS = 1000000;   // Support up to 1M documents

// ==================== Data Structures ====================
struct DocEmbedding {
    string docid;
    vector<float> embedding;
    
    // Normalize embedding (L2 normalization)
    void normalize() {
        float norm = 0.0f;
        for (float val : embedding) {
            norm += val * val;
        }
        norm = sqrt(norm);
        
        if (norm > 1e-8) {
            for (float& val : embedding) {
                val /= norm;
            }
        }
    }
};

// ==================== CSV Parser ====================
static vector<string> parse_csv_line(const string& line) {
    vector<string> fields;
    string field;
    bool in_quotes = false;
    
    for (size_t i = 0; i < line.length(); i++) {
        char c = line[i];
        
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == ',' && !in_quotes) {
            fields.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    fields.push_back(field);
    
    return fields;
}

// ==================== Load Embeddings ====================
static vector<DocEmbedding> load_embeddings(const string& csv_path, int& detected_dim) {
    vector<DocEmbedding> embeddings;
    
    ifstream ifs(csv_path);
    if (!ifs) {
        cerr << "ERROR: Cannot open embeddings file: " << csv_path << "\n";
        exit(1);
    }
    
    cerr << "Loading embeddings from " << csv_path << "...\n";
    
    string line;
    size_t line_num = 0;
    bool header_skipped = false;
    
    while (getline(ifs, line)) {
        line_num++;
        
        // Skip header if present
        if (!header_skipped && (line.find("docid") != string::npos || 
                                line.find("document_id") != string::npos ||
                                line.find("id") != string::npos)) {
            header_skipped = true;
            cerr << "  Skipping header line\n";
            continue;
        }
        
        if (line.empty()) continue;
        
        vector<string> fields = parse_csv_line(line);
        
        if (fields.size() < 2) {
            if (line_num <= 10) {
                cerr << "  Warning: Line " << line_num << " has too few fields\n";
            }
            continue;
        }
        
        DocEmbedding doc_emb;
        doc_emb.docid = fields[0];
        
        // Remove quotes if present
        if (doc_emb.docid.front() == '"' && doc_emb.docid.back() == '"') {
            doc_emb.docid = doc_emb.docid.substr(1, doc_emb.docid.length() - 2);
        }
        
        // Parse embedding vector (remaining fields)
        doc_emb.embedding.reserve(fields.size() - 1);
        
        for (size_t i = 1; i < fields.size(); i++) {
            try {
                float val = stof(fields[i]);
                doc_emb.embedding.push_back(val);
            } catch (...) {
                cerr << "  Warning: Invalid float at line " << line_num 
                     << ", field " << i << "\n";
                break;
            }
        }
        
        // Detect embedding dimension from first valid document
        if (detected_dim == 0 && !doc_emb.embedding.empty()) {
            detected_dim = doc_emb.embedding.size();
            cerr << "  Detected embedding dimension: " << detected_dim << "\n";
        }
        
        // Validate dimension
        if (!doc_emb.embedding.empty() && 
            (int)doc_emb.embedding.size() == detected_dim) {
            doc_emb.normalize();
            embeddings.push_back(move(doc_emb));
            
            if (embeddings.size() % 10000 == 0) {
                cerr << "  Loaded " << embeddings.size() << " embeddings...\n";
            }
        } else if (!doc_emb.embedding.empty()) {
            if (line_num <= 10) {
                cerr << "  Warning: Line " << line_num << " has wrong dimension: "
                     << doc_emb.embedding.size() << " (expected " << detected_dim << ")\n";
            }
        }
    }
    
    cerr << "Loaded " << embeddings.size() << " document embeddings\n";
    return embeddings;
}

// ==================== Build Index ====================
static void write_semantic_index(const vector<DocEmbedding>& embeddings,
                                 const string& output_path,
                                 int embedding_dim) {
    ofstream ofs(output_path, ios::binary);
    if (!ofs) {
        cerr << "ERROR: Cannot write semantic index: " << output_path << "\n";
        exit(1);
    }
    
    cerr << "Writing semantic index...\n";
    
    // Write header
    uint32_t magic = 0x53454D49;  // "SEMI" in hex
    ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
    
    uint32_t version = 1;
    ofs.write(reinterpret_cast<const char*>(&version), sizeof(version));
    
    uint32_t dim = embedding_dim;
    ofs.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
    
    uint32_t num_docs = embeddings.size();
    ofs.write(reinterpret_cast<const char*>(&num_docs), sizeof(num_docs));
    
    // Write docid mapping
    for (const auto& doc_emb : embeddings) {
        uint32_t docid_len = doc_emb.docid.length();
        ofs.write(reinterpret_cast<const char*>(&docid_len), sizeof(docid_len));
        ofs.write(doc_emb.docid.c_str(), docid_len);
    }
    
    // Write embeddings (contiguous block for fast access)
    for (const auto& doc_emb : embeddings) {
        for (float val : doc_emb.embedding) {
            ofs.write(reinterpret_cast<const char*>(&val), sizeof(val));
        }
    }
    
    ofs.close();
    
    // Get file size
    size_t file_size = fs::file_size(output_path);
    cerr << "Semantic index written: " << (file_size / (1024.0 * 1024.0)) 
         << " MB\n";
}

// ==================== Build Docid Mapping ====================
static void write_docid_mapping(const vector<DocEmbedding>& embeddings,
                                const string& output_path) {
    ofstream ofs(output_path);
    if (!ofs) {
        cerr << "ERROR: Cannot write docid mapping: " << output_path << "\n";
        exit(1);
    }
    
    for (size_t i = 0; i < embeddings.size(); i++) {
        ofs << embeddings[i].docid << "\t" << i << "\n";
    }
    
    ofs.close();
    cerr << "Docid mapping written: " << output_path << "\n";
}

// ==================== Statistics ====================
static void print_statistics(const vector<DocEmbedding>& embeddings, 
                            int embedding_dim) {
    cerr << "\n=== Semantic Index Statistics ===\n";
    cerr << "Total documents: " << embeddings.size() << "\n";
    cerr << "Embedding dimension: " << embedding_dim << "\n";
    cerr << "Index size: ~" << (embeddings.size() * embedding_dim * sizeof(float) / (1024.0 * 1024.0))
         << " MB (embeddings only)\n";
    
    // Check embedding quality (should be normalized)
    if (!embeddings.empty()) {
        float sum = 0.0f;
        for (float val : embeddings[0].embedding) {
            sum += val * val;
        }
        float norm = sqrt(sum);
        cerr << "Sample embedding norm: " << fixed << setprecision(6) << norm 
             << " (should be ~1.0)\n";
    }
}

// ==================== Main ====================
int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " -i embeddings.csv -o output_dir\n";
        cerr << "\nOptions:\n";
        cerr << "  -i FILE  : input embeddings CSV file (required)\n";
        cerr << "  -o DIR   : output directory (required)\n";
        cerr << "\nCSV Format:\n";
        cerr << "  docid,dim0,dim1,dim2,...\n";
        cerr << "  doc1,0.123,-0.456,0.789,...\n";
        cerr << "  ...\n";
        cerr << "\nExample:\n";
        cerr << "  " << argv[0] << " -i embeddings.csv -o index_out\n";
        return 1;
    }
    
    string input_csv;
    string output_dir;
    
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "-i" && i+1 < argc) {
            input_csv = argv[++i];
        } else if (arg == "-o" && i+1 < argc) {
            output_dir = argv[++i];
        }
    }
    
    if (input_csv.empty() || output_dir.empty()) {
        cerr << "ERROR: Both -i and -o are required\n";
        return 1;
    }
    
    if (!fs::exists(input_csv)) {
        cerr << "ERROR: Input file does not exist: " << input_csv << "\n";
        return 1;
    }
    
    // Create output directory
    if (!fs::exists(output_dir)) {
        fs::create_directories(output_dir);
    }
    
    cerr << "\n=== Semantic Indexer ===\n";
    cerr << "Input: " << input_csv << "\n";
    cerr << "Output: " << output_dir << "\n\n";
    
    auto start = chrono::high_resolution_clock::now();
    
    // Load embeddings
    int detected_dim = 0;
    vector<DocEmbedding> embeddings = load_embeddings(input_csv, detected_dim);
    
    if (embeddings.empty()) {
        cerr << "ERROR: No valid embeddings loaded\n";
        return 1;
    }
    
    // Print statistics
    print_statistics(embeddings, detected_dim);
    
    // Write semantic index
    string index_path = output_dir + "/semantic.idx";
    write_semantic_index(embeddings, index_path, detected_dim);
    
    // Write docid mapping
    string mapping_path = output_dir + "/semantic_docid.txt";
    write_docid_mapping(embeddings, mapping_path);
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::seconds>(end - start);
    
    cerr << "\n=== Indexing Complete ===\n";
    cerr << "Time taken: " << duration.count() << " seconds\n";
    cerr << "Index file: " << index_path << "\n";
    cerr << "Mapping file: " << mapping_path << "\n";
    
    return 0;
}