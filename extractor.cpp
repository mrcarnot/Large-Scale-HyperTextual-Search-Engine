// extractor.cpp - Multi-format version
// Compile: g++ -std=c++17 -O2 extractor.cpp -o extractor
// Requires RapidJSON headers (https://github.com/Tencent/rapidjson)

#include <bits/stdc++.h>
#include <filesystem>
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <chrono>

using namespace std;
namespace fs = std::filesystem;
using rapidjson::Document;
using rapidjson::Value;

// -------------------- Utility functions --------------------

static string trim(const string &s) {
    size_t a = s.find_first_not_of(" \t\n\r");
    if (a == string::npos) return "";
    size_t b = s.find_last_not_of(" \t\n\r");
    return s.substr(a, b - a + 1);
}

static string normalize_whitespace(const string &s) {
    string out;
    bool in_space = false;
    for (unsigned char c : s) {
        if (c == '\r') continue;
        if (isspace(c)) {
            if (!in_space) { out.push_back(' '); in_space = true; }
        } else {
            out.push_back((char)c); in_space = false;
        }
    }
    return trim(out);
}

static string sanitize_for_field(const string &s) {
    string t = s;
    for (char &c : t) if (c == '\t' || c == '\n' || c == '\r') c = ' ';
    return normalize_whitespace(t);
}

static bool load_json_file(const string &path, Document &doc) {
    ifstream ifs(path);
    if (!ifs.is_open()) return false;
    rapidjson::IStreamWrapper isw(ifs);
    doc.ParseStream(isw);
    return !doc.HasParseError();
}

static string get_string_member(const Value &obj, const char *key) {
    if (!obj.IsObject()) return "";
    auto it = obj.FindMember(key);
    if (it == obj.MemberEnd()) return "";
    const Value& v = it->value;
    if (v.IsString()) return v.GetString();
    if (v.IsNumber()) {
        if (v.IsInt()) return to_string(v.GetInt());
        if (v.IsInt64()) return to_string(v.GetInt64());
        if (v.IsDouble()) { ostringstream oss; oss << v.GetDouble(); return oss.str(); }
    }
    return "";
}

static string join_authors(const Document &doc) {
    if (!doc.HasMember("metadata") || !doc["metadata"].IsObject()) return "";
    const Value &meta = doc["metadata"];
    if (!meta.HasMember("authors") || !meta["authors"].IsArray()) return "";

    vector<string> names;
    for (const auto &a : meta["authors"].GetArray()) {
        if (a.IsObject()) {
            string first = get_string_member(a, "first");
            string last = get_string_member(a, "last");
            string middle;
            if (a.HasMember("middle")) {
                const Value &m = a["middle"];
                if (m.IsArray()) {
                    string mm;
                    for (auto &e : m.GetArray()) if (e.IsString()) { if (!mm.empty()) mm += " "; mm += e.GetString(); }
                    middle = mm;
                } else if (m.IsString()) middle = m.GetString();
            }
            string name = "";
            if (!first.empty()) name += first;
            if (!middle.empty()) { if (!name.empty()) name += " "; name += middle; }
            if (!last.empty()) { if (!name.empty()) name += " "; name += last; }
            if (name.empty() && a.HasMember("name") && a["name"].IsString()) name = a["name"].GetString();
            if (name.empty()) {
                if (a.HasMember("email") && a["email"].IsString()) name = a["email"].GetString();
            }
            if (!name.empty()) names.push_back(name);
        } else if (a.IsString()) {
            names.push_back(a.GetString());
        }
    }
    string out;
    for (size_t i = 0; i < names.size(); ++i) {
        if (i) out += "; ";
        out += names[i];
    }
    return out;
}

static string extract_abstract(const Document &doc) {
    if (doc.HasMember("abstract") && doc["abstract"].IsArray()) {
        string joined;
        for (const auto &el : doc["abstract"].GetArray()) {
            if (el.IsObject() && el.HasMember("text") && el["text"].IsString()) {
                if (!joined.empty()) joined += " ";
                joined += el["text"].GetString();
            } else if (el.IsString()) {
                if (!joined.empty()) joined += " ";
                joined += el.GetString();
            }
        }
        if (!trim(joined).empty()) return normalize_whitespace(joined);
    }
    if (doc.HasMember("body_text") && doc["body_text"].IsArray()) {
        string joined;
        for (const auto &bt : doc["body_text"].GetArray()) {
            string section;
            if (bt.IsObject() && bt.HasMember("section") && bt["section"].IsString()) section = bt["section"].GetString();
            if (!section.empty() && (section == "Abstract" || section == "ABSTRACT" || section == "abstract")) {
                if (bt.HasMember("text") && bt["text"].IsString()) {
                    if (!joined.empty()) joined += " ";
                    joined += bt["text"].GetString();
                }
            }
        }
        if (!trim(joined).empty()) return normalize_whitespace(joined);
    }
    return "";
}

static vector<string> extract_sections(const Document &doc) {
    vector<pair<string,string>> ordered;
    unordered_map<string,size_t> idx;
    if (!doc.HasMember("body_text") || !doc["body_text"].IsArray()) return {};
    for (const auto &bt : doc["body_text"].GetArray()) {
        string section = "Body";
        if (bt.IsObject() && bt.HasMember("section") && bt["section"].IsString()) {
            string sec = bt["section"].GetString();
            if (!trim(sec).empty()) section = sec;
        }
        string text;
        if (bt.IsObject() && bt.HasMember("text") && bt["text"].IsString()) text = bt["text"].GetString();
        text = normalize_whitespace(text);
        if (text.empty()) continue;
        auto it = idx.find(section);
        if (it == idx.end()) {
            idx[section] = ordered.size();
            ordered.emplace_back(section, text);
        } else {
            ordered[it->second].second += "\n\n" + text;
        }
    }
    vector<string> out;
    for (auto &p : ordered) {
        string combined = p.first + ": " + p.second;
        out.push_back(normalize_whitespace(combined));
    }
    return out;
}

static string extract_doi(const Document &doc) {
    if (doc.HasMember("metadata") && doc["metadata"].IsObject()) {
        const Value &m = doc["metadata"];
        if (m.HasMember("doi") && m["doi"].IsString()) return m["doi"].GetString();
        if (m.HasMember("other_ids") && m["other_ids"].IsObject()) {
            for (auto it = m["other_ids"].MemberBegin(); it != m["other_ids"].MemberEnd(); ++it) {
                if (string(it->name.GetString()) == "DOI" && it->value.IsArray() && it->value.Size() > 0) {
                    if (it->value[0].IsString()) return it->value[0].GetString();
                }
            }
        }
    }
    if (doc.HasMember("bib_entries") && doc["bib_entries"].IsObject()) {
        for (auto it = doc["bib_entries"].MemberBegin(); it != doc["bib_entries"].MemberEnd(); ++it) {
            const Value &be = it->value;
            if (be.IsObject() && be.HasMember("other_ids") && be["other_ids"].IsObject()) {
                for (auto idit = be["other_ids"].MemberBegin(); idit != be["other_ids"].MemberEnd(); ++idit) {
                    if (string(idit->name.GetString()) == "DOI" && idit->value.IsArray() && idit->value.Size() > 0) {
                        if (idit->value[0].IsString()) return idit->value[0].GetString();
                    }
                }
            }
        }
    }
    return "";
}

static string extract_pub_date(const Document &doc) {
    if (doc.HasMember("metadata") && doc["metadata"].IsObject()) {
        const Value &m = doc["metadata"];
        if (m.HasMember("publish_time") && m["publish_time"].IsString()) return m["publish_time"].GetString();
        if (m.HasMember("publish_date") && m["publish_date"].IsString()) return m["publish_date"].GetString();
        if (m.HasMember("year")) {
            if (m["year"].IsInt()) return to_string(m["year"].GetInt());
            if (m["year"].IsString()) return m["year"].GetString();
        }
    }
    if (doc.HasMember("bib_entries") && doc["bib_entries"].IsObject()) {
        for (auto it = doc["bib_entries"].MemberBegin(); it != doc["bib_entries"].MemberEnd(); ++it) {
            const Value &be = it->value;
            if (be.IsObject() && be.HasMember("year")) {
                if (be["year"].IsInt()) return to_string(be["year"].GetInt());
                if (be["year"].IsString()) return be["year"].GetString();
            }
        }
    }
    return "";
}

// -------------------- NEW: Text file parsing --------------------
struct TextDocument {
    string paper_id;
    string title;
    string abstract_text;
    string body_text;
    string authors;
    string pub_date;
    string doi;
    string source;
};

static TextDocument parse_text_file(const string &path) {
    TextDocument doc;
    ifstream ifs(path);
    if (!ifs.is_open()) return doc;

    doc.paper_id = fs::path(path).stem().string();
    doc.source = "text";
    
    string line, current_section;
    stringstream body_ss;
    bool in_abstract = false;
    
    while (getline(ifs, line)) {
        string trimmed = trim(line);
        
        // Detect sections
        if (trimmed.find("Title:") == 0 || trimmed.find("TITLE:") == 0) {
            doc.title = trim(trimmed.substr(6));
        } else if (trimmed.find("Authors:") == 0 || trimmed.find("AUTHORS:") == 0) {
            doc.authors = trim(trimmed.substr(8));
        } else if (trimmed.find("Date:") == 0 || trimmed.find("DATE:") == 0 || trimmed.find("Year:") == 0) {
            size_t pos = trimmed.find(':');
            doc.pub_date = trim(trimmed.substr(pos + 1));
        } else if (trimmed.find("DOI:") == 0) {
            doc.doi = trim(trimmed.substr(4));
        } else if (trimmed.find("Abstract:") == 0 || trimmed.find("ABSTRACT:") == 0) {
            in_abstract = true;
            string abs = trim(trimmed.substr(9));
            if (!abs.empty()) doc.abstract_text = abs;
        } else if (trimmed.find("Body:") == 0 || trimmed.find("BODY:") == 0 || trimmed.find("Content:") == 0) {
            in_abstract = false;
            size_t pos = trimmed.find(':');
            string content = trim(trimmed.substr(pos + 1));
            if (!content.empty()) body_ss << content << "\n";
        } else if (!trimmed.empty()) {
            if (in_abstract) {
                if (!doc.abstract_text.empty()) doc.abstract_text += " ";
                doc.abstract_text += trimmed;
            } else {
                body_ss << trimmed << "\n";
            }
        }
    }
    
    doc.body_text = body_ss.str();
    
    // If no structured title found, use first non-empty line
    if (doc.title.empty() && !doc.body_text.empty()) {
        istringstream iss(doc.body_text);
        string first_line;
        if (getline(iss, first_line)) {
            doc.title = trim(first_line);
            if (doc.title.length() > 200) doc.title = doc.title.substr(0, 197) + "...";
        }
    }
    
    return doc;
}

// -------------------- NEW: Manual input mode --------------------
static TextDocument get_manual_input() {
    TextDocument doc;
    string input;
    
    cout << "\n=== Manual Document Entry ===\n";
    
    cout << "Paper ID (or press Enter to auto-generate): ";
    getline(cin, input);
    doc.paper_id = trim(input);
    if (doc.paper_id.empty()) {
        auto now = chrono::system_clock::now();
        auto time = chrono::system_clock::to_time_t(now);
        doc.paper_id = "manual_" + to_string(time);
    }
    
    cout << "Title: ";
    getline(cin, doc.title);
    
    cout << "Authors (separate with semicolons): ";
    getline(cin, doc.authors);
    
    cout << "Publication Date: ";
    getline(cin, doc.pub_date);
    
    cout << "DOI (optional): ";
    getline(cin, doc.doi);
    
    cout << "Abstract (press Enter twice when done):\n";
    string abs_line;
    while (getline(cin, abs_line)) {
        if (abs_line.empty()) break;
        if (!doc.abstract_text.empty()) doc.abstract_text += " ";
        doc.abstract_text += trim(abs_line);
    }
    
    cout << "Body text (press Enter twice when done):\n";
    string body_line;
    while (getline(cin, body_line)) {
        if (body_line.empty()) break;
        doc.body_text += trim(body_line) + "\n";
    }
    
    doc.source = "manual";
    return doc;
}

// -------------------- Progress bar --------------------
void show_progress(size_t current, size_t total) {
    int barWidth = 40;
    float progress = (float)current / total;
    cout << "\r[";
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) cout << "=";
        else if (i == pos) cout << ">";
        else cout << " ";
    }
    cout << "] " << int(progress * 100.0) << "% (" << current << "/" << total << ")" << flush;
}

// -------------------- Output functions --------------------
static void write_tsv_line(ofstream &ofs, const string &paper_id, const string &title,
                          const string &abstract, const string &sections,
                          const string &authors, const string &pub_date,
                          const string &doi_or_id, const string &source) {
    ofs << sanitize_for_field(paper_id) << '\t'
        << sanitize_for_field(title) << '\t'
        << sanitize_for_field(abstract) << '\t'
        << sanitize_for_field(sections) << '\t'
        << sanitize_for_field(authors) << '\t'
        << sanitize_for_field(pub_date) << '\t'
        << sanitize_for_field(doi_or_id) << '\t'
        << sanitize_for_field(source) << '\n';
}

static void write_jsonl_line(ofstream &jofs, const string &paper_id, const string &title,
                            const string &abstract, const vector<string> &sections,
                            const string &authors, const string &pub_date,
                            const string &doi_or_id, const string &source,
                            const string &orig_file) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    writer.StartObject();
    writer.Key("paper_id"); writer.String(paper_id.c_str());
    writer.Key("title"); writer.String(title.c_str());
    writer.Key("abstract"); writer.String(abstract.c_str());
    writer.Key("sections"); writer.StartArray();
    for (auto &s : sections) writer.String(s.c_str());
    writer.EndArray();
    writer.Key("authors"); writer.String(authors.c_str());
    writer.Key("pub_date"); writer.String(pub_date.c_str());
    writer.Key("doi_or_id"); writer.String(doi_or_id.c_str());
    writer.Key("source"); writer.String(source.c_str());
    writer.Key("orig_file"); writer.String(orig_file.c_str());
    writer.EndObject();
    jofs << sb.GetString() << "\n";
}

// -------------------- Main --------------------
int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " [options] file1 [file2 ...]\n";
        cerr << "Options:\n";
        cerr << "  -d dir          Process all files in directory\n";
        cerr << "  -o out.tsv      Output TSV file (default: out.tsv)\n";
        cerr << "  --jsonl file    Also output JSONL format\n";
        cerr << "  --manual        Manual entry mode (interactive)\n";
        cerr << "  --text          Treat all files as plain text\n";
        cerr << "  --json          Treat all files as JSON (default)\n";
        cerr << "\nSupported formats: .json, .txt, .text, or manual entry\n";
        return 1;
    }

    auto start_time = chrono::high_resolution_clock::now();

    vector<string> files;
    string out_tsv = "out.tsv";
    string out_jsonl = "";
    bool manual_mode = false;
    bool force_text = false;
    bool force_json = false;
    
    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "-d") {
            if (i + 1 < argc) {
                ++i;
                string d = argv[i];
                if (!fs::exists(d) || !fs::is_directory(d)) {
                    cerr << "Directory not found: " << d << "\n";
                    return 1;
                }
                for (auto &p : fs::directory_iterator(d)) {
                    if (!p.is_regular_file()) continue;
                    string path = p.path().string();
                    string ext = p.path().extension().string();
                    if (ext == ".json" || ext == ".txt" || ext == ".text") 
                        files.push_back(path);
                }
            }
        } else if (a == "-o") {
            if (i + 1 < argc) { ++i; out_tsv = argv[i]; }
        } else if (a == "--jsonl") {
            if (i + 1 < argc) { ++i; out_jsonl = argv[i]; }
        } else if (a == "--manual") {
            manual_mode = true;
        } else if (a == "--text") {
            force_text = true;
        } else if (a == "--json") {
            force_json = true;
        } else if (a[0] != '-') {
            files.push_back(a);
        }
    }

    ofstream ofs(out_tsv);
    if (!ofs.is_open()) { cerr << "Cannot open output TSV: " << out_tsv << "\n"; return 1; }
    ofstream jofs;
    if (!out_jsonl.empty()) {
        jofs.open(out_jsonl);
        if (!jofs.is_open()) { cerr << "Cannot open JSONL: " << out_jsonl << "\n"; return 1; }
    }

    // Manual mode
    if (manual_mode) {
        TextDocument tdoc = get_manual_input();
        string sections_joined = "Body: " + sanitize_for_field(tdoc.body_text);
        string doi_or_id = tdoc.doi.empty() ? tdoc.paper_id : tdoc.doi;
        
        write_tsv_line(ofs, tdoc.paper_id, tdoc.title, tdoc.abstract_text,
                      sections_joined, tdoc.authors, tdoc.pub_date, doi_or_id, tdoc.source);
        
        if (jofs.is_open()) {
            vector<string> secs = {"Body: " + tdoc.body_text};
            write_jsonl_line(jofs, tdoc.paper_id, tdoc.title, tdoc.abstract_text,
                           secs, tdoc.authors, tdoc.pub_date, doi_or_id, tdoc.source, "manual_input");
        }
        
        cout << "\nDocument added successfully!\n";
        ofs.close();
        if (jofs.is_open()) jofs.close();
        return 0;
    }

    if (files.empty()) {
        cerr << "No input files found.\n";
        return 1;
    }

    size_t total_files = files.size();
    for (size_t i = 0; i < total_files; ++i) {
        const string &path = files[i];
        string ext = fs::path(path).extension().string();
        bool is_text = force_text || (!force_json && (ext == ".txt" || ext == ".text"));

        if (is_text) {
            // Process as text file
            TextDocument tdoc = parse_text_file(path);
            string sections_joined = "Body: " + sanitize_for_field(tdoc.body_text);
            string doi_or_id = tdoc.doi.empty() ? tdoc.paper_id : tdoc.doi;
            
            write_tsv_line(ofs, tdoc.paper_id, tdoc.title, tdoc.abstract_text,
                          sections_joined, tdoc.authors, tdoc.pub_date, doi_or_id, tdoc.source);
            
            if (jofs.is_open()) {
                vector<string> secs = {"Body: " + tdoc.body_text};
                write_jsonl_line(jofs, tdoc.paper_id, tdoc.title, tdoc.abstract_text,
                               secs, tdoc.authors, tdoc.pub_date, doi_or_id, tdoc.source, path);
            }
        } else {
            // Process as JSON file (original logic)
            Document doc;
            bool ok = load_json_file(path, doc);
            if (!ok) {
                cerr << "\nFailed to parse JSON: " << path << "\n";
                continue;
            }

            string paper_id;
            if (doc.HasMember("paper_id") && doc["paper_id"].IsString()) paper_id = doc["paper_id"].GetString();
            if (paper_id.empty() && doc.HasMember("metadata") && doc["metadata"].IsObject())
                paper_id = get_string_member(doc["metadata"], "paper_id");
            if (paper_id.empty()) paper_id = fs::path(path).stem().string();

            string title;
            if (doc.HasMember("metadata") && doc["metadata"].IsObject()) title = get_string_member(doc["metadata"], "title");
            if (title.empty() && doc.HasMember("title") && doc["title"].IsString()) title = doc["title"].GetString();
            title = sanitize_for_field(title);

            string abstract_text = sanitize_for_field(extract_abstract(doc));
            vector<string> sections = extract_sections(doc);
            if (sections.empty() && doc.HasMember("body_text") && doc["body_text"].IsArray()) {
                string f;
                for (const auto &bt : doc["body_text"].GetArray()) {
                    if (bt.IsObject() && bt.HasMember("text") && bt["text"].IsString()) {
                        if (!f.empty()) f += "\n\n";
                        f += bt["text"].GetString();
                    }
                }
                if (!f.empty()) sections.push_back("Body: " + normalize_whitespace(f));
            }

            string authors = sanitize_for_field(join_authors(doc));
            string pub_date = sanitize_for_field(extract_pub_date(doc));
            string doi = extract_doi(doc);
            string source = (!paper_id.empty() && paper_id.rfind("PMC", 0) == 0) ? "pmc" : "pdf";

            string sections_joined;
            for (size_t j = 0; j < sections.size(); ++j) {
                if (j) sections_joined += " | ";
                sections_joined += sanitize_for_field(sections[j]);
            }

            string doi_or_id = doi.empty() ? paper_id : doi;

            write_tsv_line(ofs, paper_id, title, abstract_text, sections_joined,
                          authors, pub_date, doi_or_id, source);

            if (jofs.is_open()) {
                write_jsonl_line(jofs, paper_id, title, abstract_text, sections,
                               authors, pub_date, doi_or_id, source, path);
            }
        }

        show_progress(i + 1, total_files);
    }

    ofs.close();
    if (jofs.is_open()) jofs.close();

    auto end_time = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end_time - start_time;

    cout << "\nExtraction completed. TSV written to " << out_tsv;
    if (!out_jsonl.empty()) cout << ", JSONL written to " << out_jsonl;
    cout << "\nTotal time taken: " << elapsed.count() << " seconds.\n";

    return 0;
}
