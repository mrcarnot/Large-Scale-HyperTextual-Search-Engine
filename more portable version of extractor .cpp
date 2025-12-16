// extractor.cpp
// Compile: g++ -std=c++17 -O2 extractor.cpp -o extractor
// Requires RapidJSON headers (https://github.com/Tencent/rapidjson)

// --- Standard Headers (Replaces bits/stdc++.h) ---
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <cctype>   // for isspace
#include <utility>  // for pair

// --- RapidJSON Headers ---
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

using namespace std;
namespace fs = std::filesystem;
using rapidjson::Document;
using rapidjson::Value;

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
        if (v.IsDouble()) {
            ostringstream oss; oss << v.GetDouble(); return oss.str();
        }
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

int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " [-d dir] file1.json [file2.json ...] -o out.tsv [--jsonl out.jsonl]\n";
        return 1;
    }

    vector<string> files;
    string out_tsv = "out.tsv";
    string out_jsonl = "";
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
                    if (path.size() >= 5 && path.substr(path.size() - 5) == ".json") files.push_back(path);
                }
            }
        } else if (a == "-o") {
            if (i + 1 < argc) { ++i; out_tsv = argv[i]; }
        } else if (a == "--jsonl") {
            if (i + 1 < argc) { ++i; out_jsonl = argv[i]; }
        } else {
            files.push_back(a);
        }
    }

    if (files.empty()) {
        cerr << "No input files found.\n";
        return 1;
    }

    ofstream ofs(out_tsv);
    if (!ofs.is_open()) { cerr << "Cannot open output TSV: " << out_tsv << "\n"; return 1; }
    ofstream jofs;
    if (!out_jsonl.empty()) {
        jofs.open(out_jsonl);
        if (!jofs.is_open()) { cerr << "Cannot open JSONL: " << out_jsonl << "\n"; return 1; }
    }

    for (const auto &path : files) {
        Document doc;
        bool ok = load_json_file(path, doc);
        if (!ok) {
            cerr << "Failed to parse JSON: " << path << "\n";
            continue;
        }

        string paper_id;
        if (doc.HasMember("paper_id") && doc["paper_id"].IsString()) paper_id = doc["paper_id"].GetString();
        if (paper_id.empty() && doc.HasMember("metadata") && doc["metadata"].IsObject()) {
            paper_id = get_string_member(doc["metadata"], "paper_id");
        }
        if (paper_id.empty()) paper_id = fs::path(path).stem().string();

        string title;
        if (doc.HasMember("metadata") && doc["metadata"].IsObject()) {
            title = get_string_member(doc["metadata"], "title");
        }
        if (title.empty() && doc.HasMember("title") && doc["title"].IsString()) title = doc["title"].GetString();
        title = sanitize_for_field(title);

        string abstract_text = sanitize_for_field(extract_abstract(doc));

        vector<string> sections = extract_sections(doc);
        if (sections.empty()) {
            if (doc.HasMember("body_text") && doc["body_text"].IsArray()) {
                string f;
                for (const auto &bt : doc["body_text"].GetArray()) {
                    if (bt.IsObject() && bt.HasMember("text") && bt["text"].IsString()) {
                        if (!f.empty()) f += "\n\n";
                        f += bt["text"].GetString();
                    }
                }
                if (!f.empty()) sections.push_back("Body: " + normalize_whitespace(f));
            }
        }

        string authors = sanitize_for_field(join_authors(doc));
        string pub_date = sanitize_for_field(extract_pub_date(doc));
        string doi = extract_doi(doc);
        string source = "pdf";
        if (!paper_id.empty() && paper_id.rfind("PMC", 0) == 0) source = "pmc";

        string sections_joined;
        for (size_t i = 0; i < sections.size(); ++i) {
            if (i) sections_joined += " | ";
            sections_joined += sanitize_for_field(sections[i]);
        }

        string doi_or_id = doi.empty() ? paper_id : doi;

        ofs << sanitize_for_field(paper_id) << '\t'
            << title << '\t'
            << abstract_text << '\t'
            << sections_joined << '\t'
            << authors << '\t'
            << pub_date << '\t'
            << sanitize_for_field(doi_or_id) << '\t'
            << sanitize_for_field(source) << '\n';

        if (jofs.is_open()) {
            rapidjson::StringBuffer sb;
            rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
            writer.StartObject();
            writer.Key("paper_id"); writer.String(paper_id.c_str());
            writer.Key("title"); writer.String(title.c_str());
            writer.Key("abstract"); writer.String(abstract_text.c_str());
            writer.Key("sections"); writer.StartArray();
            for (auto &s : sections) writer.String(s.c_str());
            writer.EndArray();
            writer.Key("authors"); writer.String(authors.c_str());
            writer.Key("pub_date"); writer.String(pub_date.c_str());
            writer.Key("doi_or_id"); writer.String(doi_or_id.c_str());
            writer.Key("source"); writer.String(source.c_str());
            writer.Key("orig_file"); writer.String(path.c_str());
            writer.EndObject();
            jofs << sb.GetString() << "\n";
        }
    }

    ofs.close();
    if (jofs.is_open()) jofs.close();
    cout << "Done. TSV written to " << out_tsv;
    if (!out_jsonl.empty()) cout << ", JSONL written to " << out_jsonl;
    cout << "\n";
    return 0;
}
