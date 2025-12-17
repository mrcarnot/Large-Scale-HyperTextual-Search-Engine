// cleaner.cpp
// Build (no ICU):
//   g++ -std=c++17 -O2 cleaner.cpp -o cleaner
//
// Build (with ICU to enable NFC normalization):
//   g++ -std=c++17 -O2 -DUSE_ICU -I/path/to/icu/include cleaner.cpp -L/path/to/icu/lib -licuuc -o cleaner
//
// Requires RapidJSON headers (for JSON writing/reading).
//
// Description: read TSV (extractor output) or JSONL and produce tokenized JSONL
// See the README text in the top-level comment for usage and options.

#include <bits/stdc++.h>
#include <locale>
#include <codecvt>
#include <cwctype>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/istreamwrapper.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/prettywriter.h>

#ifdef USE_ICU
  #include <unicode/unistr.h>
  #include <unicode/normalizer2.h>
  using namespace icu;
#endif

using namespace std;
using namespace rapidjson;

struct Options {
    string input;
    string input_format = "tsv"; // tsv or jsonl
    string output = "cleaned.jsonl";
    string stopwords_file = "";
    bool use_stopwords = true;
    bool stem = false;
    bool keep_original = false;
    bool remove_numbers = false;
    vector<string> fields = {"title","abstract","sections"}; // "sections" corresponds to extractor's sections field
};

// Simple Porter stemmer (original Porter algorithm). Compact implementation
// This is a straightforward port of the classic algorithm. For clarity and brevity
// we provide a minimal but functional implementation.
class PorterStemmer {
public:
    static string stem(const string &s) {
        if (s.size() <= 2) return s;
        // operate on a mutable string
        string w = s;
        step1a(w); step1b(w); step1c(w);
        step2(w); step3(w); step4(w); step5a(w); step5b(w);
        return w;
    }
private:
    static bool is_cons(const string &w, int i) {
        char ch = w[i];
        if (ch=='a'||ch=='e'||ch=='i'||ch=='o'||ch=='u') return false;
        if (ch=='y') return (i==0) ? true : !is_cons(w,i-1);
        return true;
    }
    static int measure(const string &w) {
        int n = 0;
        int i = 0;
        int len = w.size();
        while (i < len) {
            while (i < len && is_cons(w, i)) i++;
            if (i >= len) break;
            while (i < len && !is_cons(w, i)) i++;
            n++;
        }
        return n;
    }
    static bool contains_vowel(const string &w) {
        for (size_t i=0;i<w.size();++i)
            if (!is_cons(w,i)) return true;
        return false;
    }
    static bool ends_with(const string &w, const string &s) {
        if (w.size() < s.size()) return false;
        return w.compare(w.size()-s.size(), s.size(), s) == 0;
    }
    static void replace_end(string &w, const string &suffix, const string &repl) {
        if (ends_with(w, suffix)) {
            w.replace(w.size()-suffix.size(), suffix.size(), repl);
        }
    }
    static bool double_consonant(const string &w) {
        int len = w.size();
        if (len < 2) return false;
        return w[len-1] == w[len-2] && is_cons(w, len-1);
    }
    static bool cvc(const string &w) {
        int len = w.size();
        if (len < 3) return false;
        if (!is_cons(w, len-1) || is_cons(w, len-2) || !is_cons(w, len-3)) return false;
        char ch = w[len-1];
        if (ch=='w' || ch=='x' || ch=='y') return false;
        return true;
    }

    // Steps based on Porter original algorithm
    static void step1a(string &w) {
        if (ends_with(w,"sses")) w = w.substr(0,w.size()-2);
        else if (ends_with(w,"ies")) w = w.substr(0,w.size()-2);
        else if (ends_with(w,"ss")) {}
        else if (ends_with(w,"s")) w = w.substr(0,w.size()-1);
    }
    static void step1b(string &w) {
        bool flag = false;
        if (ends_with(w,"eed")) {
            string stem = w.substr(0, w.size()-3);
            if (measure(stem) > 0) w = stem + "ee";
        } else if ((ends_with(w,"ed") && contains_vowel(w.substr(0,w.size()-2))) ||
                   (ends_with(w,"ing") && contains_vowel(w.substr(0,w.size()-3)))) {
            if (ends_with(w,"ed")) w = w.substr(0,w.size()-2);
            else w = w.substr(0,w.size()-3);
            if (ends_with(w,"at") || ends_with(w,"bl") || ends_with(w,"iz")) { w += "e"; }
            else if (double_consonant(w) && !(w.back()=='l' || w.back()=='s' || w.back()=='z')) {
                w.pop_back();
            } else if (measure(w) == 1 && cvc(w)) {
                w += "e";
            }
        }
    }
    static void step1c(string &w) {
        if (ends_with(w,"y")) {
            string stem = w.substr(0,w.size()-1);
            if (contains_vowel(stem)) w.back() = 'i';
        }
    }
    static void step2(string &w) {
        static const vector<pair<string,string>> rules = {
            {"ational","ate"},{"tional","tion"},{"enci","ence"},{"anci","ance"},
            {"izer","ize"},{"abli","able"},{"alli","al"},{"entli","ent"},
            {"eli","e"},{"ousli","ous"},{"ization","ize"},{"ation","ate"},
            {"ator","ate"},{"alism","al"},{"iveness","ive"},{"fulness","ful"},
            {"ousness","ous"},{"aliti","al"},{"iviti","ive"},{"biliti","ble"}
        };
        for (auto &p : rules) {
            if (ends_with(w,p.first)) {
                string stem = w.substr(0,w.size()-p.first.size());
                if (measure(stem) > 0) { w = stem + p.second; }
                return;
            }
        }
    }
    static void step3(string &w) {
        static const vector<pair<string,string>> rules = {
            {"icate","ic"},{"ative",""},{"alize","al"},{"iciti","ic"},
            {"ical","ic"},{"ful",""},{"ness",""}
        };
        for (auto &p : rules) {
            if (ends_with(w,p.first)) {
                string stem = w.substr(0,w.size()-p.first.size());
                if (measure(stem) > 0) { w = stem + p.second; }
                return;
            }
        }
    }
    static void step4(string &w) {
        static const vector<string> lst = {
            "al","ance","ence","er","ic","able","ible","ant","ement","ment","ent",
            "ion","ou","ism","ate","iti","ous","ive","ize"
        };
        for (auto &s: lst) {
            if (ends_with(w,s)) {
                string stem = w.substr(0,w.size()-s.size());
                if (measure(stem) > 1) {
                    if (s=="ion") {
                        if (stem.size() && (stem.back()=='s' || stem.back()=='t')) w = stem;
                    } else w = stem;
                }
                return;
            }
        }
    }
    static void step5a(string &w) {
        if (ends_with(w,"e")) {
            string stem = w.substr(0,w.size()-1);
            if (measure(stem) > 1 || (measure(stem) == 1 && !cvc(stem))) w = stem;
        }
    }
    static void step5b(string &w) {
        if (measure(w) > 1 && double_consonant(w) && w.back()=='l') w.pop_back();
    }
};

// Utilities: utf8 <-> wstring conversions
static wstring utf8_to_wstring(const string &s) {
    // Note: std::wstring_convert and codecvt_utf8_utf16 are deprecated since C++17
    // but they work widely and are sufficient for many use-cases. If you want
    // production-grade Unicode support, consider using ICU.
    wstring_convert<codecvt_utf8<wchar_t>> conv;
    try { return conv.from_bytes(s); }
    catch(...) { // fallback: replace invalid sequences
        wstring out;
        for (unsigned char c : s) out.push_back((wchar_t)c);
        return out;
    }
}
static string wstring_to_utf8(const wstring &ws) {
    wstring_convert<codecvt_utf8<wchar_t>> conv;
    try { return conv.to_bytes(ws); } catch(...) {
        string out;
        for (wchar_t wc : ws) out.push_back((char)(wc & 0xFF));
        return out;
    }
}

#ifdef USE_ICU
// NFC normalization using ICU
static string normalize_nfc(const string &s) {
    // Use ICU Normalizer2 for NFC
    UErrorCode err = U_ZERO_ERROR;
    const Normalizer2 *norm = Normalizer2::getNFCInstance(err);
    if (U_FAILURE(err) || !norm) return s;
    UnicodeString src = UnicodeString::fromUTF8(StringPiece(s));
    UnicodeString dst;
    norm->normalize(src, dst, err);
    string out;
    dst.toUTF8String(out);
    return out;
}
#else
static string normalize_nfc(const string &s) {
    // No ICU: do a best-effort normalization:
    // - remove control characters
    // - collapse CR/LF to single newline
    // - do not change codepoint composition
    string t;
    for (unsigned char c : s) {
        if (c == '\r') continue;
        if (c >= 0x20) t.push_back((char)c);
        else if (c == '\n') t.push_back(' ');
    }
    return t;
}
#endif

// Load stopwords file (one per line). Lines starting with # ignored.
static unordered_set<string> load_stopwords(const string &path) {
    unordered_set<string> s;
    ifstream ifs(path);
    if (!ifs.is_open()) return s;
    string line;
    while (getline(ifs, line)) {
        // trim
        auto start = line.find_first_not_of(" \t\r\n");
        if (start==string::npos) continue;
        auto end = line.find_last_not_of(" \t\r\n");
        string tok = line.substr(start, end-start+1);
        if (tok.empty() || tok[0]=='#') continue;
        // lowercase
        transform(tok.begin(), tok.end(), tok.begin(), [](unsigned char c){ return tolower(c); });
        s.insert(tok);
    }
    return s;
}

// Tokenization & normalization pipeline for a single field.
// Returns vector of pairs (normalized_term, original_term) with positions.
struct TokenRec { string term; string orig; int pos; };

static vector<TokenRec> tokenize_field(const string &raw,
                                       const Options &opt,
                                       const unordered_set<string> &stopwords) {
    // 1) NFC normalize (if compiled with ICU, it's true NFC; otherwise best-effort)
    string norm = normalize_nfc(raw);

    // 2) Fix hyphenation across line breaks: "immuno-\nlogy" -> "immunology"
    // Replace "-\n" or "-\r\n" or "-\n\r" sequences
    {
        string tmp;
        tmp.reserve(norm.size());
        for (size_t i=0;i<norm.size();) {
            if (norm[i]=='-' && i+1 < norm.size() && (norm[i+1]=='\n' || norm[i+1]=='\r')) {
                // drop '-' and the following newline(s)
                i++;
                while (i < norm.size() && (norm[i]=='\n' || norm[i]=='\r')) i++;
                // continue without copying the hyphen/newline
            } else {
                tmp.push_back(norm[i]); ++i;
            }
        }
        norm.swap(tmp);
    }

    // 3) Replace remaining newlines/tabs with spaces
    for (char &c : norm) if (c=='\n' || c=='\r' || c=='\t') c = ' ';

    // 4) Convert to wide string for locale-aware lowercasing and classification
    wstring w = utf8_to_wstring(norm);

    // 5) Lowercase (locale-aware using towlower)
    //    Note: this does not perform full Unicode case folding, but is reasonable for many languages.
    for (wchar_t &wc : w) wc = towlower(wc);

    vector<TokenRec> out;
    out.reserve(256);
    // Tokenization rules:
    // - Accept alphanumeric sequences as token characters.
    // - Allow internal apostrophe (U+0027) if between letters; otherwise treat apostrophe as delimiter.
    // - Split on other punctuation.
    int pos = 0;
    wstring cur;
    wstring cur_orig; // track original version before lowercasing? We already lowercased; orig will be built from raw substring instead
    // To compute original token, we will extract substring from the original normalized utf8 string,
    // find corresponding utf-8 bytes from the wide-token -> but that's complex.
    // Simpler: we will keep original token reconstructed from the original (pre-lowercased) wstring.
    wstring w_orig = utf8_to_wstring(norm); // original before lowercasing
    size_t idx = 0; // index into w and w_orig (they have same length)
    size_t i = 0;
    while (i < w.size()) {
        wchar_t wc = w[i];
        bool is_alnum = iswalnum(wc);
        bool is_apostrophe = (wc == L'\''); // U+0027
        if (is_alnum || is_apostrophe) {
            // handle apostrophe: include only if between letters (in the original characters)
            if (is_apostrophe) {
                bool keep = false;
                if (i>0 && i+1 < w.size()) {
                    wchar_t before = w[i-1];
                    wchar_t after = w[i+1];
                    if (iswalpha(before) && iswalpha(after)) keep = true;
                }
                if (!keep) {
                    // treat apostrophe as delimiter -> flush cur
                    if (!cur.empty()) {
                        // push token
                        pos++;
                        // convert cur back to utf8
                        string term = wstring_to_utf8(cur);
                        string orig = wstring_to_utf8(w_orig.substr(i - cur.size(), cur.size()));
                        // number removal policy
                        bool all_digits = true;
                        for (wchar_t c2 : cur) if (!iswdigit(c2)) { all_digits = false; break; }
                        if (opt.remove_numbers && all_digits) {
                            // skip
                        } else {
                            // stopword filtering on lowercased ASCII tokens
                            string term_ascii = term;
                            // for stopword matching, normalize ASCII: simple lowercase already applied
                            if (!(opt.use_stopwords && stopwords.find(term_ascii) != stopwords.end())) {
                                // stemming if enabled
                                string final_term = term;
                                if (opt.stem) final_term = PorterStemmer::stem(final_term);
                                TokenRec rec; rec.term = final_term; rec.orig = opt.keep_original ? orig : ""; rec.pos = pos;
                                out.push_back(move(rec));
                            }
                        }
                        cur.clear();
                    }
                    i++;
                    continue;
                } else {
                    // keep apostrophe inside token
                    cur.push_back(wc);
                    i++;
                    continue;
                }
            } else {
                cur.push_back(wc);
                i++;
                continue;
            }
        } else {
            // delimiter: flush token
            if (!cur.empty()) {
                pos++;
                string term = wstring_to_utf8(cur);
                // original mapping: we approximate original by lowercased -> original may differ in case only
                string orig = wstring_to_utf8(cur); // approximate
                bool all_digits = true;
                for (wchar_t c2 : cur) if (!iswdigit(c2)) { all_digits = false; break; }
                if (opt.remove_numbers && all_digits) {
                    // skip
                } else {
                    if (!(opt.use_stopwords && stopwords.find(term) != stopwords.end())) {
                        string final_term = term;
                        if (opt.stem) final_term = PorterStemmer::stem(final_term);
                        TokenRec rec; rec.term = final_term; rec.orig = opt.keep_original ? orig : ""; rec.pos = pos;
                        out.push_back(move(rec));
                    }
                }
                cur.clear();
            }
            ++i;
            continue;
        }
    }
    // flush last
    if (!cur.empty()) {
        pos++;
        string term = wstring_to_utf8(cur);
        string orig = wstring_to_utf8(cur);
        bool all_digits = true;
        for (wchar_t c2 : cur) if (!iswdigit(c2)) { all_digits = false; break; }
        if (!(opt.remove_numbers && all_digits) && !(opt.use_stopwords && stopwords.find(term) != stopwords.end())) {
            string final_term = term;
            if (opt.stem) final_term = PorterStemmer::stem(final_term);
            TokenRec rec; rec.term = final_term; rec.orig = opt.keep_original ? orig : ""; rec.pos = pos;
            out.push_back(move(rec));
        }
    }
    return out;
}

// Helper: split string by separator, trimming
static vector<string> split_and_trim(const string &s, const string &sep = " | ") {
    vector<string> out;
    size_t start = 0;
    size_t n = s.size();
    while (start < n) {
        size_t pos = s.find(sep, start);
        if (pos == string::npos) pos = n;
        string token = s.substr(start, pos - start);
        // trim
        auto a = token.find_first_not_of(" \t\r\n");
        if (a==string::npos) token = "";
        else token = token.substr(a, token.find_last_not_of(" \t\r\n") - a + 1);
        if (!token.empty()) out.push_back(token);
        start = pos + sep.size();
    }
    return out;
}

// Parse command line args (very light)
static Options parse_args(int argc, char** argv) {
    Options opt;
    for (int i=1;i<argc;++i) {
        string a = argv[i];
        if (a=="-i" && i+1<argc) { opt.input = argv[++i]; }
        else if (a=="-if" && i+1<argc) { opt.input_format = argv[++i]; }
        else if (a=="-o" && i+1<argc) { opt.output = argv[++i]; }
        else if (a=="--stopwords" && i+1<argc) { opt.stopwords_file = argv[++i]; }
        else if (a=="--no-stopwords") { opt.use_stopwords = false; }
        else if (a=="--stem") { opt.stem = true; }
        else if (a=="--keep-original") { opt.keep_original = true; }
        else if (a=="--remove-numbers") { opt.remove_numbers = true; }
        else if (a=="--fields" && i+1<argc) {
            string f = argv[++i];
            opt.fields.clear();
            stringstream ss(f);
            string p;
            while (getline(ss,p,',')) {
                // trim
                auto a2 = p.find_first_not_of(" \t");
                if (a2==string::npos) continue;
                auto b2 = p.find_last_not_of(" \t");
                opt.fields.push_back(p.substr(a2,b2-a2+1));
            }
        } else {
            cerr << "Unknown arg: " << a << "\n";
        }
    }
    return opt;
}

// Main
int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    if (argc==1) {
        cerr << "Usage: cleaner -i input -if [tsv|jsonl] -o out.jsonl [--stopwords file] [--no-stopwords] [--stem] [--keep-original] [--remove-numbers] [--fields title,abstract,sections]\n";
        return 1;
    }

    Options opt = parse_args(argc, argv);
    unordered_set<string> stopwords;
    if (opt.use_stopwords && !opt.stopwords_file.empty()) {
        stopwords = load_stopwords(opt.stopwords_file);
        cerr << "Loaded " << stopwords.size() << " stopwords\n";
    } else if (opt.use_stopwords && opt.stopwords_file.empty()) {
        // basic default stopword list (small)
        vector<string> defaults = {
            "the","and","is","in","it","of","to","a","for","that","on","with","as","are","by","this","was","an","be","or","from"
        };
        for (auto &w: defaults) stopwords.insert(w);
        cerr << "Using built-in default stoplist (" << stopwords.size() << " words). Use --stopwords to load your own.\n";
    } else {
        cerr << "Stopword removal disabled (--no-stopwords)\n";
    }

    // open input and output
    if (opt.input.empty()) {
        cerr << "Input (-i) required\n";
        return 1;
    }
    ifstream ifs(opt.input);
    if (!ifs.is_open()) {
        cerr << "Cannot open input: " << opt.input << "\n";
        return 1;
    }
    ofstream ofs(opt.output);
    if (!ofs.is_open()) {
        cerr << "Cannot open output: " << opt.output << "\n";
        return 1;
    }

    string line;
    size_t line_no = 0;
    while (true) {
        if (opt.input_format == "tsv") {
            if (!getline(ifs, line)) break;
            line_no++;
            if (line.size()==0) continue;
            // our extractor TSV columns:
            // 1 paper_id,2 title,3 abstract,4 sections (separated by " | "),5 authors,6 pub_date,7 doi,8 source
            vector<string> cols;
            cols.reserve(8);
            size_t start = 0;
            for (int c=0;c<7;++c) {
                size_t pos = line.find('\t', start);
                if (pos == string::npos) { cols.push_back(line.substr(start)); start = line.size(); }
                else { cols.push_back(line.substr(start, pos-start)); start = pos+1; }
            }
            // last col
            if (start <= line.size()) cols.push_back(line.substr(start));
            while (cols.size() < 8) cols.push_back("");
            string docid = cols[0];
            string title = cols[1];
            string abstract = cols[2];
            string sections = cols[3];
            string authors = cols[4];
            string pub_date = cols[5];
            string source = (cols.size()>7 ? cols[7] : "");
            // Build JSON for this doc
            Document outdoc(kObjectType);
            Document::AllocatorType &alloc = outdoc.GetAllocator();
            outdoc.AddMember("docid", Value().SetString(docid.c_str(), (SizeType)docid.size(), alloc), alloc);
            Value fields_array(kArrayType);

            for (auto &fname : opt.fields) {
                if (fname == "title") {
                    auto tokens = tokenize_field(title, opt, stopwords);
                    Value fobj(kObjectType);
                    fobj.AddMember("name", Value().SetString("title", alloc), alloc);
                    Value toks(kArrayType);
                    for (auto &t : tokens) {
                        Value tobj(kObjectType);
                        tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                        tobj.AddMember("pos", t.pos, alloc);
                        if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                        toks.PushBack(tobj, alloc);
                    }
                    fobj.AddMember("tokens", toks, alloc);
                    fields_array.PushBack(fobj, alloc);
                } else if (fname == "abstract") {
                    auto tokens = tokenize_field(abstract, opt, stopwords);
                    Value fobj(kObjectType);
                    fobj.AddMember("name", Value().SetString("abstract", alloc), alloc);
                    Value toks(kArrayType);
                    for (auto &t : tokens) {
                        Value tobj(kObjectType);
                        tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                        tobj.AddMember("pos", t.pos, alloc);
                        if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                        toks.PushBack(tobj, alloc);
                    }
                    fobj.AddMember("tokens", toks, alloc);
                    fields_array.PushBack(fobj, alloc);
                } else if (fname == "sections" || fname=="body") {
                    // sections string contains pieces separated by " | "
                    auto parts = split_and_trim(sections, " | ");
                    int sec_idx = 0;
                    for (auto &part : parts) {
                        // optional: name attempt — parts may be "SectionName: text"
                        string name = "body";
                        size_t pcol = part.find(':');
                        string text = part;
                        if (pcol != string::npos && pcol < 50) {
                            name = part.substr(0,pcol);
                            text = part.substr(pcol+1);
                        } else {
                            // use name "body#N"
                            name = "body";
                        }
                        auto tokens = tokenize_field(text, opt, stopwords);
                        Value fobj(kObjectType);
                        fobj.AddMember("name", Value().SetString(name.c_str(), (SizeType)name.size(), alloc), alloc);
                        Value toks(kArrayType);
                        for (auto &t : tokens) {
                            Value tobj(kObjectType);
                            tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                            tobj.AddMember("pos", t.pos, alloc);
                            if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                            toks.PushBack(tobj, alloc);
                        }
                        fobj.AddMember("tokens", toks, alloc);
                        fields_array.PushBack(fobj, alloc);
                        sec_idx++;
                    }
                } else {
                    // unknown field: skip or add empty
                }
            }

            outdoc.AddMember("fields", fields_array, alloc);

            // meta object
            Value meta(kObjectType);
            meta.AddMember("authors", Value().SetString(authors.c_str(), (SizeType)authors.size(), alloc), alloc);
            meta.AddMember("pub_date", Value().SetString(pub_date.c_str(), (SizeType)pub_date.size(), alloc), alloc);
            meta.AddMember("source", Value().SetString(source.c_str(), (SizeType)source.size(), alloc), alloc);
            outdoc.AddMember("meta", meta, alloc);

            // write JSON line
            StringBuffer sb;
            Writer<StringBuffer> writer(sb);
            outdoc.Accept(writer);
            ofs << sb.GetString() << "\n";

        } else if (opt.input_format == "jsonl") {
            if (!getline(ifs, line)) break;
            line_no++;
            if (line.empty()) continue;
            Document indoc;
            indoc.Parse(line.c_str());
            if (indoc.HasParseError()) {
                cerr << "JSON parse error at line " << line_no << "\n";
                continue;
            }
            // Expecting extractor's JSONL shape: { "paper_id": "...", "title": "...", "abstract": "...", "sections": [...], "authors": "...", ...}
            string docid = indoc.HasMember("paper_id") && indoc["paper_id"].IsString() ? indoc["paper_id"].GetString() : (indoc.HasMember("docid") && indoc["docid"].IsString() ? indoc["docid"].GetString() : to_string(line_no));

            Document outdoc(kObjectType);
            Document::AllocatorType &alloc = outdoc.GetAllocator();
            outdoc.AddMember("docid", Value().SetString(docid.c_str(), (SizeType)docid.size(), alloc), alloc);
            Value fields_array(kArrayType);

            for (auto &fname : opt.fields) {
                if (fname == "title") {
                    string title = indoc.HasMember("title") && indoc["title"].IsString() ? indoc["title"].GetString() : "";
                    auto tokens = tokenize_field(title, opt, stopwords);
                    Value fobj(kObjectType);
                    fobj.AddMember("name", Value().SetString("title", alloc), alloc);
                    Value toks(kArrayType);
                    for (auto &t : tokens) {
                        Value tobj(kObjectType);
                        tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                        tobj.AddMember("pos", t.pos, alloc);
                        if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                        toks.PushBack(tobj, alloc);
                    }
                    fobj.AddMember("tokens", toks, alloc);
                    fields_array.PushBack(fobj, alloc);
                } else if (fname == "abstract") {
                    string abs = indoc.HasMember("abstract") && indoc["abstract"].IsString() ? indoc["abstract"].GetString() : "";
                    auto tokens = tokenize_field(abs, opt, stopwords);
                    Value fobj(kObjectType);
                    fobj.AddMember("name", Value().SetString("abstract", alloc), alloc);
                    Value toks(kArrayType);
                    for (auto &t : tokens) {
                        Value tobj(kObjectType);
                        tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                        tobj.AddMember("pos", t.pos, alloc);
                        if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                        toks.PushBack(tobj, alloc);
                    }
                    fobj.AddMember("tokens", toks, alloc);
                    fields_array.PushBack(fobj, alloc);
                } else if (fname == "sections" || fname == "body") {
                    // 'sections' might be an array of strings in extractor JSONL
                    if (indoc.HasMember("sections") && indoc["sections"].IsArray()) {
                        for (auto &el : indoc["sections"].GetArray()) {
                            if (!el.IsString()) continue;
                            string part = el.GetString();
                            string name = "body";
                            size_t pcol = part.find(':');
                            string text = part;
                            if (pcol != string::npos && pcol < 50) {
                                name = part.substr(0,pcol);
                                text = part.substr(pcol+1);
                            }
                            auto tokens = tokenize_field(text, opt, stopwords);
                            Value fobj(kObjectType);
                            fobj.AddMember("name", Value().SetString(name.c_str(), (SizeType)name.size(), alloc), alloc);
                            Value toks(kArrayType);
                            for (auto &t : tokens) {
                                Value tobj(kObjectType);
                                tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                                tobj.AddMember("pos", t.pos, alloc);
                                if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                                toks.PushBack(tobj, alloc);
                            }
                            fobj.AddMember("tokens", toks, alloc);
                            fields_array.PushBack(fobj, alloc);
                        }
                    } else if (indoc.HasMember("body") && indoc["body"].IsString()) {
                        string text = indoc["body"].GetString();
                        auto tokens = tokenize_field(text, opt, stopwords);
                        Value fobj(kObjectType);
                        fobj.AddMember("name", Value().SetString("body", alloc), alloc);
                        Value toks(kArrayType);
                        for (auto &t : tokens) {
                            Value tobj(kObjectType);
                            tobj.AddMember("term", Value().SetString(t.term.c_str(), (SizeType)t.term.size(), alloc), alloc);
                            tobj.AddMember("pos", t.pos, alloc);
                            if (opt.keep_original) tobj.AddMember("orig", Value().SetString(t.orig.c_str(), (SizeType)t.orig.size(), alloc), alloc);
                            toks.PushBack(tobj, alloc);
                        }
                        fobj.AddMember("tokens", toks, alloc);
                        fields_array.PushBack(fobj, alloc);
                    }
                }
            }

            outdoc.AddMember("fields", fields_array, alloc);
            Value meta(kObjectType);
            if (indoc.HasMember("authors") && indoc["authors"].IsString())
                meta.AddMember("authors", Value().SetString(indoc["authors"].GetString(), alloc), alloc);
            if (indoc.HasMember("pub_date") && indoc["pub_date"].IsString())
                meta.AddMember("pub_date", Value().SetString(indoc["pub_date"].GetString(), alloc), alloc);
            if (indoc.HasMember("source") && indoc["source"].IsString())
                meta.AddMember("source", Value().SetString(indoc["source"].GetString(), alloc), alloc);
            outdoc.AddMember("meta", meta, alloc);

            StringBuffer sb;
            Writer<StringBuffer> writer(sb);
            outdoc.Accept(writer);
            ofs << sb.GetString() << "\n";

        } else {
            cerr << "Unknown input format: " << opt.input_format << "\n";
            break;
        }
    }

    ofs.close();
    cerr << "Done. Tokens written to " << opt.output << "\n";
    return 0;
}
