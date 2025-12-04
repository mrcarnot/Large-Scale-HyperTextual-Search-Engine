📚 Research Paper Search Engine — Indexing Pipeline in C++
Overview

This project implements a simplified but realistic search engine indexing pipeline for academic documents using C++.
It processes PDF and PMC research papers, extracts text, cleans and normalizes it, and builds efficient indexing structures for search.

The goal is to understand how a real search engine is implemented internally:

Text extraction

Cleaning and tokenization

Term normalization

Lexicon construction

Forward indexing

Inverted indexing

Blocked indexing (SPIMI-style)

Compression using delta encoding + variable-byte encoding

🧱 Pipeline Architecture
PDF / PMC JSON files
       ↓
 extractor.cpp
       ↓
 TSV / JSONL
       ↓
 cleaner.cpp
       ↓
 cleaned.jsonl
       ↓
 indexer.cpp
       ↓
 Lexicon + Inverted Index + Forward Index


Each stage produces structured output passed to the next stage.

📁 Project Layout
Directory / File	Purpose
extractor.cpp	Extracts text + metadata from PDF / PMC JSON files
extractor.exe	Compiled extractor binary
cleaner.cpp	Normalizes, tokenizes, assigns word positions
cleaner.exe	Cleaner binary
indexer.cpp	Builds lexicon, forward index, inverted index
indexer.exe	Indexer binary
pdf/	PDF-format research paper JSON
pmc/	PMC-format research paper JSON
cleaner3.jsonl	Cleaned PDF dataset
cleaner3pmc.jsonl	Cleaned PMC dataset
index_dir4pdf/	Final index for PDF
index_dir4pmc/	Final index for PMC
postings.bin	Compressed posting lists
lexicon.txt	Dictionary (term → metadata)
terms_list.txt	Term → wordID mapping
forward_index.jsonl	Document-level index
block_*.inv	Temporary inverted index blocks
block_*.fwd.jsonl	Temporary forward index blocks
docid_map.txt	Mapping from string docIDs to numeric IDs
rapidjson-master/	JSON parsing library
🛠 Compilation
✅ Requirements

g++

C++17 or later

RapidJSON (already included)

Windows (MinGW / MSYS2) or Linux

✅ Compile All Programs

From the project root:

g++ -std=c++17 -O2 extractor.cpp -Irapidjson-master/include -o extractor.exe
g++ -std=c++17 -O2 cleaner.cpp   -Irapidjson-master/include -o cleaner.exe
g++ -std=c++17 -O2 indexer.cpp   -Irapidjson-master/include -o indexer.exe

▶️ Running the Pipeline
🔹 Step 1: Run the Extractor

For PDF:

./extractor.exe pdf out.tsv


For PMC:

./extractor.exe pmc outpmc.tsv


Produces a .tsv containing extracted text and metadata.

🔹 Step 2: Run the Cleaner

For PDFs:

./cleaner.exe out.tsv cleaner3.jsonl


For PMC:

./cleaner.exe outpmc.tsv cleaner3pmc.jsonl


Produces lines like:

{
  "docid": "PMC1234",
  "fields": [
    {"name": "title", "tokens": [{"term": "covid", "pos": 1}]}
  ]
}

🔹 Step 3: Run the Indexer

For PDF:

./indexer.exe -i cleaner3.jsonl -o index_dir4pdf --block-size 10000 --skip-interval 128


For PMC:

./indexer.exe -i cleaner3pmc.jsonl -o index_dir4pmc --block-size 10000 --skip-interval 128

📦 Output Files Explained
📘 Lexicon (lexicon.txt)
Field	Meaning
wordID	Unique ID per term
term	Word
doc_freq	Number of documents containing term
term_freq	Total occurrences
offset	Byte position in postings.bin
bytes	Size of posting list
skip_meta	Skip pointer parameters
📕 postings.bin

Binary file storing:

[doc_count]
[docID delta]
[term frequency]
[position deltas]
...


DocIDs and positions are:

delta-encoded

variable-byte encoded

Efficient and compact.

📗 Forward Index (forward_index.jsonl)
docID → all terms in this document


Example:

{"docid":"PMC1","postings":[{"wordid":4,"freq":2,"positions":[7,19]}]}


Used later for:

ranking

document length

phrase queries

snippets

📂 block_*.inv

Temporary inverted index created per memory block:

term<TAB>docID:positions;docID:positions


Example:

covid   1:4,9;3:5,8

🧠 Core Concepts
✅ What is SPIMI?

SPIMI = Single-Pass In-Memory Indexing.

Instead of storing the entire index in RAM:

process 10k documents

dump a block

merge later

✅ What is an Inverted Index?

Maps terms → documents.

Like a dictionary in a book.

✅ What is Variable Byte Encoding?

Compresses numbers using:

7 bits of data

1 bit continuation flag

Smaller numbers ≈ fewer bytes.

✅ Why delta encoding?

Instead of:

IDs: 100, 105, 108


Store:

100, 5, 3


Smaller numbers compress better.

✅ Skip pointers

Metadata to speed up:

AND queries

intersecting long posting lists

🛑 Limitations

No query parser yet

No ranking model

No GUI

No multi-threading

🚀 Future Roadmap

Implement Boolean search

Add BM25 ranking

Phrase queries

Web interface

Faster compression

Block-level skip pointers

✅ How To Verify Output

lexicon.txt offsets increase

postings.bin grows

forward_index.jsonl not empty

docid_map.txt contains mappings

block_0.inv readable




