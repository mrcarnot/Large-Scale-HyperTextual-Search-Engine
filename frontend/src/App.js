import React, { useState, useEffect, useRef } from 'react';
import {
  Search, Book, TrendingUp, Loader,
  Database, Clock, X, Brain, Upload, FileText, Check, ExternalLink
} from 'lucide-react';
import './App.css';

const API_BASE = 'http://localhost:8080/api';

const SearchEngine = () => {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [isSearching, setIsSearching] = useState(false);
  const [searchTime, setSearchTime] = useState(0);
  
  // Search Configuration
  const [searchType, setSearchType] = useState('keyword'); // keyword | hybrid (displayed as Semantic)
  const [queryMode, setQueryMode] = useState('single');    // single | phrase
  const [booleanOp, setBooleanOp] = useState('AND');       // AND | OR

  // Stats & Upload
  const [stats, setStats] = useState(null);
  const [error, setError] = useState('');
  const [isUploading, setIsUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState('');

  const searchInputRef = useRef(null);
  const suggestionsRef = useRef(null);
  const fileInputRef = useRef(null);

  /* ------------------ INITIALIZATION ------------------ */
  useEffect(() => {
    fetchStats();
  }, []);

  const fetchStats = () => {
    fetch(`${API_BASE}/stats`)
      .then(res => res.json())
      .then(setStats)
      .catch(() => setError('Backend not running on port 8080'));
  };

  /* ---------------- AUTOCOMPLETE ---------------- */
  useEffect(() => {
    if (query.length < 2) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }

    const timeout = setTimeout(async () => {
      try {
        const res = await fetch(`${API_BASE}/autocomplete?q=${encodeURIComponent(query)}&limit=6`);
        const data = await res.json();
        setSuggestions(data.suggestions || []);
        setShowSuggestions(true);
      } catch {
        setShowSuggestions(false);
      }
    }, 150);

    return () => clearTimeout(timeout);
  }, [query]);

  /* ---------------- SEARCH ---------------- */
  const handleSearch = async () => {
    if (!query.trim()) return;

    if (queryMode === 'phrase' && query.trim().split(/\s+/).length < 2) {
      setError('Phrase search requires at least two words');
      return;
    }

    setError('');
    setIsSearching(true);
    setShowSuggestions(false);

    const actualSearchType = searchType === 'semantic' ? 'hybrid' : 'keyword';

    try {
      const res = await fetch(`${API_BASE}/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query,
          top_k: 10,
          search_type: actualSearchType, 
          query_mode: queryMode,
          boolean_op: booleanOp
        })
      });

      const data = await res.json();
      setResults(data.results || []);
      setSearchTime(data.time_ms || 0);
    } catch {
      setError('Search request failed');
    } finally {
      setIsSearching(false);
    }
  };

  /* ---------------- UPLOAD ---------------- */
  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setIsUploading(true);
    setUploadStatus('Uploading & Indexing...');
    setError('');

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch(`${API_BASE}/upload`, {
        method: 'POST',
        body: formData
      });

      const data = await res.json();
      if (res.ok) {
        setUploadStatus(`Success: ${data.message}`);
        fetchStats(); 
        setTimeout(() => setUploadStatus(''), 3000);
      } else {
        setError(data.error || 'Upload failed');
        setUploadStatus('');
      }
    } catch (err) {
      setError('Upload error: ' + err.message);
      setUploadStatus('');
    } finally {
      setIsUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  return (
    <div className="app">
      {/* ---------------- HEADER ---------------- */}
      <header className="header">
        <div className="logo">
          <Book size={28} />
          <h1>AcademicSearch <span className="version">v2.0</span></h1>
        </div>
        {stats && (
          <div className="stats">
            <span><Database size={14} /> {stats.total_docs} Docs</span>
            <span><Search size={14} /> {stats.total_terms} Terms</span>
          </div>
        )}
      </header>

      {/* ---------------- MAIN CONTENT ---------------- */}
      <main className="main-container">
        
        {/* Error Notification */}
        {error && (
          <div className="error-box">
            <X size={18} /> {error}
          </div>
        )}

        {/* SEARCH SECTION */}
        <div className="search-section">
          
          {/* Top Controls: Search Type */}
          <div className="control-group type-select">
            <button
              className={`tab-btn ${searchType === 'keyword' ? 'active' : ''}`}
              onClick={() => setSearchType('keyword')}
            >
              <Search size={16} /> Keyword
            </button>
            <button
              className={`tab-btn ${searchType === 'semantic' ? 'active' : ''}`}
              onClick={() => setSearchType('semantic')}
            >
              <Brain size={16} /> Semantic
            </button>
          </div>

          {/* Sub Controls: Mode & Boolean */}
          <div className="control-group options-select">
            <div className="toggle-group">
              <span className="label">Mode:</span>
              <button
                className={queryMode === 'single' ? 'active' : ''}
                onClick={() => setQueryMode('single')}
              >
                Normal
              </button>
              <button
                className={queryMode === 'phrase' ? 'active' : ''}
                onClick={() => setQueryMode('phrase')}
              >
                Phrase
              </button>
            </div>

            <div className="toggle-group">
              <span className="label">Logic:</span>
              <button
                className={booleanOp === 'AND' ? 'active' : ''}
                onClick={() => setBooleanOp('AND')}
              >
                AND
              </button>
              <button
                className={booleanOp === 'OR' ? 'active' : ''}
                onClick={() => setBooleanOp('OR')}
              >
                OR
              </button>
            </div>
          </div>

          {/* Search Input Bar */}
          <div className="input-wrapper">
            <input
              ref={searchInputRef}
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
              placeholder={queryMode === 'phrase' ? 'Search for exact phrase...' : 'Search for documents...'}
              className="main-input"
            />
            <button className="search-btn" onClick={handleSearch} disabled={isSearching}>
              {isSearching ? <Loader className="spin" /> : <Search />}
            </button>

            {/* Suggestions Dropdown */}
            {showSuggestions && (
              <div className="suggestions-dropdown" ref={suggestionsRef}>
                {suggestions.map((s, i) => (
                  <div
                    key={i}
                    className="suggestion-item"
                    onClick={() => {
                      setQuery(s.term);
                      handleSearch();
                    }}
                  >
                    <span className="s-term">{s.term}</span>
                    <span className="s-meta">{s.doc_freq} docs</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* RESULTS SECTION */}
        <div className="results-container">
          {results.length > 0 ? (
            results.map((r, i) => (
              <div key={i} className="result-card">
                <div className="result-header">
                  <span className="rank">#{i + 1}</span>
                  {/* UPDATED LINK LOGIC HERE */}
                  <a 
                    href={`https://pmc.ncbi.nlm.nih.gov/articles/${r.docid}/`} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="doc-id"
                  >
                    {r.docid} <ExternalLink size={12} style={{ marginLeft: 4 }}/>
                  </a>
                  <span className="score">
                    <TrendingUp size={14} /> {r.score.toFixed(4)}
                  </span>
                </div>
                <p className="snippet">
                  Document matching query logic <b>{booleanOp}</b> in <b>{queryMode}</b> mode.
                </p>
                <div className="tags">
                  {Object.entries(r.term_freqs || {}).slice(0, 5).map(([t, f]) => (
                    <span key={t} className="tag">{t}: {f}</span>
                  ))}
                </div>
              </div>
            ))
          ) : (
            !isSearching && searchTime > 0 && <div className="no-results">No results found.</div>
          )}
        </div>
      </main>

      {/* ---------------- SIDEBAR / FOOTER ---------------- */}
      <div className="bottom-panel">
        <div className="upload-section">
          <h3><Upload size={16} /> Add Documents</h3>
          <p>Upload .txt files to add to index.</p>
          <div className="upload-controls">
            <input 
              type="file" 
              accept=".txt,.json" 
              ref={fileInputRef}
              onChange={handleFileUpload} 
              style={{display: 'none'}}
              id="file-upload"
            />
            <label htmlFor="file-upload" className="upload-btn">
              {isUploading ? <Loader className="spin" size={14} /> : <FileText size={14} />}
              {isUploading ? ' Indexing...' : ' Select File'}
            </label>
            {uploadStatus && <span className="upload-status"><Check size={14}/> {uploadStatus}</span>}
          </div>
        </div>
        
        <div className="perf-stats">
          <Clock size={16} /> Query Time: <b>{searchTime} ms</b>
        </div>
      </div>
    </div>
  );
};

export default SearchEngine;