// performance_monitor.hpp
// Cross-platform performance monitoring for IR system

#ifndef PERFORMANCE_MONITOR_HPP
#define PERFORMANCE_MONITOR_HPP

#include <bits/stdc++.h>

#ifdef _WIN32
    #include <windows.h>
    #include <psapi.h>
#else
    #include <sys/resource.h>
    #include <unistd.h>
#endif

using namespace std;

class PerformanceMonitor {
private:
    struct QueryStats {
        size_t total_queries = 0;
        double total_time_ms = 0.0;
        double min_time_ms = numeric_limits<double>::max();
        double max_time_ms = 0.0;
        vector<double> query_times;
    };

    struct MemoryStats {
        size_t peak_memory_kb = 0;
        size_t current_memory_kb = 0;
        size_t index_size_kb = 0;
    };

    QueryStats query_stats;
    MemoryStats memory_stats;
    chrono::time_point<chrono::high_resolution_clock> last_query_start;

    // Cross-platform memory usage
    size_t get_memory_usage_kb() {
#ifdef _WIN32
        PROCESS_MEMORY_COUNTERS pmc;
        GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
        return pmc.WorkingSetSize / 1024; // bytes → KB
#else
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        #ifdef __APPLE__
            return usage.ru_maxrss / 1024; // bytes → KB
        #else
            return usage.ru_maxrss;        // already KB on Linux
        #endif
#endif
    }

public:
    PerformanceMonitor() {}

    void start_query() {
        last_query_start = chrono::high_resolution_clock::now();
    }

    double end_query() {
        auto end = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::microseconds>(end - last_query_start);
        double time_ms = duration.count() / 1000.0;

        query_stats.total_queries++;
        query_stats.total_time_ms += time_ms;
        query_stats.min_time_ms = min(query_stats.min_time_ms, time_ms);
        query_stats.max_time_ms = max(query_stats.max_time_ms, time_ms);
        query_stats.query_times.push_back(time_ms);

        size_t mem = get_memory_usage_kb();
        memory_stats.current_memory_kb = mem;
        memory_stats.peak_memory_kb = max(memory_stats.peak_memory_kb, mem);

        return time_ms;
    }

    void set_index_size(size_t size_kb) {
        memory_stats.index_size_kb = size_kb;
    }

    double get_percentile(double p) {
        if (query_stats.query_times.empty()) return 0.0;
        vector<double> sorted = query_stats.query_times;
        sort(sorted.begin(), sorted.end());
        size_t idx = static_cast<size_t>(p * sorted.size() / 100.0);
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }

    void print_report(ostream& os = cout) {
        os << "\n=== PERFORMANCE REPORT ===\n\n";
        os << "Query Performance:\n";
        os << "  Total queries: " << query_stats.total_queries << "\n";

        if (query_stats.total_queries > 0) {
            double avg = query_stats.total_time_ms / query_stats.total_queries;
            os << fixed << setprecision(2);
            os << "  Average time: " << avg << " ms\n";
            os << "  Min time: " << query_stats.min_time_ms << " ms\n";
            os << "  Max time: " << query_stats.max_time_ms << " ms\n";
            os << "  P50: " << get_percentile(50) << " ms\n";
            os << "  P95: " << get_percentile(95) << " ms\n";
            os << "  P99: " << get_percentile(99) << " ms\n";
        }

        os << "\nMemory Usage:\n";
        os << "  Current: " << memory_stats.current_memory_kb / 1024.0 << " MB\n";
        os << "  Peak: " << memory_stats.peak_memory_kb / 1024.0 << " MB\n";
        os << "  Index size: " << memory_stats.index_size_kb / 1024.0 << " MB\n";

        os << "\n=========================\n\n";
    }

    void save_stats(const string& filename) {
        ofstream ofs(filename);
        if (!ofs) return;

        ofs << "query_count," << query_stats.total_queries << "\n";
        ofs << "avg_time_ms,"
            << (query_stats.total_queries ? query_stats.total_time_ms / query_stats.total_queries : 0)
            << "\n";
        ofs << "p95_time_ms," << get_percentile(95) << "\n";
        ofs << "peak_memory_mb," << memory_stats.peak_memory_kb / 1024.0 << "\n";
    }
};

// Indexing performance tracker
class IndexingMonitor {
private:
    chrono::time_point<chrono::high_resolution_clock> start_time;
    size_t docs_processed = 0;

public:
    void start() {
        start_time = chrono::high_resolution_clock::now();
        docs_processed = 0;
    }

    void record_doc() {
        docs_processed++;
    }

    void print_summary() {
        auto end = chrono::high_resolution_clock::now();
        auto secs = chrono::duration_cast<chrono::seconds>(end - start_time).count();
        cout << "\n=== INDEXING PERFORMANCE ===\n";
        cout << "Documents processed: " << docs_processed << "\n";
        cout << "Total time: " << secs << " seconds\n";
        cout << "Throughput: " << (docs_processed / (secs + 0.01)) << " docs/sec\n";
        cout << "============================\n\n";
    }
};

#endif // PERFORMANCE_MONITOR_HPP
