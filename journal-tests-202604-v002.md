# journal-tests-202604-v002

- generated_at: 2026-04-16 16:26:18 IST
- repo: /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker
- note: this is the corrected runtime-only fresh-check ledger; `v001-learnings` remains archived history.

| stage | dataset | raw_csv_bytes | node_count | edge_count | snapshot_size_bytes | rust_status | neo4j_status | query_corpus_size | rust_p50_ms | rust_p95_ms | rust_p99_ms | rust_mean_ms | rust_rss_bytes | rust_rss_scope | rust_rss_source | rust_verify_peak_rss | neo4j_p50_ms | neo4j_p95_ms | neo4j_p99_ms | neo4j_mean_ms | neo4j_rss_bytes | neo4j_rss_scope | neo4j_rss_source | import_duration_ms | report_path |
| --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |
| freshcheck_neo4j_smoke_1mb | 1 MB | 1068156 | 1949 | 17722 | 251390 | ok | ok | 18 | 0.00175 | 0.018477 | 0.02555 | 0.005261 | 6668288 | runtime_process_only | getrusage_self | 11059200 | 2.974563 | 10.504973 | 12.611896 | 4.385152 | 525926400 | server_process_only | psutil_server_process | 3063.740125 | /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker/reports/freshcheck_neo4j_smoke_1mb/report.json |
| freshcheck_neo4j_preflight_50mb | 50 MB | 53412207 | 97606 | 886085 | 12555088 | ok | ok | 60 | 0.002125 | 0.020296 | 0.0363 | 0.006249 | 14499840 | runtime_process_only | getrusage_self | 107954176 | 37.208291 | 43.710169 | 52.235973 | 38.203163 | 616054784 | server_process_only | psutil_server_process | 5887.129583 | /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker/reports/freshcheck_neo4j_preflight_50mb/report.json |
| freshcheck_code_sparse_2gb | 2 GB | 2187775971 | 3997988 | 36294270 | 514241964 | ok | ok | 60 | 0.004458 | 0.028146 | 0.044948 | 0.008815 | 234340352 | runtime_process_only | getrusage_self | 409452544 | 1096.492583 | 1382.781209 | 1514.533206 | 1123.882205 | 1065615360 | server_process_only | psutil_server_process | 42080.808125 | /Users/neetipatni/Desktop/Codex202604/knight-bus-graph-walker/reports/freshcheck_code_sparse_2gb/report.json |

- verdict: corrected fresh check completed against the three fixed datasets with runtime-only Rust RSS and server-process Neo4j RSS.
- note: Knight Bus build and verify costs are intentionally recorded outside the runtime-only comparison headline.

