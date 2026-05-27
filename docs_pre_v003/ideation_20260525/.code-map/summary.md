# Rough Codebase Map Summary

## Counts
- Code files: 25
- Symbols: 407
- Import/include edges: 120
- Internal file edges: 12
- External reference edges: 108
- Graph edge cap used: 160

## Tooling
- rg: yes
- ctags: no
- ast-grep: yes
- dot: no

## Top Fan-Out Files
- benchmarks/walk_hopper_v1/tests/test_walk_hopper_v1.py: 6
- benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py: 2
- benchmarks/walk_hopper_v1/build_dual_csr_snapshot.py: 1
- benchmarks/walk_hopper_v1/export_neo4j_import.py: 1
- benchmarks/walk_hopper_v1/generate_code_sparse_data.py: 1
- benchmarks/walk_hopper_v1/query_walk_snapshot.py: 1

## Top Fan-In Files
- benchmarks/walk_hopper_v1/common.py: 6
- benchmarks/walk_hopper_v1/query_walk_snapshot.py: 2
- benchmarks/walk_hopper_v1/bench_walk_vs_neo4j.py: 1
- benchmarks/walk_hopper_v1/build_dual_csr_snapshot.py: 1
- benchmarks/walk_hopper_v1/export_neo4j_import.py: 1
- benchmarks/walk_hopper_v1/generate_code_sparse_data.py: 1

## Pointer-First Retrieval Pattern
Use `symbols.tsv` and `internal_file_edges.tsv` first. Read code spans only when needed via `file:start:end`.
