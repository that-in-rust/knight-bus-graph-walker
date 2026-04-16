# End of v001, Start of v002

`v001` is closed as the first benchmarked CLI proof for Knight Bus.

What `v001` established:

- CSV truth can be compiled into an immutable dual-CSR snapshot
- parity can be checked against the truth layer
- the Rust CLI can answer hop queries and run corpus benchmarks
- the repo now has saved benchmark truth for tiny, smoke, preflight, and `code_sparse_2gb` reruns

What `v002` should focus on next:

- reduce memory pressure during build and verification
- separate parity/truth machinery from pure runtime benchmarking
- improve the honesty of runtime-only RSS claims
- continue scaling beyond the current `~2 GB` proof datasets
