# Neo4j Smoke Runbook

This repo owns the local smoke ladder for the Rust `knight-bus` binary versus Neo4j, orchestrated by the Python harness in `benchmarks/walk_hopper_v1`.

## What It Does

The smoke ladder:

1. installs Neo4j Community with Homebrew if it is missing
2. creates a repo-local benchmark virtualenv
3. generates a `1 MB` synthetic graph
4. builds the Knight Bus snapshot
5. exports Neo4j header/data CSV files
6. runs `neo4j-admin database import full` into the default `neo4j` database
7. benchmarks `knight-bus bench-corpus` vs Neo4j on the same query corpus
8. if green, repeats the process at `50 MB`
9. renders `Final-Testing-Journal.md` plus an appended truth entry in `docs/journal-tests-202604.md`

## Files And Paths

- local Neo4j env: `.env.neo4j.local`
- local Python env: `.venv-bench/`
- generated datasets: `artifacts/`
- generated benchmark reports: `reports/`

These are ignored by Git.

## Commands

Install Neo4j and initialize local credentials:

```bash
./scripts/install_neo4j_brew.sh
```

Run the smoke ladder:

```bash
./scripts/run_neo4j_smoke_ladder.sh
```

## Expected Timing

On this `16 GB / 8 CPU` Mac:

- first-time Neo4j install/setup: about `20-40 min`
- `1 MB` smoke: about `5-15 min`
- `50 MB` preflight after green smoke: about `15-45 min`

## Stop Rules

- stop if Neo4j install/setup exceeds `40 min`
- stop if the `1 MB` smoke import fails
- stop if the `1 MB` parity check fails
- do not attempt `20 GB` in this phase

## Notes

- the scripts use the default `neo4j` database because Community Edition supports one standard database
- the import path uses explicit header + data files to match the current Neo4j admin import documentation
