# Rust Codebase Onboarding Summary

- Repository root: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`
- Artifact directory: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/.rust-onboard`
- Workspace root: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker`

## Counts
- Packages discovered: 1
- Workspace packages: 1
- Non-workspace package manifests: 0
- Targets discovered: 4
- bin: 1
- lib: 1
- proc-macro: 0
- example: 0
- test: 2
- bench: 0
- custom-build: 0

## Workspace Truth
- Workspace members: `knight-bus`
- No local package dependency edges detected.

## Non-Workspace Truth
- No excluded or standalone Rust manifests detected.

## Key Observations
- Onboarding aids are present via secondary targets such as examples, tests, or benches.

## Start Here
1. Read `entrypoints.md` for likely user-facing crates and source files.
2. Use `targets.tsv` for exact build surfaces.
3. Use `packages.tsv` and `non_workspace_manifests.tsv` to explain workspace membership.
4. Use `workspace_graph.mmd` or `local_dependency_edges.tsv` when package relations matter.

## Discovery Notes
- root-package `cargo metadata` succeeded for `Cargo.toml`
