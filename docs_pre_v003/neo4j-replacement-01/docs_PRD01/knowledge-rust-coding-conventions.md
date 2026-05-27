# Rust Coding Conventions (from agent-room-of-requirements)

## Four-Word Naming Convention (4WNC)

ALL function names: EXACTLY 4 words (underscores separate)
ALL crate names: EXACTLY 4 words (hyphens separate)
ALL folder names: EXACTLY 4 words (hyphens separate)
ALL commands: EXACTLY 4 words (hyphens separate)

**Pattern**: `verb_constraint_target_qualifier()`

- Verb: `filter`, `render`, `detect`, `save`, `create`, `process`
- Constraint: `implementation`, `box_with_title`, `visualization_output`
- Target: `entities`, `unicode`, `file`, `database`
- Qualifier: `only`, `to`, `in`, `from`, `with`

**Examples**:
```rust
filter_implementation_entities_only()
render_box_with_title_unicode()
save_visualization_output_to_file()
```

## TDD-First Development

Follow STUB → RED → GREEN → REFACTOR cycle. Write tests FIRST.

## Architecture Principles

1. Executable Specifications over narratives — every claim validated by tests
2. Layered Rust Architecture: Core (L1) → Std (L2) → External (L3)
3. Dependency Injection — depend on traits, not concrete types
4. RAII Resource Management — Drop implementations for all resources
5. Performance claims must be test-validated
6. `thiserror` for libraries, `anyhow` for applications
7. Concurrency model validated with stress tests
8. MVP-First — proven architectures over theoretical abstractions

## Diagrams

ALL diagrams in Mermaid only (GitHub-compatible). No exceptions.

## Key Reliability Patterns (rust-coder-02, score 90+)

| ID | Score | Pattern |
|---|---:|---|
| RC2-02 | 98 | Newtypes and parse-don't-validate |
| RC2-12 | 97 | `thiserror` in libraries, `anyhow`/`eyre` in binaries |
| RC2-20 | 97 | No blocking and no locks across `.await` |
| RC2-21 | 96 | Bounded channels, backpressure, cancellation tokens |
| RC2-53 | 95 | Unsafe encapsulation with minimal surface, SAFETY docs, Miri |
| RC2-13 | 95 | Opaque public errors with private kind representation |
| RC2-15 | 94 | Actionable diagnostics with exact failure location and repair hint |
| RC2-22 | 94 | Cancel-safe `tokio::select!` and future ownership discipline |
| RC2-01 | 93 | Accept borrowed inputs in public APIs |
| RC2-14 | 92 | `#[non_exhaustive]` on public enums/structs that will grow |
| RC2-11 | 92 | Type-state plus DropBomb for must-complete protocols |
| RC2-28 | 91 | Doctests as executable contracts |
| RC2-41 | 90 | Panic-free arithmetic and slicing |
| RC2-45 | 90 | Interior mutability vs synchronization primitives |
| RC2-30 | 90 | Property-based tests for invariants and round trips |
| RC2-07 | 90 | Named outcome enums over booleans and sentinels |

## Reference

Full agent files in: `agent-room-of-requirements` repo (that-in-rust/agent-room-of-requirements)
