# Cypher Bolt Walk Compatibility Summary

Verdict: **passed**

Authorized scope: `knight-bus-neighborhood-walk-v1`, Neo4j Python driver `6.1.0`, direct `bolt://`, read-only auto-commit, fixed corpus `60` queries.

| engine | warm p99 ms | peak RSS bytes | result hash |
| --- | ---: | ---: | --- |
| Knight Bus | 3.970300 | 234176512 | `dbda232863c2d4249e829bc665b430a42b9cba13ab3fb92c82f11044b0969ab2` |
| Neo4j | 5.302670 | 374046720 | `dbda232863c2d4249e829bc665b430a42b9cba13ab3fb92c82f11044b0969ab2` |

Cold-open measurements and mmap residency are reported in `compatibility-receipt.json`; Neo4j server cold boot is outside this runner.
