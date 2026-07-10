# Sol-02: Stepwise Architecture Conclusions

<!-- markdownlint-disable MD013 -->

Date: 2026-07-10

Status: living decision record, expanded one answered question at a time

Purpose: distill the large architecture corpus into small, truth-seeking
conclusions that can directly guide the Neo4j-compatible Rust rewrite.

## Question 1: Is There A Pareto Family Of Neo4j Graph Algorithms?

### Short answer

**Probably yes at the algorithm-family level; not proven at the individual
procedure or percentage level.**

There is strong public evidence that Neo4j's commercial graph-analytics usage
concentrates around a small number of recurring problem classes. There is no
public representative procedure telemetry that proves the exact ordering or
the `20% + 15% + ... = 85%` values currently modeled in PRD04.

Use the seven-family set as a prioritized engineering hypothesis, not as a
measured market fact.

### Step 1: What would count as proof?

A measured Pareto claim would require representative evidence such as:

- anonymized counts of GDS procedure executions across customers;
- revenue or active-deployment weighting by algorithm family;
- workload manifests from a representative customer sample;
- a defined time window, edition mix, and deployment type.

Neo4j's public documentation exposes monitoring for current procedures, but no
representative cross-customer frequency distribution was found.

### Step 2: What evidence is actually public?

The available evidence consists of proxies:

1. Which categories Neo4j repeatedly presents as commercially important.
2. Which algorithms appear in product guides, courses, examples, and use-case
   material.
3. Which workloads Neo4j highlights for Aura Graph Analytics.
4. Which algorithms appear in production case studies and graph literature.
5. Which procedures receive production-quality implementations and extensive
   operational documentation.

These signals can establish a likely core and long tail. They cannot establish
precise usage shares.

### Step 3: The likely Pareto core

| Priority family | Representative algorithms | Recurring buyer problems | Confidence it belongs in the core |
| --- | --- | --- | --- |
| Connected components | WCC, SCC where direction matters | Entity resolution, fraud rings, master-data grouping | High |
| Community detection | Louvain, Leiden, Label Propagation | Fraud communities, segmentation, GraphRAG communities | High |
| Centrality and ranking | Degree, PageRank, ArticleRank | Influence, anomaly/risk ranking, relevance | High |
| Similarity and nearest neighbors | Node Similarity, KNN | Recommendations, entity matching, similar-risk profiles | High |
| Paths and traversal | BFS/DFS, Dijkstra, A*, shortest paths | Routing, dependencies, attack paths, investigation | High |
| Node embeddings | FastRP, Node2Vec, GraphSAGE | ML features, recommendation, classification inputs | Medium-high |
| Cohesion and motif features | Triangle Count, Local Clustering Coefficient | Fraud/cohesion features and community quality | Medium |

This is a **family shortlist**. It does not imply that every algorithm inside a
family has equal demand. For example, ordinary source-target shortest path may
be common while all-pairs shortest path is much narrower.

### Step 4: The probable long tail

The current GDS surface also includes narrower or workload-specific operations:

- DAG topological sort and longest path;
- maximum flow and minimum-cost maximum flow;
- spanning and Steiner-tree variants;
- HITS, betweenness, closeness, and harmonic centrality;
- k-core, coloring, HDBSCAN, and specialized cut algorithms;
- individual topological link-prediction scores;
- all-pairs and k-shortest-path variants;
- ML training, tuning, pipeline, and model-catalog operations;
- the Pregel extension surface for custom algorithms.

These are not proven to be rarely used. They are merely less consistently
visible across the public commercial signals reviewed. A niche algorithm can
still be decisive for a particular customer.

### Step 5: Confidence statement

| Statement | Confidence | Reason |
| --- | --- | --- |
| GDS demand is non-uniform and has a Pareto-like shape | High | A small set of categories recurs across commercial, educational, and use-case material despite a much larger catalog |
| The seven PRD04 families are a good initial core | Medium-high | They span the recurring categories and distinct execution/memory signatures |
| The seven explain about 85% of adoption | Low | No representative public telemetry supports the number |
| The order WCC > community > PageRank > similarity is correct | Low-medium | Plausible from use-case emphasis, but buyer and workload mix can reorder it |
| Long-tail procedures can be ignored | False | Surface compatibility and specialist buyers still require explicit behavior |

### Step 6: Decision for Knight Bus

Use a two-level strategy:

```text
optimized core
  = build first-class, algorithm-shaped plans for the seven families

compatible long tail
  = preserve generic projection, catalog, execution-mode, result,
    estimation, rejection, and fallback contracts
```

Consequences:

- Prioritize the seven families for storage and memory experiments.
- Do not bake their estimated percentages into public claims or irreversible
  storage decisions.
- Do not implement hundreds of procedures before the common execution spine is
  proven.
- Do not design a format that makes DAG, filtered traversal, or custom Pregel
  execution impossible merely because those workloads appear less frequent.
- Collect opt-in workload manifests or anonymous family-level counters before
  replacing modeled priority with measured priority.

### Sources and limits

1. [Neo4j GDS algorithm catalog](https://neo4j.com/docs/graph-data-science/current/algorithms/)
   confirms the broad algorithm surface and categories; it does not rank usage.
2. [Neo4j discussion of commercially observed graph features](https://neo4j.com/blog/machine-learning/how-graphs-enhance-artificial-intelligence/)
   says its highlighted categories are those often seen commercially; it is
   vendor testimony, not independently audited telemetry.
3. [Neo4j graph-algorithm guide](https://neo4j.com/blog/graph-data-science/graph-algorithms/)
   repeatedly organizes practical work around paths, centrality, communities,
   similarity, link prediction, and embeddings.
4. [Aura Graph Analytics](https://neo4j.com/docs/aura/graph-analytics/)
   names fraud detection, supply-chain optimization, and recommendation-engine
   development as typical use cases; it does not disclose algorithm frequency.
5. [Neo4j GDS introduction](https://neo4j.com/docs/graph-data-science/current/introduction/)
   documents the large algorithm surface, in-memory graph catalog, projections,
   and API maturity tiers; it does not establish a Pareto percentage.

### Falsifier and next evidence

Revise this conclusion when either of these becomes available:

- representative Neo4j procedure telemetry; or
- at least five target-customer workload manifests recording graph shape,
  procedure family, mode, frequency, scale, and memory failure.

Until then, the honest wording is:

> Public evidence supports a Pareto-like core of graph algorithm families, but
> not the exact 85% estimate or a trustworthy ranking within that core.
