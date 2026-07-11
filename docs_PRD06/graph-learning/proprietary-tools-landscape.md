# Proprietary Tools Landscape

The closed-source / commercial side of the graph, vector-search, and search
market. These systems cannot be shallow-cloned and mapped, so they enter the
study differently: as **behavior endpoints** (docs, benchmarks, published
papers, pricing/positioning) rather than inspectable source.

> Sourcing note: this file is compiled from Devin's knowledge plus each
> vendor's public positioning, not from inspectable source code. Star-count
> style verification is impossible here; treat specific product claims as
> vendor claims and verify independently before relying on them.

## 1. Graph databases (closed or closed-core)

| Product | Vendor | What it is | Why it matters to this study |
| --- | --- | --- | --- |
| Neo4j Enterprise + AuraDB/AuraDS | Neo4j | The enterprise tier (clustering, security) and managed cloud of the system this repo targets | Defines which behaviors are Community (rewritable, observable) vs Enterprise-only (not in the parity target) |
| Amazon Neptune (+ Neptune Analytics) | AWS | Managed graph DB, openCypher + Gremlin + SPARQL; descends from Blazegraph (whose archived source *is* in our corpus) | Proof that a cloud vendor treated openCypher-the-surface as the product — the "selected behavioral surface" strategy at hyperscale |
| TigerGraph | TigerGraph | Distributed native parallel graph, GSQL language | The main "analytics inside the graph DB" competitor; GSQL shows the cost of a non-standard query language |
| Azure Cosmos DB (Gremlin API) | Microsoft | Multi-model cloud DB exposing a Gremlin graph surface | Another wire-protocol-compatibility play: the engine is nothing like JanusGraph, only the surface matches |
| Aerospike Graph | Aerospike | Gremlin layer (TinkerPop) over Aerospike KV | Graph-as-a-layer over a proprietary KV — same layering our corpus sees in JanusGraph/RocksDB |
| Ultipa, Memgraph Enterprise, Dgraph closed builds | various | Closed tiers of otherwise-open engines | Track which features vendors keep closed: HA, security, some algorithms |
| PuppyGraph | PuppyGraph | Zero-ETL graph query layer over lakehouse/SQL data | The "graph as a view, not a store" thesis — adjacent to this repo's projection/read-shape architecture |

## 2. Vector search (closed)

| Product | Vendor | What it is | Why it matters |
| --- | --- | --- | --- |
| Pinecone | Pinecone | The category-defining managed vector DB; serverless separation of storage/compute | Its serverless architecture papers describe disk-resident ANN with aggressive caching — the low-RAM thesis commercially validated |
| Turbopuffer | turbopuffer | Object-storage-first vector+FTS engine (write to S3, cache hot data on NVMe/RAM) | The most direct commercial embodiment of "cold data cheap, working set small" — architecturally the closest cousin to GRAIN-style mmap snapshots |
| Google Vertex AI Vector Search (ScaNN) | Google | Managed ANN built on ScaNN quantization research | ScaNN's anisotropic quantization is published research with a closed managed service around it |
| Azure AI Search | Microsoft | Combined FTS+vector+semantic ranking service | Shows the FTS/vector convergence endpoint |
| AWS OpenSearch Serverless / S3 Vectors | AWS | Managed OpenSearch; S3-native vector storage tier | Vector search priced at object-storage economics |
| MongoDB Atlas Vector Search | MongoDB | Lucene-based vector+FTS bolted onto Atlas | Validates "search index as sidecar to the operational store" |
| Databricks Mosaic AI Vector Search | Databricks | Lakehouse-native vector index | Vector index as a *derived* artifact of governed tables — analogous to this repo's projection-build-store idea |

## 3. Search / analytics (closed or license-restricted)

| Product | Vendor | Notes |
| --- | --- | --- |
| Elasticsearch (Elastic License tiers) + Elastic Cloud | Elastic | Core moved off Apache-2.0 in 2021 (SSPL/Elastic License; AGPL option added 2024) — the fork event that created OpenSearch. License archaeology matters when citing "open" ES code |
| Algolia | Algolia | Closed SaaS search; the UX benchmark (instant search, typo tolerance) that Meilisearch/Typesense openly clone — a known-endpoint convergence story in FTS |
| Splunk | Cisco | Closed log search; its index structure (time-series buckets + bloom filters) is documented enough to study as an endpoint |
| Rockset (acquired by OpenAI, 2024) | — | Converged indexing (row+column+inverted per document) on RocksDB — the "index everything" extreme; now closed off entirely |
| Exasol, Kinetica, etc. | various | GPU/columnar analytics with graph functions — peripheral |

## 4. Graph analytics / intelligence platforms (closed)

| Product | Vendor | Notes |
| --- | --- | --- |
| Neo4j GDS Enterprise | Neo4j | The licensed tier of the GDS library our corpus holds as OpenGDS: concurrency > 4, some algorithms, model catalog. The open/closed boundary here **is** the parity-scope question for this repo |
| Linkurious, Kineviz, Hume (GraphAware) | various | Visualization/investigation layers on top of Neo4j — downstream consumers of the surface we'd need to keep compatible |
| Palantir Foundry (ontology/graph layer) | Palantir | Closed graph-of-everything; relevant only as evidence of what enterprises pay for on top of graph primitives |
| Senzing | Senzing | Closed entity-resolution engine — the killer app adjacent to WCC/similarity algorithms |

## 5. Embedded / infrastructure (closed-adjacent)

| Product | Vendor | Notes |
| --- | --- | --- |
| FoundationDB Record Layer usage inside Apple/Snowflake | — | FDB itself is open (in corpus); the interesting deployments are closed |
| Snowflake / BigQuery internal search+vector features | — | Vector functions inside closed cloud warehouses — endpoint-only study |
| Oracle Property Graph, SAP HANA Graph | Oracle/SAP | Graph engines embedded in closed RDBMSes; PGQL (Oracle) fed into the SQL/PGQ and GQL standards our corpus tracks via DuckPGQ |

## How closed systems plug into the research method

1. **Endpoint, not source**: for open repos we read code; for closed tools we
   read *behavior* — docs, wire protocols, published benchmarks, and papers
   (Pinecone serverless, ScaNN, Neptune openCypher conformance).
2. **They mark the money**: which features vendors keep closed (HA,
   concurrency limits in GDS, enterprise security) is the clearest market
   signal for what parity work is commercially meaningful.
3. **Convergence case studies**: Algolia→Meilisearch/Typesense and
   Neo4j→Neptune-openCypher are both "known endpoint, independent
   reimplementation" stories — external evidence for the PRD06 thesis.
4. **Pattern docs may cite them descriptively** (e.g. turbopuffer's
   S3-first layout alongside Quickwit's) but never as file-path evidence;
   the ≥2-repo file-cited rule applies only to open corpus members.
