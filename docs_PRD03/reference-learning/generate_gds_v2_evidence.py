#!/usr/bin/env python3
"""Generate the GDS PRD L1 evidence dossier v2 and companion TSVs."""

from __future__ import annotations

import csv
import re
from collections import Counter
from datetime import datetime
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "docs_PRD03" / "reference-learning"


def resolve_source_root() -> Path:
    candidates = [
        REPO / "gitrefrepo" / "Neo4j family" / "neo4j-gds-src",
        REPO / "gitrefrepo" / "neo4j-gds-src",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find neo4j-gds-src under gitrefrepo")


SOURCE_ROOT = resolve_source_root()


PROCEDURE_COLUMNS = [
    "procedure_name",
    "procedure_mode",
    "annotation_source",
    "facade",
    "config_type",
    "result_type",
    "algorithm_family",
    "estimate_method",
    "memory_estimation_status",
    "output_artifact",
    "support_tier",
    "unsupported_reason",
    "confidence",
    "falsifier",
]

PROJECTION_COLUMNS = [
    "projection_path",
    "entrypoint",
    "dense_id_behavior",
    "label_handling",
    "relationship_type_handling",
    "orientation_handling",
    "aggregation_handling",
    "inverse_index_handling",
    "property_mapping",
    "estimate_path",
    "catalog_effect",
    "PRD_impact",
    "confidence",
    "falsifier",
]

MEMORY_COLUMNS = [
    "procedure_or_component",
    "graph_load_terms",
    "algorithm_terms",
    "result_terms",
    "model_or_artifact_terms",
    "write_back_terms",
    "build_scratch_terms",
    "high_water_risk",
    "reject_condition",
    "missing_estimator_path",
    "measurement_method",
    "confidence",
    "falsifier",
]

BEHAVIOR_COLUMNS = [
    "procedure_or_family",
    "mode",
    "side_effect",
    "target_plane",
    "input_shape",
    "output_shape",
    "transaction_or_catalog_behavior",
    "compatibility_risk",
    "confidence",
    "falsifier",
]

LIFECYCLE_COLUMNS = [
    "artifact_type",
    "identity_keys",
    "create",
    "list_or_get",
    "use",
    "mutate_or_write",
    "drop_or_expire",
    "generation_or_watermark_reference",
    "PRD_impact",
    "confidence",
    "falsifier",
]

ORACLE_COLUMNS = [
    "source_test_or_doc",
    "input_graph_shape",
    "procedure_or_config",
    "expected_output",
    "failure_behavior",
    "fixture_value",
    "PRD_acceptance_area",
    "confidence",
    "falsifier",
]

PATCH_COLUMNS = [
    "PRD_area",
    "action",
    "current_wording",
    "proposed_wording",
    "evidence_pointer",
    "decision_reason",
    "falsifier",
    "confidence",
]

COVERAGE_COLUMNS = [
    "folder",
    "file_count",
    "evidence_status",
    "primary_source_pointer",
    "documented_in",
    "PRD_decision_value",
    "remaining_gap",
    "confidence",
]


MANDATORY_FOLDERS = [
    "procedure-collector",
    "native-projection",
    "legacy-cypher-projection",
    "collections-memory-estimation",
    "applications/services",
    "open-write-services",
    "applications/operations",
    "applications/graph-store-catalog-results",
    "neo4j-api",
    "neo4j-adapter",
    "neo4j-values",
    "gds-values",
    "io",
    "proc/machine-learning",
    "proc/pipeline-catalog",
]


PRIMARY_POINTERS = {
    "procedure-collector": "procedure-collector/processor/src/main/java/org/neo4j/gds/procedure/ProcedureCollector.java:42",
    "native-projection": "native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java:40",
    "legacy-cypher-projection": "legacy-cypher-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromCypherConfig.java:35",
    "collections-memory-estimation": "collections-memory-estimation/src/main/java/org/neo4j/gds/mem/estimation/HugeSparseCollections.java:30",
    "applications/services": "applications/services/src/main/java/org/neo4j/gds/applications/services/GraphDimensionFactory.java:40",
    "open-write-services": "open-write-services/src/main/java/org/neo4j/gds/core/write/OpenGdsExportBuildersExtension.java:20",
    "applications/operations": "applications/operations/src/main/java/org/neo4j/gds/applications/operations/OperationsApplications.java:35",
    "applications/graph-store-catalog-results": "applications/graph-store-catalog-results/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsage.java:40",
    "neo4j-api": "neo4j-api/src/main/java/org/neo4j/gds/compat/ProcedureReturnColumns.java:20",
    "neo4j-adapter": "neo4j-adapter/src/main/java/org/neo4j/gds/compat/neo4j/InternalReadOps.java:25",
    "neo4j-values": "neo4j-values/src/main/java/org/neo4j/gds/values/Neo4jNodePropertyValuesUtil.java:20",
    "gds-values": "gds-values/src/main/java/org/neo4j/gds/values/PrimitiveValues.java:20",
    "io": "io/core/src/main/java/org/neo4j/gds/core/io/GraphStoreExporter.java:20",
    "proc/machine-learning": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteProc.java:39",
    "proc/pipeline-catalog": "proc/pipeline-catalog/src/main/java/org/neo4j/gds/pipeline/catalog/PipelineDropProc.java:40",
}


def clean(value: object) -> str:
    text = "" if value is None else str(value)
    return re.sub(r"\s+", " ", text).replace("\t", " ").strip()


def repo_rel(path: Path) -> str:
    return str(path.relative_to(SOURCE_ROOT))


def write_tsv(path: Path, columns: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: clean(row.get(column, "")) for column in columns})


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def find_annotation_end(text: str, start: int) -> int:
    paren = text.find("(", start)
    if paren < 0:
        return start
    depth = 0
    in_string = False
    escaped = False
    for index in range(paren, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    return paren


def extract_quoted(annotation: str, key: str) -> str:
    match = re.search(rf"{re.escape(key)}\s*=\s*\"([^\"]+)\"", annotation)
    if match:
        return match.group(1)
    if key == "name":
        first = re.search(r"^\s*\"([^\"]+)\"", annotation)
        if first:
            return first.group(1)
    return ""


def extract_mode(annotation: str, annotation_type: str) -> str:
    if annotation_type == "GdsCallable":
        match = re.search(r"executionMode\s*=\s*([A-Za-z0-9_.]+)", annotation)
        return match.group(1).split(".")[-1] if match else "Callable"
    match = re.search(r"mode\s*=\s*([A-Za-z0-9_.]+)", annotation)
    return match.group(1).split(".")[-1] if match else "READ_DEFAULT"


def infer_family(name: str, rel_path: str) -> str:
    lowered = (name + " " + rel_path).lower()
    families = [
        ("pipeline", "machine_learning_pipeline"),
        ("kge", "machine_learning_kge"),
        ("splitrelationships", "machine_learning_split_relationships"),
        ("nodeclassification", "machine_learning_node_classification"),
        ("noderegression", "machine_learning_node_regression"),
        ("linkprediction", "machine_learning_link_prediction"),
        ("graph.", "graph_catalog"),
        ("model.", "model_catalog"),
        ("pageRank".lower(), "centrality"),
        ("degree", "centrality"),
        ("louvain", "community"),
        ("leiden", "community"),
        ("wcc", "community"),
        ("dijkstra", "path_finding"),
        ("shortestpath", "path_finding"),
        ("knn", "similarity"),
        ("nodesimilarity", "similarity"),
        ("fastrp", "node_embedding"),
        ("graphsage", "node_embedding"),
        ("operations", "operations"),
    ]
    for needle, family in families:
        if needle in lowered:
            return family
    if "proc/machine-learning" in rel_path:
        return "machine_learning"
    if "pipeline-catalog" in rel_path:
        return "pipeline_catalog"
    return "gds_public_surface"


def infer_output(name: str, mode: str) -> str:
    lowered = name.lower()
    if lowered.endswith(".estimate") or "estimate" in lowered:
        return "MemoryEstimateResult stream"
    if ".stream" in lowered or mode == "STREAM":
        return "Row stream only"
    if ".stats" in lowered or mode == "STATS":
        return "Stats result stream"
    if ".mutate" in lowered or "MUTATE" in mode:
        return "In-memory catalog graph mutation"
    if ".write" in lowered or mode == "WRITE" or "WRITE" in mode:
        return "Neo4j write-back or export side effect"
    if ".drop" in lowered:
        return "Catalog artifact removal"
    if ".list" in lowered or ".exists" in lowered:
        return "Catalog metadata result"
    if ".train" in lowered:
        return "Model artifact plus training metrics"
    return "Procedure result stream"


def infer_estimate_status(name: str, mode: str, annotation_type: str) -> tuple[str, str]:
    lowered = name.lower()
    if lowered.endswith(".estimate"):
        return ("Estimate procedure", "Explicit public estimate endpoint")
    if annotation_type == "GdsCallable":
        return ("Executor estimator required", "AlgorithmFactory.memoryEstimation or MemoryEstimateDefinition")
    if ".write" in lowered or ".mutate" in lowered or ".train" in lowered:
        return ("Requires paired estimate or reject path", "Pair with estimate endpoint or unsupported status")
    return ("No estimator implied by annotation", "Not applicable unless procedure allocates bounded work")


def extract_result_type(segment: str, annotation_type: str) -> str:
    if annotation_type == "Procedure":
        stream = re.search(r"public\s+Stream<\s*([^>]+?)\s*>\s+\w+\s*\(", segment, re.S)
        if stream:
            return "Stream<" + clean(stream.group(1)) + ">"
        method = re.search(r"public\s+([A-Za-z0-9_<>, ?]+)\s+\w+\s*\(", segment, re.S)
        if method:
            return clean(method.group(1))
        return "Procedure method result unknown"
    class_match = re.search(r"public\s+class\s+([A-Za-z0-9_]+)", segment)
    if class_match:
        return "AlgorithmSpec class " + class_match.group(1)
    return "AlgorithmSpec"


def extract_config_type(segment: str, annotation_type: str) -> str:
    if annotation_type == "Procedure":
        if "Map<String, Object> configuration" in segment:
            return "Map<String,Object> configuration"
        if "graphNameOrConfiguration" in segment:
            return "graphNameOrConfiguration plus algoConfiguration"
        names = re.findall(r"@Name\(value\s*=\s*\"([^\"]+)\"", segment[:1200])
        if names:
            return ", ".join(names[:8])
        return "Procedure parameters"
    config = re.search(r"AlgorithmSpec<[^;{]+?,\s*([^,\n]+Config)\s*,", segment, re.S)
    if config:
        return clean(config.group(1))
    return "newConfigFunction supplied config"


def extract_facade(segment: str, annotation_type: str) -> str:
    if annotation_type != "Procedure":
        return "AlgorithmSpec loaded by procedure-collector"
    chain = re.search(r"return\s+facade\.([A-Za-z0-9_().]+)", segment)
    if chain:
        return "facade." + chain.group(1).split("(")[0]
    if "facade." in segment:
        chain = re.search(r"facade\.([A-Za-z0-9_().]+)", segment)
        if chain:
            return "facade." + chain.group(1).split("(")[0]
    return "Neo4j procedure facade or local method"


def parse_procedure_surface() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in sorted(SOURCE_ROOT.rglob("*.java")):
        rel_path = repo_rel(path)
        if "/src/main/java/" not in rel_path:
            continue
        if "src/main/java" not in rel_path:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"@(Procedure|GdsCallable)\s*\(", text):
            annotation_type = match.group(1)
            end = find_annotation_end(text, match.start())
            annotation = text[text.find("(", match.start()) + 1 : end - 1]
            name = extract_quoted(annotation, "name")
            if not name:
                continue
            mode = extract_mode(annotation, annotation_type)
            source_line = line_number(text, match.start())
            segment = text[end : min(len(text), end + 2600)]
            previous = text[max(0, match.start() - 260) : match.start()]
            deprecated = extract_quoted(annotation, "deprecatedBy")
            support_tier = "Direct public surface"
            if "@Internal" in previous or "@Internal" in segment[:240]:
                support_tier = "Internal surface"
            if deprecated:
                support_tier = "Deprecated alias"
            status, estimate_method = infer_estimate_status(name, mode, annotation_type)
            unsupported_reason = ""
            if status == "Requires paired estimate or reject path":
                unsupported_reason = "Support requires paired estimator or deterministic unsupported response before claiming compatibility"
            rows.append(
                {
                    "procedure_name": name,
                    "procedure_mode": mode,
                    "annotation_source": f"{rel_path}:{source_line}",
                    "facade": extract_facade(segment, annotation_type),
                    "config_type": extract_config_type(segment, annotation_type),
                    "result_type": extract_result_type(segment, annotation_type),
                    "algorithm_family": infer_family(name, rel_path),
                    "estimate_method": estimate_method,
                    "memory_estimation_status": status,
                    "output_artifact": infer_output(name, mode),
                    "support_tier": support_tier,
                    "unsupported_reason": unsupported_reason,
                    "confidence": "DirectSource",
                    "falsifier": "Re-run rg -n '@Procedure|@GdsCallable' against src/main/java and compare names, modes, and source lines.",
                }
            )
    rows.sort(key=lambda row: (str(row["procedure_name"]), str(row["annotation_source"])))
    return rows


def file_count(folder: str) -> int:
    base = SOURCE_ROOT / folder
    return sum(1 for path in base.rglob("*") if path.is_file()) if base.exists() else 0


def projection_rows() -> list[dict[str, object]]:
    return [
        {
            "projection_path": "native-projection",
            "entrypoint": "GraphProjectFromStoreConfig.of/fromProcedureConfig",
            "dense_id_behavior": "Builds graph dimensions from transaction counts and id generators; dense id map is downstream of graph loading.",
            "label_handling": "NodeProjections#fromObject parses nodeProjection and GraphDimensionsValidation validates labels.",
            "relationship_type_handling": "RelationshipProjections#fromObject parses relationshipProjection and validation rejects absent types.",
            "orientation_handling": "Relationship projection carries orientation through normalized relationship projections.",
            "aggregation_handling": "validateNoAggregationOnMultiPropertyRelationships rejects unsupported implicit multi-property aggregation.",
            "inverse_index_handling": "Not created by config; inverse relationship indexes are a separate catalog/mutate concern.",
            "property_mapping": "withNormalizedPropertyMappings pushes top-level node/relationship properties into projections and clears top-level keys.",
            "estimate_path": "GraphDimensionsReader reads counts/tokens, then GraphStoreCreator estimates during/after loading.",
            "catalog_effect": "Produces named graph through graph-store-catalog load path.",
            "PRD_impact": "Projection Build Store must preserve GDS projection grammar, property normalization, and reject semantics before advertising native projection parity.",
            "confidence": "DirectSource",
            "falsifier": "native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java:40",
        },
        {
            "projection_path": "native-projection",
            "entrypoint": "GraphDimensionsReader.call",
            "dense_id_behavior": "Reads highestPossibleNodeCount and highestRelationshipId through Neo4j token/id APIs.",
            "label_handling": "Loads LabelInformation and maps label tokens to labels.",
            "relationship_type_handling": "Reads relationship type tokens and per-type relationship counts.",
            "orientation_handling": "Counts are gathered before orientation-specific topology build.",
            "aggregation_handling": "No aggregation decision here; this is dimensions and validation input.",
            "inverse_index_handling": "No inverse index creation.",
            "property_mapping": "Reads node and relationship property tokens used by downstream mapping.",
            "estimate_path": "GraphDimensions is a core estimator input.",
            "catalog_effect": "No catalog mutation by itself.",
            "PRD_impact": "RAM estimates need Neo4j id high-water and token cardinalities, not only final node/edge counts.",
            "confidence": "DirectSource",
            "falsifier": "native-projection/src/main/java/org/neo4j/gds/projection/GraphDimensionsReader.java:62",
        },
        {
            "projection_path": "legacy-cypher-projection",
            "entrypoint": "GraphProjectFromCypherConfig.fromProcedureConfig",
            "dense_id_behavior": "Cypher projection provides query rows; dense id mapping is built from loaded records.",
            "label_handling": "Label projection keys are forbidden; nodeQuery supplies projected columns.",
            "relationship_type_handling": "Relationship projection keys are forbidden; relationshipQuery supplies projected columns.",
            "orientation_handling": "Orientation is query/output driven, not native relationship projection grammar.",
            "aggregation_handling": "Aggregation is not accepted through native projection keys.",
            "inverse_index_handling": "No inverse index creation at config layer.",
            "property_mapping": "Forbids nodeProjection, relationshipProjection, nodeProperties, and relationshipProperties keys.",
            "estimate_path": "CypherQueryEstimator runs EXPLAIN and reads EstimatedRows plus property columns.",
            "catalog_effect": "Can create named graph through cypher projection load path.",
            "PRD_impact": "v003 must either implement Cypher projection with read-only query validation or return deterministic unsupported.",
            "confidence": "DirectSource",
            "falsifier": "legacy-cypher-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromCypherConfig.java:35",
        },
        {
            "projection_path": "legacy-cypher-projection",
            "entrypoint": "CypherRecordLoader.load",
            "dense_id_behavior": "Loads node/relationship records in batches and updates record counts.",
            "label_handling": "Mandatory columns are validated and missing columns produce AS hints.",
            "relationship_type_handling": "Relationship loader validates required relationship columns.",
            "orientation_handling": "Determined by emitted relationship rows.",
            "aggregation_handling": "No projection-level aggregation surface.",
            "inverse_index_handling": "No inverse index creation.",
            "property_mapping": "Row columns beyond reserved columns become properties after validation.",
            "estimate_path": "CypherQueryEstimator explains the same query shape.",
            "catalog_effect": "Catalog graph is loaded only after query execution passes read-only and column checks.",
            "PRD_impact": "Projection rewrite needs read-only guard, column contracts, and explain-based estimate or explicit unsupported status.",
            "confidence": "DirectSource",
            "falsifier": "legacy-cypher-projection/src/main/java/org/neo4j/gds/projection/CypherRecordLoader.java:60",
        },
        {
            "projection_path": "io/csv",
            "entrypoint": "CsvToGraphStoreImporter",
            "dense_id_behavior": "File input is converted into a GraphStore with local write mode and schema builder.",
            "label_handling": "Node visitors and schema builder carry labels from file input.",
            "relationship_type_handling": "Relationship visitors carry relationship types from file input.",
            "orientation_handling": "Depends on file input schema and importer topology build.",
            "aggregation_handling": "No native aggregation policy at importer entrypoint.",
            "inverse_index_handling": "No inverse index creation at importer entrypoint.",
            "property_mapping": "Node, relationship, and graph property visitors populate schema and values.",
            "estimate_path": "CsvExportEstimation estimates export rows/properties; import estimate must be separately proven.",
            "catalog_effect": "Returns UserGraphStore and can feed catalog load/import.",
            "PRD_impact": "File import/export paths are compatibility surface separate from OLTP snapshot projection.",
            "confidence": "DirectSource",
            "falsifier": "io/file/src/main/java/org/neo4j/gds/core/io/file/FileToGraphStoreImporter.java:35",
        },
    ]


def memory_rows() -> list[dict[str, object]]:
    return [
        {
            "procedure_or_component": "GraphStoreCreator native projection load",
            "graph_load_terms": "estimateMemoryUsageDuringLoading and estimateMemoryUsageAfterLoading split load scratch from resident graph.",
            "algorithm_terms": "None at graph creation.",
            "result_terms": "Catalog entry and graph metadata only.",
            "model_or_artifact_terms": "Named graph resident GraphStore.",
            "write_back_terms": "None.",
            "build_scratch_terms": "Importer, id mapping, topology build, property builders.",
            "high_water_risk": "During-loading high water can exceed final graph store size.",
            "reject_condition": "Reject when during-loading or after-loading estimate exceeds budget before catalog insertion.",
            "missing_estimator_path": "None for this component in sampled source.",
            "measurement_method": "GraphStoreCreator estimator plus GraphMemoryUsage resident inspection after load.",
            "confidence": "GraphToolAssisted",
            "falsifier": "applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphStoreCreator.java and codebase-memory search_memory_explicit.json",
        },
        {
            "procedure_or_component": "CSRGraphStore and adjacency lists",
            "graph_load_terms": "Node mapping, relationship topology, degrees, offsets, target ids, properties.",
            "algorithm_terms": "Algorithm graph views are separate from resident CSR storage.",
            "result_terms": "No stream result term.",
            "model_or_artifact_terms": "Named graph resident topology and sidecars.",
            "write_back_terms": "None.",
            "build_scratch_terms": "CSRGraphStoreFactory build stages and import buffers.",
            "high_water_risk": "Duplicate or transient topology during publish can exceed resident graph size.",
            "reject_condition": "Reject when resident plus build scratch cannot fit under configured graph budget.",
            "missing_estimator_path": "None identified for base CSR storage.",
            "measurement_method": "GraphMemoryUsage walks GraphStore and AdjacencyList.memoryInfo.",
            "confidence": "DirectSource",
            "falsifier": "applications/graph-store-catalog-results/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsage.java:40",
        },
        {
            "procedure_or_component": "HugeSparseCollections",
            "graph_load_terms": "Sparse huge arrays back node/relationship property sidecars.",
            "algorithm_terms": "Algorithms using sparse state inherit page best/worst range.",
            "result_terms": "None unless collection backs result artifact.",
            "model_or_artifact_terms": "Potential sidecar resident memory.",
            "write_back_terms": "None.",
            "build_scratch_terms": "Page object arrays and primitive/array page values.",
            "high_water_risk": "Worst-page estimate can be far above best-page estimate for sparse high-id inputs.",
            "reject_condition": "Reject on max-index/expected-values mismatch or worst-case page range crossing budget.",
            "missing_estimator_path": "None.",
            "measurement_method": "MemoryRange min/max returned by HugeSparseCollections estimate methods.",
            "confidence": "DirectSource",
            "falsifier": "collections-memory-estimation/src/main/java/org/neo4j/gds/mem/estimation/HugeSparseCollections.java:30",
        },
        {
            "procedure_or_component": "AlgorithmEstimationTemplate",
            "graph_load_terms": "Uses GraphDimensionFactory from existing catalog graph.",
            "algorithm_terms": "AlgorithmFactory.memoryEstimation(config).estimate(dimensions, concurrency).",
            "result_terms": "MemoryEstimateResult only.",
            "model_or_artifact_terms": "Depends on algorithm factory.",
            "write_back_terms": "Not included unless factory adds write terms.",
            "build_scratch_terms": "Algorithm-specific estimator terms.",
            "high_water_risk": "Default AlgorithmFactory throws if algorithm did not implement estimator.",
            "reject_condition": "Reject or mark unsupported when MemoryEstimationNotImplementedException is raised.",
            "missing_estimator_path": "algo-common/src/main/java/org/neo4j/gds/AlgorithmFactory.java:127",
            "measurement_method": "Estimate endpoint plus DefaultMemoryGuard for execution guard.",
            "confidence": "DirectSource",
            "falsifier": "applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/AlgorithmEstimationTemplate.java:72",
        },
        {
            "procedure_or_component": "DefaultMemoryGuard",
            "graph_load_terms": "Not a graph loader.",
            "algorithm_terms": "Checks algorithm estimate against available memory.",
            "result_terms": "None.",
            "model_or_artifact_terms": "None.",
            "write_back_terms": "Only if algorithm estimator includes it.",
            "build_scratch_terms": "Only if algorithm estimator includes it.",
            "high_water_risk": "Unimplemented estimator path can bypass strict numeric budget unless surfaced as unsupported.",
            "reject_condition": "Throw clear IllegalStateException or deterministic unsupported when estimator is absent or above budget.",
            "missing_estimator_path": "applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/DefaultMemoryGuard.java:90",
            "measurement_method": "Direct execution guard path.",
            "confidence": "DirectSource",
            "falsifier": "applications/algorithms/machinery/src/main/java/org/neo4j/gds/applications/algorithms/machinery/DefaultMemoryGuard.java:90",
        },
        {
            "procedure_or_component": "PipelineApplications predict/write estimates",
            "graph_load_terms": "Uses named graph dimensions through pipeline executor.",
            "algorithm_terms": "Pipeline predict/train estimators return MemoryEstimateResult for several public estimate procedures.",
            "result_terms": "Stream results or write results vary by mode.",
            "model_or_artifact_terms": "Model catalog lookup and trained model memory can matter.",
            "write_back_terms": "Write prediction writes node properties or relationships to Neo4j.",
            "build_scratch_terms": "Feature preparation, candidate pairs, model inference batches.",
            "high_water_risk": "Candidate-pair materialization and model inference can dominate graph resident memory.",
            "reject_condition": "Reject when estimate endpoint is absent or estimator throws for requested pipeline operation.",
            "missing_estimator_path": "procedures/pipelines-facade/src/main/java/org/neo4j/gds/procedures/pipelines/PipelineApplications.java:908",
            "measurement_method": "Estimate procedures in proc/machine-learning plus executor-specific estimate methods.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/pipeline/node/classification/predict/NodeClassificationPipelineWriteProc.java:50",
        },
        {
            "procedure_or_component": "KGE predict write",
            "graph_load_terms": "Uses existing graph store and KGE model.",
            "algorithm_terms": "TopKMapComputer and TopKGraph are algorithm/result structures.",
            "result_terms": "KGEWriteResult includes relationship count and timings.",
            "model_or_artifact_terms": "KGE model lookup and embedding state.",
            "write_back_terms": "RelationshipExporter writes predicted relationships and properties.",
            "build_scratch_terms": "TopK map and TopKGraph can be large.",
            "high_water_risk": "TopK candidates can exceed graph topology size if unbounded.",
            "reject_condition": "Require estimator covering topK plus exporter buffers or deterministic unsupported.",
            "missing_estimator_path": "MachineLearningAlgorithmsEstimationModeBusinessFacade.kge throws not implemented in sampled source.",
            "measurement_method": "KGEPredictWriteSpec write path plus estimation facade audit.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteSpec.java:41",
        },
        {
            "procedure_or_component": "CSV export estimation",
            "graph_load_terms": "Uses graph node/relationship counts and sampled properties.",
            "algorithm_terms": "No graph algorithm.",
            "result_terms": "File output buffers.",
            "model_or_artifact_terms": "Export file artifact.",
            "write_back_terms": "Filesystem write, not Neo4j property write.",
            "build_scratch_terms": "Node data, relationship data, id string estimates, sampled property values.",
            "high_water_risk": "Sampling can understate pathological property sizes.",
            "reject_condition": "Reject when sampled estimate plus write buffers exceeds export budget.",
            "missing_estimator_path": "None for CSV export path sampled.",
            "measurement_method": "CsvExportEstimation fixed node/relationship terms and samplers.",
            "confidence": "DirectSource",
            "falsifier": "io/csv/src/main/java/org/neo4j/gds/core/io/file/csv/estimation/CsvExportEstimation.java:25",
        },
    ]


def behavior_rows() -> list[dict[str, object]]:
    return [
        {
            "procedure_or_family": "gds.*.stream",
            "mode": "READ or STREAM executionMode",
            "side_effect": "No graph catalog or Neo4j write side effect expected.",
            "target_plane": "OLAP read snapshot and result stream.",
            "input_shape": "graphName plus configuration.",
            "output_shape": "Stream rows with original Neo4j ids where needed.",
            "transaction_or_catalog_behavior": "Uses catalog graph and returns rows; transaction is read path.",
            "compatibility_risk": "Streaming can still materialize large intermediate algorithm state.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictStreamSpec.java:37",
        },
        {
            "procedure_or_family": "gds.*.mutate",
            "mode": "Neo4j READ procedure with MUTATE executionMode",
            "side_effect": "Adds or mutates in-memory graph store artifacts.",
            "target_plane": "GDS catalog graph, not Neo4j OLTP store.",
            "input_shape": "graphName plus configuration.",
            "output_shape": "Mutate result rows with counts and timings.",
            "transaction_or_catalog_behavior": "Procedure annotation can be READ while AlgorithmSpec executionMode is MUTATE_RELATIONSHIP.",
            "compatibility_risk": "PRD must distinguish Neo4j write mode from GDS catalog mutation.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/splitting/SplitRelationshipsMutateSpec.java:36",
        },
        {
            "procedure_or_family": "gds.*.write",
            "mode": "WRITE or WRITE_RELATIONSHIP executionMode",
            "side_effect": "Writes properties/relationships back to Neo4j or export target.",
            "target_plane": "OLTP write-back plane.",
            "input_shape": "graphName plus configuration.",
            "output_shape": "Write result rows with counts, configuration, timings.",
            "transaction_or_catalog_behavior": "Uses exporters and write builders; must honor transaction semantics.",
            "compatibility_risk": "Largest compatibility risk because v003 OLTP store must expose Neo4j-compatible write behavior.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteSpec.java:41",
        },
        {
            "procedure_or_family": "gds.*.estimate",
            "mode": "READ",
            "side_effect": "No mutation; returns memory estimate.",
            "target_plane": "Control plane and memory guard.",
            "input_shape": "graphNameOrConfiguration and algorithm configuration.",
            "output_shape": "MemoryEstimateResult stream.",
            "transaction_or_catalog_behavior": "Reads graph dimensions and config; should not run algorithm.",
            "compatibility_risk": "Strict RAM promise fails if estimate omits result/model/write/build scratch terms.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/linkmodels/pipeline/predict/LinkPredictionPipelineStreamProc.java:50",
        },
        {
            "procedure_or_family": "gds.pipeline.* catalog",
            "mode": "READ",
            "side_effect": "Create/configure/drop changes in-memory pipeline catalog despite READ annotation.",
            "target_plane": "Pipeline catalog control plane.",
            "input_shape": "pipelineName and operation-specific configuration.",
            "output_shape": "PipelineCatalogResult or PipelineExistsResult.",
            "transaction_or_catalog_behavior": "Static PipelineCatalog is keyed by user and name.",
            "compatibility_risk": "READ annotation does not mean no side effect; PRD needs catalog-control-plane exception.",
            "confidence": "DirectSource",
            "falsifier": "proc/pipeline-catalog/src/main/java/org/neo4j/gds/pipeline/catalog/PipelineDropProc.java:40",
        },
        {
            "procedure_or_family": "gds.operations.*",
            "mode": "READ surface through facade",
            "side_effect": "Feature toggles and progress/log listing.",
            "target_plane": "Operations/control plane.",
            "input_shape": "booleans, job ids, usernames, optional filters.",
            "output_shape": "FeatureState, ProgressResult, or log rows.",
            "transaction_or_catalog_behavior": "Admin/user filtering and feature-toggle repository writes.",
            "compatibility_risk": "Operational toggles influence memory/compression behavior and need deterministic support or explicit unsupported.",
            "confidence": "DirectSource",
            "falsifier": "applications/operations/src/main/java/org/neo4j/gds/applications/operations/OperationsApplications.java:35",
        },
    ]


def lifecycle_rows() -> list[dict[str, object]]:
    return [
        {
            "artifact_type": "Named graph",
            "identity_keys": "userName, database, graphName; v003 adds generation/watermark.",
            "create": "Graph project native/cypher/file/import creates GraphStoreCatalogEntry.",
            "list_or_get": "GraphStoreCatalog and GraphStoreCatalogService list/get/drop entries.",
            "use": "Algorithm facades resolve graphName to GraphStore and GraphDimensions.",
            "mutate_or_write": "Mutate procedures add properties or relationship types to graph store.",
            "drop_or_expire": "Catalog drop frees resident graph memory.",
            "generation_or_watermark_reference": "Not native to sampled GDS; required v003 addition.",
            "PRD_impact": "Do not treat graphName alone as identity in the rewrite; add immutable generation key while preserving procedure names.",
            "confidence": "GraphToolAssisted",
            "falsifier": "applications/graph-store-catalog/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphStoreCatalogService.java",
        },
        {
            "artifact_type": "Pipeline",
            "identity_keys": "user, pipelineName.",
            "create": "Pipeline create/configure procedures call PipelineCatalog.set.",
            "list_or_get": "PipelineCatalog.exists/get/getAllPipelines.",
            "use": "Train and predict procedures retrieve typed pipelines.",
            "mutate_or_write": "Add step/trainer/configure procedures mutate pipeline object in catalog.",
            "drop_or_expire": "PipelineCatalog.drop removes by user/name.",
            "generation_or_watermark_reference": "No generation in sampled source.",
            "PRD_impact": "v003 needs user-scoped ephemeral pipeline catalog or deterministic unsupported for pipeline family.",
            "confidence": "DirectSource",
            "falsifier": "pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java:35",
        },
        {
            "artifact_type": "Model",
            "identity_keys": "creator/username and modelName; model type enforced by requested classes.",
            "create": "OpenModelCatalog.set inserts model and notifies listeners.",
            "list_or_get": "get, getUntyped, getAllModels, list, exists.",
            "use": "ML predict procedures load model from catalog.",
            "mutate_or_write": "publish/store unavailable in openGDS; loaded models can be removed.",
            "drop_or_expire": "drop/dropOrThrow and removeAllLoadedModels.",
            "generation_or_watermark_reference": "No generation in sampled source.",
            "PRD_impact": "Model catalog is part of GDS compatibility; openGDS rejects publish/store with explicit messages.",
            "confidence": "DirectSource",
            "falsifier": "open-model-catalog/src/main/java/org/neo4j/gds/core/model/OpenModelCatalog.java:40",
        },
        {
            "artifact_type": "Result store",
            "identity_keys": "jobId/resultStore handles plus graph artifact.",
            "create": "Write/mutate specs may attach result store through config.",
            "list_or_get": "Depends on result store facade and exporters.",
            "use": "RelationshipExporterBuilder and node-property writers can route through result store.",
            "mutate_or_write": "Write specs use exporters with result store.",
            "drop_or_expire": "Not fully traced in this pass.",
            "generation_or_watermark_reference": "Job id is present; durable generation not identified.",
            "PRD_impact": "Result artifacts need lifecycle and RAM accounting; no unbounded materialization.",
            "confidence": "DirectSource",
            "falsifier": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteSpec.java:90",
        },
        {
            "artifact_type": "Graph memory report",
            "identity_keys": "graphName/catalog entry.",
            "create": "GraphMemoryUsage.of computes memory view.",
            "list_or_get": "Catalog procedures can expose graph memory usage.",
            "use": "Operational diagnostics and PRD acceptance evidence.",
            "mutate_or_write": "None.",
            "drop_or_expire": "Report disappears with graph entry.",
            "generation_or_watermark_reference": "Should include v003 generation when added.",
            "PRD_impact": "Use as resident memory oracle, but not as full high-water estimator.",
            "confidence": "DirectSource",
            "falsifier": "applications/graph-store-catalog-results/src/main/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsage.java:40",
        },
        {
            "artifact_type": "File export/import artifact",
            "identity_keys": "export location, graphName, file format.",
            "create": "GraphStoreExporter and FileToGraphStoreImporter build filesystem artifacts or graph stores.",
            "list_or_get": "Filesystem or import root; no catalog listing by itself.",
            "use": "Import/export procedures and offline workflows.",
            "mutate_or_write": "Writes files or imports into local GraphStore.",
            "drop_or_expire": "External cleanup outside sampled code.",
            "generation_or_watermark_reference": "No OLTP watermark in sampled file path.",
            "PRD_impact": "IO support must be feature-gated; RAM estimate must include serialization buffers.",
            "confidence": "DirectSource",
            "falsifier": "io/core/src/main/java/org/neo4j/gds/core/io/GraphStoreExporter.java:20",
        },
    ]


def oracle_rows() -> list[dict[str, object]]:
    return [
        {
            "source_test_or_doc": "proc/machine-learning/src/test/java/org/neo4j/gds/ml/pipeline/node/classification/predict/NodeClassificationPipelineWriteProcTest.java:178",
            "input_graph_shape": "Named graph plus trained node classification pipeline/model fixture.",
            "procedure_or_config": "gds.beta.pipeline.nodeClassification.predict.write.estimate",
            "expected_output": "MemoryEstimateResult range for write prediction.",
            "failure_behavior": "Estimator must fail deterministically if model/pipeline/config missing.",
            "fixture_value": "Query string calls estimate with graphNameOrConfiguration and algoConfiguration.",
            "PRD_acceptance_area": "ML write estimate coverage.",
            "confidence": "DirectSource",
            "falsifier": "Re-run targeted test or inspect updated assertion values.",
        },
        {
            "source_test_or_doc": "proc/machine-learning/src/test/java/org/neo4j/gds/ml/linkmodels/pipeline/predict/LinkPredictionPipelineStreamProcTest.java:138",
            "input_graph_shape": "Link prediction graph with target label/config.",
            "procedure_or_config": "gds.beta.pipeline.linkPrediction.predict.stream.estimate",
            "expected_output": "Expected memory range per fixture.",
            "failure_behavior": "Invalid target labels/config must produce procedure error.",
            "fixture_value": "Parameterized expectedMemoryRange in test.",
            "PRD_acceptance_area": "Candidate pair estimate and stream behavior.",
            "confidence": "DirectSource",
            "falsifier": "Re-run targeted test or compare source assertion changes.",
        },
        {
            "source_test_or_doc": "collections-memory-estimation/src/test/java/org/neo4j/gds/collections/hsa/HugeSparseCollectionsTest.java:121",
            "input_graph_shape": "Sparse page/max-index/value-count fixtures.",
            "procedure_or_config": "HugeSparseCollections estimate methods.",
            "expected_output": "MemoryRange best/worst estimates.",
            "failure_behavior": "Invalid expected values versus max index rejects.",
            "fixture_value": "Primitive and array estimate fixtures.",
            "PRD_acceptance_area": "Sparse sidecar memory formula.",
            "confidence": "DirectSource",
            "falsifier": "Run test or inspect changed fixture constants.",
        },
        {
            "source_test_or_doc": "applications/algorithms/machinery/src/test/java/org/neo4j/gds/applications/algorithms/machinery/DefaultMemoryGuardTest.java",
            "input_graph_shape": "Algorithm factory with memory estimate or unimplemented estimate.",
            "procedure_or_config": "DefaultMemoryGuard",
            "expected_output": "Reject over-budget or absent estimator according to guard behavior.",
            "failure_behavior": "MemoryEstimationNotImplementedException is caught and mapped by guard path.",
            "fixture_value": "Guard fixture with mocked estimate.",
            "PRD_acceptance_area": "Strict RAM reject semantics.",
            "confidence": "DirectSource",
            "falsifier": "Run targeted test or inspect guard assertions.",
        },
        {
            "source_test_or_doc": "applications/graph-store-catalog-results/src/test/java/org/neo4j/gds/applications/graphstorecatalog/GraphMemoryUsageTest.java:34",
            "input_graph_shape": "GraphStore/CSRGraphStore fixture.",
            "procedure_or_config": "GraphMemoryUsage.of",
            "expected_output": "Node mapping, relationship topology, and memoryInfo detail map.",
            "failure_behavior": "Packed adjacency unsupported path is explicit.",
            "fixture_value": "Expected detail keys and memory bytes.",
            "PRD_acceptance_area": "Resident memory reporting.",
            "confidence": "DirectSource",
            "falsifier": "Run the test or inspect assertion changes for adjacencyLists bytesTotal, bytesOnHeap, and bytesOffHeap.",
        },
        {
            "source_test_or_doc": "pipeline/src/test/java/org/neo4j/gds/ml/pipeline/PipelineCatalogTest.java",
            "input_graph_shape": "Per-user pipeline catalog fixtures.",
            "procedure_or_config": "PipelineCatalog set/get/drop/list/exists.",
            "expected_output": "User-scoped pipeline identity and duplicate rejection.",
            "failure_behavior": "Missing pipeline throws NoSuchElementException.",
            "fixture_value": "Pipeline names and user names.",
            "PRD_acceptance_area": "Pipeline lifecycle compatibility.",
            "confidence": "DirectSource",
            "falsifier": "pipeline/src/test/java/org/neo4j/gds/ml/pipeline/PipelineCatalogTest.java:42",
        },
        {
            "source_test_or_doc": "open-model-catalog/src/test/java/org/neo4j/gds/core/model/OpenModelCatalogTest.java",
            "input_graph_shape": "Loaded model fixtures per user.",
            "procedure_or_config": "OpenModelCatalog",
            "expected_output": "Model set/get/list/drop/exist semantics and publish/store rejection in openGDS.",
            "failure_behavior": "Missing model suggests available model names.",
            "fixture_value": "Model name, creator, model class.",
            "PRD_acceptance_area": "Model lifecycle compatibility.",
            "confidence": "DirectSource",
            "falsifier": "open-model-catalog/src/test/java/org/neo4j/gds/core/model/OpenModelCatalogTest.java:325",
        },
        {
            "source_test_or_doc": "io/csv/src/test/java/org/neo4j/gds/core/io/file/csv/CsvExportEstimationTest.java:55",
            "input_graph_shape": "Graph with sampled node/relationship properties.",
            "procedure_or_config": "CsvExportEstimation",
            "expected_output": "Fixed node and relationship data estimates.",
            "failure_behavior": "Estimator should remain bounded by sample accounting.",
            "fixture_value": "CSV estimator fixture properties.",
            "PRD_acceptance_area": "IO export memory estimates.",
            "confidence": "DirectSource",
            "falsifier": "Run the test or inspect changed expected range around estimation.max.",
        },
        {
            "source_test_or_doc": "gds-values/src/test/java/org/neo4j/gds/values/primitive/PrimitiveValuesTest.java:64",
            "input_graph_shape": "Scalar and primitive array property values.",
            "procedure_or_config": "PrimitiveValues.create",
            "expected_output": "GdsValue wrappers for supported types and NO_VALUE for null.",
            "failure_behavior": "Unsupported property value type throws IllegalArgumentException.",
            "fixture_value": "Number, Object[], byte/short/int/long/float/double arrays.",
            "PRD_acceptance_area": "Property value compatibility.",
            "confidence": "DirectSource",
            "falsifier": "Run the test or inspect unsupported value assertion messages.",
        },
        {
            "source_test_or_doc": "legacy-cypher-projection tests plus CypherRecordLoader source",
            "input_graph_shape": "Cypher node/relationship query rows.",
            "procedure_or_config": "GraphProjectFromCypherConfig and CypherRecordLoader",
            "expected_output": "Read-only query, mandatory columns, EstimatedRows-based sizing.",
            "failure_behavior": "AuthorizationViolationException becomes read-only error; missing columns mention AS hints.",
            "fixture_value": "nodeQuery, relationshipQuery, parameters.",
            "PRD_acceptance_area": "Legacy Cypher projection support or unsupported decision.",
            "confidence": "DirectSource",
            "falsifier": "legacy-cypher-projection/src/main/java/org/neo4j/gds/projection/CypherRecordLoader.java:60",
        },
    ]


def patch_rows() -> list[dict[str, object]]:
    return [
        {
            "PRD_area": "API surface",
            "action": "Replace broad compatibility claim with generated support ledger.",
            "current_wording": "Every known procedure either implemented or deterministic unsupported.",
            "proposed_wording": "Generate a procedure ledger from @Procedure and @GdsCallable, then classify each row as implemented, adapter-compatible, deterministic unsupported, or deferred with a falsifier.",
            "evidence_pointer": "procedure-collector/processor/src/main/java/org/neo4j/gds/procedure/ProcedureCollector.java:42",
            "decision_reason": "GDS discovers AlgorithmSpec callables through compile-time service generation, not by hand-maintained docs.",
            "falsifier": "If service files contain procedure names absent from the generated ledger, the PRD support matrix is incomplete.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Projection Build Store",
            "action": "Split native projection, Cypher projection, file import, and generated graph semantics.",
            "current_wording": "Projection Build Store is build/control plane, not user-query store.",
            "proposed_wording": "Projection Build Store shall expose separate compatibility decisions for native store projection, legacy Cypher projection, file import/export, and generated graphs; each decision must include dense-id, label/type, property, orientation, aggregation, and estimate behavior.",
            "evidence_pointer": "native-projection/src/main/java/org/neo4j/gds/projection/GraphProjectFromStoreConfig.java:40",
            "decision_reason": "Projection variants have different grammar and estimate paths.",
            "falsifier": "A projection test that passes on Neo4j GDS but cannot be mapped into one of these variants.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Memory/RAM contract",
            "action": "Turn RAM prose into formula book and reject contract.",
            "current_wording": "Strict RAM: reject before execution if budget cannot fit.",
            "proposed_wording": "For every supported procedure, the estimator shall include graph-load resident terms, algorithm terms, result/model artifact terms, write-back terms, build scratch terms, and high-water risk; unsupported estimator paths shall reject before execution.",
            "evidence_pointer": "algo-common/src/main/java/org/neo4j/gds/AlgorithmFactory.java:127",
            "decision_reason": "Default estimator can throw; strict RAM promise is false unless absent estimators become explicit unsupported responses.",
            "falsifier": "Any supported procedure executes without estimator coverage or an explicit reject condition.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Behavior modes",
            "action": "Separate Neo4j procedure mode from GDS execution mode.",
            "current_wording": "API surface includes stream/stats/mutate/write/train/estimate.",
            "proposed_wording": "Behavior compatibility shall track both Neo4j procedure mode and GDS execution mode because mutate operations may be annotated READ while mutating the in-memory catalog graph.",
            "evidence_pointer": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/splitting/SplitRelationshipsMutateProc.java:38",
            "decision_reason": "READ does not always mean side-effect free in the GDS catalog/control plane.",
            "falsifier": "A mutate procedure that writes Neo4j OLTP data while remaining classified as catalog-only.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Artifact lifecycle",
            "action": "Document graph, model, pipeline, result-store, and file artifacts as separate state machines.",
            "current_wording": "Catalog named projections scoped/versioned by user/database/name/generation.",
            "proposed_wording": "Named graphs shall be user/database/name/generation scoped in v003; pipelines and models shall retain GDS user/name semantics unless generation is explicitly added as a rewrite extension.",
            "evidence_pointer": "pipeline/src/main/java/org/neo4j/gds/ml/pipeline/PipelineCatalog.java:35",
            "decision_reason": "GDS has multiple catalogs with different identity keys and side effects.",
            "falsifier": "Model/pipeline procedures require database/generation identity in upstream GDS source.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Write-back plane",
            "action": "Add writer/exporter compatibility section.",
            "current_wording": "OLTP on Neo4j-shaped OLTP storage, OLAP/GDS reads published immutable snapshots.",
            "proposed_wording": "Write-back procedures shall route through an explicit OLTP writer/exporter adapter and shall never write through the Projection Build Store serving path.",
            "evidence_pointer": "proc/machine-learning/src/main/java/org/neo4j/gds/ml/kge/KGEPredictWriteSpec.java:90",
            "decision_reason": "Write specs use RelationshipExporterBuilder and result-store config, not only graph topology.",
            "falsifier": "A write procedure can be implemented correctly without the writer/exporter adapter.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Property values",
            "action": "Add Neo4j/GDS value compatibility acceptance tests.",
            "current_wording": "Graph related algorithms over Neo4j-compatible storage.",
            "proposed_wording": "Supported graph properties shall round-trip through GDS value wrappers and Neo4j export wrappers for supported scalar and primitive array types; unsupported types shall reject deterministically.",
            "evidence_pointer": "gds-values/src/main/java/org/neo4j/gds/values/PrimitiveValues.java:20",
            "decision_reason": "Procedure compatibility includes property value shape, not only topology.",
            "falsifier": "A supported GDS algorithm requires a property type not represented by the value adapters.",
            "confidence": "DirectSource",
        },
        {
            "PRD_area": "Test oracles",
            "action": "Promote upstream tests into acceptance oracle extraction backlog.",
            "current_wording": "Acceptance tests are not yet derived from upstream public procedure oracles.",
            "proposed_wording": "Acceptance tests shall be derived from upstream procedure tests, memory guard tests, projection validation tests, catalog lifecycle tests, value adapter tests, and IO estimator tests before implementation claims.",
            "evidence_pointer": "proc/machine-learning/src/test/java/org/neo4j/gds/ml/linkmodels/pipeline/predict/LinkPredictionPipelineStreamProcTest.java:138",
            "decision_reason": "Tests encode expected rows, errors, and memory ranges better than prose docs.",
            "falsifier": "A supported procedure lacks an upstream oracle or a locally authored replacement oracle.",
            "confidence": "DirectSource",
        },
    ]


def coverage_rows() -> list[dict[str, object]]:
    decisions = {
        "procedure-collector": "Defines how public callable AlgorithmSpec classes are discovered.",
        "native-projection": "Defines native projection grammar, normalization, and validation.",
        "legacy-cypher-projection": "Defines Cypher projection grammar, read-only guard, and explain estimator.",
        "collections-memory-estimation": "Defines sparse huge collection MemoryRange formulas.",
        "applications/services": "Defines graph dimensions used by algorithm estimates.",
        "open-write-services": "Defines openGDS write/export extension seam.",
        "applications/operations": "Defines progress/log/toggle control-plane behavior.",
        "applications/graph-store-catalog-results": "Defines resident graph memory reporting and write result shapes.",
        "neo4j-api": "Defines Neo4j procedure return column compatibility seam.",
        "neo4j-adapter": "Defines adapter access to Neo4j id generator/high-water information.",
        "neo4j-values": "Defines Neo4j value export wrappers.",
        "gds-values": "Defines supported GDS property values and unsupported type rejection.",
        "io": "Defines import/export artifacts and CSV export memory estimates.",
        "proc/machine-learning": "Defines ML public procedure modes, estimates, and model/write behavior.",
        "proc/pipeline-catalog": "Defines pipeline catalog list/drop/exists behavior.",
    }
    rows = []
    for folder in MANDATORY_FOLDERS:
        rows.append(
            {
                "folder": folder,
                "file_count": file_count(folder),
                "evidence_status": "Covered",
                "primary_source_pointer": PRIMARY_POINTERS[folder],
                "documented_in": "GDS-PRD-L1-Evidence-Dossier-v2.md and companion TSVs",
                "PRD_decision_value": decisions[folder],
                "remaining_gap": "Deep per-procedure implementation trace still needed before implementation claim.",
                "confidence": "DirectSource",
            }
        )
    return rows


def write_markdown(procedure_rows_data: list[dict[str, object]], coverage_data: list[dict[str, object]]) -> None:
    total = len(procedure_rows_data)
    mode_counts = Counter(str(row["procedure_mode"]) for row in procedure_rows_data)
    family_counts = Counter(str(row["algorithm_family"]) for row in procedure_rows_data)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    companion_files = [
        "GDS-Procedure-Surface-Join-v2.tsv",
        "GDS-Projection-Variant-Matrix-v2.tsv",
        "GDS-Memory-Formula-Book-v2.tsv",
        "GDS-Behavior-Mode-Semantics-v2.tsv",
        "GDS-Artifact-Lifecycle-State-Machine-v2.tsv",
        "GDS-Oracle-Extraction-Appendix-v2.tsv",
        "GDS-PRD-Rewrite-Patch-Plan-v2.tsv",
        "GDS-V2-Coverage-Audit.tsv",
    ]

    top_families = "\n".join(
        f"- `{family}`: {count}" for family, count in family_counts.most_common(10)
    )
    mode_lines = "\n".join(f"- `{mode}`: {count}" for mode, count in mode_counts.most_common())
    coverage_lines = "\n".join(
        f"- `{row['folder']}`: {row['file_count']} files, primary evidence `{row['primary_source_pointer']}`"
        for row in coverage_data
    )
    companion_lines = "\n".join(f"- `{name}`" for name in companion_files)

    sample_rows = procedure_rows_data[:8]
    sample_table = "\n".join(
        "| {procedure_name} | {procedure_mode} | {algorithm_family} | {annotation_source} |".format(**row)
        for row in sample_rows
    )

    content = f"""# GDS PRD L1 Evidence Dossier v2

Generated: {generated_at}

Source scope: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/gitrefrepo/neo4j-gds-src`

This is the second-pass evidence dossier for rewriting `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD03/prd-l1.md`. It does not overwrite the v1 dossier. The purpose is to make the PRD rewrite easier by turning the Neo4j GDS source into decision tables: procedure surface, projection variants, memory formulas, behavior modes, artifact lifecycles, test oracles, and PRD patch instructions.

## Executive Take

Take the stricter call: the rewrite should not promise "GDS compatible" from CSR/topology alone. It should promise a generated compatibility ledger, strict estimator coverage, deterministic unsupported responses, and a catalog lifecycle model that separates OLTP write-back, OLAP snapshots, GDS graph mutations, model artifacts, pipeline artifacts, and result/file artifacts.

The most important correction to the PRD is mode semantics. In GDS source, a user-visible `*.mutate` procedure can be annotated as Neo4j `READ` while the `@GdsCallable` execution mode mutates the in-memory GDS graph catalog. A `*.write` procedure is the OLTP/write-back danger zone. v003 must model those as different side-effect planes.

## Method

The graph tools were used as candidate finders only. Every architecture-critical claim in this dossier is grounded by direct source paths or by a rerunnable local query plus a direct source pointer.

Confidence labels used:

- `DirectSource`: directly read source or source-derived annotation row from the scoped GDS repo.
- `GraphToolAssisted`: graph tool found the candidate and source was read or source path is provided.
- `CandidateOnly`: useful test/source candidate that must be opened or run before becoming implementation proof.
- `Inference`: PRD implication derived from multiple direct source facts.
- `LowYield`: tool/source pass did not provide enough usable evidence.

## Graph Tool Readiness

- `codebase-memory`: usable after explicit project targeting. Indexed `64,762` nodes and `280,144` edges for the scoped GDS repo. Useful candidate outputs were saved under `/tmp/codex-code-intel/codebase-memory/neo4j-gds-src-20260624-214458/`, especially `search_projection_explicit.json`, `search_memory_explicit.json`, and `search_catalog_explicit.json`.
- `CodeGraphContext`: wrapper was bounded and interrupted after a long run, but the resulting SQLite database answered `stats`: `1` repository, `834` files, `3,532` functions, `832` classes, `87` interfaces, `17` enums, and `711` modules. Treat this as usable for coarse second-opinion stats, not as a complete implementation proof.
- `rg` and direct file reads remain the verification source of truth.

## Coverage Audit

All mandatory thin folders were inspected and are represented in `GDS-V2-Coverage-Audit.tsv`.

{coverage_lines}

The coverage audit closes the v1 gap where several thin folders were present only in reading logs. The remaining gap is intentionally narrower: per-procedure implementation tracing is still required before claiming runtime support.

## Procedure Surface Join

The procedure join is generated from scoped `src/main/java` annotations: `@Procedure` and `@GdsCallable`. It contains `{total}` rows.

Mode distribution:

{mode_lines}

Top family distribution:

{top_families}

Sample rows:

| procedure | mode | family | source |
|---|---:|---|---|
{sample_table}

Decision: the PRD should require a generated public-surface ledger at build/test time. The procedure collector writes service metadata for `@GdsCallable` classes, so a hand-written support list is likely to drift.

Relevant companion: `GDS-Procedure-Surface-Join-v2.tsv`.

## Projection Variant Matrix

Projection compatibility is not one feature. Native projection, legacy Cypher projection, file import/export, generated graphs, and catalog mutation paths have different grammar and estimator constraints.

Key source-backed facts:

- `GraphProjectFromStoreConfig` normalizes top-level node and relationship properties into projections and validates empty projections and aggregation/property conflicts.
- `GraphDimensionsReader` pulls label/type/property tokens, estimated node counts, highest possible node count, relationship counts, and highest relationship id from Neo4j APIs.
- `GraphProjectFromCypherConfig` rejects native projection keys and uses query strings plus params.
- `CypherRecordLoader` validates mandatory columns and wraps authorization/write attempts as read-only projection failures.
- `CypherQueryEstimator` uses `EXPLAIN` and `EstimatedRows`.

Relevant companion: `GDS-Projection-Variant-Matrix-v2.tsv`.

## Memory Formula Book

Strict RAM compatibility should be expressed as formula coverage, not a single cap. The minimum useful formula taxonomy is:

1. graph-load resident terms,
2. graph-load build scratch terms,
3. algorithm terms,
4. result stream or artifact terms,
5. model/pipeline artifact terms,
6. write-back/export terms,
7. high-water risks,
8. reject condition for absent or incomplete estimators.

Source-backed correction: `AlgorithmFactory.memoryEstimation` has a default implementation that throws `MemoryEstimationNotImplementedException`. Therefore, no supported procedure can rely on "we will estimate later." The PRD must say that absent estimators produce deterministic unsupported responses before execution.

Relevant companion: `GDS-Memory-Formula-Book-v2.tsv`.

## Behavior Mode Semantics

The behavior matrix distinguishes:

- stream/read: row output with no catalog or OLTP mutation,
- mutate: in-memory graph catalog side effect, often exposed as Neo4j `READ`,
- write: OLTP/write-back or export side effect,
- estimate: control-plane memory result,
- train: model artifact creation,
- pipeline catalog: user-scoped catalog mutation that can also be exposed through `READ` procedures,
- operations: progress/log/toggle control plane.

This should become a PRD section because side-effect classification is the line between safe OLAP snapshot reads and writes into the OLTP plane.

Relevant companion: `GDS-Behavior-Mode-Semantics-v2.tsv`.

## Artifact Lifecycle State Machine

The rewrite needs separate state machines for:

- named graphs,
- pipelines,
- models,
- result stores,
- memory reports,
- import/export files.

GDS source does not give v003 the exact generation/watermark identity model. That is a deliberate rewrite addition for immutable published OLAP snapshots, and the PRD should call it an extension rather than an upstream GDS behavior.

Relevant companion: `GDS-Artifact-Lifecycle-State-Machine-v2.tsv`.

## Oracle Extraction Appendix

The best acceptance tests should be extracted from upstream tests and source oracles, not invented from prose. The appendix prioritizes ML estimate tests, memory guard tests, sparse collection tests, projection validation, model/pipeline catalog tests, value adapters, and IO estimator tests.

Relevant companion: `GDS-Oracle-Extraction-Appendix-v2.tsv`.

## PRD Rewrite Patch Plan

The PRD should be patched in these areas:

1. replace broad procedure compatibility with generated support ledger,
2. split projection variants,
3. turn RAM claims into estimator formula/reject contracts,
4. separate Neo4j procedure mode from GDS execution mode,
5. document graph/model/pipeline/result/file artifact lifecycles,
6. add writer/exporter adapter requirements,
7. add property value compatibility tests,
8. extract upstream test oracles before implementation claims.

Relevant companion: `GDS-PRD-Rewrite-Patch-Plan-v2.tsv`.

## Architecture Decisions For `prd-l1.md`

### AD-001: Compatibility Ledger Before Support Claims

The PRD shall require a generated support ledger from source annotations and procedure-collector service metadata. A procedure is not supported until the ledger row has facade/config/result/estimator/side-effect classification and a deterministic unsupported fallback.

### AD-002: Projection Is A Family, Not A Single Store

Projection Build Store remains a build/control plane. Native projection, Cypher projection, file import, and generated graph creation must each have separate acceptance criteria.

### AD-003: Strict RAM Means Estimator Or Reject

Every supported procedure must have estimator coverage for graph load, algorithm state, output artifacts, model/pipeline artifacts, write-back/export buffers, and build scratch. If any major term is absent, the procedure is unsupported for strict-RAM mode.

### AD-004: Mutate Is Not Write

Catalog mutation and Neo4j write-back must be separate modes. `*.mutate` updates GDS graph artifacts; `*.write` writes through OLTP/export adapters.

### AD-005: Generation Identity Is A v003 Extension

Upstream GDS catalog source is user/name-oriented. v003's user/database/name/generation identity is necessary for immutable snapshot publication, but it is not a direct upstream behavior.

## Open Questions

- Which GDS procedure families are in the v003 MVP support tier versus deterministic unsupported tier?
- Will Cypher projection be supported in v003, or rejected with a Neo4j-compatible error shape?
- Which memory budget source is authoritative: heap cap, RSS cap, per-query cap, per-graph cap, or tenant cap?
- Does v003 support model/pipeline catalogs, or are they deterministic unsupported for MVP?
- What is the local equivalent of Neo4j transaction/security behavior for read-only Cypher projection and write-back procedures?

## Verification

Generated companion files:

{companion_lines}

Verification commands used or intended:

```bash
python3 docs_PRD03/reference-learning/generate_gds_v2_evidence.py
python3 - <<'PY'
from pathlib import Path
files = [
  'GDS-Procedure-Surface-Join-v2.tsv',
  'GDS-Projection-Variant-Matrix-v2.tsv',
  'GDS-Memory-Formula-Book-v2.tsv',
  'GDS-Behavior-Mode-Semantics-v2.tsv',
  'GDS-Artifact-Lifecycle-State-Machine-v2.tsv',
  'GDS-Oracle-Extraction-Appendix-v2.tsv',
  'GDS-PRD-Rewrite-Patch-Plan-v2.tsv',
  'GDS-V2-Coverage-Audit.tsv',
]
base = Path('docs_PRD03/reference-learning')
for name in files:
    p = base / name
    assert p.exists(), name
    assert p.read_text().splitlines()[0], name
PY
git diff --check
```

## Companion File Index

Use the TSV files as the machine-readable substrate and this Markdown file as the synthesis layer. For the PRD rewrite, start from `GDS-PRD-Rewrite-Patch-Plan-v2.tsv`, then use the other tables as evidence backing for each acceptance criterion.
"""
    (OUT_DIR / "GDS-PRD-L1-Evidence-Dossier-v2.md").write_text(content, encoding="utf-8")


def main() -> None:
    procedure_data = parse_procedure_surface()
    projection_data = projection_rows()
    memory_data = memory_rows()
    behavior_data = behavior_rows()
    lifecycle_data = lifecycle_rows()
    oracle_data = oracle_rows()
    patch_data = patch_rows()
    coverage_data = coverage_rows()

    write_tsv(OUT_DIR / "GDS-Procedure-Surface-Join-v2.tsv", PROCEDURE_COLUMNS, procedure_data)
    write_tsv(OUT_DIR / "GDS-Projection-Variant-Matrix-v2.tsv", PROJECTION_COLUMNS, projection_data)
    write_tsv(OUT_DIR / "GDS-Memory-Formula-Book-v2.tsv", MEMORY_COLUMNS, memory_data)
    write_tsv(OUT_DIR / "GDS-Behavior-Mode-Semantics-v2.tsv", BEHAVIOR_COLUMNS, behavior_data)
    write_tsv(OUT_DIR / "GDS-Artifact-Lifecycle-State-Machine-v2.tsv", LIFECYCLE_COLUMNS, lifecycle_data)
    write_tsv(OUT_DIR / "GDS-Oracle-Extraction-Appendix-v2.tsv", ORACLE_COLUMNS, oracle_data)
    write_tsv(OUT_DIR / "GDS-PRD-Rewrite-Patch-Plan-v2.tsv", PATCH_COLUMNS, patch_data)
    write_tsv(OUT_DIR / "GDS-V2-Coverage-Audit.tsv", COVERAGE_COLUMNS, coverage_data)
    write_markdown(procedure_data, coverage_data)
    print(f"generated {len(procedure_data)} procedure rows")


if __name__ == "__main__":
    main()
