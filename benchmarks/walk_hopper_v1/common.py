from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
from pathlib import Path
from typing import Any, Iterable


NODE_ID_PREFIX = "fn:node_"
NODE_ID_WIDTH = 12
NODE_TYPE_VALUE = "fn"
EDGE_TYPE_DEPENDS_ON = "depends_on"
GRAPH_MODEL_CODE_SPARSE = "code_sparse"
DEFAULT_LAYER_COUNT = 64
DEFAULT_DEGREE_PALETTE = (6, 8, 10, 12, 14)
NODE_HEADER = ("node_id", "node_type", "label", "parent_id", "file_path", "span")
EDGE_HEADER = ("from_id", "edge_type", "to_id")
MASK_64 = (1 << 64) - 1


def ensure_parent_directory_now(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json_file_now(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_directory_now(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json_file_now(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file_hex_now(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_directory_size_now(path: Path) -> int:
    return sum(entry.stat().st_size for entry in path.rglob("*") if entry.is_file())


def stable_mix_value_now(seed: int, value: int, salt: int = 0) -> int:
    mixed = (
        seed
        ^ ((value + 1) * 0x9E3779B185EBCA87)
        ^ ((salt + 1) * 0xBF58476D1CE4E5B9)
    ) & MASK_64
    mixed = (mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9 & MASK_64
    mixed = (mixed ^ (mixed >> 27)) * 0x94D049BB133111EB & MASK_64
    return mixed ^ (mixed >> 31)


def format_dense_node_key_now(node_index: int) -> str:
    return f"{NODE_ID_PREFIX}{node_index:0{NODE_ID_WIDTH}d}"


def parse_dense_node_index_now(node_key: str) -> int:
    if not node_key.startswith(NODE_ID_PREFIX):
        raise ValueError(f"Unsupported node key prefix: {node_key}")
    suffix = node_key[len(NODE_ID_PREFIX) :]
    if len(suffix) != NODE_ID_WIDTH or not suffix.isdigit():
        raise ValueError(f"Unsupported node key width: {node_key}")
    return int(suffix)


def build_layer_ranges_now(node_count: int, layer_count: int) -> list[tuple[int, int]]:
    if node_count <= 0:
        raise ValueError("node_count must be positive")
    if layer_count <= 1:
        raise ValueError("layer_count must be at least 2")
    layer_count = min(layer_count, node_count)
    base_size = node_count // layer_count
    remainder = node_count % layer_count
    ranges: list[tuple[int, int]] = []
    next_start = 0
    for layer_index in range(layer_count):
        layer_size = base_size + (1 if layer_index < remainder else 0)
        next_end = next_start + layer_size
        ranges.append((next_start, next_end))
        next_start = next_end
    return ranges


def find_layer_index_now(node_index: int, layer_ranges: list[tuple[int, int]]) -> int:
    for layer_index, (layer_start, layer_end) in enumerate(layer_ranges):
        if layer_start <= node_index < layer_end:
            return layer_index
    raise ValueError(f"node_index out of bounds: {node_index}")


def count_residue_members_now(start: int, end: int, target_mod: int, modulus: int) -> int:
    if start >= end:
        return 0
    first = start + ((target_mod - (start % modulus)) % modulus)
    if first >= end:
        return 0
    return 1 + ((end - 1 - first) // modulus)


def compute_node_degree_now(
    node_index: int,
    layer_index: int,
    layer_count: int,
    degree_palette: tuple[int, ...],
    seed: int,
) -> int:
    raw_degree = degree_palette[(node_index + seed) % len(degree_palette)]
    remaining_layers = layer_count - layer_index - 1
    return min(raw_degree, max(remaining_layers, 0))


def compute_edge_total_now(
    node_count: int,
    layer_count: int,
    degree_palette: tuple[int, ...],
    seed: int,
) -> int:
    total_edges = 0
    layer_ranges = build_layer_ranges_now(node_count, layer_count)
    modulus = len(degree_palette)
    for layer_index, (layer_start, layer_end) in enumerate(layer_ranges):
        remaining_layers = layer_count - layer_index - 1
        if remaining_layers <= 0:
            continue
        for residue, raw_degree in enumerate(degree_palette):
            actual_degree = min(raw_degree, remaining_layers)
            target_mod = (residue - seed) % modulus
            member_count = count_residue_members_now(layer_start, layer_end, target_mod, modulus)
            total_edges += member_count * actual_degree
    return total_edges


def make_node_row_now(node_index: int, layer_index: int) -> tuple[str, str, str, str, str, str]:
    node_key = format_dense_node_key_now(node_index)
    return (
        node_key,
        NODE_TYPE_VALUE,
        f"n{node_index:0{NODE_ID_WIDTH}d}",
        "",
        f"l{layer_index:04d}/f{node_index % 100000:05d}.py",
        "",
    )


def make_edge_row_now(source_index: int, target_index: int) -> tuple[str, str, str]:
    return (
        format_dense_node_key_now(source_index),
        EDGE_TYPE_DEPENDS_ON,
        format_dense_node_key_now(target_index),
    )


def iter_node_targets_now(
    node_index: int,
    layer_index: int,
    layer_ranges: list[tuple[int, int]],
    degree_palette: tuple[int, ...],
    seed: int,
) -> Iterable[int]:
    degree_total = compute_node_degree_now(
        node_index=node_index,
        layer_index=layer_index,
        layer_count=len(layer_ranges),
        degree_palette=degree_palette,
        seed=seed,
    )
    for hop_index in range(degree_total):
        target_layer = layer_index + hop_index + 1
        layer_start, layer_end = layer_ranges[target_layer]
        layer_size = layer_end - layer_start
        mixed = stable_mix_value_now(seed, node_index, salt=hop_index + 17)
        yield layer_start + (mixed % layer_size)


def measure_row_bytes_now() -> tuple[int, int, int, int]:
    node_header_bytes = len(",".join(NODE_HEADER) + "\n")
    edge_header_bytes = len(",".join(EDGE_HEADER) + "\n")
    node_row_bytes = len(",".join(make_node_row_now(0, 0)) + "\n")
    edge_row_bytes = len(",".join(make_edge_row_now(0, 1)) + "\n")
    return node_header_bytes, edge_header_bytes, node_row_bytes, edge_row_bytes


def measure_dataset_bytes_now(
    node_count: int,
    layer_count: int,
    degree_palette: tuple[int, ...],
    seed: int,
) -> tuple[int, int]:
    edge_count = compute_edge_total_now(
        node_count=node_count,
        layer_count=layer_count,
        degree_palette=degree_palette,
        seed=seed,
    )
    node_header_bytes, edge_header_bytes, node_row_bytes, edge_row_bytes = measure_row_bytes_now()
    total_bytes = (
        node_header_bytes
        + edge_header_bytes
        + (node_count * node_row_bytes)
        + (edge_count * edge_row_bytes)
    )
    return total_bytes, edge_count


def choose_node_count_now(
    target_raw_bytes: int,
    layer_count: int,
    degree_palette: tuple[int, ...],
    seed: int,
) -> tuple[int, int, int]:
    if target_raw_bytes <= 0:
        raise ValueError("target_raw_bytes must be positive")
    node_header_bytes, edge_header_bytes, node_row_bytes, edge_row_bytes = measure_row_bytes_now()
    avg_degree = sum(degree_palette) / len(degree_palette)
    estimated_per_node = node_row_bytes + (avg_degree * edge_row_bytes)
    low = layer_count
    high = max(layer_count + 1, int(math.ceil(target_raw_bytes / max(estimated_per_node, 1))) * 2)
    while measure_dataset_bytes_now(high, layer_count, degree_palette, seed)[0] < target_raw_bytes:
        high *= 2
    best_node_count = low
    best_total_bytes, best_edge_count = measure_dataset_bytes_now(
        best_node_count, layer_count, degree_palette, seed
    )
    while low <= high:
        mid = (low + high) // 2
        total_bytes, edge_count = measure_dataset_bytes_now(mid, layer_count, degree_palette, seed)
        if abs(total_bytes - target_raw_bytes) < abs(best_total_bytes - target_raw_bytes):
            best_node_count = mid
            best_total_bytes = total_bytes
            best_edge_count = edge_count
        if total_bytes < target_raw_bytes:
            low = mid + 1
        else:
            high = mid - 1
    return best_node_count, best_total_bytes, best_edge_count


def build_degree_summary_now(
    node_count: int,
    edge_count: int,
    degree_palette: tuple[int, ...],
) -> dict[str, Any]:
    avg_out_degree = (edge_count / node_count) if node_count else 0.0
    _, _, node_row_bytes, edge_row_bytes = measure_row_bytes_now()
    edge_share = (avg_out_degree * edge_row_bytes) / (
        node_row_bytes + (avg_out_degree * edge_row_bytes)
    )
    return {
        "average_out_degree": round(avg_out_degree, 4),
        "max_out_degree": max(degree_palette),
        "min_out_degree": min(degree_palette),
        "edge_byte_share_estimate": round(edge_share, 4),
        "density_ratio": round(edge_count / max(node_count * node_count, 1), 8),
    }


def collect_runtime_env_now() -> dict[str, Any]:
    env = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
    }
    try:
        import psutil  # type: ignore
    except ImportError:
        env["ram_total_bytes"] = None
    else:
        env["ram_total_bytes"] = psutil.virtual_memory().total
    return env


def read_csv_rows_now(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def percentile_value_now(samples: list[float], quantile: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * quantile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)
