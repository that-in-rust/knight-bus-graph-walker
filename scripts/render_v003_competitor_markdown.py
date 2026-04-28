#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parents[1]))

from benchmarks.walk_hopper_v1.common import read_json_file_now
from benchmarks.walk_hopper_v1.competitor_markdown import write_v003_markdown_bundle_now


def build_arg_parser_now() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render tracked v003 markdown from the competitor summary JSON.")
    parser.add_argument("--summary-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main() -> None:
    args = build_arg_parser_now().parse_args()
    summary_payload = read_json_file_now(args.summary_json.resolve())
    write_v003_markdown_bundle_now(summary_payload, args.output_dir.resolve())


if __name__ == "__main__":
    main()
