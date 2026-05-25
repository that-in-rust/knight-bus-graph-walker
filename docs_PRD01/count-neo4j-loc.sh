#!/usr/bin/env bash
# count-neo4j-loc.sh
# Counts lines of code in the Neo4j reference clone.
# Outputs a summary to stdout and writes detailed results to
# docs_PRD01/neo4j-loc-results.md
#
# Usage: bash docs_PRD01/count-neo4j-loc.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NEO4J_DIR="$REPO_ROOT/neo4j-reference/neo4j"
OUT="$REPO_ROOT/docs_PRD01/neo4j-loc-results.md"

if [ ! -d "$NEO4J_DIR" ]; then
  echo "ERROR: Neo4j reference clone not found at $NEO4J_DIR"
  echo "Run: git clone --depth 1 https://github.com/neo4j/neo4j.git neo4j-reference/neo4j"
  exit 1
fi

echo "# Neo4j Lines of Code Analysis" > "$OUT"
echo "" >> "$OUT"
echo "Generated: $(date -u '+%Y-%m-%d %H:%M:%S UTC')" >> "$OUT"
echo "" >> "$OUT"

# --- Overall summary ---
echo "## Overall Summary" >> "$OUT"
echo "" >> "$OUT"
echo '```' >> "$OUT"
cloc "$NEO4J_DIR" --exclude-dir=.git,target,node_modules --quiet | tee -a "$OUT"
echo '```' >> "$OUT"
echo "" >> "$OUT"

# --- Per community submodule ---
echo "## Per Community Submodule" >> "$OUT"
echo "" >> "$OUT"
echo "| Module | Files | Blank | Comment | Code |" >> "$OUT"
echo "|---|---:|---:|---:|---:|" >> "$OUT"

for dir in "$NEO4J_DIR/community"/*/; do
  mod=$(basename "$dir")
  line=$(cloc "$dir" --exclude-dir=.git,target,node_modules --quiet --csv 2>/dev/null \
    | grep '^SUM\|^[0-9]' | tail -1)
  # CSV format: files,language,blank,comment,code
  files=$(echo "$line" | cut -d',' -f1)
  blank=$(echo "$line" | cut -d',' -f3)
  comment=$(echo "$line" | cut -d',' -f4)
  code=$(echo "$line" | cut -d',' -f5)
  if [ -n "$code" ] && [ "$code" != "code" ]; then
    echo "| $mod | $files | $blank | $comment | $code |" >> "$OUT"
  fi
done

echo "" >> "$OUT"

# --- Key modules breakdown by language ---
KEY_MODULES="kernel kernel-api record-storage-engine cypher bolt io index wal storage-engine-util"
echo "## Key Modules — Language Breakdown" >> "$OUT"
echo "" >> "$OUT"

for mod in $KEY_MODULES; do
  dir="$NEO4J_DIR/community/$mod"
  if [ -d "$dir" ]; then
    echo "### $mod" >> "$OUT"
    echo '```' >> "$OUT"
    cloc "$dir" --exclude-dir=.git,target,node_modules --quiet >> "$OUT"
    echo '```' >> "$OUT"
    echo "" >> "$OUT"
  fi
done

echo "Results written to: $OUT"
