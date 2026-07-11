#!/usr/bin/env bash
# verify-corpus-spec.sh — executable gates for SPEC-graph-learning-corpus-research.md (v3)
# CHK-CLONE-001/002, CHK-DISK-001, CHK-PAT-001/002, CHK-PUB-001/004.
# Run from repo root or this folder. Exit 0 = all gates green.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
LEDGER="$HERE/corpus-ledger.tsv"
CORPUS="$ROOT/reference-repos-corpus"
FAIL=0
note() { printf '%-14s %s\n' "$1" "$2"; }
bad()  { note "$1" "FAIL: $2"; FAIL=1; }
ok()   { note "$1" "ok: $2"; }

# CHK-CLONE-001: every ledger row has a local_clone path that exists (or a clone-failed flag)
missing=0; total=0
while IFS=$'\t' read -r cat repo url stars push lang clone flags desc; do
  [ "$cat" = "category" ] && continue
  total=$((total+1))
  if [ "$clone" = "-" ] || [ -z "$clone" ]; then
    case "$flags" in *clone-failed*) ;; *) missing=$((missing+1));; esac
  elif [ ! -d "$ROOT/$clone" ]; then
    missing=$((missing+1))
  fi
done < "$LEDGER"
if [ "$missing" -gt 0 ]; then bad CHK-CLONE-001 "$missing/$total rows lack an existing local clone"; else ok CHK-CLONE-001 "$total rows all cloned"; fi

# CHK-CLONE-002: every corpus clone is shallow, none sparse
if [ -d "$CORPUS" ]; then
  nonshallow=0; sparse=0
  for d in "$CORPUS"/*/; do
    [ -d "$d/.git" ] || continue
    [ "$(git -C "$d" rev-parse --is-shallow-repository 2>/dev/null)" = "true" ] || nonshallow=$((nonshallow+1))
    [ -e "$d/.git/info/sparse-checkout" ] && sparse=$((sparse+1))
  done
  if [ "$nonshallow" -gt 0 ] || [ "$sparse" -gt 0 ]; then bad CHK-CLONE-002 "$nonshallow non-shallow, $sparse sparse"; else ok CHK-CLONE-002 "all corpus clones plain shallow"; fi
else
  bad CHK-CLONE-002 "reference-repos-corpus/ does not exist yet"
fi

# CHK-DISK-001: corpus volume under 50 GB
if [ -d "$CORPUS" ]; then
  kb=$(du -sk "$CORPUS" | cut -f1)
  gb=$((kb / 1024 / 1024))
  if [ "$gb" -ge 50 ]; then bad CHK-DISK-001 "corpus at ${gb} GB >= 50 GB budget"; else ok CHK-DISK-001 "corpus at ${gb} GB"; fi
fi

# CHK-PUB-001: pattern docs come in ascii/mermaid pairs with four-word names, 150-400 lines
pairs=0
for f in "$HERE"/*-ascii.md; do
  [ -e "$f" ] || continue
  base="${f%-ascii.md}"
  name="$(basename "$f")"
  echo "$name" | grep -Eq '^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-ascii\.md$' || bad CHK-PUB-001 "$name not four-word"
  [ -e "$base-mermaid.md" ] || bad CHK-PUB-001 "$(basename "$base")-mermaid.md missing twin"
  for side in "$f" "$base-mermaid.md"; do
    [ -e "$side" ] || continue
    lines=$(wc -l < "$side")
    if [ "$lines" -lt 150 ] || [ "$lines" -gt 400 ]; then bad CHK-PUB-001 "$(basename "$side") is $lines lines (150-400)"; fi
  done
  pairs=$((pairs+1))
done
for f in "$HERE"/*-mermaid.md; do
  [ -e "$f" ] || continue
  base="${f%-mermaid.md}"
  [ -e "$base-ascii.md" ] || bad CHK-PUB-001 "$(basename "$f") lacks ascii twin"
done
if [ "$pairs" -eq 0 ]; then bad CHK-PUB-001 "no pattern pairs published yet"; else ok CHK-PUB-001 "$pairs pairs, names/shape valid"; fi

# CHK-PAT-001/002: every pattern doc cites >=2 repos and cited local paths exist
for f in "$HERE"/*-ascii.md "$HERE"/*-mermaid.md; do
  [ -e "$f" ] || continue
  cites=$(grep -oE '(reference-repos-[a-z-]+|src)/[A-Za-z0-9._/-]+' "$f" | grep -c 'reference-repos')
  repos=$(grep -oE 'reference-repos-[a-z-]+/[A-Za-z0-9._-]+' "$f" | sort -u | wc -l)
  if [ "$repos" -lt 2 ]; then bad CHK-PAT-001 "$(basename "$f") cites $repos distinct repos (<2)"; fi
  while read -r p; do
    [ -z "$p" ] && continue
    [ -e "$ROOT/$p" ] || bad CHK-PAT-002 "$(basename "$f") cites missing path $p"
  done < <(grep -oE 'reference-repos-[a-z-]+/[A-Za-z0-9._/-]+' "$f" | sort -u)
done

# CHK-PUB-004: pattern-index.md lists every pair
if [ -e "$HERE/pattern-index.md" ]; then
  for f in "$HERE"/*-ascii.md; do
    [ -e "$f" ] || continue
    stem="$(basename "$f" -ascii.md)"
    grep -q "$stem" "$HERE/pattern-index.md" || bad CHK-PUB-004 "$stem not in pattern-index.md"
  done
  ok CHK-PUB-004 "index checked"
else
  bad CHK-PUB-004 "pattern-index.md missing"
fi

if [ "$FAIL" -eq 0 ]; then echo "ALL GATES GREEN"; else echo "GATES FAILING"; fi
exit "$FAIL"
