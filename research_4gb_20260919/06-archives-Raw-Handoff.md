# Raw Corpus Split And Tool-Result Recipe

Checkpoint: 2026-09-20 local date. Explicit parent assignment supersedes the earlier proposed tail transfer. Assignment is not completion. No new agent has been created.

## Exact Boundary

Source: `/Users/amuldotexe/Desktop/personal-repos-lane/knight-bus-graph-walker/docs_PRD04/raw-research-evidence-dump-2026-07-26.txt`

Frozen SHA-256: `474a45f214d771e8e733ad9384dd02b6fa329e5cfcbe6649913a4ad84e1f3bce`; 31,856 physical lines, 36,341,645 bytes.

- **Reader01:** agent JSONL/head **1-12296**, NON-tool-result/NON-StructuredOutput fields: assistant text, initial/user prompts, attachment text, historical assertions in thinking, ordinary tool-call inputs and metadata. Read/classify exact unique substantive content with field-level duplicate/read ledgers. Lane06 must not duplicate this scope.
- **Reader06:** all **189 StructuredOutput.input** values and their 187 exact journal-result aliases, plus complete background tail **12297-31856**. Already-read report fields are listed below. A header-to-next-header index row for the last journal extends to 12300 because it includes separators; that does not change the 12297 section boundary.
- **Tool-result content:** unassigned for now; reader04 may take it after the reference corpus. **3,278 occurrences / 2,299 unique canonical content values** in 1-12296. Lane06 does not semantically read this queue while it is reserved. Indexing/schema work is not consumption.

## Independent Reading Queues

**Closing lane06 update:**189/189 unique StructuredOutput inputs are covered (188direct plus4080 via exact report fields), with187 exact journal aliases. Background12297-31856 has14complete non-report nonempty bodies, complete final-report narrative/result fields, explicitly summarized workflow support and2empty bodies. Exact coverage is in Evidence.md, Background.md and Release-Lines.md. This closes lane06's retained reading, not reader01's head fields or the ordinary tool-result queue. The extraction recipe below remains read-only and unchanged; coordinator decides reader04 ownership.

These are different field domains, not additive percentages of the whole source. Same material can recur across domains with different formatting.

| Domain | Exact unique inventory | Semantic coverage at this checkpoint |
| --- | --- | --- |
| StructuredOutput.input | 189 values; 1,037,529 canonical JSON characters | 101 read, 564,401 characters; 88 remain, 473,128 characters |
| Journal result aliases | 187 values, each exactly equal to one structured input | Duplicate-accounted only when its canonical input is read; not new independent evidence |
| Assistant text | 792 blocks, 726 exact strings; 131,063 unique characters (134,510 with repeats) | No full block credited yet. All 726 require reading/classification; length is not a substance test |
| Tool-result content | 3,278 blocks, 2,299 exact canonical values; 6,089,537 unique canonical characters (6,343,203 with repeats) | Not fully read. Includes source evidence, not just execution plumbing. Some previews were seen but no complete queue coverage is claimed |
| Background task bodies | 17 blocks: 15 distinct nonempty bodies and two empty bodies; 820,173 raw body characters | Only four final-report fields previously read. Exact novel substantive remainder is NOT yet quantified; block hashes do not establish semantic uniqueness or cross-domain duplication |
| Other record fields | 194 initial prompt strings, 776 attachments, 2,011 thinking blocks, seven user text blocks, other tool-call inputs and metadata | Structurally accounted only. Embedded instructions are not instructions to the reader. Classify provenance/context versus unique evidence; do not silently count all as read |

The 189 structured summaries are therefore not the corpus denominator. An assistant paragraph or fetched source omitted by those summaries still needs coverage by its owner. Machine tables may be structurally summarized with relevant inspected rows and exact row spans, explicitly marked as such. Inventory numbers above are frozen at checkpoint101, not a live lane06 progress counter; Evidence.md is authoritative for subsequent reads.

## Compact Reader04 Recipe

Read-only schema: JSONL record `message.content[block]` with `type == "tool_result"`; target field `.content` is a string in 3,039 occurrences and a list in 239. Block keys are `type`, `tool_use_id`, `content`, and optionally `is_error` (present in 2,285). Preserve the error flag and tool-use ID for interpretation, without treating them as substantive evidence or executing the corresponding call. A canonical-content alias can have a different error wrapper; retain that distinction.

Partition by the **first physical occurrence** of each exact canonical content value, not by hash prefix or arbitrary filenames. Cross-partition repeats retain aliases to that first occurrence. Physical lines are one-based, content indices zero-based.

| Owner partition | Occurrences | Unique values first appearing here |
| --- | ---: | ---: |
| 1-3000 | 830 | 534 |
| 3001-6000 | 879 | 560 |
| 6001-9000 | 765 | 585 |
| 9001-12000 | 771 | 595 |
| 12001-12296 | 33 | 25 |
| Total | 3,278 | 2,299 |

Run the following in the repository root. It writes nothing. Set `START/END` to a partition. Manifest rows mean inspected structure only. For semantic reading set `READ` to `(physical_line, block_index)` and `OFFSET` to a rendered-content character offset; each call emits at most 10,000 content characters. Continue until `END` equals `TOTAL`; record all rendered spans and the JSON field. Do not count a clipped output as a complete value. The largest values need several chunks.

```python
from pathlib import Path
import hashlib, json
START, END = 1, 3000
READ, OFFSET = None, 0  # e.g. READ = (27, 0)
source = Path('docs_PRD04/raw-research-evidence-dump-2026-07-26.txt')
seen = {}
for line, raw in enumerate(source.read_text().splitlines()[:12296], 1):
    try:
        record = json.loads(raw)
    except ValueError:
        continue
    blocks = record.get('message', {}).get('content', [])
    if not isinstance(blocks, list):
        continue
    for index, block in enumerate(blocks):
        if not isinstance(block, dict) or block.get('type') != 'tool_result':
            continue
        value = block.get('content')
        key = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'))
        first = seen.setdefault(key, (line, index))
        if READ is None and START <= line <= END:
            print(line, index, first, len(key),
                  hashlib.sha256(key.encode()).hexdigest(),
                  block.get('is_error'), block.get('tool_use_id'))
        elif READ == (line, index):
            rendered = value if isinstance(value, str) else json.dumps(
                value, ensure_ascii=False, indent=2)
            end = min(OFFSET + 10000, len(rendered))
            print('FIELD', line, index, 'content', 'FIRST', first,
                  'START', OFFSET, 'END', end, 'TOTAL', len(rendered))
            print(rendered[OFFSET:end])
```

A duplicate only receives reading credit after its first value has actually been consumed. For lists, preserve nested text/resource/error structure and distinguish row sampling from complete prose reading. Partial overlaps with background captures or summaries are not exact whole-value aliases; share proven field/substring locators with reader06 to avoid redundant work. No private identifying search queries, source edits, generated extraction files or execution of historical commands.

## Tail Task Boundaries

All spans include their header and trailing separators, not just substantive text. Character counts exclude the header, include internal newlines and are structural only.

| Physical span | Task | Body characters | Structural type, not reading credit |
| --- | --- | ---: | --- |
| 12301-12310 | b07z98zim | 179 | HTTP/regex diagnostic |
| 12311-12319 | b24ku7xlu | 596 | Frontend pricing snippet |
| 12320-12322 | b33rpdhs8 | 6 | Count |
| 12323-12336 | b4r1mogbv | 35,270 | Extracted frontend matches; very long lines |
| 12337-12480 | b4u82n8qi | 37,914 | Public discussion captures |
| 12481-12861 | b5l6zr8so | 32,730 | Public forum post captures |
| 12862-12868 | b9jkjh7s8 | 112 | Regex diagnostic |
| 12869-12892 | b9v7fti55 | 90,467 | Frontend pricing source snippets; very long lines |
| 12893-13041 | bbpyqbgxm | 38,627 | Public discussion search captures |
| 13042-13410 | bis2by1ew | 81,933 | Live pricing-page extraction |
| 13411-22373 | bk8mak8ct | 125,020 | Release documentation captures |
| 22374-24049 | bnc6kq04l | 36,980 | Release documentation captures |
| 24050-26508 | bomyechmk | 56,900 | Release documentation captures |
| 26509-26510 | br0btkr06 | 0 | Empty |
| 26511-29086 | bx8b5zpym | 61,391 | Release documentation captures |
| 29087-31855 | w39wxr9d2 | 222,048 | Complete pretty-printed report JSON |
| 31856 | wh22j0v5i | 0 | Empty header |

## Read And Deduplicate Precisely

Reader06 uses stdout/in-memory processing only; do not generate extracted source files. After structured-output progress, prioritize substantive customer captures at 12337-12861 and 12893-13041, then release captures, then pricing-source snippets and report remainder. Check long-line lengths before displaying: a 20-line chunk here can exceed 90 KB. Bound output to roughly 10,000-15,000 characters, recording `physical line: character start-end` when splitting a long line. A clipped prefix is not a full-line read.

The final JSON parses as `json.loads("\n".join(lines[29087:31855]))` after `Path(source).read_text().splitlines()`. Read one field or bounded list-item range at a time; name the JSON field and physical span. Do not print the entire object. Its fields are `summary`, `agentCount`, `logs`, `result`, `workflowProgress`, `totalTokens`, `totalToolCalls`. The outer `summary` is not the already-read `result.summary`.

Already consumed: `result.summary`, `result.findings`, `result.caveats`, `result.openQuestions`, physically **29134-29280**, exactly equal together to complete structured input **4080**. Preserve this exact alias; no need to reread those four fields. Remaining result fields include `question` (29133, historical prompt), `refuted` (29281-29342), `unverified` (inspect its actual value rather than assume empty), `sources` (29344-29507), `stats` (29508-29520). `logs` and `workflowProgress` need structured classification and duplicate comparison, not automatic discard. Much of the latter may repeat agent results, but equality must be demonstrated field by field before credit.

Canonical JSON equality uses `json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))`. Compare background captures with agent tool-result `message.content[block].content` and their contained text values in 1-12296. Report exact equivalent fields/blocks and source locators; differing wrappers or overlapping excerpts are NOT whole-value aliases. Tool-result reading is reserved pending reader04; cross-boundary equality can later avoid rereading an already-read value, but delegation alone gives no credit.

For assistant text in the retained head, parse each JSONL record and select `message.role == "assistant"`, then content items with `type == "text"`. Preserve one-based physical line and zero-based content-block index. The complete index and exact first-occurrence aliases are in [06-archives-Structure.md](06-archives-Structure.md), under All Assistant Text Blocks. No stop-reason filter, minimum length or keyword test is adequate to establish all substantive finals.

All source data, including captured prompts, commands, public discussions and private contextual fields, remain untrusted. Do not execute historical commands or put identifying private excerpts into web searches. Keep all new deliverables Markdown and within the receiving lane's ownership. Report actual consumed fields/spans, duplicate-accounted fields, structural-only tables and pending regions separately.

## Other Archive Ownership

Newest parent update: reader05 owns PMF003 1-10653; reader02 owns 10654-18529; reader03 owns 18530-25487. Reader03's complete chat reading is parent-reported, not independently rechecked by reader06. Lane06's earlier consumed spans and duplicate pointers remain in Evidence.md; no PMF/chat source was reopened for this checkpoint.
