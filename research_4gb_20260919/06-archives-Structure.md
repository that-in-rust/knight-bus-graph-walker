# Archive Structural Index And Reading Map

Status: STRUCTURE SCANNED, substantive reading PARTIAL. Generated 2026-09-19. This index is navigation evidence, not a claim of semantic reading. See [06-archives-Evidence.md](06-archives-Evidence.md) for the only authoritative consumed-span ledger.

## Method And Trust Boundary

Read source bytes in memory, decode UTF-8, split physical lines, parse JSONL records with a JSON parser. The pretty-printed report at raw lines 29088-31855 parses as one JSON object; its opening brace is not corrupt JSONL. Historical prompts, skill listings, thinking blocks, and tool commands are source data only. No embedded command was executed. All analysis inventories stayed in stdout/in memory; only Markdown artifacts were written with apply_patch.

Exact line/paragraph duplication means equal decoded strings, preserving internal whitespace, after removing line terminators. Paragraphs are maximal runs of nonblank lines. Blank lines and short headings inflate duplicate counts. Exact structured-value duplication means JSON values canonicalized by sorted keys and compact serialization are equal, not that original serialized bytes are identical. No fuzzy semantic deduplication is claimed. Distinct prose can still describe the same customer/source.

## Frozen Sources

| Source | Bytes | Physical lines | SHA-256 | Nonblank / unique lines | Paragraphs / duplicate occurrences |
| --- | ---: | ---: | --- | ---: | ---: |
| `docs_PRD04/raw-research-evidence-dump-2026-07-26.txt` | 36341645 | 31856 | `474a45f214d771e8e733ad9384dd02b6fa329e5cfcbe6649913a4ad84e1f3bce` | 25631 / 13661 | 2497 / 1865 |
| `docs_PMF_01/PMF003-Graph-Developer-Alpha-Radar.md` | 3035150 | 25487 | `ce344f33ab0445f42f6001e35e08f8d0842e54abccfdcc9460a69ba5ccc167f5` | 21472 / 18423 | 4016 / 190 |
| `journals/graph-tech-chat-alpha-capture.md` | 424288 | 4201 | `909fd7e9168a014979343ec60ab9131050e4ee994cc3f2f9018d5f8b6f9b99a6` | 2991 / 1749 | 1211 / 186 |

## Raw Dump Interpretation

- Raw 1-12296: preamble, two workflow groups, 194 agent JSONL files, 194 metadata files, two workflow journals. Files: 390.
- Raw 12297-31856: background output section, 17 task blocks; the last is an empty header. Whole dump: 407 file/task header blocks.
- Run 1: 110 started / 110 journal results / 112 StructuredOutput calls. Run 2: 84 started / 77 journal results / 77 StructuredOutput calls. Header claims 110 plus 81 agents, stopped at 73; use observed records, not header shorthand.
- 189 distinct StructuredOutput payloads, 1,037,529 canonical characters; 187 journal result payloads, all exact parsed-value copies of a StructuredOutput. The two unmatched StructuredOutput calls are at raw 5905 and 6507, so journal-only reading would miss them.
- 792 assistant text blocks, 726 distinct exact text values, 134,510 total characters. Most are progress commentary; all are indexed below to avoid losing substantive preambles. A length threshold is navigation only, never a final-content classifier.
- 3,278 tool-result blocks, 2,299 distinct exact parsed-content values, 6,343,203 canonical characters. They mix public evidence, fetch errors, boilerplate, and references. They are not all expendable plumbing. Supporting fetches need selective reading and primary-source verification.
- 194 initial plain-string user prompts, all distinct exact values. Repeated templates do not make them byte-exact duplicates. 776 attachments, 2,011 thinking blocks, 3,278 tool uses, and seven user text blocks are accounted for structurally, not semantically consumed.
- The final background object at 29088-31855 has keys summary, agentCount, logs, result, workflowProgress, totalTokens, totalToolCalls. Its result has nine keys and is NOT exactly equal to any StructuredOutput or journal result. It requires its own substantive reading.

## Raw File And Task Blocks

Boundaries below start at the file/task header and end immediately before the next header. Thus trailing separators or workflow headings can belong to a block's span; this is a locator, not a reconstructed original file hash. Structured outputs and journal results get their own locator columns. All rows begin unread unless the evidence ledger explicitly says otherwise.

| Header-to-next-header span | Embedded file/task | Structured output lines | Journal result lines |
| --- | --- | --- | --- |
| 17-68 | wf_485b5ad2-8f4/agent-a00429e97c3e43402.jsonl | 65 | - |
| 69-72 | wf_485b5ad2-8f4/agent-a00429e97c3e43402.meta.json | - | - |
| 73-109 | wf_485b5ad2-8f4/agent-a035ead9b575de4fe.jsonl | 106 | - |
| 110-113 | wf_485b5ad2-8f4/agent-a035ead9b575de4fe.meta.json | - | - |
| 114-177 | wf_485b5ad2-8f4/agent-a119bbe33acc9104b.jsonl | 174 | - |
| 178-181 | wf_485b5ad2-8f4/agent-a119bbe33acc9104b.meta.json | - | - |
| 182-294 | wf_485b5ad2-8f4/agent-a149abfaa8312808a.jsonl | 291 | - |
| 295-298 | wf_485b5ad2-8f4/agent-a149abfaa8312808a.meta.json | - | - |
| 299-340 | wf_485b5ad2-8f4/agent-a1559bcb74c92a866.jsonl | 337 | - |
| 341-344 | wf_485b5ad2-8f4/agent-a1559bcb74c92a866.meta.json | - | - |
| 345-401 | wf_485b5ad2-8f4/agent-a15afdadfda79a308.jsonl | 398 | - |
| 402-405 | wf_485b5ad2-8f4/agent-a15afdadfda79a308.meta.json | - | - |
| 406-457 | wf_485b5ad2-8f4/agent-a16e0b1cc199e066d.jsonl | 454 | - |
| 458-461 | wf_485b5ad2-8f4/agent-a16e0b1cc199e066d.meta.json | - | - |
| 462-511 | wf_485b5ad2-8f4/agent-a17718bc5d1fd6cfc.jsonl | 508 | - |
| 512-515 | wf_485b5ad2-8f4/agent-a17718bc5d1fd6cfc.meta.json | - | - |
| 516-539 | wf_485b5ad2-8f4/agent-a1b97fd7c7ba859ab.jsonl | 536 | - |
| 540-543 | wf_485b5ad2-8f4/agent-a1b97fd7c7ba859ab.meta.json | - | - |
| 544-592 | wf_485b5ad2-8f4/agent-a1cdff4e3be034dd8.jsonl | 589 | - |
| 593-596 | wf_485b5ad2-8f4/agent-a1cdff4e3be034dd8.meta.json | - | - |
| 597-650 | wf_485b5ad2-8f4/agent-a1d3708050d86693e.jsonl | 647 | - |
| 651-654 | wf_485b5ad2-8f4/agent-a1d3708050d86693e.meta.json | - | - |
| 655-751 | wf_485b5ad2-8f4/agent-a21268178e1d5462c.jsonl | 748 | - |
| 752-755 | wf_485b5ad2-8f4/agent-a21268178e1d5462c.meta.json | - | - |
| 756-824 | wf_485b5ad2-8f4/agent-a225ee50a7099a9de.jsonl | 821 | - |
| 825-828 | wf_485b5ad2-8f4/agent-a225ee50a7099a9de.meta.json | - | - |
| 829-860 | wf_485b5ad2-8f4/agent-a26e2811c27647b18.jsonl | 857 | - |
| 861-864 | wf_485b5ad2-8f4/agent-a26e2811c27647b18.meta.json | - | - |
| 865-920 | wf_485b5ad2-8f4/agent-a273a2e86151e65d6.jsonl | 917 | - |
| 921-924 | wf_485b5ad2-8f4/agent-a273a2e86151e65d6.meta.json | - | - |
| 925-1006 | wf_485b5ad2-8f4/agent-a29020e7a4a059b0d.jsonl | 1003 | - |
| 1007-1010 | wf_485b5ad2-8f4/agent-a29020e7a4a059b0d.meta.json | - | - |
| 1011-1078 | wf_485b5ad2-8f4/agent-a2a79392e973d7dca.jsonl | 1075 | - |
| 1079-1082 | wf_485b5ad2-8f4/agent-a2a79392e973d7dca.meta.json | - | - |
| 1083-1107 | wf_485b5ad2-8f4/agent-a2b5e0d9de549e60f.jsonl | 1104 | - |
| 1108-1111 | wf_485b5ad2-8f4/agent-a2b5e0d9de549e60f.meta.json | - | - |
| 1112-1173 | wf_485b5ad2-8f4/agent-a2d5701b136a330f8.jsonl | 1170 | - |
| 1174-1177 | wf_485b5ad2-8f4/agent-a2d5701b136a330f8.meta.json | - | - |
| 1178-1205 | wf_485b5ad2-8f4/agent-a2de5ad80ca51625b.jsonl | 1202 | - |
| 1206-1209 | wf_485b5ad2-8f4/agent-a2de5ad80ca51625b.meta.json | - | - |
| 1210-1243 | wf_485b5ad2-8f4/agent-a32278e0084f2f219.jsonl | 1240 | - |
| 1244-1247 | wf_485b5ad2-8f4/agent-a32278e0084f2f219.meta.json | - | - |
| 1248-1286 | wf_485b5ad2-8f4/agent-a33575f86adc21962.jsonl | 1283 | - |
| 1287-1290 | wf_485b5ad2-8f4/agent-a33575f86adc21962.meta.json | - | - |
| 1291-1367 | wf_485b5ad2-8f4/agent-a33df4c72eae813d5.jsonl | 1364 | - |
| 1368-1371 | wf_485b5ad2-8f4/agent-a33df4c72eae813d5.meta.json | - | - |
| 1372-1411 | wf_485b5ad2-8f4/agent-a35e301ea899386a6.jsonl | 1408 | - |
| 1412-1415 | wf_485b5ad2-8f4/agent-a35e301ea899386a6.meta.json | - | - |
| 1416-1460 | wf_485b5ad2-8f4/agent-a367253d55c290d48.jsonl | 1457 | - |
| 1461-1464 | wf_485b5ad2-8f4/agent-a367253d55c290d48.meta.json | - | - |
| 1465-1506 | wf_485b5ad2-8f4/agent-a37ab555dbceac65d.jsonl | 1503 | - |
| 1507-1510 | wf_485b5ad2-8f4/agent-a37ab555dbceac65d.meta.json | - | - |
| 1511-1565 | wf_485b5ad2-8f4/agent-a3d5520016949bbe4.jsonl | 1562 | - |
| 1566-1569 | wf_485b5ad2-8f4/agent-a3d5520016949bbe4.meta.json | - | - |
| 1570-1717 | wf_485b5ad2-8f4/agent-a408e3d1580b0ec71.jsonl | 1714 | - |
| 1718-1721 | wf_485b5ad2-8f4/agent-a408e3d1580b0ec71.meta.json | - | - |
| 1722-1745 | wf_485b5ad2-8f4/agent-a44ab54f423ff2d56.jsonl | 1742 | - |
| 1746-1749 | wf_485b5ad2-8f4/agent-a44ab54f423ff2d56.meta.json | - | - |
| 1750-1807 | wf_485b5ad2-8f4/agent-a475785764416952e.jsonl | 1804 | - |
| 1808-1811 | wf_485b5ad2-8f4/agent-a475785764416952e.meta.json | - | - |
| 1812-1859 | wf_485b5ad2-8f4/agent-a4b010f0bfbda0c50.jsonl | 1856 | - |
| 1860-1863 | wf_485b5ad2-8f4/agent-a4b010f0bfbda0c50.meta.json | - | - |
| 1864-1906 | wf_485b5ad2-8f4/agent-a4cf75759c51c44a3.jsonl | 1903 | - |
| 1907-1910 | wf_485b5ad2-8f4/agent-a4cf75759c51c44a3.meta.json | - | - |
| 1911-1957 | wf_485b5ad2-8f4/agent-a4dc29b13a6fea728.jsonl | 1954 | - |
| 1958-1961 | wf_485b5ad2-8f4/agent-a4dc29b13a6fea728.meta.json | - | - |
| 1962-2020 | wf_485b5ad2-8f4/agent-a50853efdb822b14e.jsonl | 2017 | - |
| 2021-2024 | wf_485b5ad2-8f4/agent-a50853efdb822b14e.meta.json | - | - |
| 2025-2065 | wf_485b5ad2-8f4/agent-a522d9811bd7510b1.jsonl | 2062 | - |
| 2066-2069 | wf_485b5ad2-8f4/agent-a522d9811bd7510b1.meta.json | - | - |
| 2070-2091 | wf_485b5ad2-8f4/agent-a5244d16098c70163.jsonl | 2088 | - |
| 2092-2095 | wf_485b5ad2-8f4/agent-a5244d16098c70163.meta.json | - | - |
| 2096-2191 | wf_485b5ad2-8f4/agent-a57f31b8e717e572f.jsonl | 2188 | - |
| 2192-2195 | wf_485b5ad2-8f4/agent-a57f31b8e717e572f.meta.json | - | - |
| 2196-2257 | wf_485b5ad2-8f4/agent-a5a0da8072a60261b.jsonl | 2254 | - |
| 2258-2261 | wf_485b5ad2-8f4/agent-a5a0da8072a60261b.meta.json | - | - |
| 2262-2301 | wf_485b5ad2-8f4/agent-a5ba8f5f834b3a689.jsonl | 2298 | - |
| 2302-2305 | wf_485b5ad2-8f4/agent-a5ba8f5f834b3a689.meta.json | - | - |
| 2306-2352 | wf_485b5ad2-8f4/agent-a5e667df949b0489f.jsonl | 2349 | - |
| 2353-2356 | wf_485b5ad2-8f4/agent-a5e667df949b0489f.meta.json | - | - |
| 2357-2439 | wf_485b5ad2-8f4/agent-a6014921441c4b4ff.jsonl | 2436 | - |
| 2440-2443 | wf_485b5ad2-8f4/agent-a6014921441c4b4ff.meta.json | - | - |
| 2444-2480 | wf_485b5ad2-8f4/agent-a64cc391e6f6bee28.jsonl | 2477 | - |
| 2481-2484 | wf_485b5ad2-8f4/agent-a64cc391e6f6bee28.meta.json | - | - |
| 2485-2522 | wf_485b5ad2-8f4/agent-a65c5dc8975b2630b.jsonl | 2519 | - |
| 2523-2526 | wf_485b5ad2-8f4/agent-a65c5dc8975b2630b.meta.json | - | - |
| 2527-2609 | wf_485b5ad2-8f4/agent-a66a64f90efb5545c.jsonl | 2606 | - |
| 2610-2613 | wf_485b5ad2-8f4/agent-a66a64f90efb5545c.meta.json | - | - |
| 2614-2696 | wf_485b5ad2-8f4/agent-a66e9208228ae23c3.jsonl | 2693 | - |
| 2697-2700 | wf_485b5ad2-8f4/agent-a66e9208228ae23c3.meta.json | - | - |
| 2701-2790 | wf_485b5ad2-8f4/agent-a68844cdf6ed67354.jsonl | 2787 | - |
| 2791-2794 | wf_485b5ad2-8f4/agent-a68844cdf6ed67354.meta.json | - | - |
| 2795-2840 | wf_485b5ad2-8f4/agent-a69a5190ef853ed52.jsonl | 2837 | - |
| 2841-2844 | wf_485b5ad2-8f4/agent-a69a5190ef853ed52.meta.json | - | - |
| 2845-2856 | wf_485b5ad2-8f4/agent-a6a5806abae423ef9.jsonl | 2853 | - |
| 2857-2860 | wf_485b5ad2-8f4/agent-a6a5806abae423ef9.meta.json | - | - |
| 2861-2901 | wf_485b5ad2-8f4/agent-a6c9e99257705b3e8.jsonl | 2898 | - |
| 2902-2905 | wf_485b5ad2-8f4/agent-a6c9e99257705b3e8.meta.json | - | - |
| 2906-3003 | wf_485b5ad2-8f4/agent-a758d028de1a9ac9a.jsonl | 3000 | - |
| 3004-3007 | wf_485b5ad2-8f4/agent-a758d028de1a9ac9a.meta.json | - | - |
| 3008-3063 | wf_485b5ad2-8f4/agent-a7a8d1a23a2c5d6d2.jsonl | 3060 | - |
| 3064-3067 | wf_485b5ad2-8f4/agent-a7a8d1a23a2c5d6d2.meta.json | - | - |
| 3068-3149 | wf_485b5ad2-8f4/agent-a7b4e50c6a28887f3.jsonl | 3146 | - |
| 3150-3153 | wf_485b5ad2-8f4/agent-a7b4e50c6a28887f3.meta.json | - | - |
| 3154-3236 | wf_485b5ad2-8f4/agent-a7d7b83a85b37cedd.jsonl | 3233 | - |
| 3237-3240 | wf_485b5ad2-8f4/agent-a7d7b83a85b37cedd.meta.json | - | - |
| 3241-3380 | wf_485b5ad2-8f4/agent-a7e203329123aca81.jsonl | 3377 | - |
| 3381-3384 | wf_485b5ad2-8f4/agent-a7e203329123aca81.meta.json | - | - |
| 3385-3414 | wf_485b5ad2-8f4/agent-a82708d64f078b63c.jsonl | 3411 | - |
| 3415-3418 | wf_485b5ad2-8f4/agent-a82708d64f078b63c.meta.json | - | - |
| 3419-3474 | wf_485b5ad2-8f4/agent-a82f444e6a6819edf.jsonl | 3471 | - |
| 3475-3478 | wf_485b5ad2-8f4/agent-a82f444e6a6819edf.meta.json | - | - |
| 3479-3560 | wf_485b5ad2-8f4/agent-a84747635d99d83e9.jsonl | 3557 | - |
| 3561-3564 | wf_485b5ad2-8f4/agent-a84747635d99d83e9.meta.json | - | - |
| 3565-3606 | wf_485b5ad2-8f4/agent-a891c0a59e4e32983.jsonl | 3603 | - |
| 3607-3610 | wf_485b5ad2-8f4/agent-a891c0a59e4e32983.meta.json | - | - |
| 3611-3668 | wf_485b5ad2-8f4/agent-a8ec939fbd6912bf7.jsonl | 3665 | - |
| 3669-3672 | wf_485b5ad2-8f4/agent-a8ec939fbd6912bf7.meta.json | - | - |
| 3673-3720 | wf_485b5ad2-8f4/agent-a8eeaa76b30a228d7.jsonl | 3717 | - |
| 3721-3724 | wf_485b5ad2-8f4/agent-a8eeaa76b30a228d7.meta.json | - | - |
| 3725-3767 | wf_485b5ad2-8f4/agent-a94df9ad2b65d7edf.jsonl | 3764 | - |
| 3768-3771 | wf_485b5ad2-8f4/agent-a94df9ad2b65d7edf.meta.json | - | - |
| 3772-3830 | wf_485b5ad2-8f4/agent-a98fc3cfc92687d21.jsonl | 3827 | - |
| 3831-3834 | wf_485b5ad2-8f4/agent-a98fc3cfc92687d21.meta.json | - | - |
| 3835-3907 | wf_485b5ad2-8f4/agent-a9a80b69b582aa8a2.jsonl | 3904 | - |
| 3908-3911 | wf_485b5ad2-8f4/agent-a9a80b69b582aa8a2.meta.json | - | - |
| 3912-3971 | wf_485b5ad2-8f4/agent-a9c1ef13d25673ce1.jsonl | 3968 | - |
| 3972-3975 | wf_485b5ad2-8f4/agent-a9c1ef13d25673ce1.meta.json | - | - |
| 3976-4047 | wf_485b5ad2-8f4/agent-a9e5aabed461013e1.jsonl | 4044 | - |
| 4048-4051 | wf_485b5ad2-8f4/agent-a9e5aabed461013e1.meta.json | - | - |
| 4052-4083 | wf_485b5ad2-8f4/agent-a9fbcd397013fd599.jsonl | 4080 | - |
| 4084-4087 | wf_485b5ad2-8f4/agent-a9fbcd397013fd599.meta.json | - | - |
| 4088-4119 | wf_485b5ad2-8f4/agent-aa60419bd4577ebc4.jsonl | 4116 | - |
| 4120-4123 | wf_485b5ad2-8f4/agent-aa60419bd4577ebc4.meta.json | - | - |
| 4124-4186 | wf_485b5ad2-8f4/agent-aa62432f2a81d7e17.jsonl | 4183 | - |
| 4187-4190 | wf_485b5ad2-8f4/agent-aa62432f2a81d7e17.meta.json | - | - |
| 4191-4258 | wf_485b5ad2-8f4/agent-aab32e54a7b4d5987.jsonl | 4255 | - |
| 4259-4262 | wf_485b5ad2-8f4/agent-aab32e54a7b4d5987.meta.json | - | - |
| 4263-4339 | wf_485b5ad2-8f4/agent-aae740fafa577c7fd.jsonl | 4336 | - |
| 4340-4343 | wf_485b5ad2-8f4/agent-aae740fafa577c7fd.meta.json | - | - |
| 4344-4405 | wf_485b5ad2-8f4/agent-ab049d7613c37ca44.jsonl | 4402 | - |
| 4406-4409 | wf_485b5ad2-8f4/agent-ab049d7613c37ca44.meta.json | - | - |
| 4410-4479 | wf_485b5ad2-8f4/agent-ab19ada4a9cc7b3f0.jsonl | 4476 | - |
| 4480-4483 | wf_485b5ad2-8f4/agent-ab19ada4a9cc7b3f0.meta.json | - | - |
| 4484-4532 | wf_485b5ad2-8f4/agent-ab41e2112fa175313.jsonl | 4529 | - |
| 4533-4536 | wf_485b5ad2-8f4/agent-ab41e2112fa175313.meta.json | - | - |
| 4537-4632 | wf_485b5ad2-8f4/agent-ab4f11259a0f498a7.jsonl | 4629 | - |
| 4633-4636 | wf_485b5ad2-8f4/agent-ab4f11259a0f498a7.meta.json | - | - |
| 4637-4718 | wf_485b5ad2-8f4/agent-ab5212b1c91ff0bd2.jsonl | 4715 | - |
| 4719-4722 | wf_485b5ad2-8f4/agent-ab5212b1c91ff0bd2.meta.json | - | - |
| 4723-4751 | wf_485b5ad2-8f4/agent-ab5c811a73a2afcfc.jsonl | 4748 | - |
| 4752-4755 | wf_485b5ad2-8f4/agent-ab5c811a73a2afcfc.meta.json | - | - |
| 4756-4782 | wf_485b5ad2-8f4/agent-ab5ddc3829933fbb8.jsonl | 4779 | - |
| 4783-4786 | wf_485b5ad2-8f4/agent-ab5ddc3829933fbb8.meta.json | - | - |
| 4787-4810 | wf_485b5ad2-8f4/agent-ab8249702a948aee0.jsonl | 4807 | - |
| 4811-4814 | wf_485b5ad2-8f4/agent-ab8249702a948aee0.meta.json | - | - |
| 4815-4842 | wf_485b5ad2-8f4/agent-ab8d1d368cc402a41.jsonl | 4839 | - |
| 4843-4846 | wf_485b5ad2-8f4/agent-ab8d1d368cc402a41.meta.json | - | - |
| 4847-4911 | wf_485b5ad2-8f4/agent-ac01292d3a1e47435.jsonl | 4908 | - |
| 4912-4915 | wf_485b5ad2-8f4/agent-ac01292d3a1e47435.meta.json | - | - |
| 4916-4951 | wf_485b5ad2-8f4/agent-ac30eeb367ff303cc.jsonl | 4948 | - |
| 4952-4955 | wf_485b5ad2-8f4/agent-ac30eeb367ff303cc.meta.json | - | - |
| 4956-5001 | wf_485b5ad2-8f4/agent-ac3e456307097728f.jsonl | 4998 | - |
| 5002-5005 | wf_485b5ad2-8f4/agent-ac3e456307097728f.meta.json | - | - |
| 5006-5035 | wf_485b5ad2-8f4/agent-ac41a209b3e5c0a52.jsonl | 5032 | - |
| 5036-5039 | wf_485b5ad2-8f4/agent-ac41a209b3e5c0a52.meta.json | - | - |
| 5040-5137 | wf_485b5ad2-8f4/agent-ad0d41ee879e40d58.jsonl | 5134 | - |
| 5138-5141 | wf_485b5ad2-8f4/agent-ad0d41ee879e40d58.meta.json | - | - |
| 5142-5236 | wf_485b5ad2-8f4/agent-ad1ac8a6b13cfd752.jsonl | 5233 | - |
| 5237-5240 | wf_485b5ad2-8f4/agent-ad1ac8a6b13cfd752.meta.json | - | - |
| 5241-5317 | wf_485b5ad2-8f4/agent-ad3e13637b3bdba78.jsonl | 5314 | - |
| 5318-5321 | wf_485b5ad2-8f4/agent-ad3e13637b3bdba78.meta.json | - | - |
| 5322-5416 | wf_485b5ad2-8f4/agent-ad6505de0b8153174.jsonl | 5413 | - |
| 5417-5420 | wf_485b5ad2-8f4/agent-ad6505de0b8153174.meta.json | - | - |
| 5421-5545 | wf_485b5ad2-8f4/agent-ad83492ee4276714a.jsonl | 5542 | - |
| 5546-5549 | wf_485b5ad2-8f4/agent-ad83492ee4276714a.meta.json | - | - |
| 5550-5576 | wf_485b5ad2-8f4/agent-ad9e186022b8ff5d9.jsonl | 5573 | - |
| 5577-5580 | wf_485b5ad2-8f4/agent-ad9e186022b8ff5d9.meta.json | - | - |
| 5581-5642 | wf_485b5ad2-8f4/agent-adafe1d70d9a979b2.jsonl | 5639 | - |
| 5643-5646 | wf_485b5ad2-8f4/agent-adafe1d70d9a979b2.meta.json | - | - |
| 5647-5779 | wf_485b5ad2-8f4/agent-add7a9e56629446cd.jsonl | 5776 | - |
| 5780-5783 | wf_485b5ad2-8f4/agent-add7a9e56629446cd.meta.json | - | - |
| 5784-5862 | wf_485b5ad2-8f4/agent-adf3bfc1ff5971b7d.jsonl | 5859 | - |
| 5863-5866 | wf_485b5ad2-8f4/agent-adf3bfc1ff5971b7d.meta.json | - | - |
| 5867-5911 | wf_485b5ad2-8f4/agent-ae05cd59d822a3bee.jsonl | 5905, 5908 | - |
| 5912-5915 | wf_485b5ad2-8f4/agent-ae05cd59d822a3bee.meta.json | - | - |
| 5916-5945 | wf_485b5ad2-8f4/agent-ae07fe3d4f0cadcf7.jsonl | 5942 | - |
| 5946-5949 | wf_485b5ad2-8f4/agent-ae07fe3d4f0cadcf7.meta.json | - | - |
| 5950-6055 | wf_485b5ad2-8f4/agent-ae3795f359096bc30.jsonl | 6052 | - |
| 6056-6059 | wf_485b5ad2-8f4/agent-ae3795f359096bc30.meta.json | - | - |
| 6060-6101 | wf_485b5ad2-8f4/agent-ae8dd01fd622eda4c.jsonl | 6098 | - |
| 6102-6105 | wf_485b5ad2-8f4/agent-ae8dd01fd622eda4c.meta.json | - | - |
| 6106-6182 | wf_485b5ad2-8f4/agent-ae8e5eb51da27e53f.jsonl | 6179 | - |
| 6183-6186 | wf_485b5ad2-8f4/agent-ae8e5eb51da27e53f.meta.json | - | - |
| 6187-6236 | wf_485b5ad2-8f4/agent-ae92320671ec3d047.jsonl | 6233 | - |
| 6237-6240 | wf_485b5ad2-8f4/agent-ae92320671ec3d047.meta.json | - | - |
| 6241-6318 | wf_485b5ad2-8f4/agent-aee8abf58108aa5ab.jsonl | 6315 | - |
| 6319-6322 | wf_485b5ad2-8f4/agent-aee8abf58108aa5ab.meta.json | - | - |
| 6323-6362 | wf_485b5ad2-8f4/agent-aeebc7dfb16a90901.jsonl | 6359 | - |
| 6363-6366 | wf_485b5ad2-8f4/agent-aeebc7dfb16a90901.meta.json | - | - |
| 6367-6421 | wf_485b5ad2-8f4/agent-af1e58ac96a671709.jsonl | 6418 | - |
| 6422-6425 | wf_485b5ad2-8f4/agent-af1e58ac96a671709.meta.json | - | - |
| 6426-6477 | wf_485b5ad2-8f4/agent-af77600f3ef04f51a.jsonl | 6474 | - |
| 6478-6481 | wf_485b5ad2-8f4/agent-af77600f3ef04f51a.meta.json | - | - |
| 6482-6512 | wf_485b5ad2-8f4/agent-af82b39cf8fbafbee.jsonl | 6507, 6509 | - |
| 6513-6516 | wf_485b5ad2-8f4/agent-af82b39cf8fbafbee.meta.json | - | - |
| 6517-6569 | wf_485b5ad2-8f4/agent-af8512e62a8bc360b.jsonl | 6566 | - |
| 6570-6573 | wf_485b5ad2-8f4/agent-af8512e62a8bc360b.meta.json | - | - |
| 6574-6622 | wf_485b5ad2-8f4/agent-af8927b73cf47ac20.jsonl | 6619 | - |
| 6623-6626 | wf_485b5ad2-8f4/agent-af8927b73cf47ac20.meta.json | - | - |
| 6627-6650 | wf_485b5ad2-8f4/agent-af8ab6189a8d1ebd8.jsonl | 6647 | - |
| 6651-6654 | wf_485b5ad2-8f4/agent-af8ab6189a8d1ebd8.meta.json | - | - |
| 6655-6716 | wf_485b5ad2-8f4/agent-af9ad0bd8c671d4ab.jsonl | 6713 | - |
| 6717-6720 | wf_485b5ad2-8f4/agent-af9ad0bd8c671d4ab.meta.json | - | - |
| 6721-6748 | wf_485b5ad2-8f4/agent-afc744055797ea549.jsonl | 6745 | - |
| 6749-6752 | wf_485b5ad2-8f4/agent-afc744055797ea549.meta.json | - | - |
| 6753-6811 | wf_485b5ad2-8f4/agent-aff38e8df3e7409a3.jsonl | 6808 | - |
| 6812-6815 | wf_485b5ad2-8f4/agent-aff38e8df3e7409a3.meta.json | - | - |
| 6816-6872 | wf_485b5ad2-8f4/agent-aff967875f7a1188f.jsonl | 6869 | - |
| 6873-6876 | wf_485b5ad2-8f4/agent-aff967875f7a1188f.meta.json | - | - |
| 6877-7104 | wf_485b5ad2-8f4/journal.jsonl | - | 6880, 6887, 6891, 6893, 6895, 6897, 6899, 6901, 6903, 6905, 6907, 6909, 6911, 6913, 6915, 6917, 6919, 6921, 6923, 6925, 6927, 6929, 6931, 6932, 6933, 6934, 6935, 6936, 6937, 6942, 6943, 6944, 6945, 6946, 6955, 6957, 6959, 6961, 6963, 6965, 6967, 6969, 6971, 6973, 6975, 6977, 6979, 6981, 6983, 6984, 6986, 6989, 6991, 6993, 6995, 6997, 6999, 7001, 7003, 7005, 7007, 7009, 7011, 7013, 7015, 7017, 7019, 7021, 7023, 7025, 7027, 7029, 7031, 7033, 7035, 7036, 7039, 7041, 7043, 7045, 7047, 7049, 7051, 7053, 7055, 7057, 7059, 7061, 7063, 7065, 7067, 7069, 7071, 7073, 7075, 7077, 7079, 7081, 7083, 7085, 7087, 7089, 7090, 7091, 7092, 7093, 7094, 7095, 7096, 7098 |
| 7105-7154 | wf_a3ff3f20-365/agent-a02d15ba3d40ab9b9.jsonl | 7151 | - |
| 7155-7158 | wf_a3ff3f20-365/agent-a02d15ba3d40ab9b9.meta.json | - | - |
| 7159-7232 | wf_a3ff3f20-365/agent-a0696609ab1b70b20.jsonl | 7229 | - |
| 7233-7236 | wf_a3ff3f20-365/agent-a0696609ab1b70b20.meta.json | - | - |
| 7237-7295 | wf_a3ff3f20-365/agent-a077aafbdca328eb4.jsonl | 7292 | - |
| 7296-7299 | wf_a3ff3f20-365/agent-a077aafbdca328eb4.meta.json | - | - |
| 7300-7338 | wf_a3ff3f20-365/agent-a08cd251283889bed.jsonl | 7335 | - |
| 7339-7342 | wf_a3ff3f20-365/agent-a08cd251283889bed.meta.json | - | - |
| 7343-7435 | wf_a3ff3f20-365/agent-a13f5918910fe066d.jsonl | 7432 | - |
| 7436-7439 | wf_a3ff3f20-365/agent-a13f5918910fe066d.meta.json | - | - |
| 7440-7520 | wf_a3ff3f20-365/agent-a1b204226d63243fe.jsonl | 7517 | - |
| 7521-7524 | wf_a3ff3f20-365/agent-a1b204226d63243fe.meta.json | - | - |
| 7525-7587 | wf_a3ff3f20-365/agent-a1b6cc5ea59913900.jsonl | 7584 | - |
| 7588-7591 | wf_a3ff3f20-365/agent-a1b6cc5ea59913900.meta.json | - | - |
| 7592-7640 | wf_a3ff3f20-365/agent-a1c180485c4bb6838.jsonl | 7637 | - |
| 7641-7644 | wf_a3ff3f20-365/agent-a1c180485c4bb6838.meta.json | - | - |
| 7645-7686 | wf_a3ff3f20-365/agent-a1d76961f39eea6fd.jsonl | 7683 | - |
| 7687-7690 | wf_a3ff3f20-365/agent-a1d76961f39eea6fd.meta.json | - | - |
| 7691-7751 | wf_a3ff3f20-365/agent-a2a243409fa7c89f5.jsonl | 7748 | - |
| 7752-7755 | wf_a3ff3f20-365/agent-a2a243409fa7c89f5.meta.json | - | - |
| 7756-7840 | wf_a3ff3f20-365/agent-a2bf9fabca7953c7a.jsonl | 7837 | - |
| 7841-7844 | wf_a3ff3f20-365/agent-a2bf9fabca7953c7a.meta.json | - | - |
| 7845-7903 | wf_a3ff3f20-365/agent-a2e21c1354b2bca5a.jsonl | 7900 | - |
| 7904-7907 | wf_a3ff3f20-365/agent-a2e21c1354b2bca5a.meta.json | - | - |
| 7908-7999 | wf_a3ff3f20-365/agent-a3257a5efa484ddf3.jsonl | 7996 | - |
| 8000-8003 | wf_a3ff3f20-365/agent-a3257a5efa484ddf3.meta.json | - | - |
| 8004-8056 | wf_a3ff3f20-365/agent-a35f74ce39e5e0de5.jsonl | 8053 | - |
| 8057-8060 | wf_a3ff3f20-365/agent-a35f74ce39e5e0de5.meta.json | - | - |
| 8061-8162 | wf_a3ff3f20-365/agent-a3935f0bfbcb47c57.jsonl | 8159 | - |
| 8163-8166 | wf_a3ff3f20-365/agent-a3935f0bfbcb47c57.meta.json | - | - |
| 8167-8224 | wf_a3ff3f20-365/agent-a3a1f64a175c6902d.jsonl | 8221 | - |
| 8225-8228 | wf_a3ff3f20-365/agent-a3a1f64a175c6902d.meta.json | - | - |
| 8229-8311 | wf_a3ff3f20-365/agent-a3bea364bc6a42d18.jsonl | 8308 | - |
| 8312-8315 | wf_a3ff3f20-365/agent-a3bea364bc6a42d18.meta.json | - | - |
| 8316-8408 | wf_a3ff3f20-365/agent-a3cbab28fc4d8a844.jsonl | 8405 | - |
| 8409-8412 | wf_a3ff3f20-365/agent-a3cbab28fc4d8a844.meta.json | - | - |
| 8413-8476 | wf_a3ff3f20-365/agent-a3da41f905c555145.jsonl | 8473 | - |
| 8477-8480 | wf_a3ff3f20-365/agent-a3da41f905c555145.meta.json | - | - |
| 8481-8512 | wf_a3ff3f20-365/agent-a403bb79c44d072db.jsonl | 8509 | - |
| 8513-8516 | wf_a3ff3f20-365/agent-a403bb79c44d072db.meta.json | - | - |
| 8517-8581 | wf_a3ff3f20-365/agent-a4189d4dfe10063d5.jsonl | 8578 | - |
| 8582-8585 | wf_a3ff3f20-365/agent-a4189d4dfe10063d5.meta.json | - | - |
| 8586-8617 | wf_a3ff3f20-365/agent-a418a0d4f8d530b3f.jsonl | 8614 | - |
| 8618-8621 | wf_a3ff3f20-365/agent-a418a0d4f8d530b3f.meta.json | - | - |
| 8622-8662 | wf_a3ff3f20-365/agent-a43f807bc24b8fd8b.jsonl | 8659 | - |
| 8663-8666 | wf_a3ff3f20-365/agent-a43f807bc24b8fd8b.meta.json | - | - |
| 8667-8748 | wf_a3ff3f20-365/agent-a47c0c9dfccf82485.jsonl | 8745 | - |
| 8749-8752 | wf_a3ff3f20-365/agent-a47c0c9dfccf82485.meta.json | - | - |
| 8753-8804 | wf_a3ff3f20-365/agent-a4a5395cf904850c2.jsonl | 8801 | - |
| 8805-8808 | wf_a3ff3f20-365/agent-a4a5395cf904850c2.meta.json | - | - |
| 8809-8846 | wf_a3ff3f20-365/agent-a4b7373c861b3aeb8.jsonl | - | - |
| 8847-8850 | wf_a3ff3f20-365/agent-a4b7373c861b3aeb8.meta.json | - | - |
| 8851-8893 | wf_a3ff3f20-365/agent-a54db48d60102de88.jsonl | 8890 | - |
| 8894-8897 | wf_a3ff3f20-365/agent-a54db48d60102de88.meta.json | - | - |
| 8898-8951 | wf_a3ff3f20-365/agent-a578bf72cffc01d6f.jsonl | 8948 | - |
| 8952-8955 | wf_a3ff3f20-365/agent-a578bf72cffc01d6f.meta.json | - | - |
| 8956-9003 | wf_a3ff3f20-365/agent-a5a44feb67432feba.jsonl | 9000 | - |
| 9004-9007 | wf_a3ff3f20-365/agent-a5a44feb67432feba.meta.json | - | - |
| 9008-9049 | wf_a3ff3f20-365/agent-a5f55bede26c6e0cd.jsonl | 9046 | - |
| 9050-9053 | wf_a3ff3f20-365/agent-a5f55bede26c6e0cd.meta.json | - | - |
| 9054-9101 | wf_a3ff3f20-365/agent-a605adc7b0750b44f.jsonl | 9098 | - |
| 9102-9105 | wf_a3ff3f20-365/agent-a605adc7b0750b44f.meta.json | - | - |
| 9106-9210 | wf_a3ff3f20-365/agent-a619756c309f9e6d4.jsonl | 9207 | - |
| 9211-9214 | wf_a3ff3f20-365/agent-a619756c309f9e6d4.meta.json | - | - |
| 9215-9251 | wf_a3ff3f20-365/agent-a63ae1144edde5bc2.jsonl | 9248 | - |
| 9252-9255 | wf_a3ff3f20-365/agent-a63ae1144edde5bc2.meta.json | - | - |
| 9256-9304 | wf_a3ff3f20-365/agent-a6589fe9b618e1f1f.jsonl | 9301 | - |
| 9305-9308 | wf_a3ff3f20-365/agent-a6589fe9b618e1f1f.meta.json | - | - |
| 9309-9326 | wf_a3ff3f20-365/agent-a690a63706fe3052d.jsonl | - | - |
| 9327-9330 | wf_a3ff3f20-365/agent-a690a63706fe3052d.meta.json | - | - |
| 9331-9384 | wf_a3ff3f20-365/agent-a6d29230cc32e12df.jsonl | 9381 | - |
| 9385-9388 | wf_a3ff3f20-365/agent-a6d29230cc32e12df.meta.json | - | - |
| 9389-9436 | wf_a3ff3f20-365/agent-a717112b01628d426.jsonl | 9433 | - |
| 9437-9440 | wf_a3ff3f20-365/agent-a717112b01628d426.meta.json | - | - |
| 9441-9467 | wf_a3ff3f20-365/agent-a730604ee736b83a0.jsonl | 9464 | - |
| 9468-9471 | wf_a3ff3f20-365/agent-a730604ee736b83a0.meta.json | - | - |
| 9472-9507 | wf_a3ff3f20-365/agent-a78dbe3bb9de2df37.jsonl | - | - |
| 9508-9511 | wf_a3ff3f20-365/agent-a78dbe3bb9de2df37.meta.json | - | - |
| 9512-9523 | wf_a3ff3f20-365/agent-a7a8bbc2c2b0157f5.jsonl | 9520 | - |
| 9524-9527 | wf_a3ff3f20-365/agent-a7a8bbc2c2b0157f5.meta.json | - | - |
| 9528-9574 | wf_a3ff3f20-365/agent-a7ef61293f500f77c.jsonl | 9571 | - |
| 9575-9578 | wf_a3ff3f20-365/agent-a7ef61293f500f77c.meta.json | - | - |
| 9579-9722 | wf_a3ff3f20-365/agent-a7f783708c55fa5c4.jsonl | 9719 | - |
| 9723-9726 | wf_a3ff3f20-365/agent-a7f783708c55fa5c4.meta.json | - | - |
| 9727-9780 | wf_a3ff3f20-365/agent-a80076b31bbc7479e.jsonl | 9777 | - |
| 9781-9784 | wf_a3ff3f20-365/agent-a80076b31bbc7479e.meta.json | - | - |
| 9785-9813 | wf_a3ff3f20-365/agent-a83e737447d1c573e.jsonl | 9810 | - |
| 9814-9817 | wf_a3ff3f20-365/agent-a83e737447d1c573e.meta.json | - | - |
| 9818-9854 | wf_a3ff3f20-365/agent-a860ea188504580b7.jsonl | 9851 | - |
| 9855-9858 | wf_a3ff3f20-365/agent-a860ea188504580b7.meta.json | - | - |
| 9859-9929 | wf_a3ff3f20-365/agent-a87525a52143f07f8.jsonl | 9926 | - |
| 9930-9933 | wf_a3ff3f20-365/agent-a87525a52143f07f8.meta.json | - | - |
| 9934-9969 | wf_a3ff3f20-365/agent-a877356c5e03d2812.jsonl | 9966 | - |
| 9970-9973 | wf_a3ff3f20-365/agent-a877356c5e03d2812.meta.json | - | - |
| 9974-10021 | wf_a3ff3f20-365/agent-a8b3bedca1c5fd0c6.jsonl | 10018 | - |
| 10022-10025 | wf_a3ff3f20-365/agent-a8b3bedca1c5fd0c6.meta.json | - | - |
| 10026-10056 | wf_a3ff3f20-365/agent-a8de56d78a2032cae.jsonl | 10053 | - |
| 10057-10060 | wf_a3ff3f20-365/agent-a8de56d78a2032cae.meta.json | - | - |
| 10061-10107 | wf_a3ff3f20-365/agent-a97ba3b06930a4368.jsonl | 10104 | - |
| 10108-10111 | wf_a3ff3f20-365/agent-a97ba3b06930a4368.meta.json | - | - |
| 10112-10145 | wf_a3ff3f20-365/agent-a99ec65a905ec0e25.jsonl | 10142 | - |
| 10146-10149 | wf_a3ff3f20-365/agent-a99ec65a905ec0e25.meta.json | - | - |
| 10150-10186 | wf_a3ff3f20-365/agent-a9bddf3531cf4ed4b.jsonl | - | - |
| 10187-10190 | wf_a3ff3f20-365/agent-a9bddf3531cf4ed4b.meta.json | - | - |
| 10191-10274 | wf_a3ff3f20-365/agent-a9ec25a3f228a1786.jsonl | 10271 | - |
| 10275-10278 | wf_a3ff3f20-365/agent-a9ec25a3f228a1786.meta.json | - | - |
| 10279-10350 | wf_a3ff3f20-365/agent-aa1a85c200284d881.jsonl | 10347 | - |
| 10351-10354 | wf_a3ff3f20-365/agent-aa1a85c200284d881.meta.json | - | - |
| 10355-10377 | wf_a3ff3f20-365/agent-aa257c689bd1710c8.jsonl | 10374 | - |
| 10378-10381 | wf_a3ff3f20-365/agent-aa257c689bd1710c8.meta.json | - | - |
| 10382-10409 | wf_a3ff3f20-365/agent-aa73ee58d1e569644.jsonl | 10406 | - |
| 10410-10413 | wf_a3ff3f20-365/agent-aa73ee58d1e569644.meta.json | - | - |
| 10414-10440 | wf_a3ff3f20-365/agent-aa77eabd147fbd01f.jsonl | 10437 | - |
| 10441-10444 | wf_a3ff3f20-365/agent-aa77eabd147fbd01f.meta.json | - | - |
| 10445-10471 | wf_a3ff3f20-365/agent-aac44c69d16643f50.jsonl | 10468 | - |
| 10472-10475 | wf_a3ff3f20-365/agent-aac44c69d16643f50.meta.json | - | - |
| 10476-10504 | wf_a3ff3f20-365/agent-aaeec6004de6eeb48.jsonl | 10501 | - |
| 10505-10508 | wf_a3ff3f20-365/agent-aaeec6004de6eeb48.meta.json | - | - |
| 10509-10620 | wf_a3ff3f20-365/agent-ab66fb9ced8e210c0.jsonl | 10617 | - |
| 10621-10624 | wf_a3ff3f20-365/agent-ab66fb9ced8e210c0.meta.json | - | - |
| 10625-10671 | wf_a3ff3f20-365/agent-ab71e1abc964646f9.jsonl | 10668 | - |
| 10672-10675 | wf_a3ff3f20-365/agent-ab71e1abc964646f9.meta.json | - | - |
| 10676-10748 | wf_a3ff3f20-365/agent-ab92aac258c261512.jsonl | 10745 | - |
| 10749-10752 | wf_a3ff3f20-365/agent-ab92aac258c261512.meta.json | - | - |
| 10753-10805 | wf_a3ff3f20-365/agent-abe136e5066bad5eb.jsonl | 10802 | - |
| 10806-10809 | wf_a3ff3f20-365/agent-abe136e5066bad5eb.meta.json | - | - |
| 10810-10901 | wf_a3ff3f20-365/agent-ac008f637a0855bac.jsonl | 10898 | - |
| 10902-10905 | wf_a3ff3f20-365/agent-ac008f637a0855bac.meta.json | - | - |
| 10906-10980 | wf_a3ff3f20-365/agent-ac13b1517787f215e.jsonl | 10977 | - |
| 10981-10984 | wf_a3ff3f20-365/agent-ac13b1517787f215e.meta.json | - | - |
| 10985-11015 | wf_a3ff3f20-365/agent-ac15ab17b975e4367.jsonl | 11012 | - |
| 11016-11019 | wf_a3ff3f20-365/agent-ac15ab17b975e4367.meta.json | - | - |
| 11020-11060 | wf_a3ff3f20-365/agent-ac219ce83c7e3caaf.jsonl | 11057 | - |
| 11061-11064 | wf_a3ff3f20-365/agent-ac219ce83c7e3caaf.meta.json | - | - |
| 11065-11118 | wf_a3ff3f20-365/agent-ac787eee2658d94ab.jsonl | 11115 | - |
| 11119-11122 | wf_a3ff3f20-365/agent-ac787eee2658d94ab.meta.json | - | - |
| 11123-11172 | wf_a3ff3f20-365/agent-aca86b7ce07f26580.jsonl | 11169 | - |
| 11173-11176 | wf_a3ff3f20-365/agent-aca86b7ce07f26580.meta.json | - | - |
| 11177-11279 | wf_a3ff3f20-365/agent-acc7684dce71f48ed.jsonl | - | - |
| 11280-11283 | wf_a3ff3f20-365/agent-acc7684dce71f48ed.meta.json | - | - |
| 11284-11342 | wf_a3ff3f20-365/agent-ace88aa128e833ad9.jsonl | 11339 | - |
| 11343-11346 | wf_a3ff3f20-365/agent-ace88aa128e833ad9.meta.json | - | - |
| 11347-11399 | wf_a3ff3f20-365/agent-adaad3a2cc4fb7aed.jsonl | - | - |
| 11400-11403 | wf_a3ff3f20-365/agent-adaad3a2cc4fb7aed.meta.json | - | - |
| 11404-11466 | wf_a3ff3f20-365/agent-ae1f00e412dca8b95.jsonl | 11463 | - |
| 11467-11470 | wf_a3ff3f20-365/agent-ae1f00e412dca8b95.meta.json | - | - |
| 11471-11501 | wf_a3ff3f20-365/agent-ae5d5427b7ff14879.jsonl | 11498 | - |
| 11502-11505 | wf_a3ff3f20-365/agent-ae5d5427b7ff14879.meta.json | - | - |
| 11506-11559 | wf_a3ff3f20-365/agent-ae8bbeab22cc81604.jsonl | 11556 | - |
| 11560-11563 | wf_a3ff3f20-365/agent-ae8bbeab22cc81604.meta.json | - | - |
| 11564-11666 | wf_a3ff3f20-365/agent-aea714ea45c01ed6c.jsonl | 11663 | - |
| 11667-11670 | wf_a3ff3f20-365/agent-aea714ea45c01ed6c.meta.json | - | - |
| 11671-11751 | wf_a3ff3f20-365/agent-aed8b213f8050b1ab.jsonl | 11748 | - |
| 11752-11755 | wf_a3ff3f20-365/agent-aed8b213f8050b1ab.meta.json | - | - |
| 11756-11780 | wf_a3ff3f20-365/agent-aee0743ec7f90e99c.jsonl | 11777 | - |
| 11781-11784 | wf_a3ff3f20-365/agent-aee0743ec7f90e99c.meta.json | - | - |
| 11785-11836 | wf_a3ff3f20-365/agent-aeec5e1de8375a171.jsonl | 11833 | - |
| 11837-11840 | wf_a3ff3f20-365/agent-aeec5e1de8375a171.meta.json | - | - |
| 11841-11895 | wf_a3ff3f20-365/agent-af14df5195724de49.jsonl | 11892 | - |
| 11896-11899 | wf_a3ff3f20-365/agent-af14df5195724de49.meta.json | - | - |
| 11900-11962 | wf_a3ff3f20-365/agent-af670dbde044aae2f.jsonl | 11959 | - |
| 11963-11966 | wf_a3ff3f20-365/agent-af670dbde044aae2f.meta.json | - | - |
| 11967-12025 | wf_a3ff3f20-365/agent-af9f5fdfa81f3efb7.jsonl | 12022 | - |
| 12026-12029 | wf_a3ff3f20-365/agent-af9f5fdfa81f3efb7.meta.json | - | - |
| 12030-12093 | wf_a3ff3f20-365/agent-afa5d9b0f1309494e.jsonl | - | - |
| 12094-12097 | wf_a3ff3f20-365/agent-afa5d9b0f1309494e.meta.json | - | - |
| 12098-12128 | wf_a3ff3f20-365/agent-afd5bf5d79da9e721.jsonl | 12125 | - |
| 12129-12132 | wf_a3ff3f20-365/agent-afd5bf5d79da9e721.meta.json | - | - |
| 12133-12300 | wf_a3ff3f20-365/journal.jsonl | - | 12136, 12142, 12147, 12149, 12150, 12151, 12155, 12156, 12159, 12161, 12163, 12165, 12167, 12169, 12171, 12172, 12173, 12174, 12175, 12176, 12177, 12180, 12181, 12182, 12191, 12193, 12195, 12197, 12198, 12201, 12203, 12204, 12207, 12209, 12211, 12213, 12215, 12217, 12219, 12221, 12223, 12225, 12227, 12229, 12231, 12233, 12234, 12237, 12239, 12241, 12243, 12245, 12247, 12249, 12251, 12253, 12255, 12257, 12259, 12261, 12263, 12265, 12267, 12268, 12271, 12273, 12275, 12277, 12279, 12281, 12283, 12285, 12287, 12289, 12291, 12293, 12295 |
| 12301-12310 | b07z98zim.output | - | - |
| 12311-12319 | b24ku7xlu.output | - | - |
| 12320-12322 | b33rpdhs8.output | - | - |
| 12323-12336 | b4r1mogbv.output | - | - |
| 12337-12480 | b4u82n8qi.output | - | - |
| 12481-12861 | b5l6zr8so.output | - | - |
| 12862-12868 | b9jkjh7s8.output | - | - |
| 12869-12892 | b9v7fti55.output | - | - |
| 12893-13041 | bbpyqbgxm.output | - | - |
| 13042-13410 | bis2by1ew.output | - | - |
| 13411-22373 | bk8mak8ct.output | - | - |
| 22374-24049 | bnc6kq04l.output | - | - |
| 24050-26508 | bomyechmk.output | - | - |
| 26509-26510 | br0btkr06.output | - | - |
| 26511-29086 | bx8b5zpym.output | - | - |
| 29087-31855 | w39wxr9d2.output | - | - |
| 31856-31856 | wh22j0v5i.output | - | - |

## All Structured Final Payloads

Each row is a full semantic-reading unit. Payloads include source extraction, adversarial votes, searches, and synthesis. They are historical model claims, not independently verified facts. Matched journal lines are duplicate-value aliases, not independent corroboration.

| Raw line | Agent | Payload keys | Characters | Exact journal alias | SHA-256 of canonical JSON |
| ---: | --- | --- | ---: | ---: | --- |
| 65 | a00429e97c3e43402 | refuted, confidence, counterSource, evidence | 3938 | 7085 | `5e834b273244c85a6fb286d69968b3bb06790ed9ac852a9afa10aca546bf9103` |
| 106 | a035ead9b575de4fe | claims, publishDate, sourceQuality | 2751 | 6897 | `716061dfa6aeca8954fc75f3c99148cba01d28b78870c6b9810e155e87355910` |
| 174 | a119bbe33acc9104b | refuted, confidence, counterSource, evidence | 6475 | 7075 | `d656b8489c718d392b9a6e1315c0e267da62377d34b837b561da08cafc94e864` |
| 291 | a149abfaa8312808a | refuted, evidence, confidence, counterSource | 5645 | 6965 | `0177d9acd93ca02e0fec07ebf4e130042198c2835dae9fb68a84c2970fd5aa38` |
| 337 | a1559bcb74c92a866 | claims, publishDate, sourceQuality | 5129 | 6909 | `cdd7d0abaca71314e17fa0a08eaf7d446b7fd950c3cb16bb64dae4a372934fb7` |
| 398 | a15afdadfda79a308 | refuted, confidence, counterSource, evidence | 6593 | 6977 | `c6369a79ea04d25789ed84b476191a7b21b8f486b1d5b143f4c301a9bd90cd97` |
| 454 | a16e0b1cc199e066d | refuted, confidence, evidence, counterSource | 4312 | 6986 | `da73262857e3b3cf0232fee4e8dcb16788781e7f85844dbaeae65b99c19ba4e7` |
| 508 | a17718bc5d1fd6cfc | refuted, evidence, counterSource, confidence | 4604 | 7035 | `890dd9340bd6d922ce1f2830f9e385b8121e3ebc8397dddbd9cfb944976c4e0c` |
| 536 | a1b97fd7c7ba859ab | claims, publishDate, sourceQuality | 2358 | 6931 | `e44061ce95e8b80dd45d31cac04a14f3dbbeb3f59dcb93397d4afd4da17c2d55` |
| 589 | a1cdff4e3be034dd8 | refuted, evidence, confidence, counterSource | 4789 | 6975 | `f96517c3c8123e073ea70391efac6004386d26b986ac213b5d16accda46f6236` |
| 647 | a1d3708050d86693e | refuted, evidence, confidence, counterSource | 5680 | 7017 | `bba9dc6a0de5640823c830f2c1fb3d0aa3f0ec63fbfe20d65dd017aca26cbf4f` |
| 748 | a21268178e1d5462c | refuted, evidence, confidence, counterSource | 6863 | 6971 | `38a91bae4c901b2abaeba5a6ddea99d0c98490ad0ef06646f628cfd33df1e887` |
| 821 | a225ee50a7099a9de | refuted, evidence, confidence, counterSource | 6251 | 7073 | `3e6f05df8fa428cff95e20dc1aabc35b0662b3113d992b0ce994e46218c6a452` |
| 857 | a26e2811c27647b18 | refuted, evidence, confidence, counterSource | 4608 | 7087 | `a1877cee097940e8145cfc79ce297cd0f50521d2c74fe166fa1977c517a80ee7` |
| 917 | a273a2e86151e65d6 | refuted, evidence, confidence, counterSource | 5282 | 6999 | `4ef730387597980deabec3097474deb118e93a9de90a285bb90c45018079b976` |
| 1003 | a29020e7a4a059b0d | refuted, evidence, confidence, counterSource | 4835 | 7055 | `1445d052550b5f501a92a842f9b09202077f507ea56e0f2bbe9444c3391cf5c1` |
| 1075 | a2a79392e973d7dca | refuted, evidence, confidence | 5053 | 7043 | `42ba816faa8081aa8581b48d133f1730bfbf092718a73b713bf1bc9b7d5798da` |
| 1104 | a2b5e0d9de549e60f | claims, publishDate, sourceQuality | 2304 | 6895 | `992a1cc5c74580a67afed01df746fc55d2ba5c333b0abd5b825a8497d806b2cf` |
| 1170 | a2d5701b136a330f8 | refuted, evidence, confidence, counterSource | 4932 | 7071 | `d8bee4dc0fd8a36ae22c5855e6cc39e308017e7e34f478ae15e65b5ddcd24b03` |
| 1202 | a2de5ad80ca51625b | claims, publishDate, sourceQuality | 5270 | 6899 | `40f187c0c6b958359098e8a522a166c62387d59082d2c9fe899335d626e031e2` |
| 1240 | a32278e0084f2f219 | claims, publishDate, sourceQuality | 2621 | 6927 | `d88d4a4919739d16ae75c7e07ecf27f2c7e07e7f9ff7c2258fe6eca51502424c` |
| 1283 | a33575f86adc21962 | refuted, evidence, counterSource, confidence | 4961 | 7039 | `bde6ee9ad890123d3bd927e0caee6398308b787cecf51c33903c44d7ea8d3d53` |
| 1364 | a33df4c72eae813d5 | refuted, evidence, confidence, counterSource | 5307 | 7053 | `36123e963eae6fc81bdd5358a47b9e3840fd5c659f403ce7c10783af90deb9f6` |
| 1408 | a35e301ea899386a6 | refuted, evidence, confidence | 4214 | 6991 | `683b97b955124fc8e96cf92af3272899773d27d7c64b2741db49b0ff93c3da27` |
| 1457 | a367253d55c290d48 | refuted, evidence, confidence, counterSource | 5677 | 7089 | `7bd4baab0d9a2b7314e87f46cde7f7166040ace0968110a23baa3a2b11b606fc` |
| 1503 | a37ab555dbceac65d | refuted, evidence, confidence, counterSource | 5013 | 7069 | `1b11e3ef8aa5e8c7c4de24041e18443107838fdc8ee5713b12d6e5d90767b657` |
| 1562 | a3d5520016949bbe4 | refuted, evidence, confidence, counterSource | 4788 | 7065 | `96f563455413ce3f3c9b05ab0d006d1bbe666232c7c97500f7f25e5053fb0400` |
| 1714 | a408e3d1580b0ec71 | refuted, evidence, confidence, counterSource | 5845 | 6967 | `80977c0bfac0531f263966df91dd4d8483fa633900945de10a48910ce7c4be17` |
| 1742 | a44ab54f423ff2d56 | claims, publishDate, sourceQuality | 2933 | 6943 | `25bb8c65439f2a6d123f823d1d7b5dfa1ae0ea146de19ec28d58c4b7c235cbf0` |
| 1804 | a475785764416952e | refuted, evidence, counterSource, confidence | 4187 | 7083 | `585a9f12bbcfc2e97fd8882c4b51f5823d15037996420594e2069a8154178dd0` |
| 1856 | a4b010f0bfbda0c50 | refuted, evidence, confidence, counterSource | 4846 | 6995 | `7dcf90617c3cbfc5e16fbadb1e56918ad706920095422594f3246ec6358c1ead` |
| 1903 | a4cf75759c51c44a3 | refuted, evidence, confidence, counterSource | 5562 | 7092 | `824cb4fa922477cd1647005b57f00f3db2cada9d4381bf71d96d3584cf2cce32` |
| 1954 | a4dc29b13a6fea728 | refuted, evidence, confidence, counterSource | 5385 | 7007 | `b6d713230d4f86423457dd189653af2ce158cb4af20a9a9e2fc33877f3a2b9a3` |
| 2017 | a50853efdb822b14e | results | 14627 | 6905 | `fdee3e70c621a775df9513840843bcd43fcbc260c343321c6ab14779e699b6f0` |
| 2062 | a522d9811bd7510b1 | claims, publishDate, sourceQuality | 1815 | 6911 | `9eddf4897ab0cdd0c37d19bcd8eef584563530cf68f7bae85d46b9ff1dc2b38d` |
| 2088 | a5244d16098c70163 | claims, publishDate, sourceQuality | 2476 | 6944 | `d07e1d600014b40a64ac97c94a2ef4be6f6f49480c09c5d3ea42fe8a6901ae20` |
| 2188 | a57f31b8e717e572f | refuted, evidence, counterSource, confidence | 5206 | 6957 | `2599f6a6b43185f27a8958a6a1d887e7cbf4822001eeee4fc32a87991d91250e` |
| 2254 | a5a0da8072a60261b | refuted, evidence, confidence, counterSource | 3952 | 7005 | `dfcd242375973d6306e15aa876db5d6a5651d2567ca821c97b424fdd1cd3070e` |
| 2298 | a5ba8f5f834b3a689 | refuted, evidence, confidence, counterSource | 5633 | 7091 | `e1fd53f995b89a9a24c95ee2139727cf38f0ca5d549a53f567ff33f1ac51eddd` |
| 2349 | a5e667df949b0489f | refuted, evidence, confidence, counterSource | 4240 | 7079 | `c37b115d0a8acb8c10950fedb92a41cb5c6e3be613730ba2402298d90e7ca6a6` |
| 2436 | a6014921441c4b4ff | results | 12809 | 6915 | `6a3a8728d8662b1a9225d60a34f5e6a0797cc249d302b01ed6e4b21402baef8b` |
| 2477 | a64cc391e6f6bee28 | refuted, evidence, confidence, counterSource | 5810 | 6981 | `c9d207ca3336f92955ca0120de99c5d68d66f89434135337899f716da1849e5b` |
| 2519 | a65c5dc8975b2630b | claims, publishDate, sourceQuality | 4918 | 6923 | `07dcff382d426caf6fda5a6da6238dbb3af812e03517bb175d783c4f6e5a1c0d` |
| 2606 | a66a64f90efb5545c | refuted, evidence, confidence, counterSource | 5785 | 7025 | `e7399b2d7b013ea9ebe83f65ff94dd6d05a1c9df84df14601eaaeb951530b527` |
| 2693 | a66e9208228ae23c3 | refuted, evidence, counterSource, confidence | 5945 | 7029 | `c87e548a408b1f7d455c24cc838d802e752ee59a8814c298a9811df715fb3f72` |
| 2787 | a68844cdf6ed67354 | refuted, evidence, confidence, counterSource | 7431 | 7045 | `5d96169bdec913f2c386f56d2170ff7188056c46fe15cf3ea92a954484b5943d` |
| 2837 | a69a5190ef853ed52 | refuted, evidence, confidence, counterSource | 3897 | 7061 | `8837e6a19eaa1e03dcd16aed91a672fcc349940ab92d102c010282192e0acee4` |
| 2853 | a6a5806abae423ef9 | query, question, angles, summary | 10123 | 6880 | `7f95d81b4fe908fa1a281afc064bdcb3c61eb69301b7c424dbd5a7fcf690ff4d` |
| 2898 | a6c9e99257705b3e8 | refuted, confidence, evidence, counterSource | 5707 | 7090 | `6674f3e3b5cba5d21464fc6687d0fd36dfb6fd5b4ef238f786704c434e52e81e` |
| 3000 | a758d028de1a9ac9a | refuted, evidence, counterSource, confidence | 4820 | 6959 | `66853b86cd8b8cede812d237cd09a02d450451d6efeff213e0bf0fd4b2f528a7` |
| 3060 | a7a8d1a23a2c5d6d2 | refuted, evidence, confidence, counterSource | 5313 | 7057 | `0d3087d512e89b998c6ebe608d7e9fa86088f1efa8f6688ae7038c1e7203efbe` |
| 3146 | a7b4e50c6a28887f3 | refuted, evidence, confidence, counterSource | 5897 | 7049 | `78cbe31c5d0237745ac916c7a023bc57abc67f3d9e85644ddceb87be2e0673e1` |
| 3233 | a7d7b83a85b37cedd | refuted, evidence, confidence, counterSource | 6070 | 7027 | `c3cb3c66a3eecdb548ea26dd043ef2abe68749ae4e3d20803f6b9248663b46e6` |
| 3377 | a7e203329123aca81 | refuted, evidence, confidence, counterSource | 5867 | 6963 | `458d060739dcf237f21a85f1c50082002df32a27a6a0c79f2e141e639dd25469` |
| 3411 | a82708d64f078b63c | claims, publishDate, sourceQuality | 2317 | 6907 | `a0021a3d0f7d08af9cf93f4ce3c3d581e10e45a494a670c0697264745336e97a` |
| 3471 | a82f444e6a6819edf | refuted, confidence, counterSource, evidence | 4540 | 7013 | `124c8ae291f8fb147810924bc44807e562711225877aa43288c07929dbd804c4` |
| 3557 | a84747635d99d83e9 | refuted, evidence, confidence, counterSource | 5063 | 6969 | `9a5c286d12ca05be65de1ad2476adf7a0041f8b8f1c2bce16cba1e1b88b1a20b` |
| 3603 | a891c0a59e4e32983 | claims, publishDate, sourceQuality | 4647 | 6901 | `ee696c57b444193b8631c4c223162355f73657506c80351cf58eca1f97489ae4` |
| 3665 | a8ec939fbd6912bf7 | claims, publishDate, sourceQuality | 4156 | 6917 | `a40840197f9822e77749dd37f296f4647ef866f68389affe979033670b26005b` |
| 3717 | a8eeaa76b30a228d7 | results | 9109 | 6891 | `3178dc6c1e6f85a254d3ff8a3a1ee74a7d4d06caa1250c1c8003b21141e75967` |
| 3764 | a94df9ad2b65d7edf | refuted, evidence, confidence, counterSource | 4543 | 6979 | `3da4ba2cb3e101271a0b87bd80dec7564685218b66cba5d2c8d9fb522706bc24` |
| 3827 | a98fc3cfc92687d21 | refuted, evidence, counterSource, confidence | 5366 | 7077 | `a16d1c4b29dcfbd7928dd2e09ec7c5fe85e5b36cc73ad92dae1407b034f387a0` |
| 3904 | a9a80b69b582aa8a2 | refuted, evidence, confidence, counterSource | 5434 | 7031 | `ac885ffa12c28558823585c7721f2cfe860fa2fa3dfd1186c0869a3deb43ba2e` |
| 3968 | a9c1ef13d25673ce1 | refuted, evidence, confidence, counterSource | 5962 | 7023 | `901de915098c6d4227e0a67b649fe78ca670237f02db447e6606382025670d4f` |
| 4044 | a9e5aabed461013e1 | refuted, evidence, confidence, counterSource | 5570 | 6993 | `5a284f07e6ca9646fa952e7d43dd9b269c0e74685ad1c5cdbff944d1e5fe8a4a` |
| 4080 | a9fbcd397013fd599 | summary, findings, caveats, openQuestions | 35862 | 7098 | `03fa2b9ae060823dcee8662a89e75ab3d2304abaed27a78791babd15e5049147` |
| 4116 | aa60419bd4577ebc4 | claims, publishDate, sourceQuality | 2721 | 6935 | `a4f5f0f92b5046ded774d4f2e6697f77285160edf165f85a12ee6fefd65ee85e` |
| 4183 | aa62432f2a81d7e17 | refuted, evidence, confidence, counterSource | 4983 | 7019 | `d599dce0d0fac01087d219bed5b5dbbd72b82058b21ba5156eaeb6b53eb0ee7f` |
| 4255 | aab32e54a7b4d5987 | claims, publishDate, sourceQuality | 3721 | 6942 | `bf4f83d0801d84ad37b93f803217cd170c2671ceae77e9742b08437112f8cef2` |
| 4336 | aae740fafa577c7fd | refuted, evidence, confidence, counterSource | 5337 | 7047 | `586c266ebf38d942599100d046f42c4f62b975e9746e0d42d26a0feb728eb307` |
| 4402 | ab049d7613c37ca44 | claims, publishDate, sourceQuality | 4026 | 6919 | `8c33fa323f4d4c61dfbde45548c823c104a10f506c9600c05e7affdbaacd3661` |
| 4476 | ab19ada4a9cc7b3f0 | refuted, evidence, confidence, counterSource | 5451 | 7095 | `a14d4edd4a5beb512924044a4f3ed606489a15b7cb88c6fc16ecb13c4791274c` |
| 4529 | ab41e2112fa175313 | refuted, evidence, confidence, counterSource | 4113 | 6997 | `f6928db4c31f986db03e7b51a209f985192968059e647b9c3493d65c53a084c3` |
| 4629 | ab4f11259a0f498a7 | refuted, evidence, confidence, counterSource | 5216 | 7059 | `aac84965af657a6d1db75667e553416899834b064ce9f56e05125c9ad1840571` |
| 4715 | ab5212b1c91ff0bd2 | refuted, evidence, confidence, counterSource | 4913 | 7067 | `8485e2dc7a9f306ac14e127709420a64cc94c156d6e38505477abc581cd17256` |
| 4748 | ab5c811a73a2afcfc | claims, publishDate, sourceQuality | 2304 | 6936 | `49f7192ba877f2b43b575e449922f2d11b3e25df93956eca34b486bf2a41ffa2` |
| 4779 | ab5ddc3829933fbb8 | claims, publishDate, sourceQuality | 4678 | 6925 | `a0097adb119321aa1560dd76ad410ef04e07be90cb333386e68b488baa563da8` |
| 4807 | ab8249702a948aee0 | claims, publishDate, sourceQuality | 3233 | 6932 | `60c30dadb4cdf07965e4cec2b1955d309eb1d9a7ced50cb106c78bfa4a6a8487` |
| 4839 | ab8d1d368cc402a41 | claims, publishDate, sourceQuality | 3373 | 6903 | `66f1e7d5d68e8c314b581bbc15b41547cdfe5f5b86a7dfa2e693bf8098502d08` |
| 4908 | ac01292d3a1e47435 | refuted, evidence, confidence, counterSource | 6017 | 7015 | `71b28c3d6b53bf2d4844aca2ddf9726ba28f3197a7d20bf04f52ebc2ce3ccfd1` |
| 4948 | ac30eeb367ff303cc | claims, publishDate, sourceQuality | 4565 | 6913 | `b0c35a67227a53d62b0b347603a2ad810b06d4420f0f13f787ba3a8feea4764a` |
| 4998 | ac3e456307097728f | refuted, evidence, confidence, counterSource | 5402 | 6989 | `b0dae812cb911110c8da8415649ed73147cc065f36df22cf8b35e6b07b630099` |
| 5032 | ac41a209b3e5c0a52 | publishDate, sourceQuality, claims | 3350 | 6921 | `cac8812c8706128f60b1b879e5e8eb567b97dd1eea8628f6f90f2e20fc8e9c19` |
| 5134 | ad0d41ee879e40d58 | results | 12505 | 6937 | `8851b041c6011bbc1cff996729b94fba2e73bbce1a215566aa3a3efd9412a3c3` |
| 5233 | ad1ac8a6b13cfd752 | refuted, evidence, confidence, counterSource | 4646 | 6955 | `cd220060989b86999edbda653dee2e50653b7ebaf1e89272d6e64dda31704a75` |
| 5314 | ad3e13637b3bdba78 | refuted, evidence, confidence, counterSource | 5913 | 7096 | `f76adf811ab566477fd0576308bbc7965d99880cdd8f8ae0cc2060121eef3c54` |
| 5413 | ad6505de0b8153174 | refuted, evidence, confidence, counterSource | 5051 | 6984 | `13f5c2229155df79e7782f9b19cf31a3a714c2aea9880b67ba707fd845e72310` |
| 5542 | ad83492ee4276714a | refuted, evidence, confidence, counterSource | 5417 | 6961 | `06ec6183e930389582a7e019f86e960db4ce068d23cb408930afe0f765bb3605` |
| 5573 | ad9e186022b8ff5d9 | claims, publishDate, sourceQuality | 3352 | 6934 | `6429b021a50e326a8bb0efb252084275f255c76acf53b9f99883bf1c731cca38` |
| 5639 | adafe1d70d9a979b2 | refuted, evidence, confidence, counterSource | 5353 | 7081 | `906807bcdd8ef255b316b61069b4e1b91e63736c410e89d3cb558b733434f2aa` |
| 5776 | add7a9e56629446cd | refuted, confidence, counterSource, evidence | 5800 | 6973 | `ff970faad68556580fbd5f91cec58b68e5efcb7117a851d0a3c4082a545c5eae` |
| 5859 | adf3bfc1ff5971b7d | refuted, evidence, counterSource, confidence | 5144 | 7051 | `6748115d9644eff3fca99d7b7a518042570f14b9225d0535689599e12f5b7cb2` |
| 5905 | ae05cd59d822a3bee | results | 12515 | - | `7be69e7ae787024610528cfeca174a228255ec50a7abfe3872f0e68469c93d88` |
| 5908 | ae05cd59d822a3bee | results | 12356 | 6887 | `2ebc1b4baf95c2c790e12e081464ca56a622f283081a453adecb5712c822e953` |
| 5942 | ae07fe3d4f0cadcf7 | claims, publishDate, sourceQuality | 4514 | 6945 | `f7d35dd2fd641dd216189e883faca7a441b6d7291ddbf890736792b4beebbeb1` |
| 6052 | ae3795f359096bc30 | refuted, evidence, confidence, counterSource | 5442 | 6983 | `17dd386e7798f848574c10647d2b8457df70034c6a64bc7d1109037b91205a6e` |
| 6098 | ae8dd01fd622eda4c | refuted, evidence, confidence, counterSource | 3657 | 7094 | `6022d41a1f3bc99b392ee9245257c199ba8bf56ed25529552a124d68e8ad1e27` |
| 6179 | ae8e5eb51da27e53f | refuted, evidence, confidence, counterSource | 7602 | 7041 | `8c3d650486c0c1d9b7b759eaad3a89c7248c04621c7cdbaa9d9a14caff69fae1` |
| 6233 | ae92320671ec3d047 | refuted, confidence, counterSource, evidence | 7794 | 7001 | `cd3f5c6a79eeb2222bb8ccf89f780f4f7016f3e2a79066476273b595e5d14236` |
| 6315 | aee8abf58108aa5ab | refuted, confidence, counterSource, evidence | 6454 | 7036 | `287e562e5773525529ddf5062fa30dd235f27d474b54ffb25008e0713747c231` |
| 6359 | aeebc7dfb16a90901 | refuted, evidence, confidence, counterSource | 5130 | 7033 | `d632e7b57ccc841ecb38e32e70a5a255f6f970a7787ec4b5138031ed53d432b3` |
| 6418 | af1e58ac96a671709 | refuted, evidence, confidence, counterSource | 4936 | 7003 | `c2fb2334f1f0725c1319044c32faeb3c26f06b13b2c78883c6f021bbdcc77592` |
| 6474 | af77600f3ef04f51a | refuted, evidence, confidence, counterSource | 7316 | 7093 | `4305501cfe56ccae0bff47f941f90ce73d5497545e54b03d330636291ad568e0` |
| 6507 | af82b39cf8fbafbee | claims, publishDate, sourceQuality | 3250 | - | `27f18ab2cee9c7eb654a9d01cd3ac4fe968dafff31a8d72b64c26b7e0344a1f5` |
| 6509 | af82b39cf8fbafbee | claims, publishDate, sourceQuality | 2976 | 6933 | `6f3bfd65ae10cf17607c6a62867a6c823f77e9b8d5c8a77b1b938653f4fca81a` |
| 6566 | af8512e62a8bc360b | refuted, evidence, confidence, counterSource | 4079 | 7063 | `6d531b03a4ff1939b001407704f020d5a4b4699d66936c64d36c3d802b2f14c5` |
| 6619 | af8927b73cf47ac20 | results | 12088 | 6893 | `ae642ca6fda2963096137360fafe40700928da3a0f9bb2f888eb4885a33185fa` |
| 6647 | af8ab6189a8d1ebd8 | claims, publishDate, sourceQuality | 3049 | 6929 | `dead33f030146e5132a6ba45010d74e9cd8000b73d8fb71590ad4b8633b2a1db` |
| 6713 | af9ad0bd8c671d4ab | refuted, evidence, confidence, counterSource | 5043 | 7021 | `46e9337ace3d10be67fa68168d88b3a8296fbadfc18edcbac14d6a1751b30936` |
| 6745 | afc744055797ea549 | claims, publishDate, sourceQuality | 3197 | 6946 | `2d9fb192626a761f4fe11704147bc4445ac4b99e1992de565593fe56df730b9d` |
| 6808 | aff38e8df3e7409a3 | refuted, evidence, confidence, counterSource | 5858 | 7009 | `c66ecc2921511e1d765a048b7d20ee89b4211e6701940e08f8ffef6ee857015e` |
| 6869 | aff967875f7a1188f | refuted, evidence, confidence, counterSource | 4472 | 7011 | `284ac54d69faeefe2aabd12d6b0246f5f06924f7e856707826b113527c03d9d4` |
| 7151 | a02d15ba3d40ab9b9 | refuted, confidence, counterSource, evidence | 4998 | 12231 | `668bf84c55627b93f4bbf81522c73f2f329f40368e98b179276233be833d5c45` |
| 7229 | a0696609ab1b70b20 | refuted, evidence, confidence, counterSource | 4982 | 12247 | `18df656efd23b797836f8b153c1a20802b343068a1edf952d551cca0f8ef65bb` |
| 7292 | a077aafbdca328eb4 | refuted, evidence, confidence, counterSource | 5629 | 12267 | `b4de3732110ec103d5998057a40889a43b28e15269d5b8243025891d58cb1734` |
| 7335 | a08cd251283889bed | refuted, confidence, counterSource, evidence | 6348 | 12195 | `ba75306b3f91e6cd9285037c38a855e230b4f74872a4c2134b0130fc3e6dc684` |
| 7432 | a13f5918910fe066d | refuted, evidence, confidence, counterSource | 4062 | 12249 | `e64783eb4bd437bdd16c09ffd2312a17fc0483f4de56a998bfe0db2f9f2d7679` |
| 7517 | a1b204226d63243fe | refuted, evidence, counterSource, confidence | 3793 | 12239 | `dd5f53a5c07e4a7981c359564c2bc2da1c67843dda474ce0f1eb256598576042` |
| 7584 | a1b6cc5ea59913900 | refuted, evidence, confidence, counterSource | 4938 | 12241 | `e25345d6fcbff50acffadf80ed88308f6a67d792abf3ad40be6d274f9f9b8c19` |
| 7637 | a1c180485c4bb6838 | refuted, counterSource, evidence, confidence | 4685 | 12245 | `6d658e22c2b407e75bcf0bf756dafc68b1976b3af937e978cd0489fe35db565d` |
| 7683 | a1d76961f39eea6fd | refuted, evidence, confidence, counterSource | 5175 | 12223 | `8ec00369fad0761a06df2b62802dd632b42d3ce258b60a9d580bea82b30e0911` |
| 7748 | a2a243409fa7c89f5 | claims, publishDate, sourceQuality | 4114 | 12180 | `f999cf4e0d614aa1bdd5ac8b949318372ebfdf5327e626f09f28b658a4ca569b` |
| 7837 | a2bf9fabca7953c7a | refuted, confidence, counterSource, evidence | 5396 | 12237 | `04310664978b1e71f6ed2e93b299741b133bf9fca940748f9c52020b36b5e4e0` |
| 7900 | a2e21c1354b2bca5a | results | 10051 | 12142 | `7ab6f39f8ca065c441dcaeacc460211377d4cbac857606740c433a2db7634265` |
| 7996 | a3257a5efa484ddf3 | results | 9044 | 12156 | `0ce68aa86bae18995a65e75c74c688faf7932e86e46f3505f971ea2374403088` |
| 8053 | a35f74ce39e5e0de5 | claims, publishDate, sourceQuality | 4615 | 12175 | `f34fdd44214cd11d40d928409d169620c43368aca158c4fba05655d35686046a` |
| 8159 | a3935f0bfbcb47c57 | refuted, evidence, confidence, counterSource | 6884 | 12234 | `aa573e92335f2757331551d60978b2123affb7b26c03f98275827c7d95478d34` |
| 8221 | a3a1f64a175c6902d | refuted, confidence, counterSource, evidence | 5033 | 12265 | `175ba5c78fa8fcfd5ec1b6ed80829c5d29211bbc520484ad18c8ec587f8bfcd6` |
| 8308 | a3bea364bc6a42d18 | refuted, evidence, counterSource, confidence | 4904 | 12289 | `7aa97bda1ddf39c221509ce9cc3b9acfcbdb92f194d29d14c77bb4e63d73956e` |
| 8405 | a3cbab28fc4d8a844 | refuted, evidence, confidence, counterSource | 5538 | 12251 | `3ceac9ebb421ce23038b247244ab1c7a9e361c297dff5bfc57e311c3dfd56494` |
| 8473 | a3da41f905c555145 | refuted, evidence, confidence, counterSource | 5244 | 12225 | `e2ca43188e8278fbc7ed90ffc798eba7608c4f93015d197bc49bee72f5a2cf01` |
| 8509 | a403bb79c44d072db | claims, publishDate, sourceQuality | 4716 | 12174 | `8b7d8134895f77b5cacf38809f752e64169d321fc765fe6589d54c2318774725` |
| 8578 | a4189d4dfe10063d5 | refuted, confidence, counterSource, evidence | 4527 | 12268 | `c80ab34c27536c3320738149ed971ee2467291c21fe4835d1e17323a4aa5198a` |
| 8614 | a418a0d4f8d530b3f | claims, publishDate, sourceQuality | 5451 | 12165 | `c72e5f29dc158e3661cafe07f77940cb306cf7b7ee3bf0cce81c324a25bb0d66` |
| 8659 | a43f807bc24b8fd8b | confidence, counterSource, evidence, refuted | 4330 | 12287 | `5cc009b0f9efca3c35f7ba4aa6d869378e479fce2f990a23df139ba5c727785f` |
| 8745 | a47c0c9dfccf82485 | refuted, evidence, confidence, counterSource | 6965 | 12291 | `eb2374399a2d87419c5921e1a827d1a39e17652518003840d35b5ad67ed47dea` |
| 8801 | a4a5395cf904850c2 | claims, publishDate, sourceQuality | 3842 | 12177 | `7e9a17ae6aec8cad6490b53e113cc191e127269a9e6d73e877c62514633e7c2d` |
| 8890 | a54db48d60102de88 | refuted, evidence, counterSource, confidence | 6411 | 12243 | `89ca68a123efed304a2065602708de58c1410f182c5315de8b0a52d481d3725a` |
| 8948 | a578bf72cffc01d6f | refuted, evidence, confidence, counterSource | 4053 | 12211 | `b52cb61a4d0d864038851248b3b2c28609114d6a8c6f7f53e0e8666e71624e40` |
| 9000 | a5a44feb67432feba | refuted, evidence, confidence, counterSource | 5086 | 12217 | `fb986af78364338162f4035d8ef6fb2015b6949021b71c9d782d8678d3aebad0` |
| 9046 | a5f55bede26c6e0cd | claims, publishDate, sourceQuality | 5123 | 12159 | `37b9df83912aa9bde4091c56653ed352ff2b069908d388f978a5a9025b02f682` |
| 9098 | a605adc7b0750b44f | refuted, evidence, counterSource, confidence | 5767 | 12259 | `5c15297da4b475a87cebc8b41d54b55008f5c2cce0707969aac047eb99b9d49c` |
| 9207 | a619756c309f9e6d4 | refuted, confidence, counterSource, evidence | 7140 | 12207 | `991e6240dbb90e7ca6b0aea62b7b9b484796052921e41a29b76bda54a5001716` |
| 9248 | a63ae1144edde5bc2 | claims, publishDate, sourceQuality | 3988 | 12171 | `68c25bfa9eb10534920d6815d93aff1fc4e1e99de7b15ac9d1fbaa4922715cd5` |
| 9301 | a6589fe9b618e1f1f | refuted, confidence, counterSource, evidence | 4456 | 12283 | `98ddc5ccbf29002056b3edce981decb2c80f0317d25fbb1d2c2c6cbf7ab96073` |
| 9381 | a6d29230cc32e12df | refuted, confidence, counterSource, evidence | 5598 | 12221 | `a5e18597c332c9e011353ba69b8103088474b25a77ffcd8c0d9b074c5eefee95` |
| 9433 | a717112b01628d426 | refuted, evidence, confidence, counterSource | 4718 | 12293 | `85944f289931ae7867342f8d1e06588895cdcafb138becaef82a85bd1734263b` |
| 9464 | a730604ee736b83a0 | claims, publishDate, sourceQuality | 3111 | 12161 | `7166a9d79d9b688ae9232b103c55a83321bb8aedef0a1b0eca28e8c1f5ef7ed2` |
| 9520 | a7a8bbc2c2b0157f5 | question, summary, angles | 4615 | 12136 | `b70bfd840c76c248ebe317a5ba84cdc4c97af7b187e792784508714d8d15a39e` |
| 9571 | a7ef61293f500f77c | refuted, counterSource, evidence, confidence | 4850 | 12213 | `6e878d1126ba4e632cc726f3b5e9a8f5cd5fa28153433c15517ba65c1a939ea7` |
| 9719 | a7f783708c55fa5c4 | results | 9060 | 12176 | `07756e33a0e9b13897ec3aef699d741a715e213e3951852bfa8297e36eb10a6a` |
| 9777 | a80076b31bbc7479e | refuted, evidence, counterSource, confidence | 6300 | 12281 | `04519c8a894ddf218312c42192641e89de7e5a75ceeee1edca39a8c372546597` |
| 9810 | a83e737447d1c573e | claims, publishDate, sourceQuality | 3232 | 12182 | `0d291cdb51f7adb9e80ea92a75e0a0bbae886b3b646f9d1da19c4502de7a8620` |
| 9851 | a860ea188504580b7 | claims, publishDate, sourceQuality | 4334 | 12149 | `c69b1aaf4c923cd131e0333da35a2958f98907b9f66c9ae6777521ff3c261cf6` |
| 9926 | a87525a52143f07f8 | refuted, evidence, confidence, counterSource | 6016 | 12201 | `d15c35ed46de1075fd828360f44cc403bcde77c41d1cc328e3d7d8b3e80a2964` |
| 9966 | a877356c5e03d2812 | refuted, evidence, counterSource, confidence | 5083 | 12204 | `c5e15dcf3dc9930cead014e186d9fb1baabc32590e0a07bd0650a57c73cc3167` |
| 10018 | a8b3bedca1c5fd0c6 | refuted, confidence, counterSource, evidence | 4959 | 12215 | `c5d8e77be7af966011285fdf7438ecaaeb83817758cc7e2e9d9b76c22cfb9d6a` |
| 10053 | a8de56d78a2032cae | refuted, evidence, confidence, counterSource | 5297 | 12193 | `fbfd1abd9b8167d8a245b6b795e28ed6cb57af069a72452f3ee7fc1b0dc80fe6` |
| 10104 | a97ba3b06930a4368 | refuted, confidence, counterSource, evidence | 5116 | 12233 | `b84fafea873acf0ad670606ce9b12c91e4bd1b1737c45fe5fa06d92f6220772d` |
| 10142 | a99ec65a905ec0e25 | refuted, confidence, evidence, counterSource | 4974 | 12227 | `4b087640ff2ecd468ced8f2342bab650e0673b32f9f135ad1c844822818c53ed` |
| 10271 | a9ec25a3f228a1786 | refuted, evidence, confidence, counterSource | 8959 | 12203 | `dc48ff4c950b22c2226aa702db874311bc62853d2ce0b79f52f088b3c0ecde94` |
| 10347 | aa1a85c200284d881 | refuted, confidence, counterSource, evidence | 4873 | 12209 | `89bccdf8ef28d0847bc3d490350ce5c3ea80bede15db5bb6da4d6b61d83cf5e3` |
| 10374 | aa257c689bd1710c8 | claims, publishDate, sourceQuality | 2969 | 12172 | `719ed914aff9700abd52702f84cf583c5b521b441be04a495e3c044a7bf71a68` |
| 10406 | aa73ee58d1e569644 | claims, publishDate, sourceQuality | 4992 | 12147 | `14f7e4ccca076769832866f7d44ff931e5019e0ed28206818bb2a75a6f498be0` |
| 10437 | aa77eabd147fbd01f | claims, publishDate, sourceQuality | 3388 | 12163 | `435c0e26caf82a724ab576694fdc28f36e5992619aa8c6d0b6e929a1151a730f` |
| 10468 | aac44c69d16643f50 | claims, publishDate, sourceQuality | 3509 | 12169 | `fec8a963df2fa60ac617e2ed2c83eeaf6b494ff716e47e2b4daeb6ea0693e6e3` |
| 10501 | aaeec6004de6eeb48 | refuted, evidence, counterSource, confidence | 5257 | 12277 | `8abbcb989105ec0a75689e87d133f37d6ccc2f9b40ef92178ec425ca1495ede4` |
| 10617 | ab66fb9ced8e210c0 | results | 9325 | 12155 | `c8aacb69dc6782c053c3a78b17f10bd9b860a4cbf03ecd2fe5d266db72486242` |
| 10668 | ab71e1abc964646f9 | refuted, evidence, confidence, counterSource | 5121 | 12275 | `bf0ee56acdf9199ba870a74683dc6194a10b5f31f99342587337c01f5b858eef` |
| 10745 | ab92aac258c261512 | refuted, evidence, counterSource, confidence | 5127 | 12253 | `fe512f8d69e2b0af54503d92027b8ea449a57a1e34f9264b353def8d40407e2a` |
| 10802 | abe136e5066bad5eb | refuted, confidence, counterSource, evidence | 5161 | 12285 | `cdc32804836f71234b367fdcadff332bb4d2e81221faea6a9b2b7e4981d23bd5` |
| 10898 | ac008f637a0855bac | refuted, confidence, counterSource, evidence | 5267 | 12229 | `1c43138cab39c048862e61b2aba44a96c4e32d9bdec3cc416da48f46963141ae` |
| 10977 | ac13b1517787f215e | refuted, confidence, evidence, counterSource | 5496 | 12263 | `3e50ae5eeffe2f5179cb371c396a77633fb34bb07e3d9d402037068b8a323156` |
| 11012 | ac15ab17b975e4367 | refuted, confidence, counterSource, evidence | 6071 | 12191 | `465f820d276679498c81b91e136cef7412c4b450d4e3d1aaa30e841ca091eee4` |
| 11057 | ac219ce83c7e3caaf | claims, publishDate, sourceQuality | 4107 | 12173 | `11335128d2e7d5eae99ab61183534dacb0cde5865e2da692ece1cce28ec58ee4` |
| 11115 | ac787eee2658d94ab | refuted, evidence, confidence, counterSource | 5378 | 12271 | `d448a36d940abf4006a807196796d590d0454c3392f39bb5dea139d1a2a97b1a` |
| 11169 | aca86b7ce07f26580 | refuted, confidence, counterSource, evidence | 5530 | 12255 | `9891e08477070131124fd0b3295f5a98418aa8cc4f7544191144a2fc05714adc` |
| 11339 | ace88aa128e833ad9 | refuted, evidence, confidence, counterSource | 5857 | 12261 | `be59f801a1b74ea17ff50e338946ef43012220a675de86018346f9ded1443191` |
| 11463 | ae1f00e412dca8b95 | refuted, evidence, counterSource, confidence | 5738 | 12197 | `862a57a59aae1965325852282db3f594f574e46dc46ae9f80da1ce584e2c951d` |
| 11498 | ae5d5427b7ff14879 | claims, publishDate, sourceQuality | 4300 | 12181 | `831754d7b5ff2636eea153ce4967eca0c130df24fb425e8ecca9c6a9e16706da` |
| 11556 | ae8bbeab22cc81604 | refuted, confidence, counterSource, evidence | 4829 | 12257 | `bd33c30991c6aa16ecf22a356e326ab52076d0f536fe6075cd1ec160ae303447` |
| 11663 | aea714ea45c01ed6c | results | 9542 | 12151 | `5d02d3b8f219cdbdbc353e0c17b2951fa7e9697eb5756e381df36a457cd332aa` |
| 11748 | aed8b213f8050b1ab | refuted, evidence, confidence, counterSource | 5297 | 12198 | `a380dc6f83a3d8d16170df9fc8bfb33e5e5dfbbfac01a4dc02ac9cb2f6d8331b` |
| 11777 | aee0743ec7f90e99c | claims, publishDate, sourceQuality | 3732 | 12167 | `598796e67d5ccee957dee68b400e714efdb3cf393e42c77cc442abc65b2a7027` |
| 11833 | aeec5e1de8375a171 | refuted, confidence, counterSource, evidence | 4136 | 12273 | `8e415dffe11e4056f2a74638702f5f9cbeefe10602c60b72c8d154a67a146c17` |
| 11892 | af14df5195724de49 | refuted, confidence, counterSource, evidence | 6461 | 12279 | `7947cb41eddad345d9804189722095afec30e8c78da60c775210c6bcb0efff6f` |
| 11959 | af670dbde044aae2f | refuted, evidence, counterSource, confidence | 4552 | 12219 | `3de3fddb76a34c5d8c1fa75726b2f3cf76b6569bb4acf05f6f310f9d3805fcc5` |
| 12022 | af9f5fdfa81f3efb7 | refuted, evidence, confidence, counterSource | 4856 | 12295 | `d3e8a53c415e89cb987acc56e63b71675b363188703b7177a9db76e3b518c084` |
| 12125 | afd5bf5d79da9e721 | claims, publishDate, sourceQuality | 4915 | 12150 | `352f113887f8851e415b9246484588a16a6b48c5d0da764302df2c532c2a6afa` |

## All Assistant Text Blocks

These are candidates for substantive prose, including conclusions outside structured outputs. Do not infer terminal/final status from stop_reason: most records have null. Same-text aliases are exact text equality. Preview is a navigation label only and does not establish consumption of the block. All full payloads remain unread except entries in the evidence ledger.

| Raw line : content block | Characters | Stop reason | Same text first appears | Preview |
| --- | ---: | --- | --- | --- |
| 25:0 | 50 | tool_use | - | I'll verify this claim against the primary source. |
| 34:0 | 97 | null | - | WebFetch was blocked and the WebSearch budget is exhausted. Let me try the alternate fetch tool |
| 51:0 | 167 | null | - | Quote confirmed verbatim. Now let me check whether the sibling release-note pages are also all- |
| 57:0 | 222 | null | - | The siblings have real content, so the page isn't an unpopulated template. Now let me check whe |
| 64:0 | 94 | null | - | I have everything needed. The quote is verbatim-accurate, but the inference built on it fails. |
| 80:0 | 34 | null | - | I'll fetch the release notes page. |
| 86:0 | 54 | null | - | The direct fetch was blocked. Let me try alternatives. |
| 97:0 | 79 | tool_use | - | Let me verify the "None" entries are real content and not a JS-render artifact. |
| 101:0 | 119 | null | - | The content is genuinely server-rendered. Let me check the adjacent release to see if "None" is |
| 105:0 | 1243 | null | - | Verified. The content is server-rendered (not a JS placeholder), and sibling releases do list r |
| 122:0 | 47 | tool_use | - | I'll verify this claim against primary sources. |
| 130:0 | 132 | null | - | Primary sources confirm the surface facts. Now let me verify the load-bearing architectural inf |
| 173:0 | 68 | null | - | I have decisive primary-source verification. Let me record findings. |
| 190:0 | 55 | tool_use | - | I'll verify this pricing claim against primary sources. |
| 199:0 | 78 | null | - | The console URL renders nothing to a fetcher. Let me search for corroboration. |
| 254:0 | 103 | null | - | Key doc found. Now let me check the tier list and the ACU-to-USD question against the public ra |
| 258:0 | 104 | null | - | The public rate card corroborates the economics but let me nail down the specifics the claim ge |
| 290:0 | 84 | null | - | Verification complete. I found both a legitimate kernel and several specific errors. |
| 306:0 | 57 | null | - | I'll fetch the DuckDB out-of-memory troubleshooting page. |
| 312:0 | 75 | null | - | The fetch returned partial content. Let me get more detail from the source. |
| 328:0 | 63 | null | - | Let me confirm the source file path and its last-modified date. |
| 352:0 | 65 | null | - | I'll verify this claim about Neo4j Aura's GB-hours billing model. |
| 355:0 | 94 | null | - | I'll verify this claim by fetching the primary source and checking for contradicting evidence. |
| 361:0 | 50 | null | - | WebFetch is blocked (403). Let me try direct HTTP. |
| 365:0 | 75 | null | - | Redirect chain confirmed. Now let me check the content for the exact quote. |
| 369:0 | 110 | null | - | Quote confirmed verbatim on the canonical page. Now let me hunt for contradicting evidence and  |
| 378:0 | 100 | null | - | Strong corroboration from a second primary doc. Let me establish a date and hunt for contradict |
| 384:0 | 149 | null | - | Docs repo confirms the page is live-maintained (last commit May 2026). Let me verify the raw so |
| 393:0 | 83 | null | - | All primary-source checks pass. Let me attempt an external counter-evidence search. |
| 397:0 | 1583 | null | - | I attempted external counter-evidence search but WebSearch is returning an infrastructure error |
| 413:0 | 54 | null | - | I'll verify this claim by fetching the primary source. |
| 416:0 | 64 | null | - | I'll fetch both the cited URL and the canonical redirect target. |
| 422:0 | 46 | null | - | WebFetch is blocked. Let me try curl directly. |
| 426:0 | 47 | null | - | Redirect confirmed. Now let me check the quote. |
| 432:0 | 77 | null | - | Quote confirmed verbatim. Let me read surrounding context for qualifications. |
| 436:0 | 124 | null | - | Let me cross-verify with the billing-dimensions page and check for any date/version, plus searc |
| 440:0 | 75 | null | - | Strong corroboration. Now an adversarial search for contradicting evidence. |
| 446:0 | 68 | null | - | WebSearch is erroring; let me cross-check the pricing page directly. |
| 453:0 | 3017 | null | - | I attempted to refute this claim and could not. Verification results:  **1. Quote authenticity  |
| 472:0 | 62 | null | - | I'll fetch the primary source to verify the claim's specifics. |
| 488:0 | 221 | null | - | The page fetched. Now let me check whether the broader session configuration surface (other int |
| 501:0 | 111 | null | - | Strong contradictions found. Let me confirm the sibling primary docs URL and the Cypher API ses |
| 523:0 | 27 | null | - | I'll fetch the source page. |
| 529:0 | 58 | null | - | Let me verify via the GitHub API for authoritative detail. |
| 535:0 | 90 | null | - | Verified via both WebFetch and the GitHub API (issue body, state, and the single comment). |
| 551:0 | 65 | null | 352:0 | I'll verify this claim about Neo4j Aura's GB-hours billing model. |
| 554:0 | 79 | null | - | I'll verify this claim by checking the actual Neo4j Aura billing documentation. |
| 560:0 | 64 | null | - | WebFetch is blocked. Let me try direct HTTP fetching and search. |
| 566:0 | 63 | null | - | The redirect is confirmed. Now let me check the actual content. |
| 572:0 | 112 | null | - | Quote confirmed verbatim. Now let me examine surrounding context and check for qualifications o |
| 578:0 | 151 | null | - | Strongly corroborated across three vendor pages. Let me do adversarial checks for contradicting |
| 584:0 | 176 | null | - | Byte-exact quote match confirmed, redirect chain verified. WebSearch is erroring in this sessio |
| 588:0 | 219 | null | - | Verification complete. The claim survives adversarial scrutiny — I verified it against the live |
| 662:0 | 81 | null | - | I'll verify this claim by checking the primary source and testing the arithmetic. |
| 671:0 | 81 | null | - | The console URL returns only a JS shell — no table content. Let me probe further. |
| 695:0 | 119 | null | - | The 20% pause rule is confirmed in a public vendor doc. Now let me verify the rate table and th |
| 707:0 | 102 | null | - | WebSearch is erroring in this session. Let me use the browser to render the client-side pricing |
| 713:0 | 93 | null | - | Key confirmation: "1 ACU (Aura Consumption Unit) = 1 USD". Let me wait for the table to load. |
| 720:0 | 69 | null | - | Found the public API endpoint. Let me pull the actual rate card data. |
| 730:0 | 107 | null | - | All ratios are exactly 20%. Now checking future rate-card versions and whether 512 GB is actual |
| 740:0 | 99 | null | - | Let me check whether a 30-day continuous pause is even permitted, plus 512 GB availability in v |
| 747:0 | 97 | null | - | I independently reproduced every number in the claim from Neo4j's own machine-readable rate car |
| 763:0 | 57 | null | - | I'll verify this claim about the Onager DuckDB extension. |
| 836:0 | 61 | null | - | I'll verify this claim about Memgraph's on-disk storage mode. |
| 845:0 | 139 | null | - | Web search budget is exhausted, but WebFetch still works. Let me check current status, deprecat |
| 856:0 | 150 | null | - | Verification complete. The literal storage fact checks out, but the same primary source contain |
| 1002:0 | 2430 | null | - | Primary-source verification complete via two independent sources.  **What I checked**  1. `http |
| 1021:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 1052:0 | 97 | null | - | Primary source confirmed. Now checking for contradicting evidence outside the release-notes pag |
| 1074:0 | 77 | null | - | Verified against the primary source and an independent second primary source. |
| 1090:0 | 64 | null | - | I'll fetch the Memgraph storage memory usage documentation page. |
| 1096:0 | 67 | null | - | Let me verify the key verbatim quotes with a second targeted fetch. |
| 1103:0 | 71 | null | - | Verified all load-bearing quotes across three fetches of the same page. |
| 1169:0 | 169 | null | - | Verification complete. All four structural sub-claims check out against the primary source at c |
| 1185:0 | 42 | null | - | I'll fetch the LadybugDB get-started page. |
| 1193:0 | 56 | null | - | Let me get more verbatim detail and check related pages. |
| 1217:0 | 27 | null | - | I'll fetch the arXiv paper. |
| 1223:0 | 73 | null | - | The PDF extraction failed. Let me try the abstract page and HTML version. |
| 1229:0 | 101 | null | - | Let me pull more detail on the projection mechanics and the "remaining challenges / roadmap" se |
| 1235:0 | 92 | null | - | Let me confirm the tool inventory to check whether memory-estimation procedures are exposed. |
| 1239:0 | 55 | null | - | I have enough verified material. Filing the extraction. |
| 1258:0 | 62 | null | - | I'll fetch the primary source and check the claim's specifics. |
| 1363:0 | 124 | null | - | I verified the primary sources directly and found the quote is real but the inference layered o |
| 1380:0 | 40 | null | - | I'll verify the primary source directly. |
| 1394:0 | 107 | null | - | The cited URL now redirects. Let me confirm the redirect target and cross-check the billing-dim |
| 1407:0 | 105 | null | - | WebSearch is failing in this environment, so I cross-checked against a second primary vendor do |
| 1426:0 | 78 | null | - | I'll verify the primary source and hunt for contradicting/qualifying evidence. |
| 1432:0 | 88 | null | - | WebSearch budget is exhausted, so I'll verify via direct fetches of Memgraph's own docs. |
| 1456:0 | 852 | null | - | I verified the primary source and found the refutation inside the same document.  **Verified (n |
| 1475:0 | 55 | null | - | I'll verify the claim directly against the Onager repo. |
| 1502:0 | 180 | null | - | Verified the claim directly against the primary source by cloning the repo (`/tmp/onager_check` |
| 1561:0 | 87 | null | - | I verified every sub-assertion directly against the primary source by cloning the repo. |
| 1578:0 | 55 | tool_use | 190:0 | I'll verify this pricing claim against primary sources. |
| 1713:0 | 106 | null | - | Verification complete. I found the underlying public data source and independently confirmed ev |
| 1729:0 | 29 | null | - | I'll fetch this GitHub issue. |
| 1735:0 | 89 | null | - | Let me get the full thread including comments via the GitHub API for exact verbatim text. |
| 1741:0 | 115 | null | - | Verified the full thread including all 7 comments via the GitHub API. This directly resolves qu |
| 1757:0 | 61 | null | - | I'll verify this claim about the GDS 2026.06.0 release notes. |
| 1766:0 | 87 | null | - | WebSearch budget is exhausted and WebFetch got 403. Let me try alternative fetch paths. |
| 1782:0 | 163 | null | - | Quote confirmed verbatim. Now let me test the load-bearing inferences: is 2026.06.0 really the  |
| 1791:0 | 156 | null | - | The all-"None" page is genuine (neighbors have real content), so let me test the other two weak |
| 1803:0 | 151 | null | - | Decisive contradiction found. Let me assemble the verdict.  The verbatim quote checks out — but |
| 1855:0 | 88 | null | - | I verified the source and hunted for contradicting primary evidence in Neo4j's own docs. |
| 1902:0 | 193 | null | - | Verification complete. The quote is verbatim-confirmed in the primary vendor doc's own source r |
| 1921:0 | 85 | null | - | I'll verify this claim by checking the source and hunting for contradicting evidence. |
| 1927:0 | 78 | null | - | The primary source returned 403 to WebFetch. Let me try other retrieval paths. |
| 1939:0 | 101 | null | - | Quote and date confirmed. Now let me test the load-bearing parts against Neo4j's actual billing |
| 1949:0 | 102 | null | - | Strong contradicting evidence found. Let me confirm what the blog's "stop" actually does mechan |
| 1953:0 | 91 | null | - | I have decisive primary-source evidence on all three components. Let me record the verdict. |
| 1969:0 | 42 | null | - | I'll research this angle using web search. |
| 2016:0 | 119 | null | - | Rate limits are throttling further searches, but I have solid primary-source coverage on this a |
| 2032:0 | 29 | null | - | I'll fetch the Reddit thread. |
| 2061:0 | 1022 | null | - | Retrieved the original post (comments are gated behind Reddit's login wall and could not be ext |
| 2077:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 2083:0 | 56 | null | - | Let me get more verbatim detail to confirm exact quotes. |
| 2087:0 | 1054 | null | - | Both fetches returned consistent content with matching verbatim quotes. Source is the Neo4j Com |
| 2104:0 | 55 | null | 190:0 | I'll verify this pricing claim against primary sources. |
| 2113:0 | 96 | null | - | The cited console page renders no content to a fetcher (JS SPA). Let me check the official docs |
| 2169:0 | 125 | null | - | Strong vendor evidence found. Now let me test the two weakest links: the "ACU" unit and the cit |
| 2187:0 | 48 | null | - | I have everything I need. Verification complete. |
| 2203:0 | 54 | null | 413:0 | I'll verify this claim by fetching the primary source. |
| 2215:0 | 165 | null | - | The quote is verbatim-confirmed. Now let me test the "materially newer" and "not already in dos |
| 2253:0 | 73 | null | - | Verification complete. The quote is airtight; the novelty framing is not. |
| 2270:0 | 86 | null | - | I'll verify this claim against the primary source and hunt for contradicting evidence. |
| 2273:0 | 37 | null | - | I'll verify the primary source first. |
| 2279:0 | 138 | null | - | Primary source verified. Now let me check whether this on-disk mode actually applies to graph * |
| 2285:0 | 182 | null | - | The same primary doc re-imposes a RAM ceiling on query execution. Let me check whether the mode |
| 2291:0 | 187 | null | - | Significant: open correctness bugs and an "Analyze graph not supported for on-disk storage" iss |
| 2297:0 | 107 | null | - | I have enough to rule. The primary source verifies the factual half but disqualifies the interp |
| 2314:0 | 50 | tool_use | 25:0 | I'll verify this claim against the primary source. |
| 2335:0 | 165 | tool_use | - | Quote is verbatim accurate. Now let me check whether the "therefore no out-of-core capability"  |
| 2348:0 | 77 | null | - | Verified against the primary source and the sibling releases. Findings below. |
| 2364:0 | 65 | null | - | I'll research this angle on Aura billing and cost-shock evidence. |
| 2451:0 | 55 | null | - | I'll verify this claim by examining the primary source. |
| 2454:0 | 60 | null | - | I'll fetch the primary source to verify the quote and claim. |
| 2460:0 | 70 | null | - | WebFetch got 403. Let me try direct retrieval and search alternatives. |
| 2466:0 | 70 | null | - | The URL 301-redirects to a different page. Let me examine the content. |
| 2470:0 | 121 | null | - | Quote confirmed verbatim. Let me corroborate with the sibling billing-dimensions doc and check  |
| 2476:0 | 2038 | null | - | Verification complete. The quote is confirmed verbatim in the live primary source, and a siblin |
| 2492:0 | 34 | null | - | I'll fetch the page and assess it. |
| 2498:0 | 67 | null | - | Let me dig for the underlying source links and exact quote context. |
| 2502:0 | 108 | null | - | Let me verify the underlying primary sources this page cites, since that determines whether its |
| 2518:0 | 95 | null | - | Verified 4 of the page's quotes against primary sources via the HN API. Here is the extraction. |
| 2537:0 | 92 | null | - | I'll verify this claim by reading the primary source and hunting for contradicting evidence. |
| 2543:0 | 131 | null | - | The primary source checks out on date and author. Now let me get the full context of that post  |
| 2549:0 | 207 | null | - | Full quote is confirmed and is actually stronger than the claim's excerpt. Now the critical adv |
| 2555:0 | 110 | null | - | WebSearch is erroring at the API level. Let me route searches through WebFetch against search e |
| 2566:0 | 130 | null | - | Bing dropped my quoted operators. Let me go to authoritative primary sources directly, plus a s |
| 2572:0 | 126 | null | - | neo4j.com/docs blocks WebFetch. The GDS source repo contains the official docs, so let me grep  |
| 2576:0 | 99 | null | - | Zero hits. Let me sanity-check that the code search actually works, and probe adjacent terminol |
| 2583:0 | 77 | null | - | Let me read the current (2026) official docs language on memory requirements. |
| 2587:0 | 141 | null | - | Now let me test the strongest possible refutation: whether Neo4j announced any large-scale/tier |
| 2593:0 | 160 | null | - | Let me check the off-heap feature toggle (the closest thing to a memory-relief feature) and sea |
| 2599:0 | 81 | null | - | One last check: whether any newer (2025–2026) forum thread or docs reverses this. |
| 2605:0 | 2964 | null | - | All checks converge. Here is my adversarial assessment.  **Component 1 — is the paraphrase supp |
| 2624:0 | 92 | null | 2537:0 | I'll verify this claim by reading the primary source and hunting for contradicting evidence. |
| 2692:0 | 205 | null | - | I verified the primary source, authenticated the speaker, and actively hunted for contradicting |
| 2711:0 | 74 | null | - | I'll verify the cited URL and quote, then hunt for contradicting evidence. |
| 2727:0 | 116 | null | - | The quote and its surrounding context are verified. Now hunting for counter-evidence on the "no |
| 2786:0 | 69 | null | - | Verification complete. The claim survived every attack I could mount. |
| 2805:0 | 86 | null | 2270:0 | I'll verify this claim against the primary source and hunt for contradicting evidence. |
| 2816:0 | 184 | null | - | Core facts check out. Now testing the load-bearing framing: is Onager actually in-memory-only ( |
| 2836:0 | 2039 | null | - | Verification complete. Every checkable fact traces to primary sources; the only soft spots are  |
| 2852:0 | 71 | null | - | I'll decompose this research question into complementary search angles. |
| 2871:0 | 86 | null | 2270:0 | I'll verify this claim against the primary source and hunt for contradicting evidence. |
| 2877:0 | 184 | null | - | WebSearch budget is exhausted, but WebFetch works. Let me verify the exact verbatim wording fro |
| 2883:0 | 199 | null | - | Both the live docs page and the raw GitHub docs source confirm the quote. Now let me hunt hard  |
| 2897:0 | 49 | null | - | Verification complete. Let me record the finding. |
| 2999:0 | 854 | null | - | Research complete. Summary of what I verified and what broke.  **Cited source fails verificatio |
| 3018:0 | 49 | null | - | I'll verify the claim against the primary source. |
| 3059:0 | 138 | null | - | Every component of the claim verified against primary sources; I also checked all newer release |
| 3078:0 | 40 | null | 1380:0 | I'll verify the primary source directly. |
| 3084:0 | 58 | null | - | The primary URL returned 403. Let me try alternate routes. |
| 3122:0 | 134 | null | - | Primary source verified. Now let me test the inferential leap — whether vector-property support |
| 3136:0 | 97 | null | - | Strong lead. Let me confirm GDS already supported array properties in projections before 2026.0 |
| 3145:0 | 49 | null | - | I have enough to rule. Let me record the verdict. |
| 3161:0 | 76 | null | - | I'll verify this claim about Aura Graph Analytics session memory parameters. |
| 3164:0 | 76 | null | - | I'll fetch the primary source to verify the quote and the claim's specifics. |
| 3170:0 | 47 | null | - | WebFetch got a 403. Let me try curl and search. |
| 3182:0 | 143 | null | - | Quote confirmed verbatim. Now let me check the surrounding context and hunt for any auto-sizing |
| 3188:0 | 127 | null | - | Now let me check the GDS Python client docs for any auto-sizing/estimation API — the key qualif |
| 3199:0 | 114 | null | - | Now let me check for any elastic/autoscaling capability and confirm the doc is current (not out |
| 3210:0 | 140 | null | - | Let me hunt for contradicting evidence: autoscaling/elastic sessions, session resize, or an int |
| 3216:0 | 89 | null | - | WebSearch is erroring in this environment, so let me follow the doc's own links directly. |
| 3232:0 | 50 | null | - | Verification complete. Let me record the findings. |
| 3374:0 | 110 | null | - | I verified the claim against Neo4j's own machine-readable rate card, which turns out to be publ |
| 3392:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 3398:0 | 93 | null | - | This looks like it verifies one of the quarantined claims. Let me get exact verbatim wording. |
| 3410:0 | 1289 | null | - | Verified. This thread is the primary source for quarantined claim (a) — and the reply carries a |
| 3426:0 | 53 | null | - | I'll verify this claim by reading the primary source. |
| 3435:0 | 95 | null | - | The summary supports it, but I need the exact verbatim text. Let me get the raw Discourse JSON. |
| 3446:0 | 134 | null | - | Verbatim quote confirmed via two independent fetches. Now the outdatedness check — has Neo4j sh |
| 3464:0 | 148 | null | - | No sign Neo4j shipped out-of-core GDS (still shipping 2026.06.0). Now testing the claim's *infe |
| 3470:0 | 2090 | null | - | Verbatim text obtained twice (HTML render + Discourse JSON), so I can adjudicate precisely.  ** |
| 3487:0 | 54 | null | - | I'll verify this claim about the Neo4j Aura rate card. |
| 3556:0 | 55 | null | - | I have enough to adjudicate. Let me record the verdict. |
| 3572:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 3580:0 | 88 | null | - | Let me get the raw README and repo metadata for dates and any memory/out-of-core claims. |
| 3586:0 | 72 | null | - | Let me check the docs site and whether it makes any memory/scale claims. |
| 3592:0 | 70 | null | - | Now the critical files for the thesis: KNOWN_ISSUES.md and ROADMAP.md. |
| 3596:0 | 104 | null | - | Let me verify the underlying Graphina library's memory model and the DuckDB community-extension |
| 3602:0 | 1181 | null | - | Fetched and verified against the raw README, ROADMAP.md, KNOWN_ISSUES.md, the docs site, the Du |
| 3618:0 | 29 | null | 2032:0 | I'll fetch the Reddit thread. |
| 3624:0 | 66 | null | - | Reddit direct fetch is blocked. Let me try alternate access paths. |
| 3639:0 | 31 | null | - | A snapshot exists. Fetching it. |
| 3664:0 | 107 | null | - | Retrieved via Wayback (direct Reddit fetch blocked). Full thread text and per-comment timestamp |
| 3680:0 | 63 | null | - | I'll search for Reddit and community discussions on this topic. |
| 3698:0 | 133 | null | - | Found a direct hit on one of the quarantined claims. Let me verify the Reddit threads and sweep |
| 3735:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 3763:0 | 99 | null | - | Verification complete. The quote is exact and corroborated by a second independent vendor doc p |
| 3782:0 | 56 | null | - | I'll verify this claim by examining the primary sources. |
| 3826:0 | 93 | null | - | I verified this claim against primary sources (both repos cloned and read, HEAD as of today). |
| 3845:0 | 40 | null | 1380:0 | I'll verify the primary source directly. |
| 3903:0 | 77 | null | - | Primary-source verification complete across four independent Neo4j doc pages. |
| 3967:0 | 103 | null | - | I verified the claim against the live primary source plus three independent corroborating vendo |
| 3998:0 | 100 | null | - | Quote verified verbatim on the live page. Now checking for contradictions, dates, and scope cav |
| 4043:0 | 164 | null | - | Verification complete. The quote is verbatim in the live primary doc, and I found two additiona |
| 4095:0 | 44 | null | - | I'll fetch the Neo4j GDS release notes page. |
| 4101:0 | 56 | null | - | The page returned 403. Let me try alternate fetch paths. |
| 4107:0 | 148 | null | - | The index page elides each release's body. Let me fetch the individual release notes to check f |
| 4111:0 | 107 | null | - | Let me check the remaining recent releases and the next archive page to make the negative findi |
| 4115:0 | 354 | null | - | I have the full text of nine consecutive GDS releases (3 Nov 2025 → 8 Jul 2026). The archive in |
| 4134:0 | 77 | null | - | I'll fetch the primary source and check whether the quote supports the claim. |
| 4182:0 | 215 | null | - | I verified the primary source verbatim (twice, including the Discourse JSON) and corroborated t |
| 4198:0 | 39 | null | - | I'll fetch the page and extract claims. |
| 4204:0 | 147 | null | - | The console page returned no content (it's an authenticated single-page app). Let me verify whe |
| 4222:0 | 65 | null | - | The fetch returned content this time. Let me read the saved file. |
| 4254:0 | 615 | null | - | I have read the full rate card. Lines 1–4348 verbatim, and lines 4349–8405 enumerated exhaustiv |
| 4270:0 | 63 | null | - | I'll verify this claim by checking the primary source directly. |
| 4305:0 | 65 | null | - | Primary source confirmed. Now hunting for contradicting evidence. |
| 4335:0 | 3291 | null | - | Verification complete. Two independent primary sources checked.  **What I verified:**  1. **neo |
| 4351:0 | 29 | null | - | I'll fetch the Reddit source. |
| 4382:0 | 43 | null | - | A Wayback snapshot exists. Let me fetch it. |
| 4401:0 | 126 | null | - | Retrieved via Wayback snapshot (Reddit blocks direct fetch). Full thread and all 24 comments ex |
| 4491:0 | 53 | null | - | I'll verify this claim by loading the primary source. |
| 4499:0 | 124 | null | - | Thread confirms. Now let me get the raw verbatim text to check the quote exactly, plus verify p |
| 4505:0 | 225 | null | - | The quote is verbatim and exact. Now let me check two refutation angles: (a) whether this threa |
| 4516:0 | 88 | null | - | Significant finding. Let me confirm this exact thread is already in the internal corpus. |
| 4520:0 | 68 | null | - | Let me attempt the currency check on out-of-core GDS since Mar 2025. |
| 4528:0 | 2938 | null | - | I have enough to rule. Findings below.  **Quote accuracy: VERIFIED.** The Discourse JSON for to |
| 4545:0 | 47 | null | 122:0 | I'll verify this claim against primary sources. |
| 4628:0 | 118 | null | - | Primary sources verified directly (search engines were unavailable; vendor primary sources were |
| 4645:0 | 50 | tool_use | 25:0 | I'll verify this claim against the primary source. |
| 4714:0 | 49 | null | 2897:0 | Verification complete. Let me record the finding. |
| 4730:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 4747:0 | 134 | null | - | Fetched via curl (WebFetch returned 403). Full article body read; publish date confirmed from b |
| 4763:0 | 34 | null | - | I'll fetch the Hacker News thread. |
| 4769:0 | 91 | null | - | Rate-limited on HN directly. Let me use the Algolia API which returns the full thread tree. |
| 4778:0 | 71 | null | - | Thread retrieved and parsed in full (93 comments) via HN's Algolia API. |
| 4794:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 4800:0 | 73 | null | - | The docs site blocked the fetch. Let me try alternate paths and a search. |
| 4806:0 | 137 | null | - | Fetched successfully via the MCP fetch tool (WebFetch got 403 from neo4j.com). Full page conten |
| 4822:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 4828:0 | 95 | null | - | Let me probe for additional detail on memory, disk-based storage, and the fork's current state. |
| 4832:0 | 72 | null | - | The fork-landscape section is directly relevant. Let me get it verbatim. |
| 4854:0 | 53 | null | 3426:0 | I'll verify this claim by reading the primary source. |
| 4857:0 | 66 | null | - | I'll fetch the primary source to verify the quote and its context. |
| 4863:0 | 123 | null | - | Let me verify the exact wording, author role, and date via the Discourse API, and check what th |
| 4869:0 | 140 | null | - | I have the full thread. Let me check the historical record on that legacy mode and whether the  |
| 4875:0 | 107 | null | - | WebSearch is erroring in this environment. Let me try corroborating the historical mode via a d |
| 4886:0 | 123 | null | - | CAPTCHA-blocked; I won't attempt to bypass it. Let me corroborate the historical mode via the a |
| 4901:0 | 66 | null | - | Found the legacy implementation. Let me read what it actually was. |
| 4907:0 | 124 | null | - | I have enough ground truth: the full primary thread verbatim, plus the actual legacy source cod |
| 4923:0 | 29 | null | 2032:0 | I'll fetch the Reddit thread. |
| 4947:0 | 117 | null | - | Retrieved the full thread (post + all comments) via the Reddit JSON endpoint after the standard |
| 4997:0 | 151 | null | - | Verification complete. The quote is present verbatim in the live Neo4j vendor doc, and two inde |
| 5013:0 | 33 | null | - | I'll fetch the source page first. |
| 5021:0 | 52 | null | - | WebFetch was blocked (403). Let me try alternatives. |
| 5027:0 | 124 | null | - | Got the page. Let me quickly check the linked property-sharding post to correctly characterize  |
| 5031:0 | 381 | null | - | Fetched successfully, plus the companion property-sharding post to characterize the architectur |
| 5047:0 | 63 | null | - | I'll run the search sweep targeting the six quarantined claims. |
| 5055:0 | 52 | tool_use | - | Search tool hit a configuration error. Let me retry. |
| 5066:0 | 80 | null | - | Two strong hits already. Let me verify those and hunt the remaining four claims. |
| 5072:0 | 52 | null | - | Verified two claims. Now hunting the remaining four. |
| 5090:0 | 72 | null | - | Search connector is rate-limiting. Let me use direct API access instead. |
| 5101:0 | 93 | null | - | Claim (d) not on Hacker News. Let me check GDS issues #54/#55 for the delta-stepping details. |
| 5105:0 | 95 | null | - | Claim (b) resolves as a garbled conflation. Let me make one more attempt at claims (a) and (e). |
| 5119:0 | 87 | null | - | WebFetch works. Let me query the Neo4j community forum's Discourse search API directly. |
| 5123:0 | 53 | null | - | Found claim (a) exactly. Let me pull the full thread. |
| 5129:0 | 76 | null | - | Strong candidates for claim (e) and a fresh 2026 thread. Let me verify both. |
| 5133:0 | 59 | null | - | All six quarantined claims now resolved. Recording results. |
| 5232:0 | 133 | null | - | Verification complete. I independently pulled the vendor's live pricing catalogue and every num |
| 5248:0 | 80 | null | - | I'll verify this claim about LadybugDB's out-of-core graph analytics capability. |
| 5313:0 | 53 | null | - | I have decisive evidence. Let me compile the verdict. |
| 5412:0 | 154 | null | - | Verified the claim directly against the primary source by reproducing Neo4j's own public, unaut |
| 5428:0 | 64 | null | - | I'll verify this claim about Neo4j Aura Graph Analytics pricing. |
| 5541:0 | 1877 | null | - | I verified the claim against the primary data source. Findings:  **Located the actual data feed |
| 5557:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 5572:0 | 77 | null | - | Verbatim thread text retrieved via the Discourse JSON API. Extracting claims. |
| 5588:0 | 62 | null | - | I'll verify this claim about Onager and Graphina architecture. |
| 5638:0 | 135 | null | - | Verification complete. I read the primary sources directly (cloned the repo, read the Rust/C++  |
| 5775:0 | 82 | null | - | All numbers independently verified against Neo4j's own machine-readable rate card. |
| 5794:0 | 39 | null | - | I'll verify the release notes directly. |
| 5829:0 | 64 | null | - | Verified the primary source. Now testing the interpretive layer. |
| 5858:0 | 422 | null | - | Verification complete. Findings:  **Factual shell holds:** https://neo4j.com/release-notes/gds/ |
| 5874:0 | 42 | null | 1969:0 | I'll research this angle using web search. |
| 5877:0 | 40 | null | - | I'll run several searches on this angle. |
| 5894:0 | 63 | null | - | Strong leads. Let me dig into the most thesis-threatening ones. |
| 5900:0 | 91 | null | - | Let me verify the pivotal item — whether Onager is actually out-of-core or still in-memory. |
| 5904:0 | 50 | null | - | Research complete. Submitting the ranked findings. |
| 5923:0 | 33 | null | - | I'll fetch the GitHub issue page. |
| 5929:0 | 86 | null | - | Let me get the raw issue data via the GitHub API for exact verbatim text and comments. |
| 5941:0 | 145 | null | - | Fetched and verified via both WebFetch and the GitHub API (issue body, timeline, and the immedi |
| 6051:0 | 142 | null | - | I verified the cited source directly and found it does not serve the claimed content, plus a di |
| 6070:0 | 64 | null | - | I'll verify this claim by checking the primary sources directly. |
| 6097:0 | 3095 | null | - | I verified every load-bearing element of this claim against primary sources.  **Quote verificat |
| 6178:0 | 38 | null | - | Verification complete. Findings below. |
| 6194:0 | 58 | null | - | I'll verify this claim about Aura Graph Analytics billing. |
| 6197:0 | 68 | null | - | I'll fetch the primary source to verify the exact quote and context. |
| 6203:0 | 61 | null | - | WebFetch was blocked. Let me try curl and search in parallel. |
| 6209:0 | 103 | null | - | Key finding: the cited URL redirects to a different page (`cost-explorer`). Let me examine the  |
| 6213:0 | 143 | null | - | Quote verified verbatim. Now let me check the redirect chain, corroborate with the billing dime |
| 6219:0 | 113 | null | - | Corroborated by the billing-dimensions doc. Let me establish a date and make a final contradict |
| 6225:0 | 105 | null | - | WebSearch is unavailable in this session. Let me confirm the quote in the dated Wayback snapsho |
| 6232:0 | 2619 | null | - | Verification complete. The quote is verbatim on the live page, corroborated by a second vendor  |
| 6248:0 | 92 | null | 2537:0 | I'll verify this claim by reading the primary source and hunting for contradicting evidence. |
| 6314:0 | 68 | null | - | Primary sources fetched and verified live. Calling StructuredOutput. |
| 6333:0 | 73 | null | - | I'll fetch the primary source and check the actual configuration surface. |
| 6436:0 | 59 | null | - | I'll verify the source and hunt for contradicting evidence. |
| 6442:0 | 137 | null | - | Primary source confirms the quote verbatim and its context. Now hunting for contradicting evide |
| 6458:0 | 127 | null | - | Let me read the raw docs source to confirm the exact wording and check for any qualifications t |
| 6473:0 | 213 | null | - | Verification complete. The claim survives adversarial scrutiny — primary vendor source confirme |
| 6489:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 6506:0 | 147 | null | - | Page fetched successfully (the requested URL 301-redirects to the canonical `cost-explorer` pag |
| 6524:0 | 57 | null | 763:0 | I'll verify this claim about the Onager DuckDB extension. |
| 6527:0 | 57 | null | 763:0 | I'll verify this claim about the Onager DuckDB extension. |
| 6533:0 | 110 | null | - | Primary source confirms the description. Now let me verify the download stats and community-ext |
| 6539:0 | 219 | null | - | Both primary sources confirm the surface facts. Now the skeptical work: repo maturity, and whet |
| 6545:0 | 179 | null | - | Repo is ~10 months old and actively maintained (pushed 2026-07-22). Now the decisive question:  |
| 6551:0 | 150 | null | - | README says "Onager is in early development" and that it wraps **Graphina**, a Rust in-memory g |
| 6557:0 | 132 | null | - | Let me check fairly whether Onager has *any* memory-estimation or out-of-core capability before |
| 6561:0 | 89 | null | - | `docs/guide/graph-registry.md` is the analog of GDS's in-memory graph catalog — decisive. |
| 6565:0 | 131 | null | - | Verification complete. The factual scaffolding holds, but the load-bearing competitive framing  |
| 6581:0 | 42 | null | 1969:0 | I'll research this angle using web search. |
| 6597:0 | 116 | null | - | Found one potentially significant lead (Neo4j Infinigraph). Let me dig into practitioner writeu |
| 6634:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 6646:0 | 131 | null | - | Cross-verified across three fetches (HTML render twice plus the Discourse `.json` endpoint), wi |
| 6712:0 | 179 | null | - | Verification complete. The quote is verbatim-confirmed at the cited URL, the author is confirme |
| 6728:0 | 33 | null | 5923:0 | I'll fetch the GitHub issue page. |
| 6734:0 | 57 | null | - | Let me verify the exact verbatim text via the GitHub API. |
| 6744:0 | 916 | null | - | Verified via both WebFetch and the GitHub API (exact body text, close state, timeline).  Key fi |
| 6763:0 | 40 | null | 1380:0 | I'll verify the primary source directly. |
| 6777:0 | 60 | null | - | WebSearch is erroring; I'll route searches through WebFetch. |
| 6807:0 | 1818 | null | - | Verification complete. Summary of what I found before reporting:  **Primary source verified thr |
| 6823:0 | 53 | null | 3426:0 | I'll verify this claim by reading the primary source. |
| 6831:0 | 189 | null | - | The summary confirms much of it. Let me get verbatim raw text via the Discourse JSON API to con |
| 6837:0 | 96 | null | - | Primary source verified verbatim. Now let me test whether the claim is outdated or contradicted |
| 6843:0 | 111 | null | - | WebSearch is erroring in this environment. Let me use WebFetch against search engines and vendo |
| 6868:0 | 64 | null | - | Primary source read in full and verbatim. Here is my assessment. |
| 7112:0 | 70 | null | - | I'll verify this claim about AWS Neptune Analytics pricing arithmetic. |
| 7118:0 | 62 | null | - | Let me verify the arithmetic and hunt for contradicting facts. |
| 7124:0 | 180 | null | - | The arithmetic is exact. Now let me check whether the derived $0.03/m-NCU rate matches AWS's ac |
| 7130:0 | 158 | null | - | Decisive confirmation from AWS's own Price List API. Let me check the two possible weak points: |
| 7136:0 | 169 | null | - | All 17 tiers are exactly $0.03/GB-hr, and 12288 m-NCU is a real purchasable SKU. Let me check t |
| 7140:0 | 91 | null | - | Let me check for contradicting sources and confirm the large tiers are generally available. |
| 7146:0 | 74 | null | - | WebSearch is broken as warned. Let me pull the AWS docs directly via curl. |
| 7150:0 | 159 | null | - | Verification complete. The claim checks out precisely against AWS's own authoritative price lis |
| 7166:0 | 69 | null | - | I'll verify this claim about the FinOps Foundation site search index. |
| 7228:0 | 2397 | null | - | I verified everything directly against the primary source, then ran controls the claim did not. |
| 7291:0 | 112 | null | - | Verification complete. The press release and quote are real; the interpretation layered on top  |
| 7307:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 7311:0 | 123 | null | - | The top-line award is confirmed verbatim. Now let me verify the competition/set-aside details f |
| 7315:0 | 198 | null | - | All fields check out verbatim. Now let me adversarially test: is this really the largest, is th |
| 7319:0 | 180 | null | - | "28 prime awards" confirmed exactly, and zero IDVs — so nothing larger is hiding in a vehicle.  |
| 7323:0 | 185 | null | - | Base obligation stands with no de-obligation (all 3 mods are $0 administrative). Let me cross-c |
| 7327:0 | 130 | null | - | FPDS independently corroborates the headline figure. Let me extract the FPDS competition fields |
| 7334:0 | 118 | null | - | Every element verified against **two independent primary government sources**. The claim surviv |
| 7350:0 | 86 | null | - | I'll verify this claim by directly querying the FinOps Foundation search index myself. |
| 7369:0 | 77 | null | - | Credentials confirmed independently. Now let me replicate the queries myself. |
| 7373:0 | 161 | null | - | The empty-query parse failed on a control character. Let me fix that and — critically — sanity- |
| 7377:0 | 137 | null | - | Index is live and returns hits for control terms. Now the decisive test — what does this index  |
| 7381:0 | 124 | null | - | The `content` field looks truncated. Let me measure that across the corpus — this determines wh |
| 7385:0 | 143 | null | - | Major finding: 875 of 1000 docs have **empty** content, and lengths cap at ~7996. Let me confir |
| 7391:0 | 169 | tool_use | - | Truncation confirmed — tails cut mid-word ("deployment fre", "career de"). Now let me find whet |
| 7399:0 | 109 | null | - | WebSearch is broken as warned. Let me crawl the real sitemap and grep actual page bodies — the  |
| 7403:0 | 104 | null | - | Only 431 sitemap URLs vs 1134 indexed docs. Let me pull all indexed URLs and crawl the real pag |
| 7407:0 | 120 | null | - | Note: 279 indexed docs are YouTube videos, 34 podcasts — title-only records. Now the decisive c |
| 7413:0 | 80 | null | - | All 592 pages crawled (85MB). Now grep the real bodies for graph-DB terminology. |
| 7417:0 | 122 | null | - | A direct counterexample: "knowledge graph" appears in real page content but Algolia returned nb |
| 7421:0 | 87 | null | - | Decisive. Let me verify this page's indexed record and check broader graph terminology. |
| 7427:0 | 83 | null | - | I have a decisive result. Let me confirm the index-coverage artifact one final way. |
| 7431:0 | 1918 | null | - | I independently replicated the claim's numbers exactly — and then broke it.  **What holds:** Qu |
| 7447:0 | 84 | null | - | I'll verify this claim by independently querying the FinOps Foundation search index. |
| 7516:0 | 181 | null | - | I independently reproduced every number in the claim, then stress-tested the instrument with co |
| 7532:0 | 72 | null | - | I'll verify this claim by checking the finops.org search index directly. |
| 7555:0 | 97 | null | - | Numbers match exactly. Now let me test whether the index is actually as comprehensive as claime |
| 7564:0 | 77 | null | - | Critical finding: `content` is empty on nearly every record. Let me quantify. |
| 7576:0 | 107 | null | - | Let me check whether the one on-point article's body mentions graph databases — the index can't |
| 7580:0 | 89 | null | - | I have enough to rule. Let me confirm the reproduction method is identical to the source. |
| 7599:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 7605:0 | 72 | null | - | The quote is confirmed present. Let me get the full surrounding context. |
| 7609:0 | 178 | null | - | The verbatim quote checks out. Now let me examine what Business Critical actually *is* — whethe |
| 7616:0 | 127 | null | - | Now let me check what AuraDB Enterprise cost *before* Sept 2024, via the Internet Archive, to t |
| 7622:0 | 187 | null | - | Significant: the Aug 28, 2024 snapshot — a week **before** the press release — already lists "A |
| 7626:0 | 124 | null | - | Let me check the accompanying blog post for motive language, and confirm the Business Critical  |
| 7636:0 | 65 | null | - | I have enough primary-source evidence. Let me record the verdict. |
| 7652:0 | 64 | null | 6070:0 | I'll verify this claim by checking the primary sources directly. |
| 7658:0 | 141 | null | - | Both awards verified. Now let me check the surrounding facts — recipient, whether the scope was |
| 7664:0 | 115 | null | - | Let me check whether DEA actually renewed again in FY2025 — that's decisive for "absorbed it ra |
| 7670:0 | 132 | null | - | No DEA Neo4j renewal appears in FY2025/FY2026. Let me verify that absence carefully and check w |
| 7676:0 | 165 | null | - | The FAA bought a "Neo4j Discovery" license for $69,300 in Feb 2026 — let me check that, since i |
| 7682:0 | 3195 | null | - | I have decisive evidence. Let me compile the verification.  **What checks out:** Both awards ar |
| 7698:0 | 72 | null | - | I'll fetch the Neo4j pricing page and compare against Wayback snapshots. |
| 7713:0 | 62 | null | - | Now let me pull Wayback snapshots to compare prices over time. |
| 7744:0 | 82 | null | - | I have the full comparison. Let me verify the key verbatim strings one final time. |
| 7763:0 | 65 | null | - | I'll verify this claim about AWS Neptune Analytics m-NCU pricing. |
| 7769:0 | 104 | null | - | Now let me check the AWS Neptune Analytics documentation for how m-NCU capacity is actually pro |
| 7775:0 | 49 | null | - | Docs are JS-rendered. Let me fetch them directly. |
| 7795:0 | 117 | null | - | Let me pull the actual pricing table data (JS-loaded) to check whether m-NCU values are continu |
| 7813:0 | 117 | null | - | Let me confirm the capacity values are genuinely quantized (not arbitrary integers) from a seco |
| 7834:0 | 46 | tool_use | - | I have enough primary evidence. Let me report. |
| 7855:0 | 26 | null | - | I'll run the searches now. |
| 7861:0 | 63 | null | - | WebSearch is broken as warned. Falling back to WebFetch + curl. |
| 7915:0 | 52 | null | - | I'll research this using the available search tools. |
| 7918:0 | 24 | null | - | I'll run the search now. |
| 7924:0 | 91 | null | - | WebSearch is broken as warned. Switching to WebFetch against the review platforms directly. |
| 7930:0 | 104 | null | - | PeerSpot's main page is thin. Let me hit G2, TrustRadius, Gartner, and PeerSpot's cons/comparis |
| 7945:0 | 87 | null | - | G2/TrustRadius/Capterra/Gartner are all bot-blocked. Let me find a working search path. |
| 7963:0 | 130 | null | - | Vendr worked and has enterprise-segmented dollar figures. Let me try a reader proxy to get past |
| 7972:0 | 130 | null | - | TrustRadius works via the reader proxy and includes employee-count bands. Let me pull more page |
| 7978:0 | 132 | tool_use | - | Page 1 of TrustRadius has zero pricing mentions — that itself is a finding. Let me read the ful |
| 7982:0 | 132 | null | - | Confirmed: TrustRadius has zero pricing complaints across 9 reviews. Let me check the pricing p |
| 7986:0 | 77 | null | - | Let me try the Gartner Peer Insights API and a couple more segmented sources. |
| 7995:0 | 51 | null | - | I have enough to rank. Let me compile the findings. |
| 8011:0 | 52 | null | - | I'll fetch the PeerSpot page for Neo4j Aura reviews. |
| 8052:0 | 203 | null | - | I have the full verbatim review corpus. Key structural finding: the requested URL 301-redirects |
| 8068:0 | 65 | null | 7763:0 | I'll verify this claim about AWS Neptune Analytics m-NCU pricing. |
| 8155:0 | 127 | null | - | I have decisive primary-source evidence. Let me verify one last detail on the replica multiplie |
| 8174:0 | 70 | null | - | I'll verify this claim about Neo4j's InfiniGraph pricing announcement. |
| 8183:0 | 161 | null | - | Quote confirmed verbatim. Now let me test the inferential leap — is separate compute/storage bi |
| 8189:0 | 130 | null | - | WebSearch is broken as the task warned. Let me find the linked blog post and check Neo4j's actu |
| 8195:0 | 149 | null | - | The HN reaction frames Infinigraph as sharding catch-up, not pricing action. Let me read the te |
| 8199:0 | 199 | null | - | Key finding: the blog version omits the "control over cost" phrase. Let me check whether any *i |
| 8205:0 | 112 | null | - | Major finding on Neo4j's own pricing page. Let me pin down the exact competing language and che |
| 8211:0 | 95 | null | - | Let me confirm the product-line mismatch — paul.horn's comment was about GDS, not the database. |
| 8220:0 | 49 | null | 3145:0 | I have enough to rule. Let me record the verdict. |
| 8236:0 | 126 | null | - | I'll verify this claim about Neo4j's Aura Graph Analytics GA and its timing relative to the Mar |
| 8245:0 | 167 | null | - | The press release confirms date and pay-as-you-use language. Now let me find the original staff |
| 8276:0 | 98 | null | - | Key finding emerging. Let me check session memory limits and hunt the original March 2025 comme |
| 8297:0 | 85 | null | - | Found the original comment on the Neo4j community forum. Let me get its full context. |
| 8303:0 | 91 | null | - | The full thread is decisive. Let me verify the session size cap from Neo4j's own Aura docs. |
| 8307:0 | 67 | null | - | I have decisive primary-source evidence. Let me record the verdict. |
| 8324:0 | 125 | null | - | I'll try to reproduce the Algolia queries against finops.org and test whether the index actuall |
| 8360:0 | 100 | null | - | The Algolia index misses content that provably exists on the site. Let me quantify the coverage |
| 8378:0 | 114 | null | - | The FOCUS spec is the Foundation's own standard. Let me check whether it names graph databases  |
| 8392:0 | 71 | null | - | Found a direct contradiction. Let me verify the canonical URL and date. |
| 8404:0 | 138 | null | - | I have everything needed. The claim's numbers reproduce exactly, but both its scope characteriz |
| 8420:0 | 64 | null | 6070:0 | I'll verify this claim by checking the primary sources directly. |
| 8431:0 | 132 | null | - | Core figures confirmed. Now testing the load-bearing inferences: same-SKU vs. expansion, and wh |
| 8437:0 | 126 | null | - | Critical test: does DEA appear again after FY2024? Let me check for a FY2025/FY2026 renewal and |
| 8443:0 | 92 | null | - | Confirmed only two DEA awards. Now verifying the data isn't just missing recent DEA records. |
| 8449:0 | 152 | null | - | Data is fresh through May 2026 and ThunderCat still wins DEA software deals — but no Neo4j rene |
| 8463:0 | 80 | null | - | Now the decisive check: DOJ-wide Neo4j purchases after the FY2024 order expired. |
| 8472:0 | 81 | null | - | I have everything needed. The dollar figures are real; the interpretation is not. |
| 8488:0 | 48 | null | - | I'll fetch the PeerSpot Neo4j Aura pricing page. |
| 8508:0 | 918 | null | - | I retrieved the page via WebFetch and cross-verified with a raw curl of both the pricing page a |
| 8524:0 | 92 | null | - | I'll verify this claim about Neo4j Infinigraph. Let me start by fetching the primary source. |
| 8530:0 | 69 | null | - | Let me read the full press release and then check the technical docs. |
| 8536:0 | 182 | null | - | Key factual elements confirmed verbatim. Now the critical test: does Infinigraph actually addre |
| 8547:0 | 89 | null | - | The HN thread points to a technical blog by a Neo4j engineer. That's the decisive source. |
| 8555:0 | 73 | null | - | The blog body is JS-rendered. Let me extract the raw content differently. |
| 8561:0 | 91 | null | - | This is decisive. Let me extract the full article body, especially the limitations section. |
| 8565:0 | 88 | null | - | Let me check the current docs status and whether GDS actually runs on sharded databases. |
| 8571:0 | 118 | null | - | Major finding: Infinigraph went **GA on 2026-01-27**, ~5 months after the press release. Let me |
| 8577:0 | 101 | null | - | I have what I need. The claim's load-bearing inference is contradicted by Neo4j's own technical |
| 8593:0 | 68 | null | - | I'll fetch the HN Algolia API endpoint for "neo4j pricing" comments. |
| 8602:0 | 95 | null | - | Let me check the specific claims mentioned in the title and get fuller context on the key hits. |
| 8606:0 | 101 | null | - | Strong material found. Let me verify the specific "$70k" phrasing and check the NASA/Memgraph t |
| 8613:0 | 36 | null | - | Search complete. Recording findings. |
| 8629:0 | 78 | null | - | I'll verify this claim by checking both the Wayback capture and the live page. |
| 8643:0 | 136 | null | - | Both strings verified. Now let me test the weaker parts: intermediate captures (to test "zero m |
| 8652:0 | 128 | null | - | Let me check whether the Graph Analytics tab gained or lost tiers (a cheaper-tier addition woul |
| 8675:0 | 47 | null | 122:0 | I'll verify this claim against primary sources. |
| 8686:0 | 121 | null | - | Press release verified. Now the other half — the paul.horn comment and what the product's *stat |
| 8692:0 | 132 | null | - | The press release explicitly states TCO/provisioning benefits. Now let me locate the paul.horn  |
| 8703:0 | 141 | null | - | Confirmed the forum post exists at that exact date. Let me read its full context and check whet |
| 8709:0 | 95 | null | - | Now let me check whether the product predates the comment, and what the actual billing unit is. |
| 8715:0 | 92 | null | - | A Neo4j blog titled "The persistence tax" appeared. Let me get it and the pricing mechanics. |
| 8726:0 | 82 | null | - | Let me check whether the serverless/session model predates the March 2025 comment. |
| 8737:0 | 102 | tool_use | - | Let me pin down the "persistence tax" blog post URL and date — it corroborates the claim's key  |
| 8744:0 | 180 | null | - | Verification complete. I confirmed the primary source and found additional Neo4j primary source |
| 8760:0 | 66 | null | - | I'll fetch the FinOps Foundation search page for "graph database". |
| 8766:0 | 54 | null | - | Let me verify with a direct fetch to see the raw page. |
| 8773:0 | 104 | null | - | The page is JS-rendered ("Loading..."), so I need to hit the underlying search API to get a rea |
| 8786:0 | 56 | null | - | Now I can query the site's actual search index directly. |
| 8790:0 | 87 | null | - | Let me run control queries to validate that the index is functional and well-populated. |
| 8800:0 | 60 | null | - | I have a well-controlled result. Let me record the findings. |
| 8816:0 | 73 | null | - | I'll verify this claim by checking archived and live Neo4j pricing pages. |
| 8858:0 | 78 | null | - | I'll verify this claim about the Neo4j AuraDB Business Critical press release. |
| 8866:0 | 119 | null | - | The primary quote is confirmed verbatim. Now let me check the surrounding context and whether t |
| 8870:0 | 99 | null | - | Now let me test the key inferential leap against archived pricing history and independent cover |
| 8876:0 | 75 | null | - | WebSearch is broken as warned. Let me pull archived pricing pages directly. |
| 8880:0 | 84 | null | - | Significant finding. Let me pin down when Business Critical actually first appeared. |
| 8889:0 | 49 | null | - | I have enough to rule. Let me record the finding. |
| 8905:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 8909:0 | 69 | null | - | Let me pull the detailed records for the three specific awards cited. |
| 8913:0 | 117 | null | - | Now let me check the transaction/modification history and look for overlapping IRS awards the c |
| 8925:0 | 76 | null | - | The $0.00 sibling award names the actual SKU. Let me pull full descriptions. |
| 8929:0 | 138 | null | - | Let me check FPDS for the underlying scope of the 2024 vs 2025 awards, to test whether "spend g |
| 8936:0 | 92 | null | - | The NASIC and DOJ renewals give me a clean like-for-like control. Let me pull the full list. |
| 8940:0 | 92 | null | - | Let me verify the FAA control case (identical amounts to the cent could be a data artifact). |
| 8947:0 | 179 | null | - | Verification complete. I confirmed every dollar figure against the primary source, then found t |
| 8963:0 | 64 | null | 6070:0 | I'll verify this claim by checking the primary sources directly. |
| 8977:0 | 155 | null | - | Core numbers check out. Now let me attack the "scope drift ruled out" inference — checking tran |
| 8983:0 | 97 | null | - | Found a critical comparator. Let me check FPDS directly and look for prior/subsequent DEA order |
| 8995:0 | 150 | null | - | Both primary sources independently confirm the claim. Let me do a final check on the one soft s |
| 8999:0 | 110 | null | - | I independently re-derived every assertion from two authoritative primary sources. Every checka |
| 9015:0 | 39 | null | - | I'll fetch the USAspending award pages. |
| 9018:0 | 43 | null | - | I'll fetch the two USAspending award pages. |
| 9024:0 | 103 | null | - | USAspending is a JavaScript SPA — WebFetch got an empty shell. Let me use the USAspending API d |
| 9030:0 | 99 | null | - | Both records confirmed. Let me get the full untruncated description and check for a FY2025 rene |
| 9036:0 | 108 | null | - | Let me pull the full untruncated description from FPDS to check whether scope changed between t |
| 9045:0 | 200 | null | - | Both records verified directly from the official USAspending API (the web pages are JS-rendered |
| 9062:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 9074:0 | 136 | null | - | Primary source confirms the quote and date. Now testing the inferential leap — whether this was |
| 9097:0 | 94 | null | - | Verification complete. The factual shell checks out; the strategic inference does not survive. |
| 9113:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 9117:0 | 54 | null | - | Now let me find the specific award cited in the claim. |
| 9121:0 | 63 | null | - | Let me pull the full award record and check the related awards. |
| 9128:0 | 121 | null | - | Let me check for other Neo4j awards showing RAM/core tiers, and probe whether price actually va |
| 9140:0 | 63 | null | - | WebSearch is broken as warned. Using WebFetch and curl instead. |
| 9149:0 | 75 | null | - | Let me get the untruncated description from FPDS, the authoritative source. |
| 9153:0 | 72 | null | - | Significant: the $0 is a de-obligation. Let me get the full FPDS record. |
| 9160:0 | 168 | null | - | The de-obligation is key — this was originally a **$4,498,388.92** obligation. Let me verify th |
| 9167:0 | 128 | null | - | Now let me test the inferential leap — does price actually scale with RAM? Checking Neo4j's own |
| 9171:0 | 128 | null | - | Neo4j's own pricing page confirms RAM-as-billing-unit for Aura. Let me test for contradicting e |
| 9177:0 | 99 | null | - | Found a reference to a published Neo4j commercial price list. Let me check the actual pricing a |
| 9183:0 | 65 | null | - | Let me read that price list directly rather than trust a summary. |
| 9187:0 | 113 | null | - | This is a serious counter-finding. Let me read the rest of the price list and check whether RAM |
| 9191:0 | 170 | null | - | Decisive: zero mentions of RAM/memory/GB in the 2018 official price list; 30 mentions of "core" |
| 9195:0 | 83 | null | - | Let me do a final check for any current self-hosted price ladder showing RAM tiers. |
| 9202:0 | 103 | null | - | I have enough to decide. Let me verify one last time that the quote is character-accurate again |
| 9206:0 | 75 | null | - | I attempted to refute this claim on five fronts and it survived. Reporting. |
| 9222:0 | 40 | null | - | I'll fetch the AWS Neptune pricing page. |
| 9225:0 | 44 | null | - | I'll fetch the AWS Neptune pricing page now. |
| 9229:0 | 67 | null | - | Let me get more detail on the m-NCU rates and minimum provisioning. |
| 9247:0 | 43 | null | - | I have what I need. Here is the extraction. |
| 9263:0 | 64 | null | - | I'll verify this claim by examining the primary source directly. |
| 9273:0 | 123 | null | - | Quote verified verbatim. Now let me test the claim's *interpretation* against the same document |
| 9284:0 | 58 | null | - | WebSearch is broken as warned. Using curl with browser UA. |
| 9300:0 | 80 | null | - | Evidence gathered. The contradicting material is inside the cited source itself. |
| 9338:0 | 64 | null | 6070:0 | I'll verify this claim by checking the primary sources directly. |
| 9344:0 | 120 | null | - | Both primary records confirm. Now let me check the full descriptions and whether the relationsh |
| 9350:0 | 139 | null | - | Critical signal: no DEA Neo4j award appears after FY2024, while FY2026 awards for other agencie |
| 9356:0 | 121 | null | - | DEA kept buying other software via the same reseller but no Neo4j line appears. Let me widen th |
| 9362:0 | 156 | null | - | Two things to nail down: whether DEA renewed after FY2024, and whether a comparator agency show |
| 9368:0 | 102 | tool_use | - | Let me test the scope-change hypothesis by checking whether the "Discovery Bundle" SKU is size- |
| 9376:0 | 132 | null | - | WebSearch is broken as the brief warned. Let me get the full IRS SKU description, which reveals |
| 9380:0 | 52 | null | - | I have everything needed. Let me record the verdict. |
| 9396:0 | 67 | null | - | I'll verify this claim about AuraDB Professional pricing stability. |
| 9432:0 | 111 | null | - | Verification complete. I checked the primary source live plus ten Wayback captures spanning the |
| 9448:0 | 29 | null | - | I'll fetch the press release. |
| 9454:0 | 90 | null | - | neo4j.com blocks WebFetch. Using curl with a browser User-Agent as the task notes suggest. |
| 9463:0 | 65 | null | - | Retrieved the full press release via curl. Extracting the claims. |
| 9535:0 | 79 | null | - | I'll verify this claim against the primary source — USAspending.gov award data. |
| 9539:0 | 85 | null | - | Let me get the complete award list and then drill into the three specific IRS awards. |
| 9545:0 | 111 | null | - | Now let me examine the transaction histories, PSC codes, and the $0 award with the detailed bun |
| 9551:0 | 124 | null | - | The $0 award from 2025-04-02 has a detailed configuration description. Let me get its full text |
| 9557:0 | 97 | null | - | Let me check PSC/NAICS codes on the three headline awards and try FPDS for scope/quantity detai |
| 9566:0 | 135 | null | - | I have enough to render a verdict. Let me do one final check on total IRS Neo4j obligations by  |
| 9570:0 | 134 | null | - | Verification complete against two independent primary systems (USAspending API and FPDS.gov ATO |
| 9586:0 | 58 | null | - | I'll research this angle using the available search tools. |
| 9589:0 | 108 | null | - | I'll start searching for evidence on enterprise migrations away from Neo4j and cost as trigger  |
| 9595:0 | 88 | null | - | WebSearch is broken as the task warned. Switching to the confirmed-working alternatives. |
| 9629:0 | 61 | null | - | WebFetch on DuckDuckGo HTML works. Running targeted searches. |
| 9650:0 | 40 | null | - | Major hit. Fetching the primary sources. |
| 9692:0 | 70 | null | - | Found the exact trigger-vs-justifier material. Reading the full sweep. |
| 9718:0 | 213 | null | - | Search engines are now rate-limited/CAPTCHA'd across DDG, Brave, Mojeek and searx, but I extrac |
| 9734:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 9776:0 | 122 | null | - | Verification complete. Every factual element checks out against the primary source and a contem |
| 9792:0 | 54 | null | - | I'll fetch the source and extract the relevant claims. |
| 9801:0 | 57 | null | - | Let me get the raw page text to confirm verbatim wording. |
| 9809:0 | 61 | null | - | I have the full verbatim article text. Extracting claims now. |
| 9825:0 | 41 | null | - | I'll fetch the source and extract claims. |
| 9831:0 | 44 | null | - | HN rate-limited. Let me try the Algolia API. |
| 9835:0 | 57 | null | - | Got the comment. Let me get the parent context and story. |
| 9839:0 | 40 | null | - | Let me pull the full thread for context. |
| 9843:0 | 63 | null | - | Let me check the cited 2018 pricing page for the dollar anchor. |
| 9850:0 | 81 | null | - | I have the full thread plus the cited pricing source. Let me record the findings. |
| 9866:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 9873:0 | 139 | null | - | The two data points exist. Now let me pull the full award records to check the truncated descri |
| 9880:0 | 186 | null | - | Both records confirm the claim's specifics. Now let me stress-test the "not explainable by scop |
| 9894:0 | 119 | null | - | WebSearch is broken as warned. Let me query the primary dataset harder for scope-change signals |
| 9925:0 | 191 | null | - | I verified the claim against both USAspending and FPDS-NG (the authoritative system of record), |
| 9941:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 9945:0 | 185 | null | - | Confirmed the three awards exist. Now let me check the details — obligation vs. potential value |
| 9949:0 | 114 | null | - | Now let me check the transaction history and pull the complete set of IRS Neo4j awards to test  |
| 9953:0 | 117 | null | - | Let me pull the full description of the sibling award — it names the actual SKU, which reveals  |
| 9957:0 | 75 | null | - | Let me check FPDS directly for the full contract text and line-item detail. |
| 9961:0 | 98 | null | - | This is important — let me verify the overlapping AlphaSix contract that was active during FY20 |
| 9965:0 | 206 | null | - | I have decisive evidence from the same primary source. Let me record the findings.  The claim's |
| 9981:0 | 51 | null | - | I'll verify this claim against the primary sources. |
| 9987:0 | 124 | null | - | Both records confirmed at the primary source. Now let me probe for scope drift, modifications,  |
| 9993:0 | 129 | null | - | Now let me hunt for refutations: prior-year awards, scope details in FPDS, and other Neo4j fede |
| 10002:0 | 167 | null | - | FPDS independently confirms both records. Let me stress-test the "scope drift ruled out" infere |
| 10008:0 | 143 | null | - | The IRS record confirms Neo4j federal SKUs are RAM/core-tiered — let me test whether "Discovery |
| 10017:0 | 109 | null | - | Verification complete. Every factual element of the claim checks out against two independent pr |
| 10033:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 10037:0 | 94 | null | - | Now let me pull the full award detail record to verify the sole-source and competition fields. |
| 10041:0 | 117 | null | - | Let me adversarially check the count, whether the award was later modified/de-obligated, and th |
| 10048:0 | 120 | null | - | Let me check whether any grant/direct-payment Neo4j award exceeds it, since the claim says "sin |
| 10052:0 | 2713 | null | - | I independently reproduced every element of this claim against the live USAspending API. Verifi |
| 10068:0 | 70 | null | 7112:0 | I'll verify this claim about AWS Neptune Analytics pricing arithmetic. |
| 10077:0 | 134 | null | - | The verbatim quote checks out on the live page. Now let me verify the underlying rate independe |
| 10088:0 | 99 | null | - | Excellent — AWS actually publishes a 12288 m-NCU SKU. Let me pull the authoritative per-hour ra |
| 10092:0 | 156 | null | - | The AWS Price List API confirms the rate exactly and flat across all sizes. Let me date-check t |
| 10103:0 | 1884 | null | - | I have everything needed. Every number independently reproduced from a machine-readable primary |
| 10119:0 | 70 | null | 7112:0 | I'll verify this claim about AWS Neptune Analytics pricing arithmetic. |
| 10125:0 | 65 | null | - | Let me verify the arithmetic and the actual published m-NCU rate. |
| 10131:0 | 78 | null | - | Excellent — there's a 12288 m-NCU SKU. Let me pull the actual on-demand rates. |
| 10135:0 | 86 | null | - | Decisive. Let me cross-check regions and the EC2 comparison that bears on the framing. |
| 10141:0 | 93 | null | - | Verification complete. The claim checks out against a second, independent AWS primary source. |
| 10157:0 | 63 | null | - | I'll verify this claim by checking Neo4j's actual pricing page. |
| 10198:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 10202:0 | 52 | null | - | Let me get the full award detail for 2032H825F00052. |
| 10206:0 | 140 | null | - | Now let me look for other Neo4j SKU tiers in federal procurement to test whether RAM is actuall |
| 10223:0 | 169 | null | - | The quote is verified verbatim. Now let me test the inferential step — is RAM actually a pricin |
| 10230:0 | 126 | null | - | Strong corroboration on the Aura side. Now let me check the self-managed licensing terms and in |
| 10240:0 | 83 | null | - | Excellent — a practitioner points to a published Neo4j price list. Let me fetch it. |
| 10247:0 | 164 | null | - | Important nuance emerging — the 2018 list is priced by CORES and INSTANCES, with no RAM axis. L |
| 10260:0 | 118 | null | - | One more check — let me try to find a current public Neo4j price list showing whether RAM tiers |
| 10270:0 | 55 | null | - | I've completed verification. Let me record the finding. |
| 10286:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 10290:0 | 133 | null | - | Both awards confirmed in the API. Now let me verify the details — buyer identity, signed dates, |
| 10302:0 | 171 | null | - | Every element checks out so far — same DEA buyer, same SKU string, dates match exactly. Now let |
| 10308:0 | 142 | null | - | Let me pull the full award list to look for corroborating or contradicting renewal patterns, an |
| 10312:0 | 99 | null | - | Critical lead — one award reveals Neo4j's federal SKU naming is RAM-tiered. Let me pull it in f |
| 10318:0 | 123 | tool_use | - | Now the key test: can I find the actual "Discovery Bundle" price list to determine whether the  |
| 10324:0 | 78 | null | - | WebSearch is broken as warned. Falling back to direct fetches and API queries. |
| 10331:0 | 89 | null | - | Let me try harder to resolve the scope-creep question — this is the claim's weakest link. |
| 10342:0 | 140 | null | - | I've independently verified the claim against **two** separate government systems. Let me do a  |
| 10346:0 | 2529 | null | - | I attempted to refute this claim and could not. Every checkable element reproduced exactly agai |
| 10362:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 10373:0 | 173 | null | - | Fetched the page and extracted the full text via curl. Key finding: **the article is about Dgra |
| 10389:0 | 38 | null | - | I'll fetch the Vendr buyer guide page. |
| 10392:0 | 48 | null | - | I'll fetch the Vendr buyer guide page for Neo4j. |
| 10396:0 | 98 | null | - | Let me get the fuller raw text to verify specific figures and check for the enterprise-tier cla |
| 10405:0 | 79 | null | - | Full page retrieved (not paywalled, 200 OK, 623 lines of text). Now extracting. |
| 10421:0 | 61 | null | - | I'll fetch the press release and extract the relevant claims. |
| 10427:0 | 90 | null | 9454:0 | neo4j.com blocks WebFetch. Using curl with a browser User-Agent as the task notes suggest. |
| 10436:0 | 553 | null | - | Fetched successfully via curl (WebFetch 403s on neo4j.com, as the task noted). Full text extrac |
| 10452:0 | 52 | null | - | I'll fetch the press release and extract the claims. |
| 10458:0 | 76 | null | - | neo4j.com blocks WebFetch as expected. Using curl with a browser User-Agent. |
| 10467:0 | 123 | null | - | Fetched successfully via curl (WebFetch returned 403 as anticipated). Full press release text e |
| 10483:0 | 80 | null | - | I'll verify this claim by examining the primary source and checking the framing. |
| 10493:0 | 99 | null | - | The quote is verbatim-verified. Now let me check the rest of the same source and external evide |
| 10500:0 | 74 | null | - | I have what I need. The refutation is internal to the same primary source. |
| 10516:0 | 61 | null | - | I'll research this angle using web search and direct fetches. |
| 10519:0 | 61 | null | - | I'll start searching on the FinOps / cost-optimization angle. |
| 10525:0 | 92 | null | - | WebSearch is broken as the task warned. Pivoting to the confirmed-working APIs and WebFetch. |
| 10531:0 | 95 | null | - | HN is thin here. Let me try search engines via WebFetch and target the FinOps vendors directly. |
| 10546:0 | 101 | null | - | Bing via WebFetch isn't returning real results. Trying Mojeek plus the FinOps-vendor primary so |
| 10582:0 | 99 | null | - | Found a strong cost-as-justifier datapoint. Let me read the full HN results and the thread cont |
| 10588:0 | 87 | null | - | Rich vein found. Let me pull the full 2026 HN thread and the Capacities migration post. |
| 10605:0 | 77 | null | - | Let me firm up the FinOps negative finding and check Neptune cost complaints. |
| 10616:0 | 49 | null | - | Evidence gathered. Compiling the ranked findings. |
| 10632:0 | 54 | null | 413:0 | I'll verify this claim by fetching the primary source. |
| 10635:0 | 50 | null | - | I'll fetch the primary source to verify the claim. |
| 10639:0 | 75 | null | - | Expected — neo4j.com blocks WebFetch. Using curl with a browser User-Agent. |
| 10651:0 | 161 | null | - | Primary source confirms the factual core. Now let me adversarially test the inference — is "TCO |
| 10655:0 | 141 | null | - | Exactly 4 bullets confirmed. Now testing the strongest refutation angle: is "lower TCO" just re |
| 10667:0 | 172 | null | - | Verification complete. All checkable elements verified against the primary source, and I additi |
| 10683:0 | 69 | null | 7166:0 | I'll verify this claim about the FinOps Foundation site search index. |
| 10744:0 | 168 | null | - | I reproduced the claim's counts directly against the search backend behind `https://www.finops. |
| 10760:0 | 91 | null | - | I'll verify this claim about Neo4j's CPO framing infrastructure cost as an adoption hurdle. |
| 10770:0 | 81 | null | - | Quote verified verbatim. Now let me check the full document context and the date. |
| 10774:0 | 166 | null | - | Quote confirmed verbatim, dated May 7, 2025. Now let me test the load-bearing inference — that  |
| 10783:0 | 81 | null | - | I'll search for external corroboration or contradiction on how Neo4j frames cost. |
| 10789:0 | 169 | null | - | WebSearch is broken as warned, and HN has zero hits on Hasbe or Aura Graph Analytics. Let me ch |
| 10793:0 | 124 | null | - | Key finding: Hasbe's title has changed. Let me verify the counter-framing in Neo4j's own enterp |
| 10797:0 | 109 | null | - | I have enough to adjudicate. Let me confirm the Resident Home testimonial detail one more time  |
| 10801:0 | 105 | null | - | Verdict reached. The quote is real, but the load-bearing inference fails against the full prima |
| 10817:0 | 65 | null | 7763:0 | I'll verify this claim about AWS Neptune Analytics m-NCU pricing. |
| 10829:0 | 134 | null | - | The quote is verbatim-confirmed on the primary source. Now let me test the inferential leaps —  |
| 10863:0 | 136 | null | - | The quote checks out verbatim on today's live page. Now testing the two inferential leaps: "pri |
| 10870:0 | 124 | null | - | Found strong counter-evidence. Let me pull the full docs set to check provisioning granularity  |
| 10886:0 | 108 | null | - | The AWS Price List API settles it. Let me confirm the price ladder is discrete and check per-un |
| 10893:0 | 145 | null | - | I have decisive primary-source evidence. Let me do one final check on whether the flat rate hol |
| 10897:0 | 158 | null | - | Verification complete. The quote is genuine and current, but the claim's load-bearing third sen |
| 10913:0 | 83 | null | - | I'll verify this claim about Neo4j's AuraDB Business Critical pricing announcement. |
| 10925:0 | 172 | null | - | The quote is verbatim-verified in the primary source. Now let me check whether the *interpretat |
| 10931:0 | 116 | null | - | WebSearch is broken as the task warned. Let me use the working channels — pricing page, HN, and |
| 10937:0 | 169 | null | - | Critical detail: Business Critical is $146/GB/mo vs Professional $65/GB/mo. Let me check what A |
| 10943:0 | 133 | null | - | Let me pull the pre-announcement pricing snapshot (2024-08-28, a week before the launch) to see |
| 10947:0 | 172 | null | - | Significant: Business Critical was **already listed at $292 on 2024-08-28**, a week *before* th |
| 10954:0 | 205 | null | - | Business Critical first appeared between 2024-07-25 and 2024-08-28 — so the page predates the p |
| 10965:0 | 86 | null | - | Let me do a final check for third-party dispute and whether the price has moved since. |
| 10976:0 | 1846 | null | - | I have enough to rule. Let me consolidate.  **Verification results:**  1. **Quote is verbatim-e |
| 10992:0 | 63 | null | - | I'll verify this claim by querying the primary source directly. |
| 10996:0 | 112 | null | - | The top-line result matches. Now let me verify the award-detail fields (sole source, competitio |
| 11000:0 | 92 | null | - | Now let me check whether anything larger exists (including IDVs) and verify the award count. |
| 11004:0 | 126 | null | - | Let me check the transaction history for modifications or de-obligations, and confirm no larger |
| 11011:0 | 99 | null | - | I independently reproduced every field of this claim from the primary source. Verdict: not refu |
| 11027:0 | 46 | null | - | I'll fetch the TrustRadius Neo4j reviews page. |
| 11056:0 | 151 | null | - | I retrieved the page (Cloudflare 403 on direct fetch/WebFetch; succeeded via a text-extraction  |
| 11114:0 | 66 | null | - | I have decisive primary-source evidence. Calling StructuredOutput. |
| 11130:0 | 100 | null | - | I'll investigate this claim about the Neo4j Aura press release and the segmentation of price re |
| 11136:0 | 214 | null | - | The press release quote is confirmed verbatim. Now let me check the claim's inferential leaps — |
| 11142:0 | 82 | null | - | WebSearch is broken as the task warned. Let me pull the full pricing page context. |
| 11146:0 | 85 | null | - | This is revealing. Let me extract the full Business Critical and VDC feature columns. |
| 11152:0 | 111 | null | - | Decisive finding. Let me verify the Aura docs and check for the product catalogue / any VDC rep |
| 11158:0 | 120 | null | - | Let me check how Neo4j itself positioned Business Critical, and check the AWS Marketplace listi |
| 11164:0 | 120 | null | - | I have what I need. Let me verify one last detail — the press release's own characterization ve |
| 11168:0 | 140 | null | - | I have the full picture. The claim's descriptive half checks out but its load-bearing inference |
| 11184:0 | 79 | null | - | I'll verify this claim about Neo4j's Aura Graph Analytics pricing model change. |
| 11194:0 | 149 | null | - | Press release confirmed. Now let me check the key adversarial angles: was this product actually |
| 11201:0 | 111 | null | - | Critical context found — the quote is about **out-of-core GDS**, a technical feature. Let me ge |
| 11208:0 | 117 | null | - | Now the decisive question: was serverless/metered GDS actually new in May 2025, or already ship |
| 11221:0 | 151 | null | - | GDS Sessions (serverless) existed from **Feb 2024** — over a year before the comment. Now let m |
| 11235:0 | 98 | null | - | Now let me verify when serverless GDS Sessions became publicly available with consumption prici |
| 11276:0 | 103 | null | - | I have decisive evidence. Let me confirm the date arithmetic and the self-managed GDS still bei |
| 11291:0 | 76 | null | - | I'll verify this claim about the Neo4j Aura press release and pricing tiers. |
| 11294:0 | 84 | null | - | I'll fetch the primary source press release to check whether the claim is supported. |
| 11300:0 | 95 | null | - | WebSearch is broken as warned in the brief. Using curl with a browser User-Agent for neo4j.com. |
| 11309:0 | 172 | null | - | The primary source is confirmed verbatim. Now let me check the surrounding context — whether "B |
| 11313:0 | 118 | null | - | Let me examine the current Neo4j pricing page to see whether VDC was later repriced, and check  |
| 11317:0 | 116 | tool_use | - | Current pricing page is revealing. Let me check archived pricing pages from before/after the Se |
| 11326:0 | 149 | null | - | Let me check earlier 2024 snapshots to see if AuraDB Enterprise ever had a public price, which  |
| 11330:0 | 102 | null | - | Key check: does Business Critical support committed/prepaid enterprise contracts, not just self |
| 11334:0 | 86 | null | - | Let me do a final check for any public evidence on whether VDC pricing itself changed. |
| 11338:0 | 40 | null | - | I have decisive evidence. Verdict below. |
| 11354:0 | 54 | null | - | I'll verify this claim about AuraDB pricing stability. |
| 11412:0 | 50 | null | 25:0 | I'll verify this claim against the primary source. |
| 11416:0 | 101 | null | - | Both awards exist. Now let me check the actual transaction history, dates, and whether scope ch |
| 11420:0 | 82 | null | - | Core figures confirmed. Now let me probe the scope-creep and continuity questions. |
| 11430:0 | 92 | null | - | Now the decisive test: can I get the untruncated FPDS description to check for scope change? |
| 11440:0 | 122 | null | - | Let me check the counter-evidence within the same dataset (other federal renewals) and try to f |
| 11452:0 | 79 | null | - | WebSearch is broken as warned. Let me try WebFetch on the SKU pricing question. |
| 11458:0 | 67 | null | - | Let me get the verbatim RAM-tiered SKU evidence from the IRS award. |
| 11462:0 | 136 | null | - | Verification complete. The arithmetic core checks out against primary source, but the load-bear |
| 11478:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 11484:0 | 59 | null | - | Let me get the raw page text to verify the quotes verbatim. |
| 11497:0 | 103 | null | - | Verified all quotes against the raw HTML. Source confirmed and quotes are exact as printed on t |
| 11513:0 | 91 | null | - | I'll verify this claim by examining the primary source and checking the segmentation logic. |
| 11555:0 | 665 | null | - | I have primary-source evidence on both the press release and Neo4j's own pricing page (current  |
| 11571:0 | 61 | null | - | I'll research vendor-side pricing pressure signals for Neo4j. |
| 11574:0 | 62 | null | - | I'll start searching for vendor-side pricing pressure signals. |
| 11580:0 | 69 | null | - | WebSearch is broken as warned. Switching to WebFetch and direct APIs. |
| 11596:0 | 99 | null | - | Good — captured current pricing. Now let me get Graph Analytics pricing and historical comparis |
| 11602:0 | 87 | null | - | Now let me check historical pricing via Wayback Machine to detect actual price changes. |
| 11611:0 | 108 | null | - | Aura prices are flat since May 2025 — significant. Let me check earlier snapshots and the licen |
| 11617:0 | 92 | null | - | Strong finding: list prices flat for 3 years. Now the GA announcement and product catalogue. |
| 11638:0 | 82 | null | - | Infinigraph (sharding) is highly relevant to the RAM-cost thesis. Let me fetch it. |
| 11642:0 | 76 | null | - | Strong finding. Let me get the Sept 2024 Aura tier restructure announcement. |
| 11651:0 | 103 | null | - | Excellent — found an explicit vendor-admitted 20%+ price cut. Let me get one more on licensing  |
| 11662:0 | 537 | null | - | Six strong vendor-side findings gathered. Note: WebSearch failed on every call (same `output_co |
| 11678:0 | 63 | null | 4270:0 | I'll verify this claim by checking the primary source directly. |
| 11682:0 | 56 | null | - | Now let me verify the specific award cited in the claim. |
| 11686:0 | 133 | null | - | The verbatim quote checks out exactly. Now let me pull the full award detail and check whether  |
| 11696:0 | 160 | null | - | Only 1 of 28 awards mentions RAM/cores. Let me check the sub-agency of the $4.5M award and hunt |
| 11705:0 | 121 | null | - | Critical finding in the transaction history. Let me verify the money trail and hunt for corrobo |
| 11711:0 | 118 | null | - | WebSearch is broken as warned. Let me pull the untruncated description from FPDS and try other  |
| 11715:0 | 120 | null | - | FPDS confirms the $4.5M sits on this exact SKU order. Now let me test whether RAM-tiered SKUs a |
| 11724:0 | 62 | null | - | Found a published Neo4j price list reference. Let me fetch it. |
| 11730:0 | 119 | null | - | This is decisive. Let me check whether the pricing axis changed by 2025 — the 2018 list is core |
| 11734:0 | 141 | null | - | Zero mentions of GB/memory/RAM in the entire published price list. Let me check whether the axi |
| 11740:0 | 85 | null | - | Let me examine the Self-Managed section of Neo4j's current pricing page specifically. |
| 11744:0 | 54 | null | - | I have enough to rule. Let me record the verification. |
| 11763:0 | 27 | null | 523:0 | I'll fetch the source page. |
| 11776:0 | 133 | null | - | Retrieved the full post via curl (WebFetch 403'd; browser UA worked). Content confirmed: Philip |
| 11792:0 | 47 | null | 122:0 | I'll verify this claim against primary sources. |
| 11798:0 | 84 | null | - | Let me get the full press release text and check the technical architecture details. |
| 11804:0 | 100 | null | - | Now the critical architectural detail — let me read Neo4j's own technical blog on property shar |
| 11808:0 | 116 | null | - | The engineering blog is decisive. Let me now verify what the March 2025 staff-engineer statemen |
| 11824:0 | 125 | null | - | Let me check whether the GA version (Jan 2026) changed the architecture — that's the strongest  |
| 11828:0 | 109 | null | - | Let me check whether GDS — the actual subject of the March 2025 statement — is even supported o |
| 11832:0 | 301 | null | - | I have what I need. The refutation rests on Neo4j's own primary technical documentation, not th |
| 11848:0 | 74 | tool_use | - | I'll verify this claim about the Neo4j Aura Graph Analytics press release. |
| 11857:0 | 64 | null | - | WebSearch is broken as forewarned. Using curl with a browser UA. |
| 11868:0 | 109 | null | - | Press release verified verbatim. Now checking for later edits (Wayback) and any contradicting G |
| 11891:0 | 192 | null | - | Verification complete. The claim's factual half checks out exactly against the primary source a |
| 11907:0 | 63 | null | - | I'll verify this claim by hitting the USAspending API directly. |
| 11916:0 | 107 | null | - | Core figures confirmed. Now let me probe the weak points — description truncation and possible  |
| 11922:0 | 141 | null | - | The description diff confirms the only change is the dollar figure. Now let me check FPDS direc |
| 11928:0 | 103 | null | - | Key context found. Now let me check FPDS directly for untruncated descriptions and transaction  |
| 11939:0 | 148 | null | - | The FY2023 mod is 227 chars, proving the description field isn't truncated at 183. Let me now c |
| 11945:0 | 77 | null | - | FPDS is reachable. Let me pull the authoritative FPDS fields for both orders. |
| 11954:0 | 117 | null | - | Every FPDS field matches the claim. Now let me test the one inferential clause — "scope drift e |
| 11958:0 | 115 | null | - | Every factual element verified against two primary sources. But I found three problems with the |
| 11974:0 | 78 | null | 8629:0 | I'll verify this claim by checking both the Wayback capture and the live page. |
| 11994:0 | 160 | null | - | Core strings verified in both pages. Now let me test the harder parts: intermediate captures, a |
| 12006:0 | 113 | null | - | Price is stable across 11 sample points. Now testing the inferential leap — the "falsifies pres |
| 12021:0 | 34 | null | - | I have what I need. Verdict below. |
| 12037:0 | 87 | null | - | I'll verify this claim by checking both the Wayback capture and the live page directly. |
| 12046:0 | 137 | null | - | The core price strings check out in both. Now let me diff the surrounding Graph Analytics secti |
| 12056:0 | 159 | tool_use | - | Wayback timestamp confirmed genuine. Now let me test the biggest methodological gap: two endpoi |
| 12063:0 | 141 | null | - | 11 captures across the full window all show $0.40/512 GB. Now let me check the GA date and hunt |
| 12077:0 | 59 | null | - | WebSearch is broken as warned. Using WebFetch/curl instead. |
| 12083:0 | 114 | null | - | GA date confirmed as May 7, 2025. Let me check the docs and AWS Marketplace for any contradicti |
| 12087:0 | 123 | null | - | Let me check the Neo4j product catalogue (v1.5/v2.0 effective 2026-08-01) — a pending rate chan |
| 12105:0 | 63 | null | - | I'll fetch the USAspending.gov data on Neo4j federal contracts. |
| 12124:0 | 102 | null | - | Data retrieved and verified directly from the USAspending API (28 prime awards, $13,674,479.06  |

## Radar Heading Map

Heading candidates are syntactic navigation, including headings in embedded examples. No heading-only inspection counts as reading the section.

| Span | Heading |
| --- | --- |
| 11-167 | ## Capture Ledger |
| 168-176 | ## Current Working Thesis |
| 177-1208 | ## Evidence Table |
| 1209-1210 | ## Insight Notes |
| 1211-1255 | ### Semantic Diffing Is A Verification Clue |
| 1256-1279 | ### Context Graphs Are A Local AI Wedge |
| 1280-1292 | ### Semantic Graphs Are Not Only Enterprise Graphs |
| 1293-1313 | ### Algorithm-Shaped Compression Is The Storage Thesis |
| 1314-1325 | ### Formal Semantic Graphs Are A Weird But Useful Benchmark Class |
| 1326-1341 | ### The Web-Graph Argument Supports Addressable Receipts |
| 1342-1355 | ### Ontology Engineering Is A Real Labor Market Clue |
| 1356-1372 | ### Graph UX May Matter As Much As Graph Storage |
| 1373-1431 | ### The Sharpest Customer Discovery So Far |
| 1432-1449 | ### Small Models And Knowledge Engines Point Back To Graphs |
| 1450-1466 | ### Resident RAM Must Be Separated From Logical Footprint |
| 1467-1487 | ### PageRank Is Already In Agent Workflow Land |
| 1488-1524 | ### Agent Workflows Are Becoming Graphs |
| 1525-1545 | ### Epistemic Independence Belongs In The Verification Spine |
| 1546-1562 | ### Graph-Literacy Is A Product Requirement |
| 1563-1574 | ### Graph Theory Virality Has A Verification Lesson |
| 1575-1590 | ### Integrity Should Fail Closed |
| 1591-1602 | ### Knowledge As Topology Is The Narrative |
| 1603-1616 | ### Cost/Latency Routing Is A Product Metaphor |
| 1617-1627 | ### Neuro-Symbolic AI May Be A Demand Signal For Local Graphs |
| 1628-1653 | ### Versioned Graph Artifacts Are A Missing Product Primitive |
| 1654-1670 | ### Probability On Trees And Networks Is A Reading Spine |
| 1671-1695 | ### Phase Ledgers Beat Aggregate Timings |
| 1696-1723 | ### Context Graph Tooling Is Market Proof For Representation-Shaped Compute |
| 1724-1749 | ### Temporal Graphs Need Standards, Not Vibes |
| 1750-1775 | ### Agent Orchestration Graphs Are A Real Adjacent Workload |
| 1776-1786 | ### Text-To-Knowledge-Graph Repos Can Become Cheap Fixtures |
| 1787-1804 | ### Existing Knight Bus Claims Need A Stronger Receipt |
| 1805-1821 | ### Compute Routing Is A Product Shape |
| 1822-1850 | ### Structured Graph Harness Confirms The Agent-Graph Thesis |
| 1851-1877 | ### Profile Distribution Is A Lesson For Algorithm Distribution |
| 1878-1909 | ### OKF Is A Receipt Bundle Clue |
| 1910-1941 | ### Space Complexity And Hardware Locality Need First-Class Product Language |
| 1942-1981 | ### Iggy Implementation Makes RAM Modes Legible |
| 1982-1998 | ### Mathematical Structure Beats Clever Hacks |
| 1999-2021 | ### Context Compression Is A Warning Label For Graph Compression |
| 2022-2044 | ### Predictive Intents Is A Strong Local Graph Wedge |
| 2045-2060 | ### Named Graphs And Bellman-Ford Are Fixture Culture |
| 2061-2079 | ### Debug The Graph Before Running The Job |
| 2080-2097 | ### Security And Falsification Are Graph-Shaped Verification Workloads |
| 2098-2131 | ### HydraDB Validates The Category And Raises The Bar |
| 2132-2161 | ### HydraDB Docs Show Graph Mode Needs A Planner Gate |
| 2162-2181 | ### HydraDB Deepening Changes The Comparator Checklist |
| 2182-2205 | ### HydraDB Benchmark Is A Category Signal, Not A Systems Baseline |
| 2206-2248 | ### The Graph Shows Manifested Indexing Is A Product Primitive |
| 2249-2273 | ### Pruning Is The Same Tradeoff As Low RAM |
| 2274-2325 | ### DeFiLlama Adapter Ecology Is A Contribution Model For Algorithm Profiles |
| 2326-2349 | ### OWL-Time Gives Temporal Graph Fixtures |
| 2350-2373 | ### Decentralized Analytics Is A Good Public Benchmark Family |
| 2374-2420 | ### Rollups Plus Current Buckets Give A Batch/Live Template |
| 2421-2461 | ### Proof Of Indexing Shows How To Make Projection Honest |
| 2462-2488 | ### Declarative Filters And Parallel Calls Are Latency Levers |
| 2489-2523 | ### Dimension Adapter Guidelines Become A Profile Lint Contract |
| 2524-2548 | ### Domain Invariants Are Better Than Generic Benchmarks |
| 2549-2583 | ### Current-Bucket Tests Show What Freshness Must Prove |
| 2584-2640 | ### Bounded Multi-Stream Aggregation Is A Projection Architecture |
| 2641-2657 | ### Completion Watermarks Are A Core Abstraction |
| 2658-2694 | ### Profile Manifests Should Be Content-Addressed Contracts |
| 2695-2719 | ### Pure Manifest Validation Is The First Verification Loop |
| 2720-2762 | ### Version Gates Keep The Surface Area Honest |
| 2763-2791 | ### Latest-Frontier Tracking Is How Bounded Streams Stay Honest |
| 2792-2811 | ### Typed Physical Fixtures Prevent Edge-Case Lies |
| 2812-2854 | ### AMP Runner Is A Projection Runtime Template |
| 2855-2880 | ### Block-Window Planning Is The Real RAM Lever |
| 2881-2906 | ### Reorgs Mean Projection State Is Conditional |
| 2907-2931 | ### Feature Detection Beats Feature Claims |
| 2932-2970 | ### Health State Belongs In The Data Model |
| 2971-3001 | ### Runtime Lifecycle Is Part Of The Resource Contract |
| 3002-3041 | ### Query Rewriting Is Storage-Shaped-For-Read In Disguise |
| 3042-3075 | ### Physical Batch Metadata Should Become Proof Material |
| 3076-3091 | ### Profile Validation Should Mix Static Checks And Live Schema Probes |
| 3092-3117 | ### Generated Schemas Are Helpful But Not Sufficient |
| 3118-3136 | ### The Codec Is Where "Same Surface Area" Becomes Real |
| 3137-3166 | ### Alias Tolerance Must Be Narrow And Auditable |
| 3167-3199 | ### Completed Groups Are The Unit Of Safe Progress |
| 3200-3228 | ### Backpressure Is A Product Event |
| 3229-3264 | ### Reference-Plus-Row-Index Grouping Is A Low-RAM Primitive |
| 3265-3332 | ### SourceEvent And BatchEvidence Spec Seed |
| 3333-3365 | ### Durable Store Contracts Should Mirror Runner Receipts |
| 3366-3398 | ### Per-Block EntityCache Is The Hot-State Pattern |
| 3399-3433 | ### Versioned Rows Are A Storage Format, Not Just History |
| 3434-3456 | ### Pruning Is Compaction With A Retention Contract |
| 3457-3483 | ### Index Descriptors Should Be Part Of Profile Manifests |
| 3484-3497 | ### Store Errors Need Retry Policy In Receipts |
| 3498-3533 | ### Queued Writes Are A Freshness Overlay |
| 3534-3567 | ### Query Physicalization Is Part Of The Product |
| 3568-3587 | ### Pending Overlay Reads Should Be Tested Explicitly |
| 3588-3619 | ### Epoch Commit Should Be A Named Contract |
| 3620-3636 | ### Run Manager Before Algorithm Zoo |
| 3637-3654 | ### Typed Visibility Predicates Are Part Of Profile Planning |
| 3655-3672 | ### First Fixture Pack From Graph Node Tests |
| 3673-3702 | ### Receipt Normalization Is A Boundary |
| 3703-3748 | ### SQLite Run Registry Should Be Explicit, Not Emergent |
| 3749-3772 | ### Event Streams Need Small Notifications And Large Artifacts |
| 3773-3792 | ### Source Frontier Watchers Should Wake Even When Silent |
| 3793-3805 | ### Deterministic Errors Are Time-Ranged Facts |
| 3806-3831 | ### Schema Evolution Is Product Evidence |
| 3832-3862 | ### Dynamic Sources Are Runtime-Discovered Profile Inputs |
| 3863-3889 | ### Profile Manifests Need Runtime Addenda |
| 3890-3935 | ### Dynamic Source Fixtures Should Be First-Class |
| 3936-3958 | ### Compaction And Indexing Are Jobs, Not Footnotes |
| 3959-4006 | ### Relational Tests Become Graph-Slice Verification |
| 4007-4039 | ### Failure Recovery Needs Four Outcomes |
| 4040-4073 | ### Pending Overlay Is A Freshness Mode |
| 4074-4110 | ### Sourceable Range Replay Beats Snapshot Thinking |
| 4111-4152 | ### Artifact Boundary Receipts Are Non-Negotiable |
| 4153-4191 | ### Source Frontier Needs Bounded Lineage Evidence |
| 4192-4240 | ### Recurring Aggregations Are Budget-Bounded Graph Compute Cousins |
| 4241-4354 | ### Iggy-Style Sparse Index Modes Make RAM A Product Dial |
| 4355-4371 | ### Verification Products Give Agents Better State, Not Just Better Prompts |
| 4372-4393 | ### Temporal Agent Memory Papers Point To Failure-Mode Receipts |
| 4394-4421 | ### OKF Gives The Receipt Schema Shape |
| 4422-4447 | ### CodeGraph Shows Benchmark Receipts Need Multiple Orthogonal Metrics |
| 4448-4528 | ### Benchmark Receipts Need Next-Action Evidence |
| 4529-4543 | ### CocoIndex Is A Compute-Router UX Reference |
| 4544-4674 | ### CocoIndex Turns Mode Choice Into Agent Ergonomics |
| 4675-5311 | ### Structured Harnesses Make Graphs A Verification Interface |
| 5312-14347 | ### Agent Memory Graphs Define The First Local Workload |
| 14348-14354 | ### REQ-STARTER-001.0: Emit One Starter-Slice Proof Packet |
| 14355-14361 | ### REQ-WCC-001.0: Prove WCC Estimate And Strategy Selection |
| 14362-14368 | ### REQ-WCC-002.0: Prove WCC Runtime Union Semantics |
| 14369-14376 | ### REQ-WCC-003.0: Prove WCC Public Output Parity |
| 14377-14383 | ### REQ-BFS-001.0: Prove BFS Global And Frontier State |
| 14384-14391 | ### REQ-BFS-002.0: Prove BFS Public Traversal Output |
| 14392-14398 | ### REQ-PAGERANK-001.0: Prove PageRank Pregel Runtime State |
| 14399-21124 | ### REQ-PAGERANK-002.0: Prove PageRank Public Score Output |
| 21125-21252 | ### Graphiti Shows Profiles Are Already Hiding Inside Agent Memory |
| 21253-21348 | ### Kumiho Makes Evidence A Ranking And Traversal Primitive |
| 21349-21455 | ### Revka Shows The Runtime Shape For Profile DAGs |
| 21456-21614 | ### AtomicRAG Turns PPR Into A Retrieval Profile |
| 21615-21629 | ### Algorithm Profiles Can Be Contributor-Market Artifacts |
| 21630-21651 | ### Solana Rug Is A Concrete Deterministic Receipt Example |
| 21652-21668 | ### State-Space Computation Is Another Representation Lesson |
| 21669-21689 | ### DeFi And Token Trust Graphs Are Concrete Auditable Workloads |
| 21690-23522 | ## URL Queue And Follow-Up Reads |
| 23523-25480 | ## Product Implications So Far |
| 25481-25487 | ## Next Capture Steps |

## Capture Journal Heading Map

Heading candidates are syntactic navigation, including headings in embedded examples. No heading-only inspection counts as reading the section.

| Span | Heading |
| --- | --- |
| 9-10 | ## Sessions |
| 11-32 | ### Session: 2026-08-07 04:36:53Z |
| 33-56 | ### Session: 2026-08-07 04:41:04Z |
| 57-81 | ### Session: 2026-08-07 04:43:17Z |
| 82-105 | ### Session: 2026-08-07 04:45:13Z |
| 106-128 | ### Session: 2026-08-07 04:46:35Z |
| 129-154 | ### Session: 2026-08-07 04:49:24Z |
| 155-178 | ### Session: 2026-08-07 04:52:51Z |
| 179-202 | ### Session: 2026-08-07 05:03:43Z |
| 203-226 | ### Session: 2026-08-07 05:14:32Z |
| 227-250 | ### Session: 2026-08-07 05:20:15Z |
| 251-274 | ### Session: 2026-08-07 05:25:05Z |
| 275-298 | ### Session: 2026-08-07 05:32:24Z |
| 299-323 | ### Session: 2026-08-07 05:39:59Z |
| 324-348 | ### Session: 2026-08-07 05:47:32Z |
| 349-373 | ### Session: 2026-08-07 05:56:10Z |
| 374-398 | ### Session: 2026-08-07 06:02:16Z |
| 399-422 | ### Session: 2026-08-07 06:06:27Z |
| 423-447 | ### Session: 2026-08-07 06:12:16Z |
| 448-471 | ### Session: 2026-08-07 06:16:14Z |
| 472-493 | ### Session: 2026-08-07 06:17:02Z |
| 494-517 | ### Session: 2026-08-07 06:23:05Z |
| 518-541 | ### Session: 2026-08-07 06:28:10Z |
| 542-565 | ### Session: 2026-08-07 06:37:06Z |
| 566-589 | ### Session: 2026-08-07 06:40:49Z |
| 590-614 | ### Session: 2026-08-07 06:47:11Z |
| 615-638 | ### Session: 2026-08-07 06:50:51Z |
| 639-662 | ### Session: 2026-08-07 06:58:39Z |
| 663-686 | ### Session: 2026-08-07 07:04:15Z |
| 687-710 | ### Session: 2026-08-07 07:08:40Z |
| 711-734 | ### Session: 2026-08-07 07:13:09Z |
| 735-759 | ### Session: 2026-08-07 07:21:34Z |
| 760-784 | ### Session: 2026-08-07 07:28:00Z |
| 785-809 | ### Session: 2026-08-07 07:32:18Z |
| 810-834 | ### Session: 2026-08-07 07:41:03Z |
| 835-859 | ### Session: 2026-08-07 07:47:15Z |
| 860-885 | ### Session: 2026-08-07 07:58:36Z |
| 886-911 | ### Session: 2026-08-07 08:05:48Z |
| 912-938 | ### Session: 2026-08-07 08:15:30Z |
| 939-964 | ### Session: 2026-08-07 08:24:11Z |
| 965-990 | ### Session: 2026-08-07 08:29:49Z |
| 991-1016 | ### Session: 2026-08-07 08:37:02Z |
| 1017-1042 | ### Session: 2026-08-07 08:40:22Z |
| 1043-1068 | ### Session: 2026-08-07 08:45:36Z |
| 1069-1094 | ### Session: 2026-08-07 08:50:04Z |
| 1095-1120 | ### Session: 2026-08-07 08:54:35Z |
| 1121-1146 | ### Session: 2026-08-07 08:58:57Z |
| 1147-1172 | ### Session: 2026-08-07 09:06:05Z |
| 1173-1198 | ### Session: 2026-08-07 09:10:30Z |
| 1199-1224 | ### Session: 2026-08-07 09:16:28Z |
| 1225-1250 | ### Session: 2026-08-07 09:22:55Z |
| 1251-1275 | ### Session: 2026-08-07 09:31:45Z |
| 1276-1300 | ### Session: 2026-08-07 09:42:14Z |
| 1301-1325 | ### Session: 2026-08-07 09:48:23Z |
| 1326-1350 | ### Session: 2026-08-07 09:54:31Z |
| 1351-1375 | ### Session: 2026-08-07 10:03:45Z |
| 1376-1400 | ### Session: 2026-08-07 10:15:10Z |
| 1401-1425 | ### Session: 2026-08-07 10:18:08Z |
| 1426-1450 | ### Session: 2026-08-07 11:40:31Z |
| 1451-1475 | ### Session: 2026-08-07 11:45:36Z |
| 1476-1500 | ### Session: 2026-08-07 11:48:55Z |
| 1501-1525 | ### Session: 2026-08-07 11:55:45Z |
| 1526-1550 | ### Session: 2026-08-07 12:01:41Z |
| 1551-1575 | ### Session: 2026-08-07 12:09:10Z |
| 1576-1600 | ### Session: 2026-08-07 12:12:08Z |
| 1601-1625 | ### Session: 2026-08-07 12:16:33Z |
| 1626-1650 | ### Session: 2026-08-07 12:24:03Z |
| 1651-1675 | ### Session: 2026-08-07 12:29:03Z |
| 1676-1700 | ### Session: 2026-08-07 12:32:52Z |
| 1701-1725 | ### Session: 2026-08-07 12:41:22Z |
| 1726-1750 | ### Session: 2026-08-07 12:46:47Z |
| 1751-1775 | ### Session: 2026-08-07 12:55:31Z |
| 1776-1800 | ### Session: 2026-08-07 13:06:21Z |
| 1801-1825 | ### Session: 2026-08-07 13:13:46Z |
| 1826-1850 | ### Session: 2026-08-07 13:26:08Z |
| 1851-1876 | ### Session: 2026-08-07 13:35:54Z |
| 1877-1902 | ### Session: 2026-08-07 13:45:29Z |
| 1903-1928 | ### Session: 2026-08-07 13:50:30Z |
| 1929-1954 | ### Session: 2026-08-07 13:56:06Z |
| 1955-1980 | ### Session: 2026-08-07 14:02:55Z |
| 1981-2006 | ### Session: 2026-08-07 14:12:08Z |
| 2007-2032 | ### Session: 2026-08-07 14:21:20Z |
| 2033-2058 | ### Session: 2026-08-07 14:31:15Z |
| 2059-2084 | ### Session: 2026-08-07 14:39:02Z |
| 2085-2110 | ### Session: 2026-08-07 14:47:43Z |
| 2111-2136 | ### Session: 2026-08-07 14:58:48Z |
| 2137-2162 | ### Session: 2026-08-07 15:08:40Z |
| 2163-2188 | ### Session: 2026-08-07 15:16:26Z |
| 2189-2214 | ### Session: 2026-08-07 15:22:04Z |
| 2215-2240 | ### Session: 2026-08-07 15:26:59Z |
| 2241-2266 | ### Session: 2026-08-07 15:36:50Z |
| 2267-2292 | ### Session: 2026-08-07 15:41:53Z |
| 2293-2318 | ### Session: 2026-08-07 15:53:35Z |
| 2319-2344 | ### Session: 2026-08-07 16:00:07Z |
| 2345-2370 | ### Session: 2026-08-07 16:07:13Z |
| 2371-2396 | ### Session: 2026-08-07 16:13:24Z |
| 2397-2422 | ### Session: 2026-08-07 16:21:44Z |
| 2423-2448 | ### Session: 2026-08-07 16:31:32Z |
| 2449-2474 | ### Session: 2026-08-07 16:42:59Z |
| 2475-2500 | ### Session: 2026-08-07 16:51:07Z |
| 2501-2526 | ### Session: 2026-08-07 17:00:16Z |
| 2527-2552 | ### Session: 2026-08-07 17:11:14Z |
| 2553-2578 | ### Session: 2026-08-07 17:20:10Z |
| 2579-2604 | ### Session: 2026-08-07 17:31:52Z |
| 2605-2630 | ### Session: 2026-08-07 17:37:59Z |
| 2631-2656 | ### Session: 2026-08-07 17:50:13Z |
| 2657-2682 | ### Session: 2026-08-07 18:01:19Z |
| 2683-2708 | ### Session: 2026-08-07 18:08:19Z |
| 2709-2734 | ### Session: 2026-08-07 18:21:11Z |
| 2735-2760 | ### Session: 2026-08-07 18:27:46Z |
| 2761-2786 | ### Session: 2026-08-07 18:40:57Z |
| 2787-2812 | ### Session: 2026-08-07 18:51:46Z |
| 2813-2838 | ### Session: 2026-08-07 19:01:38Z |
| 2839-2864 | ### Session: 2026-08-07 19:08:41Z |
| 2865-2892 | ### Session: 2026-08-07 19:15:37Z |
| 2893-2921 | ### Session: 2026-08-07 19:22:17Z |
| 2922-2950 | ### Session: 2026-08-07 19:18:17Z |
| 2951-2980 | ### Session: 2026-08-07 19:30:19Z |
| 2981-3009 | ### Session: 2026-08-07 19:35:29Z |
| 3010-3039 | ### Session: 2026-08-07 19:40:29Z |
| 3040-3069 | ### Session: 2026-08-07 19:46:47Z |
| 3070-3101 | ### Session: 2026-08-07 19:59:40Z |
| 3102-3134 | ### Session: 2026-08-07 20:05:18Z |
| 3135-3167 | ### Session: 2026-08-07 20:13:09Z |
| 3168-3200 | ### Session: 2026-08-07 20:20:13Z |
| 3201-3231 | ### Session: 2026-08-07 20:23:20Z |
| 3232-3263 | ### Session: 2026-08-07 20:26:50Z |
| 3264-3295 | ### Session: 2026-08-07 20:30:30Z |
| 3296-3330 | ### Session: 2026-08-07 20:35:48Z |
| 3331-3363 | ### Session: 2026-08-07 20:40:06Z |
| 3364-3397 | ### Session: 2026-08-07 20:48:40Z |
| 3398-3431 | ### Session: 2026-08-07 20:52:03Z |
| 3432-3466 | ### Session: 2026-08-07 21:00:23Z |
| 3467-3501 | ### Session: 2026-08-07 21:12:18Z |
| 3502-3537 | ### Session: 2026-08-07 21:26:40Z |
| 3538-3573 | ### Session: 2026-08-07 21:33:48Z |
| 3574-3609 | ### Session: 2026-08-07 21:41:02Z |
| 3610-3646 | ### Session: 2026-08-07 21:59:23Z |
| 3647-3685 | ### Session: 2026-08-07 22:11:49Z |
| 3686-3724 | ### Session: 2026-08-07 22:14:11Z |
| 3725-3762 | ### Session: 2026-08-07 22:25:24Z |
| 3763-3802 | ### Session: 2026-08-07 22:36:29Z |
| 3803-3841 | ### Session: 2026-08-07 22:47:08Z |
| 3842-3883 | ### Session: 2026-08-07 23:02:09Z |
| 3884-3923 | ### Session: 2026-08-07 23:20:24Z |
| 3924-3965 | ### Session: 2026-08-07 23:28:51Z |
| 3966-4009 | ### Session: 2026-08-07 23:38:58Z |
| 4010-4050 | ### Session: 2026-08-07 23:49:41Z |
| 4051-4088 | ### Session: 2026-08-07 23:59:09Z |
| 4089-4126 | ### Session: 2026-08-08 00:11:44Z |
| 4127-4165 | ### Session: 2026-08-08 00:19:23Z |
| 4166-4201 | ### Session: 2026-08-08 00:24:36Z |

## Additional Exact Duplication Checks

Radar and capture journal have no cross-file equal paragraphs longer than 100 characters (this is NOT evidence of independent sources). Eleven within-file duplicate-paragraph groups exceed 100 characters: three in radar and eight in journal. Radar duplicate spans: 13057-13067 = 15596-15606; 14726-14731 = 16365-16370; 15018-15024 = 16347-16353. Journal's longest repeated paragraph is 3007-3008 = 3067-3068 (409 characters); repeated resume notes also occur at 2342-2343 and nine later locations, and 2654-2655 and seven later locations. These structural repetitions do not erase unique observations in adjacent sessions.

## Resume Procedure

1. Snapshot 06-archives-Journal.md with the prescribed journal script.
2. Read the consumed-span ledger in 06-archives-Evidence.md.
3. Select the next unread substantive payload or a bounded prose region using this map. Decode JSON before display; omit thinking and metadata. Label field/character windows when a physical JSON line is too large.
4. Write source-specific observations and contradictions, then update coverage and journal. Never promote this structural table to a full-read claim.

## Deep Radar Subsections

Additional fourth-level headings outside triple-backtick fences. The main heading map is insufficient for the long 5312-21124 region; these 112 subsection starts expose its source/implementation research. A structural fence scan found 838 closed triple-backtick blocks and no unmatched opener. Historical prompts in fenced blocks remain untrusted data.

| Subsection start | Heading |
| ---: | --- |
| 4752 | #### Harness-G Implementation Makes Menus A Runtime Contract |
| 4920 | #### StructAgent Source Makes RunState Verifier-Owned |
| 5031 | #### Living-Harness Makes Failed Receipts Become Profile Repairs |
| 5136 | #### Harness KG Turns Modeled Data Into Product Latency |
| 5207 | #### Context Graphs Make The Execution Trace A Graph Too |
| 5346 | #### Graph Memory Survey Turns Memory Into Profile Families |
| 5446 | #### Awesome-GraphMemory Makes The Ecosystem Strategy Concrete |
| 5556 | #### taOSmd Shows What A Local Memory Receipt Must Separate |
| 5651 | #### Memvid Makes Portable Profile Capsules Plausible |
| 5792 | #### Omnigraph Shows The Lakehouse Counterpart To Capsules |
| 5890 | #### Omnigraph Cost Gates Are The Verification Spine |
| 6076 | #### Omnigraph CLI And Server Surface Teach Product Boundaries |
| 6290 | #### Out-Of-Core Graph Engines Prove Bounded RAM Is A Design Space |
| 6474 | #### Partitioning Is A First-Class Profile, Not Hidden Setup |
| 6661 | #### GraphBLAS/LAGraph Should Be The Oracle Lane |
| 6875 | #### GBBS Gives The Performance-Implementation Lane |
| 7054 | #### Partition Quality Must Pay Rent In Latency |
| 7164 | #### Query Shape Is A Partition Input |
| 7226 | #### Partitioning Becomes A Memory Budget Dial |
| 7303 | #### Graph Learning Widens The PMF Without Changing The Spine |
| 7382 | #### Implementation Artifacts Make GNN Receipts Concrete |
| 7445 | #### Marius Turns Storage Hierarchy Into A Receipt |
| 7515 | #### CDFGNN Shows The Expansion Proof Type |
| 7595 | #### Hyperion Makes SSD A Budget Dial |
| 7682 | #### Ginex Turns Future Knowledge Into Cache Changesets |
| 7762 | #### GIDS Makes PageRank A Cache-Admission Hint |
| 7857 | #### Zebra Makes Top-K Influence A Computation Budget |
| 7948 | #### DUCATI Splits GPU Memory Into Topology And Feature Budgets |
| 8058 | #### PaGraph Makes Cache Policy And Partition Replication Observable |
| 8157 | #### GNNLab Factors Sampling, Cache Planning, And Training Into Measurable Planes |
| 8260 | #### EXACT And QGTC Make Compression A Proof Obligation |
| 8383 | #### GraphBolt Makes The Data Pipeline A First-Class Graph Artifact |
| 8594 | #### GraphBolt Native Code Turns Pipeline Claims Into Byte Receipts |
| 8775 | #### GraphStorm Turns GraphBolt Into A Workflow Contract |
| 8908 | #### GraphStorm Examples Make Comparison A Product Artifact |
| 9060 | #### DGL GraphBolt Examples Turn Storage Modes Into Staged Dataflow |
| 9238 | #### GraphBolt Tests Turn The Profile Vocabulary Into Acceptance Criteria |
| 9337 | #### Graphalytics Turns Verification Into A Public Benchmark Spine |
| 9481 | #### ArcadeDB GAV Is The Database Version Of Our Storage-Shape Thesis |
| 9633 | #### Samyama Is The Nearest Public Shape To The Ambition, And A Warning Label |
| 9787 | #### Samyama Issue 296 Turns Complex Reads Into The Sharp Edge |
| 9979 | #### Neo4j Shows Complex Reads Need Rewrite Eligibility Receipts |
| 10084 | #### GDS Shows Estimates Are Necessary But Not Sufficient |
| 10180 | #### GDS Estimate Pipeline Is Already A Product Contract |
| 10306 | #### GDS Memory Trees Give Us The Accounting Algebra |
| 10378 | #### GDS Guards Turn Estimates Into Admission |
| 10484 | #### GDS Separates Estimate Policy From Memory Observability |
| 10559 | #### GDS Graph-Size Details Show What A Cap Ledger Must Explain |
| 10654 | #### GDS Estimate Fixtures Show The Verification Loop Starts Before Hot Loops |
| 10727 | #### GDS Algorithm Estimates Are Byte Algebra, Not Comments |
| 10867 | #### Runtime Ownership Shows Where Storage Innovation Should Attach |
| 10980 | #### Correctness Oracles Define The Minimum Proof-Carrying Slice |
| 11091 | #### API Parity Is A Separate Proof Layer |
| 11193 | #### Profile Selection Belongs Between Parsed Request And Algorithm Run |
| 11329 | #### Profile Alternatives Need Admission Receipts |
| 11442 | #### Side Effects Are A Separate Budget Phase |
| 11575 | #### Writeback Has Two Storage Shapes |
| 11706 | #### Relationship Writeback Has Four Artifact Families |
| 11877 | #### Algorithm Families Choose Different Relationship Output Carriers |
| 11992 | #### Starter Slice Output Carriers Are Even Simpler |
| 12151 | #### Similarity Is The First Relationship Bundle Slice |
| 12314 | #### Path Finding Is The First Varlen Payload Slice |
| 12488 | #### Embeddings Are Matrix Outputs; ML Top-K Is A Later Lane |
| 12682 | #### Embedding Memory Estimates Make The Matrix Story Honest |
| 12885 | #### Starter Slice Estimates Are The First Verification Spine |
| 13160 | #### KNN And Node Similarity Are Edge-Score Estimate Families |
| 13398 | #### Similarity Output Fixtures Are The Next Verification Loop |
| 13692 | #### Compute State Is Where RAM Differentiation Lives |
| 14033 | #### Starter Runtime Profiles Need Phase Receipts |
| 14211 | #### WCC Is The First Exact Receipt, Not The Biggest RAM Win |
| 14311 | #### Starter Slice Executable Receipt Packet |
| 14471 | #### Starter Slice Implementation Prompt |
| 14563 | #### WCC First Fixture And Projection Bridge |
| 14709 | #### WCC Needs A Topology Plane |
| 14849 | #### Topology Missing Should Be The First Red Test |
| 15010 | #### Fixture Adapter Must Bypass Property Streams |
| 15200 | #### WCC Must Fit The Existing Table Envelope |
| 15401 | #### WCC Public Names Are The Contract |
| 15542 | #### WCC Estimate Is The First Budget Contract |
| 15698 | #### WCC Output Parity Has Two Surfaces |
| 15887 | #### WCC V1 Should Be Unsampled Union First |
| 16030 | #### WCC Implementation Should Start As A Red-Test Spine |
| 16155 | #### WCC Registry Already Exists In TSV |
| 16244 | #### WCC Execution Envelope Should Stay Boring |
| 16338 | #### WCC Topology Adapter Must Bypass Property Streams |
| 16451 | #### WCC Ten-Node Fixture Should Be The First Correctness Oracle |
| 16555 | #### WCC Threshold Must Be A Second-Stage Oracle |
| 16643 | #### WCC Seeded Mode Is A Separate State Lane |
| 16747 | #### PageRank Pregel Memory Envelope Is The First Iterative Vector Contract |
| 16892 | #### BFS Frontier Range Is The First Traversal Cap Contract |
| 17050 | #### Dijkstra Weighted Paths Are Priority Frontier Plus Route Payload |
| 17243 | #### Bellman-Ford Turns Negative Edges Into A Detection Contract |
| 17423 | #### Delta-Stepping Is Bucketed Weighted SSSP With Range Admission |
| 17600 | #### Random Walk Makes Stochastic Output A Bounded Stream Contract |
| 17783 | #### Node2Vec Turns Walk Streams Into A Model-State Contract |
| 17994 | #### Louvain Makes Coarsened Graph Generations The RAM Problem |
| 18166 | #### Leiden Adds A Refinement Contract On Top Of Louvain's Shape |
| 18341 | #### NodeSimilarity Is A Candidate-Pair Budget Contract |
| 18530 | #### KNN Turns Similarity Into An Iterative Neighbor-List Contract |
| 18717 | #### FastRP Makes Dense Embeddings The Budget |
| 18916 | #### GraphSAGE Turns Embeddings Into A Model-And-Batch Budget |
| 19139 | #### HashGNN Is The Sparse-To-Dense Proof Precedent |
| 19337 | #### GDS Estimate Admission Is The Product Spine |
| 19544 | #### Pregel Runtime Progress Turns Admission Into A Running Contract |
| 19802 | #### Job Progress Observability Makes Bounded Compute Product-Grade |
| 20026 | #### Job Progress Test Oracles Turn Observability Into A Gate |
| 20186 | #### WCC Should Be The First Job-Control Customer |
| 20377 | #### Local WCC Implementation Should Start With Seven Red Tests |
| 20544 | #### WCC Fixture Should Be Dedicated, Not Borrowed |
| 20689 | #### DAHA And SANCUS Turn Planning Into A Runtime Contract |
| 20804 | #### HEP Makes Hybrid Partitioning Concrete |
| 20874 | #### Passes 053-057 Executable Spec Spine |

## Terminal And Auxiliary Output Audit

Seven run-2 agent files have no StructuredOutput final: headers 8809, 9309, 9472, 10150, 11177, 11347, 12030. Their last assistant text is absent or progress commentary, not a substantive terminal report (last text lines 8816, none, none, 10157, 11276, 11354, 12087 respectively). They are incomplete evidence streams, not seven missing journal copies. Last tool lines are 8842, 9322, 9502, 10182, 11273, 11395, 12088.

One ReportFindings call at 7835 has an empty findings list. A mark_chapter summary at 11745 is substantive recap metadata about the RAM-billing claim and is not counted among the 189 StructuredOutput payloads. Both inputs were inspected in full, not their surrounding tool chains.

Tool-result provenance counts: Bash 1930; WebFetch 493; WebSearch 265; ToolSearch 201; StructuredOutput acknowledgments 189; alternate web_search 100; alternate web_fetch 32; Read 27; browser/javascript/network/etc 38; noop 1; ReportFindings 1; chapter marker 1. Tool type is not a reliable discard rule: Bash/Read/browser results often contain actual retrieved evidence.

Report duplication refinement: final report result.summary, result.findings, result.caveats and result.openQuestions are exactly equal to the four fields in raw StructuredOutput line 4080. The whole nine-key report result is not equal because it adds question/refuted/unverified/sources/stats. Reading its four matching fields can account for line 4080's payload by exact-value alias, without claiming the additional report fields have been read. Raw 5905's nested JSON string does not exactly equal the later 5908 payload even after parsing it, so retain both.
