# Scale runs: how long each step took, at how many LOBs and tables

Every record is one step of one run (`lfc-medallion-pack timings` after the run, from the Jobs and Pipelines APIs;
[scale-runs.jsonl](scale-runs.jsonl) holds them). This page is that file as a table (`lfc-medallion-pack report
references/scale-runs.jsonl`); `lfc-medallion-pack predict references/scale-runs.jsonl --tables N` fits each step to
its tables. TPC-B (4 tables per LOB) on Lakebase PostgreSQL 17, serverless pipelines, one workspace, 2026-11-21.

Kinds: `first` is each pipeline's first snapshot, `steady` an update with no new tables, `grown` a pipeline given more
LOBs whose gateway wasn't restarted (it ingests none of them). An ingest step's own tables are after "its".
Phases (seconds): wait for resources, initializing, setting up tables, running.

| date | module | data | LOBs | tables | pipelines (tables each) | step | seconds | phases (s) | flows | note |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | source | 124 |  |  | 1 gateway started with the deploy |
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | deploy | 11.0 |  |  |  |
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | ingest_1, its 12 | 124 | wait 12, init 41, setup 16, run 40 | 25 |  |
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | transform | 84 | wait 11, init 38, setup 9, run 15 | 11 |  |
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | verify | 63 |  |  |  |
| 2026-11-21 | scale | no | 3 | 12 | 1 (12) | gateway_explore_1, its 12 (239 tables found) | 1.97 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | source | 1700 |  |  | grown from 3 LOBs: pipelines 2-5 new (first snapshot); pipeline 1 got 59 more LOBs but its gateway wasn't restarted, so they weren't ingested (verify failed) |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | deploy | 28.0 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_1, its 248 | 64 | wait 10, init 21, setup 6, run 16 | 13 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_2, its 248 | 798 | wait 10, init 170, setup 165, run 435 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_3, its 248 | 808 | wait 11, init 168, setup 164, run 449 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_4, its 248 | 817 | wait 10, init 172, setup 166, run 455 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_5, its 8 | 114 | wait 14, init 40, setup 14, run 33 | 17 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | transform | 356 | wait 10, init 209, setup 4, run 120 | 11 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | verify | 58 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | verify | 41 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_1, its 248 (239 tables found) | 1.97 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_2, its 248 (1227 tables found) | 4.0 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_3, its 248 (1227 tables found) | 3.84 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_4, its 248 (1227 tables found) | 4.61 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_5, its 8 (1227 tables found) | 3.77 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_1, its 248 | 810 | wait 12, init 183, setup 164, run 438 | 485 | after restarting gateway 1: pipeline 1's first snapshot of its 59 new LOBs; 2-5 steady |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_2, its 248 | 688 | wait 12, init 183, setup 104, run 341 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_3, its 248 | 668 | wait 11, init 168, setup 94, run 345 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_4, its 248 | 688 | wait 12, init 175, setup 101, run 347 | 497 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | ingest_5, its 8 | 63 | wait 11, init 19, setup 4, run 14 | 9 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | transform | 617 | wait 13, init 301, setup 5, run 282 | 11 |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | verify | 66 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_1, its 248 (1267 tables found) | 3.35 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_2, its 248 (1227 tables found) | 4.0 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_3, its 248 (1227 tables found) | 3.84 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_4, its 248 (1227 tables found) | 4.61 |  |  |  |
| 2026-11-21 | scale | no | 250 | 1000 | 5 (248) | gateway_explore_5, its 8 (1227 tables found) | 3.77 |  |  |  |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | source | 392 |  |  | data: 10 LOBs at SF 1, 1,000,000 history rows |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | deploy | 48.0 |  |  |  |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | ingest_1, its 40 | 235 | wait 11, init 69, setup 34, run 111 | 81 |  |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | transform | 94 | wait 10, init 43, setup 8, run 23 | 11 |  |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | verify | 74 |  |  |  |
| 2026-11-21 | bank | yes | 10 | 40 | 1 (40) | gateway_explore_1, its 40 (1267 tables found) | 3.63 |  |  |  |
