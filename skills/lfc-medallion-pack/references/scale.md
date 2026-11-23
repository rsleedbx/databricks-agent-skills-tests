# Scale runs: recording how long each step takes, at how many LOBs and tables

Measured results depend on the configuration (source, workspace, compute, sizes), so the records are kept with that
configuration, outside this repo. This page says how to make them and read them.

Every record is one step of one run: `lfc-medallion-pack timings` after the run, from the Jobs and Pipelines APIs
(SKILL.md, "After every run"). Render records as a table with `lfc-medallion-pack report <records.jsonl>`.
`lfc-medallion-pack predict <records.jsonl> --tables N` fits each step to its tables, a line through two sizes or more.

- **Kinds:**
  - `first`: each pipeline's first snapshot;
  - `steady`: an update with no new tables;
  - `grown`: a pipeline given more LOBs whose gateway wasn't restarted, so it ingests none of them.
- **An ingest step's own tables** come after "its".
- **Phases** (seconds): wait for resources, initializing, setting up tables, running.

What each step grows with (TPC-B on Lakebase PostgreSQL, serverless pipelines):

| Step | Grows with |
|---|---|
| source (empty) | LOBs: a few statements each, one JDBC session per statement |
| deploy | resources |
| ingest, first snapshot | the pipeline's tables, then its rows |
| ingest, steady | the pipeline's tables even with no data: a cost a flow a triggered update, in initializing, setting up tables and running |
| transform (n:1, silver, gold) | the sources' tables (finding them at initializing), then rows |
| merge (the merge route) | the changed LOBs a table (planning each change feed), the MERGEs and their commits: [merge paths](../../lfc-medallion/references/merge-paths.md) |
| verify | tables and LOBs: one query a table over every LOB |
| gateway explore | constant per gateway |

Plan a deployment from `predict` on records of the same configuration, at two sizes or more. Before a run, write
down what it should show (the run gate in SKILL.md).
