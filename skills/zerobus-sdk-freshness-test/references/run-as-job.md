# Run the producer as a job, in the workspace's region

From a laptop, the network to the region is part of every number. A one-time job on a single-node cluster runs
`producer.py` next to the workspace. The `#!` line and the `uv` block are ignored there, so the SDK comes from the
task's library.

1. Upload the script: `databricks workspace import <folder>/producer.py --file producer.py --format AUTO --profile <PROFILE>`.
2. Put the service principal's secret in a secret scope, so it reaches the job as an environment variable and never
   appears in its parameters.
3. Submit a run (`databricks jobs submit --json @submit.json --profile <PROFILE>`):

```json
{
  "run_name": "zerobus-sdk-freshness-test producer",
  "tasks": [{
    "task_key": "producer",
    "new_cluster": {
      "spark_version": "17.3.x-scala2.13", "node_type_id": "<a 4-8 core node>", "num_workers": 0,
      "data_security_mode": "SINGLE_USER",
      "spark_conf": {"spark.master": "local[*]", "spark.databricks.cluster.profile": "singleNode"},
      "custom_tags": {"ResourceClass": "SingleNode"},
      "spark_env_vars": {"ZEROBUS_SECRET": "{{secrets/<scope>/<key>}}"}
    },
    "libraries": [{"pypi": {"package": "databricks-zerobus-ingest-sdk>=1.10.0"}}],
    "spark_python_task": {
      "python_file": "<folder>/producer.py",
      "parameters": ["--endpoint", "<workspace-id>.zerobus.<region>.<cloud domain>", "--workspace-url", "https://<workspace host>",
                     "--table", "<catalog>.<schema>.<table>", "--client-id", "<application id>", "--secret-env", "ZEROBUS_SECRET",
                     "--minutes", "15", "--streams", "10", "--burst-at", "5", "--burst-minutes", "2", "--burst-x", "10",
                     "--out", "/Volumes/<catalog>/<schema>/<volume>/<run>/run.json"]
    }
  }]
}
```

- **`--out` on a Volume** keeps `run.json` after the cluster is gone; copy it down with `databricks fs cp
  dbfs:/Volumes/<catalog>/<schema>/<volume>/<run>/run.json .` for `report.py --run`.
- **The run's progress lines** are in the task's output (`databricks jobs get-run-output <task run id>`).
- **Cluster start takes minutes.** Start the probe once the run is `RUNNING`, or give it `--seconds` for the start-up
  as well; samples before the first event are null.
- **Node size:** one Python process builds and sends every event. At 30,000 events/s over 10 streams, an 8-core node
  kept up; a smaller one may not, and the producer's progress lines show it.
