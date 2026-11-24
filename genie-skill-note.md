> **In Genie Code or a notebook:** run this skill's Python in the notebook's own process: import its module and call
> its functions; for a command-line script, call its `main([...])` with the same arguments as a list. Never run its
> commands or scripts with `%sh`, `!` or `subprocess`: the Databricks CLI refuses to run on notebook compute, and the
> Python SDK has no login in a child process. Install packages with `%pip`; run SQL with `spark.sql`.
