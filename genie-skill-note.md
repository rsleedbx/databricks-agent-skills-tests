> **In Genie Code or a notebook:** this skill's shell commands (`bash ...`, `*.sh`, `databricks ...`) are for a laptop.
> Never run them with `%sh`, `!` or `subprocess`: the Databricks CLI refuses to run on notebook compute, and the Python
> SDK has no login in a child process. Use the skill's Python in the notebook's own process: import its module and
> call its functions, or a script's `main([...])` with the same arguments as a list. Install packages with `%pip`; run
> SQL with `spark.sql`. A step that only has a shell form isn't available here: say so instead of working around it.
