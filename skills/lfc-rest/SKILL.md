---
name: lfc-rest
description: "The one way this repo's Python calls a Databricks workspace: REST paths and their JSON through lfc_rest.Rest, with the Databricks SDK only signing in (a profile on a laptop, the notebook's own login in a workspace) and carrying the request. get/post/put/patch/delete, pages (next_page_token), ls (Files API directories), poll_until. Use for any new workspace call instead of the SDK's typed services or the CLI."
---

# REST through one client

```python
from lfc_rest import Rest              # skills/lfc-rest/scripts on sys.path
api = Rest.connect("<profile>")        # in a notebook: Rest.connect(); a WorkspaceClient you hold: Rest.of(w)
api.get("/api/2.0/pipelines/<id>")
for e in api.pages("/api/2.0/pipelines/<id>/events", "events", max_results=250): ...
api.ls("/Volumes/<c>/<s>/<v>/<dir>")
```

Why REST: the answer is the API's own JSON, every field (the SDK's typed list of pipeline events drops each event's
`details`), and the same call reads the same in the REST docs, the CLI's `databricks api` and here.

Kept from the SDK, since it carries each request (`api_client.do`): the login both ways, OAuth refresh, retries on
429 and 503, typed errors (`NotFound`, `PermissionDenied`). Given up: typed models, waiters (`poll_until` instead),
automatic paging (`pages`), chunked large-file transfers (`w.files`), the SDK tracking field changes for us.

- `Rest.connect(profile, retry_s=20)`: a shorter retry window for probes whose refusal is the answer. The SDK retries
  some errors that never pass: `clusters/list-zones` on a serverless-only Azure workspace (BadRequest "does not have
  any associated worker environments") for its default 300 s.
- When the SDK gives up retrying, `Rest.call` raises the API's last error, not the SDK's "Timed out after ...".
- Tests: `Rest(lambda method, path, query, body: {...})`, a fake answering as the API does.

Used by: lfc-workspace-setup `ready.py`, lfc-pipeline-watch `lfc_watch`.
