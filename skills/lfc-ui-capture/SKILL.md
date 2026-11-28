---
name: lfc-ui-capture
description: "Capture the Lakeflow Connect UI wizard as numbered, masked PNGs and GIFs, by driving your own signed-in Chrome with the Chrome DevTools tools (never Playwright: device-trust sign-in refuses the browsers automation starts). Covers attaching to Chrome and when to reconnect, the shared pipeline/schema/job name, the screenshot steps, and finishing a run. Use for 'capture the UI', 'UI demo', 'screenshots of the wizard', or when the Chrome tools show only about:blank."
---

# Lakeflow Connect UI captures from your own Chrome

The demos, their folders, and the finishing script are in [`ui-demos/`](../../ui-demos/README.md). This skill is the procedure.

## 0. QBC on PostgreSQL, SQL Server, MySQL: run the script

`ui-demos/lfc_ui/wizard.py` does sections 2 to 4 below on its own, in about 3 minutes: Playwright attached to your Chrome (`connect_over_cdp` to the socket in Chrome's `DevToolsActivePort` file; it never launches a browser). Usual way: `../.venv/bin/python -m lfc_ui.auth check` (if it fails, `auth save`: attaches to the user's Chrome once, they click **Allow**, it writes the workspace's cookies to `auth.json`), then `cd ui-demos && ../.venv/bin/python -m lfc_ui.wizard <postgres|sqlserver|mysql> --launch`: Playwright's own headless browser with that session, no prompt, and the MCP server can stay on. Without `--launch` it attaches to the user's Chrome instead: turn the Chrome DevTools MCP server **off** first (Chrome allows one client; a second gets 403), and the user clicks **Allow** (no script can). On failure it leaves `fail.png`, `fail.aria.txt` and `trace.zip` in the run folder: turn the MCP server back on, do that step by hand as below, and fix the step in `wizard.py`. For a new source or mode, do it by hand once, then add it to the script.

Script runs save `runs/<name>/png/`, masked in the page before every screenshot (list rows and options that don't name this run's objects, and every email but `--me`), so they can be committed; there's no `redacted/` copy. The steps below (hand captures) save `clear/`, which isn't masked: keep it local and share only `redacted/`.

## 1. Attach to your Chrome (check every session)

```bash
python3 skills/lfc-ui-capture/scripts/check_chrome_attach.py     # changes nothing; prints the fix when something's off
```

It checks that the `chrome-devtools` entry in `~/.claude.json` has `--autoConnect`, that your Chrome is running, and when the tools' server started. Then call `list_pages`:

- **Your tabs show up** (at least `chrome://inspect/#remote-debugging`): attached. Open the workspace with `new_page`; a page without a login form means you're signed in.
- **Only `about:blank`:** the server runs without `--autoConnect` and opened a browser of its own. Add the flag if the script says it's missing, then **`/mcp` → `chrome-devtools` → Reconnect**, and accept Chrome's prompt to allow the connection.

**When a reconnect is needed:** not every session. Claude Code starts the server with the session, so with the flag in the config a new session attaches on its own, and you only accept Chrome's prompt. Reconnect when the flag was added or changed during the session (the running server read the old config), or when Chrome was restarted. Seen 2026-10-18: after a reboot something rewrote the entry without `--autoConnect`, and the server opened a blank browser of its own until the flag was put back and the server reconnected.

All Chrome profiles run in one Chrome, so tabs from every profile window (for example a "Databricks" profile) are reachable once attached. **Turn remote debugging off when done**: while it's on, any local program can drive your signed-in sessions.

## 2. Name everything once

Take the epoch once, when the user says yes to the plan, and use one name for the pipeline, its schema, its schedule job, and the run folder: `<firstname><lastname>_<connection>_<epoch>`, every character that isn't a letter or digit turned into `_` (the **Default names** rule of the databricks-lakeflow-connect skill, so UI, bundle, and REST runs match):

```bash
EPOCH=$(date +%s); NAME=janedoe_lfcddemo_azure_pg_$EPOCH; RUN=ui-demos/db/postgres/qbc/runs/$NAME; mkdir -p $RUN/clear
```

**Source tables:** by convention `intpk` and `dtix` in schema `lfcddemo` (MySQL: database `lfcddemo`); see `ui-demos/README.md`. The wizard has no append-only choice, so set `dtix` to `scd_type: APPEND_ONLY` in the YAML view before the first run, in this order: pick both tables in the wizard, **Save and continue** on Destination (its validation fails on `dtix` but saves the tables; switching to YAML before a save drops them), switch to YAML, add the line, **Save and run pipeline**.

**Ask before creating anything** in a shared workspace: say the name, catalog, and tables, and wait for one yes. "Create pipeline and continue" in the wizard creates the pipeline.

## 3. Screenshot each step, then finish

Each step, in this order:

1. `take_snapshot` to find the uids: the element just finished (the field just typed into, or the option just picked), and the one about to be clicked.
2. **Box them in red:** `evaluate_script` with [`scripts/highlight.js`](scripts/highlight.js) as `function` and `args: [<finished-uid>, <next-uid>]` (leave out either when there's none, such as the first screen). It draws a thick red box around each, replacing the previous step's boxes; they ignore the mouse, so the click still lands.
3. `take_screenshot` with `filePath` `$RUN/clear/NNN_<step>.png` (`001_add_data.png`, `002_select_connection.png`, ...): the PNG shows what was just done and where the click goes next.
4. `click` / `fill`. Run [`scripts/clear-highlight.js`](scripts/clear-highlight.js) before a screenshot that should have no boxes, such as the final graph. After the first update, save the pipeline's graph view last. Then:

```bash
cd ui-demos/db/postgres/qbc
PYTHONPATH=../../.. ../../../../.venv/bin/python -m lfc_ui.capture runs/$NAME \
  --secret <account-id> --secret <workspace-id> --meta workspace=<host> --meta pipeline_id=<id>
```

**Driving the wizard's fields** (seen 2026-10-18):

- `fill` on a field that already has text **adds to it**, and `Meta+A` doesn't select the text either. To replace a value, set it the React way in `evaluate_script`: the `HTMLInputElement` value setter, then `input` and `change` events. Check the value before clicking Create or Save.
- Dropdown options (catalog, cursor column, primary key) often aren't in `take_snapshot`: open the list, then click the `[role="option"]` whose text matches, in `evaluate_script`.
- The YAML and JSON views are Monaco editors with no global `monaco`. The first edit turns the view into a diff ("Unsaved changes") and only the first typed character lands: focus the right-hand editor's `textarea`, move with the arrow keys, type the rest, and read the visible `.view-line` elements to check it.
- A native browser dialog (for example switching the wizard to YAML) freezes every tool call until it's answered: `handle_dialog`.
- The REST API rejects the browser session (401), so read pipelines and jobs from the UI or with the CLI.

It writes `redacted/` (digit IDs masked, last four kept), `metrics.yaml` (seconds between screenshots), and both GIFs. Captures stay local (`runs/` is gitignored): share only the redacted copies.

## 4. Clean up

Once the graph of a completed update is saved, **delete the pipeline and its schedule job** (Jobs & Pipelines → the job → More actions → Delete job; the pipeline → More actions → Delete). Delete the job first, or it runs the pipeline again on its next tick (seen: the hourly job ran once before it was deleted). Then drop the run's schema (`databricks schemas delete main.<name> --force`): it holds the tables, event log, and staging volume. The script does all three. The PNGs stay in `runs/<name>/` on the laptop.
