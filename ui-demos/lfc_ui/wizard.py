"""Drive the Lakeflow Connect query-based (QBC) wizard in your own signed-in Chrome and capture each step.

Playwright attaches to the Chrome you already use (the socket named in its DevToolsActivePort file, opened by
chrome://inspect/#remote-debugging), so no new browser has to get past single sign-on. Chrome allows one client on
that socket: turn the Chrome DevTools MCP server off (/mcp) before a run, and back on for the fallback.

Each step boxes the field just finished and the next click in red, blurs other people's objects, saves
runs/<name>/png/NNN_<step>.png, then acts.
On any failure it saves fail.png, fail.aria.txt (the accessibility tree) and trace.zip in the run folder and exits 1:
get past the changed screen by hand, then fix that step here.

    cd ui-demos && ../.venv/bin/python -m lfc_ui.wizard postgres          # or sqlserver, mysql; --keep to keep the pipeline
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import pathlib
import re
import subprocess
import sys
import time

from playwright.sync_api import Locator, Page, sync_playwright

from .capture import finish_run

HOST = "https://e2-demo-field-eng.cloud.databricks.com"
CREATED: dict = {}  # a gateway runs (and bills) continuously: stopped if the run fails
WORKSPACE_ID = "1444828305810485"
PROFILE = "e2fieldeng"
CATALOG = "main"
SCHEMA = "lfcddemo"  # by convention: intpk (pk, cursor dt, SCD type 1) and dtix (no key, cursor dt, append-only)
SOURCES = {
    "postgres": {"type": "POSTGRESQL", "connection": "lfcddemo-azure-pg", "database": "oigix9su"},
    "sqlserver": {"type": "SQLSERVER", "connection": "lfcddemo-azure-sqlserver", "database": "vaevee3u"},
    "mysql": {"type": "MYSQL", "connection": "lfcddemo-azure-mysql", "database": None},  # databases are listed directly
}
LONG = 180_000  # ms: pipeline create, compute start, an update


class Run:
    def __init__(self, page: Page, folder: pathlib.Path, keep: str = "", me: str = "") -> None:
        self.page, self.folder, self.n, self.keep, self.me = page, folder, 0, keep, me.lower()
        (folder / "png").mkdir(parents=True, exist_ok=True)

    def box(self, *locs: Locator | None) -> None:
        """Thick red box around each element, replacing the previous step's boxes; they ignore the mouse."""
        locs = [loc for loc in locs if loc is not None]

        def quiet(f):  # an element the page is re-rendering: retry once, then leave its box out
            for _ in range(2):
                try:
                    return f()
                except Exception:
                    time.sleep(0.5)
            return None

        for loc in reversed(locs):  # scroll first (the next click last wins), then measure: scrolling moves boxes
            quiet(lambda: loc.scroll_into_view_if_needed(timeout=5_000))
        rects = [b for loc in locs if (b := quiet(lambda: loc.bounding_box(timeout=5_000)))]
        self.page.evaluate(
            """rects => {
                document.querySelectorAll('[data-lfc-highlight]').forEach(e => e.remove());
                for (const r of rects) {
                    const d = document.createElement('div');
                    d.setAttribute('data-lfc-highlight', '');
                    Object.assign(d.style, {position: 'fixed', left: `${r.x - 6}px`, top: `${r.y - 6}px`,
                        width: `${r.width + 12}px`, height: `${r.height + 12}px`, border: '5px solid #ff0000',
                        borderRadius: '4px', boxSizing: 'border-box', pointerEvents: 'none', zIndex: 2147483647});
                    document.body.appendChild(d);
                }
            }""",
            rects,
        )

    def mask(self) -> None:
        """Blur what belongs to other people (this repository is public): rows and options of lists that don't
        mention this run's own objects, and any email address but the owner's."""
        self.page.evaluate(
            """([keep, me]) => {
                const own = new RegExp(keep, 'i');
                const rows = document.querySelectorAll('main [role=row], main tr, [role=option], [role=listbox] li');
                for (const r of rows) {
                    if (r.querySelector('[role=columnheader], th')) continue;
                    const t = r.innerText || '';
                    if (t.trim() && !own.test(t)) r.style.filter = 'blur(6px)';
                }
                const walk = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                const email = /[\\w.+-]+@[\\w-]+\\.[\\w.]+/g;
                for (let n; (n = walk.nextNode());) {
                    const hits = (n.nodeValue.match(email) || []).filter(e => e.toLowerCase() !== me);
                    if (hits.length && n.parentElement) n.parentElement.style.filter = 'blur(6px)';
                }
            }""",
            [self.keep, self.me],
        )

    def shot(self, step: str, *locs: Locator | None) -> None:
        self.box(*locs)
        self.mask()
        self.n += 1
        self.page.screenshot(path=str(self.folder / "png" / f"{self.n:03d}_{step}.png"))
        print(f"  {self.n:03d} {step}", flush=True)

    def clear(self) -> None:
        self.box()


def row(loc: Locator) -> Locator:
    """The table row holding a checkbox or radio, so the box covers the whole line."""
    return loc.locator("xpath=ancestor::*[@role='row' or self::tr][1]")


def option(page: Page, text: str) -> Locator:
    return page.get_by_role("option", name=text, exact=True)


def pick(r: Run, combo: Locator, text: str, step: str, done: Locator | None = None) -> None:
    """Open a dropdown, box the option, screenshot, pick it."""
    combo.click()
    opt = option(r.page, text)
    opt.wait_for(timeout=60_000)  # cursor and key lists wait for the source's columns
    r.shot(step, done, opt)
    opt.click()
    r.page.keyboard.press("Escape")


def wizard(r: Run, src: dict, name: str, mode: str = "qbc", max_wait: int = 600) -> dict:
    """mode: qbc (query-based), cdc (integrated CDC, one pipeline), gateway (standard CDC: gateway + pipeline)."""
    p = r.page
    base, epoch = name.rsplit("_", 1)
    pipeline_name = f"{base}_ap_{epoch}" if mode == "gateway" else name  # standard CDC: applier and gateway
    p.goto(f"{HOST}/ingestion/add?o={WORKSPACE_ID}")
    tile = p.locator(f'a[href*="sourceType={src["type"]}"]').first
    tile.wait_for()
    r.shot("add_data", tile)
    tile.click()

    conn = p.get_by_role("radio", name=f"Choose connection {src['connection']}", exact=True)
    conn.wait_for()
    p.wait_for_function("n => { const e = document.querySelector(`[aria-label=\"Choose connection ${n}\"]`); return e && !e.disabled; }", arg=src["connection"])
    r.shot("select_connection", row(conn))
    conn.click()
    nxt = p.get_by_role("button", name="Next", exact=True)
    r.shot("connection_next", row(conn), nxt)
    nxt.click()

    kind = p.get_by_role("radio", name=re.compile("^Query based capture.*Runs a managed query" if mode == "qbc" else "^Change data capture.*Captures"))
    r.shot("ingestion_setup", kind)
    kind.click()
    pname = p.get_by_role("textbox", name="Pipeline name", exact=True)
    r.shot("query_based_capture" if mode == "qbc" else "change_data_capture", kind, pname)
    pname.fill(pipeline_name)
    cat = p.get_by_role("textbox", name="Select catalog", exact=True).first  # CDC has a second pair for staging
    r.shot("pipeline_name", pname, cat)
    cat.click()
    cat.press_sequentially(CATALOG)
    opt = option(p, CATALOG)
    r.shot("event_log_catalog", cat, opt)
    opt.click()
    sch = p.get_by_role("textbox", name="Select schema", exact=True).first
    sch.click()
    sch.press_sequentially(name)
    create = p.get_by_role("button", name="Create schema", exact=True)
    p.get_by_text("No schemas found").first.wait_for()  # the name is free
    r.shot("event_log_schema_create", sch, create)
    create.click()
    dialog = p.get_by_role("dialog")
    dname = dialog.get_by_role("textbox", name="Schema name")
    dname.fill(name)
    dcreate = dialog.get_by_role("button", name="Create", exact=True)
    r.shot("new_schema_name", dname, dcreate)
    dcreate.click()
    p.get_by_text("Schema created").first.wait_for()
    sch = p.get_by_role("textbox", name="Select schema", exact=True).first
    go = p.get_by_role("button", name=re.compile("^Create pipelines? and continue$"))
    r.shot("schema_selected", sch, go)
    serverless = p.get_by_role("radio", name=re.compile("^Serverless.*Uses serverless"))
    if mode != "qbc" and serverless.count():  # CDC defaults to Classic
        r.shot("compute_classic_default", serverless)
        serverless.click()
    r.shot("compute_serverless", serverless if serverless.count() else None, go)
    if mode != "qbc":
        adv = p.get_by_role("button", name="Advanced settings", exact=True)
        if adv.get_attribute("aria-expanded") != "true":
            adv.click()
        pm = p.get_by_role("radio", name=re.compile("^Integrated CDC connector" if mode == "cdc" else "^Gateway \\+ ingestion pipeline"))
        if pm.count():  # PostgreSQL has no choice: gateway only
            pm.click()
            r.shot("pipeline_mode_" + mode, pm, go)
    if mode == "gateway":
        gname = p.get_by_role("textbox", name="Gateway name", exact=True)
        gname.fill(f"{base}_gw_{epoch}")
        for box in p.get_by_role("textbox", name="Select schema", exact=True).all():  # gateway mode leaves them empty
            if not box.input_value():
                box.click()
                box.press_sequentially(name)
                option(p, name).click()
        r.shot("gateway_name_staging", gname, go)
    go.click()

    p.wait_for_url(re.compile("pipelineId="), timeout=LONG)
    pipeline_id = re.search(r"[?&]pipelineId=([0-9a-f-]+)", p.url).group(1)
    print(f"  pipeline {pipeline_id}", flush=True)
    gateway_id = None
    if mode == "gateway":
        gateway_id = CREATED["gateway"] = re.search(r"gatewayPipelineId=([0-9a-f-]+)", p.url).group(1)
        print(f"  gateway {gateway_id}: {make_serverless(gateway_id)}", flush=True)
    p.wait_for_function("() => !/Waiting for compute|Loading source table skeleton|Validating pipeline/.test(document.querySelector('main')?.innerText || '')", timeout=LONG)

    if mode == "qbc" and src["database"]:  # CDC lists the connection's database directly
        db = p.get_by_role("textbox", name="Add a Database name", exact=True)
        db.fill(src["database"])
        add = p.get_by_role("button", name="Add Database name", exact=True)
        r.shot("add_database", db, add)
        add.click()
    expand = p.get_by_role("button", name=f"Expand {SCHEMA}", exact=True)
    p.get_by_role("button", name=re.compile("^Expand ")).first.wait_for(timeout=LONG)
    for _ in range(60):  # the schema list is virtualized: a big database renders only the first rows
        if expand.count():
            break
        rows = p.get_by_role("button", name=re.compile("^Expand "))
        rows.last.scroll_into_view_if_needed()
        p.mouse.wheel(0, 600)
        time.sleep(0.5)
    expand.wait_for(timeout=30_000)
    r.shot("source_schema", row(expand))
    expand.click()

    intpk = p.get_by_role("checkbox", name="Select intpk", exact=True)
    r.shot("source_tables", row(intpk))
    intpk.click()
    if mode == "qbc":
        cursor = p.get_by_role("combobox", name=re.compile("^Cursor column"))
        r.shot("intpk_settings", row(p.get_by_role("checkbox", name="Unselect intpk", exact=True)), cursor)
        pick(r, cursor, "dt", "intpk_cursor_dt")
        keys = p.get_by_role("combobox", name=re.compile("^Primary keys"))
        pick(r, keys, "pk", "intpk_primary_key_pk", cursor)
    else:  # CDC takes keys and order from the source: nothing to pick
        keys = p.get_by_text(f"{SCHEMA}.intpk").first
        keys.wait_for()
        r.shot("intpk_settings", row(p.get_by_role("checkbox", name="Unselect intpk", exact=True)), keys)
    dtix = p.get_by_role("checkbox", name="Select dtix", exact=True)
    r.shot("intpk_done", keys, row(dtix))
    dtix.click()
    p.get_by_text(f"{SCHEMA}.dtix").first.wait_for()
    if mode == "qbc":
        cursor = p.get_by_role("combobox", name=re.compile("^Cursor column"))
        r.shot("dtix_settings", row(p.get_by_role("checkbox", name="Unselect dtix", exact=True)), cursor)
        pick(r, cursor, "dt", "dtix_cursor_dt")
    else:
        cursor = p.get_by_text(f"{SCHEMA}.dtix").first
        r.shot("dtix_settings", row(p.get_by_role("checkbox", name="Unselect dtix", exact=True)), cursor)
    nxt = p.get_by_role("button", name="Next", exact=True)
    r.shot("source_next", cursor, nxt)
    nxt.click()

    dest = p.get_by_role("radio", name=f"Select {CATALOG}.{name}", exact=True)
    dest.wait_for()
    if dest.get_attribute("aria-checked") != "true" and not dest.is_checked():
        dest.click()
    btn = p.get_by_role("button", name=re.compile("^(Next|Save and continue)$"))
    label = btn.inner_text().strip()
    r.shot("destination_schema", row(dest), btn)
    btn.click()
    if mode != "qbc":
        # CDC: the pipeline saves, then Database setup checks the source's settings and permissions
        p.get_by_role("heading", name="Database setup", exact=True, level=3).wait_for(timeout=LONG)
        validate = p.get_by_role("button", name="Validate", exact=True)
        r.shot("database_setup", validate)
        validate.click()
        p.wait_for_function("() => !/Pending validation|Validating/.test(document.querySelector('main')?.innerText || '')", timeout=600_000)  # MySQL: over 3 minutes
        nxt = p.locator("button:not([data-component-id*=pagination])", has_text=re.compile("^Next$"))
        r.shot("database_validated", p.locator("main table").last, nxt)
        nxt.click()
    if mode == "qbc" and label == "Save and continue":
        # it validates, which fails on dtix (no key, still SCD type 1), but it saves the tables
        p.get_by_text(re.compile("doesn't have a primary key|Configure pipeline schedules")).first.wait_for(timeout=LONG)
        r.shot("validation_dtix_no_key")
    else:
        # Schedules: the wizard made an hourly job "<name> job" when it created the pipeline; give it the name
        p.get_by_text("Configure pipeline schedules").first.wait_for(timeout=LONG)
        r.shot("schedules")
        p.get_by_role("button", name="Schedule actions").first.click()
        p.get_by_role("menuitem", name="Edit").click()
        job = p.get_by_role("dialog").get_by_role("textbox", name="Job name")
        job.fill(name)
        save = p.get_by_role("dialog").get_by_role("button", name="Save")
        r.shot("job_name", job, save)
        save.click()
        p.get_by_role("dialog").wait_for(state="hidden")
        if mode == "qbc":
            # Next didn't save the tables; Save and close does (switching to YAML before a save drops them)
            close = p.get_by_role("button", name="Save and close", exact=True)
            r.shot("save_and_close", close)
            r.clear()
            close.click()
            p.wait_for_url(re.compile(f"/pipelines/{pipeline_id}"), timeout=LONG)

    run = p.get_by_role("button", name="Save and run pipeline", exact=True)
    if mode == "qbc":
        # no append-only choice in the wizard: set it in the saved pipeline's YAML. CDC refuses APPEND_ONLY
        # ("can only be used for incremental ingestion of query based connector"): keyless dtix stays SCD type 1
        p.goto(f"{HOST}/ingestion/pipelines/setup?sourceType={src['type']}&o={WORKSPACE_ID}&pipelineId={pipeline_id}&setupFormat=yaml")
        append_only(r)
        r.shot("yaml_dtix_append_only", p.locator(".monaco-editor .view-line", has_text="scd_type: APPEND_ONLY").last, run)
    else:
        r.shot("save_and_run", run)
    r.clear()
    run.click()
    p.wait_for_url(re.compile("/updates/"), timeout=LONG)
    update_id = re.search(r"/updates/([0-9a-f-]+)", p.url).group(1)
    print(f"  update {update_id}", flush=True)
    state = p.locator("main").get_by_text(re.compile(r"^(Completed|Failed|Canceled)$")).first
    # the max-wait clock starts once the update is Running (not while it queues or waits for compute)
    p.locator("main").get_by_text(re.compile(r"^(Running|Completed|Failed|Canceled)")).first.wait_for(timeout=LONG)
    t0 = time.time()
    try:
        state.wait_for(timeout=max_wait * 1000)
        final, step = state.inner_text().strip(), "pipeline_graph_completed"
    except Exception:
        # integrated CDC runs until its lag converges or its cap: capture it running, then stop it
        p.reload()  # the update page doesn't always redraw its graph while it waits
        p.get_by_role("button", name="Edit pipeline").wait_for(timeout=LONG)
        time.sleep(5)
        r.shot("pipeline_graph_running")
        p.get_by_test_id("stop-pipeline-button").click()  # the header's; the Tables panel has another Stop
        confirm = p.get_by_role("dialog").get_by_role("button", name=re.compile("^Stop"))
        if confirm.count():
            confirm.first.click()
        state.wait_for(timeout=LONG)
        final, step = f"Stopped after {round((time.time() - t0) / 60, 1)} min", "pipeline_graph_stopped"
    time.sleep(3)
    rows = [t for t in p.locator("main [role=row], main tr").all_inner_texts() if re.match(r"^(dtix|intpk)\b", t.strip())]
    if gateway_id:  # the applier, with the gateway's status; then click through to the gateway, running
        glink = p.locator("main a", has_text="_gw_").first
        r.shot(step, p.get_by_text("Gateway status").first, glink)
        with p.context.expect_page() as new:
            glink.click()
        g = new.value
        g.set_viewport_size(p.viewport_size)
        g.get_by_text(re.compile(r"^Running")).first.wait_for(timeout=LONG)
        time.sleep(5)
        r.page = g
        r.shot("gateway_running", g.get_by_text(re.compile(r"^Running")).first)
        r.page = p
        g.close()
    else:
        r.shot(step)
    return {"pipeline_id": pipeline_id, "gateway_id": gateway_id, "update": update_id, "state": final,
            "update_minutes": round((time.time() - t0) / 60, 1), "tables": [" ".join(t.split()) for t in rows]}


def numbered_lines(p: Page) -> list[tuple[int, str]]:
    """The YAML editor's rendered lines with their line numbers (from the gutter, so scrolling doesn't matter)."""
    return p.evaluate(
        """ed => {
            const lines = [...ed.querySelectorAll('.view-line')].map(e => [parseInt(e.style.top), e.innerText.replace(/\\u00a0/g, ' ')]);
            const nums = Object.fromEntries([...ed.querySelectorAll('.line-numbers')].map(e => [parseInt(e.parentElement.style.top), parseInt(e.innerText)]));
            return lines.filter(([t]) => nums[t]).sort((a, b) => a[0] - b[0]).map(([t, x]) => [nums[t], x]);
        }""",
        p.locator(".monaco-editor").last.element_handle(),
    )


def go_to_line(p: Page, n: int) -> None:
    """Put the cursor at the end of line n of the last editor (the right-hand side once it's a diff view): scroll
    until the line is drawn, click it, End. Monaco here has no Go to line shortcut."""
    ed = p.locator(".monaco-editor").last
    for _ in range(40):
        nums = [k for k, _ in numbered_lines(p)]
        if n in nums:
            break
        box = ed.bounding_box()
        p.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        p.mouse.wheel(0, 200 if not nums or n > max(nums) else -200)
        time.sleep(0.3)
    top = p.evaluate(
        """([ed, n]) => { for (const e of ed.querySelectorAll('.line-numbers')) if (parseInt(e.innerText) === n)
            return parseInt(e.parentElement.style.top); return null; }""",
        [ed.element_handle(), n],
    )
    if top is None:
        raise RuntimeError(f"line {n} isn't drawn in the editor")
    ed.locator(f'.view-line[style*="top:{top}px"]').first.click()
    p.keyboard.press("End")


def append_only(r: Run) -> None:
    """Add `scd_type: APPEND_ONLY` to dtix (keyless: SCD type 1 would merge duplicate rows) in the YAML editor."""
    p = r.page
    p.locator(".monaco-editor").last.wait_for()
    ed = p.locator(".monaco-editor").last
    box = ed.bounding_box()
    p.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    for _ in range(10):  # to the end, where dtix (the last table) is
        p.mouse.wheel(0, 2000)
        time.sleep(0.2)
    lines = numbered_lines(p)
    src = [n for n, l in lines if l.strip() == "source_table: dtix"]
    if not src:
        raise RuntimeError("YAML has no dtix table: the wizard's choices weren't saved before switching")
    i = src[-1]
    indent = next(len(l) - len(l.lstrip()) for n, l in lines if n == i)
    tc = next((n for n, l in lines if n > i and l.strip() == "table_configuration:" and len(l) - len(l.lstrip()) == indent), None)
    anchor = tc or i  # QBC has table_configuration (the cursor); CDC may not
    first, rest = ("s", ["cd_type: APPEND_ONLY"]) if tc else ("t", ["able_configuration:", "scd_type: APPEND_ONLY"])
    go_to_line(p, anchor)
    p.keyboard.press("Enter")
    p.keyboard.type(first)  # the first keystroke turns the view into a diff editor; type the rest in its right side
    time.sleep(1.5)
    go_to_line(p, anchor + 1)
    for k, text in enumerate(rest):
        if k:
            p.keyboard.press("Enter")
        p.keyboard.type(text)
    seen: list[str] = []
    for _ in range(10):  # the editor redraws a moment after typing; the diff view has three editors: read them all
        seen = p.evaluate("() => [...document.querySelectorAll('.monaco-editor .view-line')].map(e => e.innerText.replace(/\\u00a0/g, ' '))")
        if any("scd_type: APPEND_ONLY" in l for l in seen):
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("scd_type line didn't land; editor shows: " + " / ".join(l.strip() for l in seen[-14:]))
    got = dict(numbered_lines(p))
    if got.get(anchor + 1, "").strip() != "scd_type: APPEND_ONLY" and got.get(anchor + 2, "").strip() != "scd_type: APPEND_ONLY":
        raise RuntimeError(f"scd_type landed in the wrong place: line {anchor + 1} reads {got.get(anchor + 1)!r}")


_ENV: dict | None = None


def cli(*args: str) -> str:
    """The Databricks CLI for PROFILE; when its login has expired, a token from the account login (minted once)."""
    global _ENV
    if _ENV is None:
        _ENV = dict(os.environ, PATH="/opt/homebrew/bin:" + os.environ.get("PATH", ""))  # the newer CLI first
        tok = subprocess.run([sys.executable, str(REPO / "skills/account-token-fallback/scripts/ws_token.py"), PROFILE],
                             capture_output=True, text=True, env=_ENV, timeout=120).stdout.strip()
        if tok:
            _ENV["DATABRICKS_CONFIG_FILE"] = tok
    return subprocess.run(["databricks", *args, "--profile", PROFILE, "-o", "json"],
                          capture_output=True, text=True, env=_ENV, timeout=300).stdout


def make_serverless(pipeline_id: str) -> str:
    """The wizard has no compute choice for the gateway (it's created classic): switch it to serverless."""
    spec = json.loads(cli("pipelines", "get", pipeline_id))["spec"]
    spec["serverless"] = True
    spec.pop("clusters", None)
    spec.pop("id", None)
    f = REPO / "ui-demos" / f".gw-{pipeline_id}.json"
    f.write_text(json.dumps(spec))
    cli("pipelines", "update", pipeline_id, "--json", f"@{f}")
    f.unlink()
    return "serverless=" + str(json.loads(cli("pipelines", "get", pipeline_id))["spec"].get("serverless"))


def cleanup(name: str, pipeline_id: str, gateway_id: str | None = None) -> None:
    """Delete the schedule job (first, or it runs the pipeline again), the pipelines, then the run's schema."""
    for job_name in (name, f"{name} job"):
        for j in json.loads(cli("jobs", "list", "--name", job_name) or "[]"):
            cli("jobs", "delete", str(j["job_id"]))
            print(f"  deleted job {j['job_id']}")
    for pid in filter(None, (pipeline_id, gateway_id)):  # deleting the gateway also stops it
        cli("pipelines", "delete", pid)
        print(f"  deleted pipeline {pid}")
    cli("schemas", "delete", f"{CATALOG}.{name}", "--force")  # with its tables, event log, and staging volume
    print(f"  dropped schema {CATALOG}.{name}")


REPO = pathlib.Path(__file__).resolve().parents[2]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("source", choices=sorted(SOURCES))
    ap.add_argument("--mode", choices=["qbc", "cdc", "gateway"], default="qbc", help="query-based, integrated CDC, or standard CDC (gateway)")
    ap.add_argument("--max-wait", type=float, default=10, help="minutes to wait once the update is Running (default 10); then capture it running, stop it, and go on")
    ap.add_argument("--me", default=os.environ.get("LFC_UI_EMAIL", ""), help="your email: the only one left unblurred")
    ap.add_argument("--owner", default=re.sub(r"[^a-z0-9]", "", getpass.getuser().lower()))
    ap.add_argument("--keep", action="store_true", help="keep the job, pipelines, and schema")
    ap.add_argument("--launch", action="store_true", help="start Playwright's own browser with auth.json (lfc_ui.auth save) instead of attaching to Chrome: no Allow prompt, and the Chrome DevTools MCP server can stay on")
    ap.add_argument("--headed", action="store_true", help="with --launch: show the browser window")
    ap.add_argument("--width", type=int, default=1600, help="with --launch: page width in pixels (PNG size at 1x)")
    ap.add_argument("--height", type=int, default=900, help="with --launch: page height in pixels")
    a = ap.parse_args()
    src = SOURCES[a.source]
    name = re.sub(r"[^a-z0-9]", "_", f"{a.owner}_{src['connection']}_{int(time.time())}".lower())
    folder = REPO / "ui-demos/db" / a.source / a.mode / "runs" / name
    print(name, flush=True)
    port, path = ("", "") if a.launch else (pathlib.Path.home() / "Library/Application Support/Google/Chrome/DevToolsActivePort").read_text().split()[:2]
    with sync_playwright() as pw:
        if a.launch:
            auth = REPO / "ui-demos/auth.json"
            browser = pw.chromium.launch(headless=not a.headed)
            ctx = browser.new_context(storage_state=str(auth), viewport={"width": a.width, "height": a.height})
        else:
            browser = pw.chromium.connect_over_cdp(f"ws://127.0.0.1:{port}{path}", timeout=60_000)
            ctx = browser.contexts[0]
        ctx.tracing.start(screenshots=True, snapshots=True)
        page = ctx.new_page()
        page.set_default_timeout(30_000)
        page.on("dialog", lambda d: d.accept())
        base = name.rsplit("_", 1)[0]
        keep = "|".join(map(re.escape, [base, SCHEMA, "intpk", "dtix", src["connection"], src["database"] or SCHEMA,
                                        "staging", "extractor_", CATALOG + "."])) + f"|^{CATALOG}$"
        r = Run(page, folder, keep, a.me)
        result: dict = {}
        try:
            result = wizard(r, src, name, a.mode, int(a.max_wait * 60))
        except Exception as e:  # leave everything needed to take over by hand
            page.screenshot(path=str(folder / "fail.png"))
            (folder / "fail.aria.txt").write_text(page.locator("body").aria_snapshot())
            ctx.tracing.stop(path=str(folder / "trace.zip"))
            if gid := CREATED.get("gateway"):
                cli("pipelines", "stop", gid)
                print(f"  stopped gateway {gid} (delete it, and the applier, when done)", file=sys.stderr)
            print(f"FAILED after step {r.n:03d}: {type(e).__name__}: {e}\n  url: {page.url}\n  see {folder}/fail.png, fail.aria.txt, trace.zip", file=sys.stderr)
            return 1
        ctx.tracing.stop(path=str(folder / "trace.zip"))
        page.close()
        if a.launch:
            browser.close()
    print(json.dumps(result, indent=1))
    done = result.get("state", "").startswith(("Completed", "Stopped"))
    if not a.keep and done:
        cleanup(name, result["pipeline_id"], result.get("gateway_id"))
    finish_run(folder, [WORKSPACE_ID], {"workspace": HOST.split("//")[1].split(".")[0], "source": a.source,
                                        "connection": src["connection"], **result})
    return 0 if done else 1


if __name__ == "__main__":
    sys.exit(main())
