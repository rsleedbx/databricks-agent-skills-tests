---
name: public-skill-conventions
description: "Write or change a skill in the public Databricks agent skills repo so it follows what the other skills there do: docs first, frontmatter, file names, section names, error tables, links, release stages, wording, the generated bundle, version and sign-off, and small focused PRs. Use before writing or committing a change to a public skill, before opening its PR, and when a reviewer asks why a skill looks different from the others."
---

# Conventions of the public Databricks skills

A reviewer reads a public skill next to about 30 others. What differs from them gets questioned, so follow them unless the skill needs something they don't have. Size and loading are in **skill-token-budget**; this skill covers the rest. Check a skill with:

```bash
python3 .claude/skills/public-skill-conventions/scripts/conventions-check.py <skills-repo>/skills <skill> --base upstream/main
```

It compares the skill with every other skill in that checkout, and with its own SKILL.md section names and table headers at `--base`. `FAIL` lines break a rule below; `WARN` lines are a section name or an error table header no other skill uses (keep one only if the skill needs it), an unlinked reference file, a pipeline `target:`, generated `agents/openai.yaml` text, or a script with no offline test.

## Before writing

- **Docs first.** Read the feature's docs pages before anything else, and link them. The skill adds only what the pages don't say and what an eval case shows an agent gets wrong without the line. A finding from your own tests goes in only after that check; when the docs page is wrong or silent, the fix is usually a docs feedback item, not skill text.
- **The docs are the authority both ways.** Don't claim more than a page says (a setting or source no page names), and don't deny more either ("not supported, don't even try it" when the docs are only silent). Where a page is silent, give the agent the documented route as the action and a way to check the result (validate the bundle, test the connection with a read), the way the other skills do; don't write notes about what the docs lack ("the docs don't say", "assume it doesn't"). "Not documented" in a table cell is enough.
- **Small, and no external dependencies.** No other systems' setup (cloud vault roles and tenants, cloud networking, managed-database parameters, other tools), and no fetching or scraping pages around the fetch tool: point to the docs page or the **databricks-docs** skill. What a docs page or `databricks bundle schema` already holds stays there; grow a skill by a row in a table of page links, not a section per combination.
- **Follow the prior example.** Before adding a file, an eval case or a PR, open the closest existing one (the example a reviewer cites first) and match where it lives, its fields, the PR template's sections and the evidence it shows. Where yours can't match, say why in the PR.

## Frontmatter and files

- **Frontmatter:** `name`, `description`, `compatibility` (for example `Requires databricks CLI (>= v0.294.0)`), `metadata.version`, `parent: databricks-core`. Every skill has the first two, nearly all have the rest.
- **`metadata.version`:** bump it for a large change and say in the PR that it's a suggestion; the reviewers decide. The plugin release version is the release workflow's.
- **Reference names:** descriptive kebab-case (`standard-cdc-gateway.md`), no number prefix. Link each from a table in SKILL.md (the skill's own header, for example `| Topic | File | When to read |`).
- **Folders:** `references/` for detail. `agents/` and `assets/` are generated (`python3 scripts/skills.py sync`) only when missing; the generator cuts the description off and writes "Use $<skill> for <description>", so hand-write `short_description` and `default_prompt` in `agents/openai.yaml`, each saying how the skill differs from its siblings. `scripts/` and `examples/` are rare: code samples usually live in the references.
- **Scripts:** a check a script runs only with the network (a catalog `build`) also runs offline in the repo's `tests/` (`<skill>_test.py`, stdlib `unittest`). A script that prints links prints only pages it found; a script that edits files in the skill folder is for maintainers: say so in its docstring, not in SKILL.md.
- **Generated files:** `manifest.json` and every `plugins/databricks/<provider>/` copy come from `python3 scripts/skills.py generate`; never edit them by hand. Run `generate`, then `validate` (CI fails on drift), and commit the copies with the source change. The PR then shows each skill file once per provider: the source files are the change.

## Sections

- **Keep the skill's existing section names.** Moving content to a reference is fine; renaming `## Detailed guides` or merging two sections makes a reviewer compare by hand. The common names: `## Related Skills`, `## Common Issues` (or `## Troubleshooting`), `## Workflow`, `## Quick Start`.
- **Errors go in `## Common Issues`** (or a troubleshooting reference it links): a table of the exact error text and what to do. How to read an error and recover (read the full message, fix one thing, retry, clean up) opens that section; it's not a section of its own.
- **Error tables use a header the other skills use**, the skill's own first: `Symptom | Likely cause | Fix` in the Lakeflow Connect skill; elsewhere `Issue | Solution`, `Issue | Cause | Solution`, `Error | Cause | Solution`, `Symptom | Cause | Fix`. The trigger (a setting, a source state, a limit) is the cause column, not a column of its own. Quote error text exactly, in backticks or quotes.
- **Recovery beyond a fix line** (retries, replays, getting data back) goes in its own reference, like the Zerobus skill's `recovery.md`.

## Content

- **Behavior, not test reports:** state what the product does and what to do. No "tested", "seen on", "last seen", run dates, workspace names, row counts from a run, or which build a run used. Example values in code are fine.
- **Release stages as the docs page says,** checked on the page before each change (`curl` the page; a feature with no label is GA). Say Beta or Public Preview where the page does, and the setting it needs (for example `channel: PREVIEW`) only while the page requires it.
- **Pipeline examples** use `schema:`, not the deprecated `target:`; agents copy examples as written.
- **Consistent groupings:** a list in the description (categories, sources) matches what the skill's scripts return for the same names.
- **When content moves** (files merged, deleted, a skill split): `python3 .claude/skills/public-skill-conventions/scripts/moved-lines.py <skills-repo>/skills <skill> --base upstream/main --links` lists the options, states, roles, limits and docs URLs the skill had at the base that no skill has now, and the other skills' lines naming it. Carry each term over or say in the PR why it's dropped; point each link to the skill that owns its topic now. `--lines` adds every line with no close match (long after a rewrite).
- **Links:** Databricks hosts only. Docs links cloud-neutral, `https://docs.databricks.com/<path>` (it redirects to the reader's cloud), not `/aws/en/`, `/azure/` or `learn.microsoft.com/azure/databricks`. Relative links between the skill's files.
- **Nothing outside the repo:** no test repos, tickets, chat channels, internal tools, or account and resource names, in the skill or in the repo's `CLAUDE.md` and README. Placeholders (`<PROFILE>`, `<catalog>`) for anything the user picks.
- **Profiles:** `--profile <PROFILE>` on CLI examples; never pick one for the user (the parent skill says how to choose).

## Committing

- One signed-off commit per batch of changes (`git commit -s`, the repo's DCO check), with a message that says what an agent gets.
- Rebase the branch onto the upstream default branch before a review, so the compare shows only this change; check the skill folder is the same before and after.

## Pull requests

- **Small and focused:** one skill and one change per PR. Don't stack PRs or make one depend on another; when a change needs another skill's change first, land that one first. Every open PR and its dependencies is something a reviewer has to keep track of.
- **Keep it current:** rebase onto the default branch before each push. A PR that falls far behind needs a large catch-up that reviewers then re-read.
- **Only what was asked:** in a review round, change the lines the comments name. A cleanup or a restructure elsewhere is its own PR, or is said up front in the reply.
- **Evidence:** eval results per case in three columns (no skills, the default branch's skills, this PR), and the size of each changed skill at the default branch, the head last reviewed and this PR.
- **Check with a small run:** a change no eval case's prompt or rubric touches needs no run (say so in the PR); otherwise run only the cases it touches.
