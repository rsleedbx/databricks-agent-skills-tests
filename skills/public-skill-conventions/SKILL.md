---
name: public-skill-conventions
description: "Write or change a skill in the public Databricks agent skills repo so it follows what the other skills there do: frontmatter, file names, section names, error tables, links, release stages, wording, the generated bundle, version and sign-off. Use before committing a change to a public skill, and when a reviewer asks why a skill looks different from the others."
---

# Conventions of the public Databricks skills

A reviewer reads a public skill next to about 30 others. What differs from them gets questioned, so follow them unless the skill needs something they don't have. Size and loading are in **skill-token-budget**; this skill covers the rest. Check a skill with:

```bash
python3 skills/public-skill-conventions/scripts/conventions-check.py <skills-repo>/skills <skill> --base upstream/main
```

It compares the skill with every other skill in that checkout, and with its own SKILL.md section names and table headers at `--base`. `FAIL` lines break a rule below; `WARN` lines are a section name or an error table header no other skill uses (keep one only if the skill needs it), an unlinked reference file, a pipeline `target:`, generated `agents/openai.yaml` text, or a script with no offline test.

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
- **After a split or a new skill,** search the other skills for links to the old skill on the moved topic (`grep -rn <old-skill> skills/`) and point them to the new one.
- **Links:** Databricks hosts only. Docs links cloud-neutral, `https://docs.databricks.com/<path>` (it redirects to the reader's cloud), not `/aws/en/`, `/azure/` or `learn.microsoft.com/azure/databricks`. Relative links between the skill's files.
- **Nothing outside the repo:** no test repos, tickets, chat channels, internal tools, or account and resource names, in the skill or in the repo's `CLAUDE.md` and README. Placeholders (`<PROFILE>`, `<catalog>`) for anything the user picks.
- **Profiles:** `--profile <PROFILE>` on CLI examples; never pick one for the user (the parent skill says how to choose).

## Committing

- One signed-off commit per batch of changes (`git commit -s`, the repo's DCO check), with a message that says what an agent gets.
- Rebase the branch onto the upstream default branch before a review, so the compare shows only this change; check the skill folder is the same before and after.
