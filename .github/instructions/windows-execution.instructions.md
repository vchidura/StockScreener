---
description: "Use when running or debugging Windows PowerShell commands, Python or Node scripts, SQL audits, tests, builds, VS Code tasks, environment loading, or missing terminal output in StockScreener. Prevents quoting, indentation, duplicate-job, and output-capture failures."
applyTo: "backend/scripts/**,frontend/package.json,.vscode/tasks.json"
---

# Windows Execution Runbook

This is workspace guidance, not permission to run a mutating operation. Read once
before command execution, including read-only tasks where no file is being edited.

## Choose The Execution Path

1. Identify the exact purpose, executable, working directory, environment source,
   expected output, side effects, and completion criterion.
2. Inspect the current [task definitions](../../.vscode/tasks.json). Reuse a suitable
   task only after checking its command and arguments. An old label is not authority.
3. Prefer direct process tasks for Python/Node executables, with each argument a
   separate string. This avoids PowerShell parsing the program source.
4. Use a shell only when shell syntax is actually needed. Prefer the VS Code task
   runner when the shared terminal fails to resolve commands or capture output.
5. Keep complex logic in an existing suitable script. Add a small script only when
   no suitable home exists; create/edit it with the editor patch tool. Do not create
   a new task or script for every retry. Remove only your own superseded attempts.

## Do And Do Not

| Do | Do Not |
|---|---|
| Invoke a Python script or `-m` module directly. | Pipe nested Python/SQL through a PowerShell here-string. |
| Put executable arguments in a process task's `args` array. | Wrap an entire executable plus its arguments in one string. |
| Reuse the repository virtual environment. | Assume the terminal's `python` resolves to the same interpreter. |
| Use `npm.cmd` on Windows, preferably through the existing build task. | Retry blocked `npm.ps1` or change machine-wide execution policy. |
| Load the backend environment before DB-dependent imports. | Print credentials or assume inline Python finds the correct `.env`. |
| Inspect the first decisive error. | Rerun an identical expensive job because output was blank. |
| Check result status and coverage as well as exit code. | Interpret `READY`, `COMPLETE`, or exit zero as universal integrity checks. |
| Preserve file encoding using editor patch tools. | Rewrite source via `Get-Content` / `Set-Content`; PowerShell 5.1 can corrupt Unicode. |
| Read actual task type and use its returned task ID. | Assume every task ID begins with `shell:`; process tasks differ. |
| Edit an existing task when appropriate. | Call task creation again with the same label; it can append duplicates. |

## Known Wrapper Failures

- Multiline task commands have lost nested Python indentation and stripped quotes
  in this environment. Avoid Python `-c` / `exec` and here-string pipelines for
  audits, loops, functions, or SQL. A script file is the default, not the last resort.
- Simple inline expressions may work, but one quote/indentation failure ends that
  approach. Switch to a direct script/process invocation, not another escape scheme.
- The terminal wrapper has rewritten literal `&&` even inside JavaScript strings.
  Keep program source out of terminal command strings. Do not rewrite existing npm
  scripts merely because they use `&&` internally.
- If `Get-CimInstance`, `Set-Location`, or a known executable unexpectedly cannot
  resolve, treat the shared shell as suspect. Use a fresh process/task context;
  do not repeatedly spawn nested shells or alter the operating system configuration.
- A previously observed successful shell workaround is evidence, not a preferred
  recipe. Do not copy old giant quoted commands into new tasks.

## Minimal Process Task

Adapt an existing task when practical. This shape runs a focused existing test
without embedding code or adding an extra PowerShell layer:

```json
{
  "label": "Check materialization reader tests",
  "type": "process",
  "command": "${workspaceFolder}/backend/.venv/Scripts/python.exe",
  "args": ["-m", "pytest", "backend/tests/test_equity_materialization_api.py", "-q"],
  "options": { "cwd": "${workspaceFolder}" },
  "problemMatcher": []
}
```

Do not add this example as another task when an equivalent already exists. For a
Python script, replace the module arguments with the script path and its arguments.
Use task IDs returned by the editor; inspected examples have included
`process: Verify daily screening readers` and
`shell: Build portal after forward alert cutover`.

## Commands And Environment

Run from the repository root unless the task explicitly sets another directory:

```powershell
.\backend\.venv\Scripts\python.exe -m pytest backend/tests/test_equity_materialization_api.py -q
node --test frontend/tests/scannerEvidence.test.mjs
npm.cmd --prefix frontend run build
```

- Check [frontend scripts](../../frontend/package.json) before inventing `npm test`.
  This repository has used Node's built-in test runner for `.test.mjs` files.
- `backend/database.py` validates DB settings during import. A standalone audit
  should explicitly load `backend/.env`, using a path derived from its script
  location, before importing database/equity modules. Reuse a nearby script's
  bootstrap pattern. Do not rely on the invoking terminal's working directory.
- Use the [documented startup commands](../../README.md) only when starting a
  service is authorized. For the API, retain the explicit backend environment file.
- Use unambiguous root-relative paths or `${workspaceFolder}` in reusable tasks;
  do not hardcode a user's drive, username, interpreter installation, or PID.
- Check a proposed script's syntax before expensive execution when practical;
  then test a small representative input before a wide read or authorized write.

## Completion And Recovery Contract

For each nontrivial run retain only: purpose, exact invocation/task ID, started
time, state, decisive result, and next action. States: NOT_STARTED, RUNNING,
COMPLETED, FAILED, or UNKNOWN. Record the report's data-quality verdict separately.

1. Use synchronous execution for one-shot work. Keep asynchronous mode for services
   and watchers. Never launch a duplicate to obtain output.
2. A completed synchronous result needs no polling. For an editor task, obtain its
   task output/exit status. Use terminal continuation only when the tool explicitly
   returned a running/background/input-needed execution; use that exact ID.
3. Missing output means UNKNOWN. Recover captured output and inspect the exact
   matching process once before deciding to retry. No sleeps or repeated polling.
4. Distinguish wrapper failure from application failure. The former changes the
   execution path; the latter gets one focused diagnosis and a grounded correction.
   If corrected execution is still ambiguous, stop and report the blocker rather
   than starting another speculative run.
5. Do not replay a mutation after interruption until durable state/idempotency is
   checked. Failed or missed publications must not be silently backdated.

## Read-Only Audit Discipline

- A `--once` flag usually performs work. A GET route may compute or fetch. Inspect
  the owning code; use explicit verified `--status` / `--verify` modes where available.
- Prefer bounded, parameterized queries. Use a read-only transaction and statement
  timeout; use repeatable-read when several queries must describe one snapshot.
- Prefer existing reports over building a second validator. Separate missing bars,
  absent analysis checkpoints, stale downstream views, and historical failures.
- Honor contracts such as cent-rounded setup prices, exchange sessions, actual
  observation times, identity, and configured freshness windows before declaring a
  mismatch. Do not weaken a contract to silence an error.
- Compare errors with dated repair records before calling an old failure a new
  incident. Never alter frozen manifests or counters just to make a check pass.
- Do not change providers, worker lineup, publication policies, or database state
  as part of a read-only evaluation. Present a separate proposed action.

## Output Budget And Browser Checks

- Ask for a status, counts, timings, and at most a few representative failures.
  Query aggregates first; retrieve detailed rows only for an identified discrepancy.
- Emit compact summaries, but not giant one-line JSON that the viewer truncates.
  Put detailed audit evidence in one report and link it; do not paste it repeatedly.
- Browser verification should target a known page and condition. Background pages
  can pause animation frames or prevent pointer stability; bring the page forward
  before interaction. Avoid unbounded `requestAnimationFrame` waits and sleeps.
- If a pointer check fails, inspect attached state and geometry once. DOM activation
  can test a handler, but does not certify real pointer interaction or visual layout.
  State that limitation; do not claim a stale screenshot proves the current page.
- Verify current listener addresses. IPv4-only network output does not prove that
  an IPv6-bound Vite server is absent. Never stop/restart a service based only on
  an old PID, a missing IPv4 row, or a browser timeout.

## Maintain This Guide

Add a lesson only after observing its failure signature and validating the safer
path. Include a concise trigger, preferred action, and verification. Merge or
replace the existing rule; do not append transcripts, transient runtime facts,
credentials, or every unsuccessful command. Keep detailed incident history in
dated reports and task state in [scoped context](../../docs/AGENT_CONTEXT.md).