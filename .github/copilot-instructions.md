# StockScreener Agent Instructions

Keep this always-loaded file short. Detailed guidance is loaded only when needed.

## Route Context

- Start from the user's named file, symbol, command, or failure. Read the nearest
  owner and one relevant test before widening the search.
- For Massive/Polygon provider, API, schema, field, entitlement or market-data
  questions, use the Massive MCP server as the primary external authority before
  general web search. Search its endpoint catalog first; actual provider-data
  queries still require explicit scope authorization and must never expose secrets.
- Before running commands, read [Windows execution](instructions/windows-execution.instructions.md)
  once for the current task. It also applies to read-only audits and commands
  that do not touch a matching file.
- For multi-step Stock Alerts work, load only
  [stock-alerts](../docs/agent-context/stock-alerts.md). For Options Alerts work,
  load only [options-alerts](../docs/agent-context/options-alerts.md). For realtime
  scaling or V2 schema work, load only
  [advanced-realtime](../docs/agent-context/advanced-realtime.md) and, when a schema
  or retention decision is needed, the
  [V2 design](../docs/REALTIME_SCHEMA_V2_DESIGN.md). Follow
  [context retention](../docs/AGENT_CONTEXT.md) for compact handoffs. Do not load
  retired checkpoints, the entire repository memory or prior conversation history.
- Start a new chat when changing model, workstream or design/implementation/runtime
  phase. Carry forward one checkpoint, exact file anchors and one focused check.
- Large files are references, not context: never view a file over about 20 KB whole
  (README, `REALTIME_SCHEMA_V2_DESIGN.md`, dated reports, long tests). `grep` headings or
  symbols, then `view` a `view_range`. `docs/` holds about 500 MB of JSON/CSV artifacts:
  grep it only with `glob: "*.md"`.
- A resume or handoff prompt names one checkpoint, exact anchors and one check (about 40
  lines at most). It must not tell the next session to read several large documents.
- Runtime/startup authority: [README](../README.md) and
  [deployment](../docs/DEPLOYMENT.md) (read the needed section only). Treat audit
  reports as dated evidence, not current process state or standing permission to act.

## Execution Defaults

- Follow the Windows runbook (read once before commands): existing VS Code task first,
  `type: process` with separate arguments, reusable logic in a script file and never
  substantial Python/SQL/JavaScript in PowerShell `-c`, heredocs or here-strings. Change
  method after the first quoting or wrapper failure; do not retry quoting variations or
  start a second copy of a job.
- Blank output is UNKNOWN, not success or proof that nothing ran. Recover the task output
  and exact job state before retrying. Report evidence and blockers.

## Scope And Evidence

- Questions, reviews, and audits are read-only unless the user authorizes changes.
  Do not turn a data-availability complaint into a policy change without agreement.
- Starting/stopping workers, fetching provider data, backfilling, replaying,
  publishing, migrating, or repairing data requires explicit scope authorization.
  Never relax freshness, deadlines, identity checks, or execution gates to get a pass.
- Preserve concurrent/user changes, immutable bars/evidence, frozen manifests,
  original timestamps, and existing live services outside the approved scope. `docs/`
  is gitignored, so `git status` hides doc edits: re-read a doc just before editing it
  and re-grep after, since another session may be writing the same file.
- After an edit, run the cheapest relevant behavioral check before widening scope.
  Distinguish command completion, test success, report verdict, freshness, and full
  historical coverage. `READY` or exit code zero alone does not prove data integrity.
- Keep commands/results concise. On failure, inspect the decisive error and its
  local cause instead of replaying large logs. Never print or retain secrets.
- At meaningful checkpoints, update the task's compact context. Promote verified,
  reusable lessons into the scoped guide; replace obsolete guidance rather than
  appending an endless session diary.