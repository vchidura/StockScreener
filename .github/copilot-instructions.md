# StockScreener Agent Instructions

Keep this always-loaded file short. Detailed guidance is loaded only when needed.

## Route Context

- Start from the user's named file, symbol, command, or failure. Read the nearest
  owner and one relevant test before widening the search.
- Before running commands, read [Windows execution](instructions/windows-execution.instructions.md)
  once for the current task. It also applies to read-only audits and commands
  that do not touch a matching file.
- For multi-step work or a resumed task, follow [context retention](../docs/AGENT_CONTEXT.md)
  and load only that task's checkpoint. Do not load every checkpoint or the entire
  repository memory/history. Do not reread unchanged guidance on every tool call.
- Runtime/startup authority: [README](../README.md) and
  [deployment](../docs/DEPLOYMENT.md). Treat audit reports as dated evidence, not
  current process state or standing permission to act.

## Execution Defaults

- Prefer an existing VS Code task. Inspect its current command, arguments, type,
  working directory, and side effects before running it.
- For Python/Node programs, prefer `type: process` with an executable and separate
  argument array. Shell tasks are for actual shell syntax. Reuse the task label;
  do not create another task to try another quoting variation.
- Never embed substantial Python/SQL/JavaScript in PowerShell `-c`, `-Command`,
  heredocs, or here-string pipelines. Put reusable logic in an appropriate script
  using editor patch tools; invoke the script directly.
- A quoting/indentation error or a shell that cannot resolve basic commands is an
  execution-wrapper failure. Switch method after the first such failure. Do not
  spend repeated attempts changing quotes or start another copy of the job.
- Blank output is UNKNOWN, not success or proof that nothing ran. Recover the
  task output and exact job state before retrying. Report evidence and blockers.

## Scope And Evidence

- Questions, reviews, and audits are read-only unless the user authorizes changes.
  Do not turn a data-availability complaint into a policy change without agreement.
- Starting/stopping workers, fetching provider data, backfilling, replaying,
  publishing, migrating, or repairing data requires explicit scope authorization.
  Never relax freshness, deadlines, identity checks, or execution gates to get a pass.
- Preserve concurrent/user changes, immutable bars/evidence, frozen manifests,
  original timestamps, and existing live services outside the approved scope.
- After an edit, run the cheapest relevant behavioral check before widening scope.
  Distinguish command completion, test success, report verdict, freshness, and full
  historical coverage. `READY` or exit code zero alone does not prove data integrity.
- Keep commands/results concise. On failure, inspect the decisive error and its
  local cause instead of replaying large logs. Never print or retain secrets.
- At meaningful checkpoints, update the task's compact context. Promote verified,
  reusable lessons into the scoped guide; replace obsolete guidance rather than
  appending an endless session diary.