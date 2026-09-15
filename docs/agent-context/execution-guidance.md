# Execution Guidance Checkpoint

Updated: 2026-09-14, initial implementation
State: IN_PROGRESS

## Objective And Authority

- Latest request: prevent repeated script execution failures and retain concise,
  evolving, scoped context for future prompts across this solution.
- User selected StockScreener workspace scope, not the cross-project user profile.
- Approved actions: add repository guidance and context documents; validate them.
- Out of scope: runtime/application changes, worker starts/stops, data fixes,
  task-list cleanup, global settings, automatic hooks, or new dependencies.
- Done when: default instructions route to the scoped guide; examples and links
  validate; the context maintenance/resume procedure is documented.

## Relevant Context

- Default entry: [.github/copilot-instructions.md](../../.github/copilot-instructions.md).
- Commands: [Windows execution](../../.github/instructions/windows-execution.instructions.md).
- Handoffs: [context specification](../AGENT_CONTEXT.md).
- Operational commands remain owned by [README](../../README.md) and
  [deployment](../DEPLOYMENT.md), not this checkpoint.
- Read current task definitions before reuse; the user changed them after the audit.

## Verified State

- OBSERVED: repeated multiline inline Python/SQL commands lost indentation or
  quotes through the Windows task/shell wrapper; a direct script task completed.
- OBSERVED: the shared terminal sometimes returned blank output or could not
  resolve basic commands. A named VS Code task returned output and exit status.
- OBSERVED: task creation can append duplicate labels; task types include both
  `process` and `shell`, requiring the actual task ID instead of a guessed prefix.
- OBSERVED: current research files can legitimately differ from a frozen replay's
  source manifest. Do not overwrite the manifest to make verification pass.
- INFERRED: short default routing plus task-specific context should reduce repeated
  exploration and token usage; savings have not been measured.
- NOT_CHECKED: automatic attachment in a future newly opened chat and user settings
  that may disable workspace instructions.

## Execution And Changes

- Added four guidance/context files; no application or runtime edits in this task.
- Default instruction file passed editor diagnostics.
- A bounded read-only customization subagent checked existing process task shapes,
  frontend scripts, and import-time environment validation. It changed nothing.
- Remaining checks: all guidance diagnostics, links/frontmatter, and one harmless
  existing process-test recipe. Do not use data repair/publication tasks as a probe.

## Next Step

- Validate the configuration and command recipe, then replace this section with
  the final evidence and mark COMPLETE.
- Future sessions: enhance the scoped guide only with verified reusable lessons;
  use a separate topic checkpoint for unrelated implementation or data work.