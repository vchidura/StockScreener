# Agent Context Specification

## Purpose And Loading

Keep useful context across prompts and compaction without replaying the entire
conversation. Guidance is repository-scoped and versionable; it does not grant
operational authority or override the user's latest request.

| Layer | Location | Load When | Keep |
|---|---|---|---|
| Default rules | [.github/copilot-instructions.md](../.github/copilot-instructions.md) | Automatically when workspace instructions are enabled | Short routing, execution safeguards, scope rules |
| Execution guide | [.github/instructions/windows-execution.instructions.md](../.github/instructions/windows-execution.instructions.md) | Before commands; automatic file scope or explicit read | Verified Windows recipes and failure handling |
| Task checkpoint | `docs/agent-context/<topic>.md` | Continuing that topic or starting substantial related work | Current objective, decisions, evidence, next step |
| Reference evidence | Existing design/runbook/audit documents | Only for a claim or boundary needed now | Detailed architecture, data contracts, dated results |
| Agent memory | Existing repository/session memory, if available | Targeted recall, not a full history dump | A pointer and reusable lesson, not a duplicate ledger |

Do not use an always-matching file instruction to load the full runbook. Linked
documents are not guaranteed to be automatically expanded; the default instruction
explicitly directs the agent to read the relevant one. If workspace custom
instructions are disabled, attach the default file and relevant checkpoint manually.

## Local Work Loop

1. Restate the latest objective and scope briefly. Determine whether the request is
   explanation, review, read-only audit, code change, or authorized runtime work.
2. Read the concrete anchor, nearest owner, and a cheap relevant check. Write one
   falsifiable hypothesis, not a broad inventory of the repository.
3. For implementation, make the smallest grounded change and immediately validate
   that slice. For review/audit, collect decisive evidence without modifying behavior.
4. On failure, classify it once: wrapper, environment, application, data contract,
   or actual data-quality issue. Follow the execution runbook instead of varying
   quotes, creating duplicate tasks, or repeatedly expanding scope.
5. Broaden only when a result requires it. Stop checking when the agreed criteria
   are met; avoid repeated full builds or full-universe audits for reassurance.
6. Finish with the result, verification, limitations, and any proposed next action.
   Do not represent a recommendation as already implemented or approved.

## Checkpoint Rules

- Reuse one topic file for an active workstream. Do not create a checkpoint for a
  trivial answer, each prompt, each failed command, or every data snapshot.
- Use a descriptive topic name, not a global mutable `current.md`. Independent
  concurrent tasks must not overwrite each other's objectives or authorization.
- Keep a checkpoint around 40-80 lines, preferably under 1,000 tokens. Summarize
  or link older evidence when it exceeds that size. These are working budgets,
  not a reason to omit a safety-critical fact.
- Update at a changed decision, validated milestone, blocker, handoff, completion,
  or known context-compaction boundary. Do not edit it after every tool call.
- Read its current contents before editing. Preserve unrelated/concurrent changes.
  Keep only the latest state and links to earlier evidence, not an append-only diary.
- Runtime observations expire: record when checked, then recheck the specific fact
  before acting. Old ports, PIDs, job handles, source generations, or permissions
  are not reusable launch/kill authority.
- Clearly separate OBSERVED, INFERRED, and NOT_CHECKED. A test proves its actual
  assertion; a passing status endpoint is not a full data or security certification.
- Never retain secrets, raw environment files, full datasets, repetitive logs, or
  absolute user-specific paths. Record configuration key names and safe status only.

## Checkpoint Template

```markdown
# <Topic>
Updated: <UTC date/time or dated milestone>
State: <IN_PROGRESS | BLOCKED | COMPLETE>

## Objective And Authority
- Latest request:
- Approved actions:
- Out of scope / requires approval:
- Done when:

## Relevant Context
- Owning files/symbols:
- Contracts and decisions to preserve:
- Reference documents:

## Verified State
- OBSERVED: <finding, as-of time, check/evidence link>
- INFERRED: <hypothesis and cheap disconfirming check>
- NOT_CHECKED: <important remaining uncertainty>

## Execution And Changes
- Changed files and purpose:
- Last check: <exact command/task ID; completion state; result/verdict>
- Active operation, only if any: <tool-returned ID, scope, started time>
- Failed approach to avoid: <signature and validated replacement, not transcript>

## Next Step
- One concrete next action, or none when complete:
- Blocker / question requiring the user:
```

## Resume Protocol

1. Read the latest user message first, then the matching checkpoint. A new request
   can narrow, supersede, pause, or cancel earlier work.
2. Read only changed/touched files and referenced evidence needed for the next step.
   Use current files and actual command results over stale summaries when they differ.
3. Recover any active execution before rerunning it. Do not assume it completed,
   failed, or still owns a PID based on the checkpoint alone.
4. Continue from the recorded next action; do not rediscover the whole project or
   repeat completed expensive work unless changed inputs invalidate its evidence.

## Evolving The Context

Classify new information before retaining it:

- A reusable execution lesson belongs in the scoped execution guide, with evidence
  that the replacement method worked. One failed attempt is not a universal rule.
- A task-specific fact belongs in that topic's checkpoint.
- A durable product/runtime contract belongs in its owning design or deployment
  document. Do not silently change the contract through an agent instruction.
- A large dated investigation belongs in an audit report. Its checkpoint needs the
  verdict, key exception, and link, not hundreds of lines of output.
- Temporary or unverified guesses normally need no persistent entry.

Replace obsolete rules and reconcile contradictions when discovered. Prefer one
canonical source of truth per subject. A memory entry should point to that source.
Do not bulk-import the current repository memory or all prior conversations.

## Workstream Index

- [Execution guidance](agent-context/execution-guidance.md): this customization's
  scope, decisions, and validation checkpoint. Other topics get a file only when
  substantial continuing work needs one.
- [Downstream publications](agent-context/downstream-publications.md): scoped
  Screening/forward Alerts recovery, retained results, and runtime dependencies.

## Checking The Customization

- Use `.github/copilot-instructions.md` as the single default entry point, not a
  duplicate root `AGENTS.md` with the same content.
- Keep instruction frontmatter valid YAML, with quoted description and specific
  `applyTo` globs. Normal documentation and the default file need no frontmatter.
- Validate locations, internal links, frontmatter, current command examples, and
  editor diagnostics. Where practical, exercise one existing harmless process
  task; do not launch workers or add enforcement hooks merely to test guidance.
- In a new chat, confirm VS Code includes workspace instructions in its context
  or references. Repository files cannot force an editor with customization
  disabled to load them. These rules guide agents; they are not a shell sandbox.