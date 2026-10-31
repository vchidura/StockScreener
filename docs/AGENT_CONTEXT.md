# Agent Context

## Purpose

Keep enough context to resume current work without replaying a long conversation or
loading historical implementation diaries. Guidance does not grant operational
authority and never overrides the latest user request.

## Load Only What Is Needed

| Context | Load when |
|---|---|
| `.github/copilot-instructions.md` | Automatically; short routing and safety rules |
| `.github/instructions/windows-execution.instructions.md` | Once before commands |
| `docs/agent-context/advanced-realtime.md` | Advanced realtime scaling and V2 schema work |
| `docs/agent-context/stock-alerts.md` | Stock Alerts service, result, API or UI work |
| `docs/agent-context/options-alerts.md` | Options detector, evidence, outcome, API or UI work |
| `README.md` and `docs/DEPLOYMENT.md` | Current startup or runtime contract is needed |
| Dated design/audit reports | A specific current claim depends on their evidence |

Do not bulk-load repository memory, previous sessions, all design documents or both
alert checkpoints. Source and current command results override stale summaries.

## Work Package Budget

- Keep one chat to one workstream and one phase: review, implementation or runtime.
- Start a new chat when changing model, workstream or phase. Carry forward only the
  matching checkpoint, exact file anchors and one focused completion check.
- Around 20-25 turns, compact or hand off. Do not let repeated `Continue` prompts
  turn one implementation chat into a permanent project ledger.
- Prefer fewer than eight touched files per work package. Split larger changes at an
  independently testable contract boundary.
- Delegate broad read-only discovery to an isolated subagent and retain its concise
  findings, not every file read, in the main implementation context.

## Local Work Loop

1. Classify the request as explanation, review, code change or authorized runtime work.
2. Read the named anchor, nearest owner and one relevant test.
3. State one falsifiable local hypothesis and one cheap check.
4. Make the smallest grounded edit and immediately run that check.
5. Broaden only when the result requires it; do not repeat completed wide checks.
6. Finish with result, verification, limitation and the next bounded action.

## Checkpoint Contract

- Keep each active checkpoint at 40-80 lines and preferably below 1,000 tokens.
- Store only current objective, authority, owners, contracts, verified state, focused
  checks, blocker and next action. Replace obsolete text instead of appending history.
- Mark runtime facts `OBSERVED` with time, `INFERRED`, or `NOT_CHECKED`.
- Never retain secrets, raw logs, full datasets, PIDs as authority or repeated output.
- Put detailed dated evidence in an audit/report and link it only when needed.
- Use session memory for temporary work. Do not create another persistent workstream
  checkpoint without explicit scope expansion.

## Resume Packet

```text
Objective:
Mode: read-only | implementation | runtime
Checkpoint: stock-alerts | options-alerts
Anchors:
Allowed actions:
Do not change:
Done when:
Focused validation:
```

On resume, read the latest user request first, then one checkpoint. Recover any active
execution before rerunning it. Recheck process identity, configuration and freshness;
old status, ports or PIDs do not authorize lifecycle action.

A resume prompt is the packet above plus at most a short list of open owner decisions:
about 40 lines. It points at the checkpoint instead of restating it, and it never lists
several large documents to read. Verification commands live in the checkpoint or a script
(for V2: `backend/scripts/run_v2_test_sets.py`), not in prose or in the prompt.

## Active Workstreams

- [Advanced Realtime](agent-context/advanced-realtime.md): continuous-worker
  consolidation checkpoint for `stocks_realtime_v2`; dated reviews and qualification
  attempts are historical evidence, not the runtime plan.
- [Stock Alerts](agent-context/stock-alerts.md): unified service, independent strategy
  state, shared results reader, UI semantics and focused validation.
- [Options Alerts](agent-context/options-alerts.md): detector/evidence/outcome contracts,
  current development version, UI semantics and prospective future work.

Earlier rollout, recovery, migration and research narratives are dated evidence, not
active context. Preserve immutable artifacts, but do not load them by default.