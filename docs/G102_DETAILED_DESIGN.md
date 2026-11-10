# G102 detailed design (index)

Split on Nov 10, 2026 into four documents, because one file mixed the target design, its status, the runbook and the
evidence, and terms such as "draft", "live", "done" and "follow-up" could not be reconciled:

| Document | Holds |
| --- | --- |
| [`g102/G102_TARGET_DESIGN.md`](g102/G102_TARGET_DESIGN.md) | Normative target design and invariants (INV-1 to INV-10); no status |
| [`g102/G102_IMPLEMENTATION_MATRIX.md`](g102/G102_IMPLEMENTATION_MATRIX.md) | One row per item: DESIGNED / IMPLEMENTED / VERIFIED / DEFERRED, code, evidence, next step |
| [`g102/G102_RUNBOOK.md`](g102/G102_RUNBOOK.md) | Rollout, rollback, switches, rehearsal on a copy, pre-open audit, manual correction, incidents |
| [`g102/G102_EVIDENCE.md`](g102/G102_EVIDENCE.md) | Dated rehearsal and live evidence (Nov 7 onward), moved verbatim |

The full pre-split text is in git history (this file at commit `bd32971` and later).

**Where the old sections went** (other documents and notes cite these numbers):

| Old section | Now |
| --- | --- |
| 1.1-1.4 Needs, change types, measured failures, out of scope | Target 1; failures and their fixes in the matrix |
| 1.5 Code footprint, 1.6 Benefit check and audit | Evidence, Nov 7 |
| 2 Approach (in place, rollback) | Target context; runbook 3-4 |
| 3 A0 session list | Target 3; build and replay in evidence, Nov 7; status matrix A0 |
| 4.1-4.9 Bars without the cohort | Target 4; "before" state and the order change in evidence, Nov 7 |
| 4.10 Rehearsal and completion plan | Evidence, Nov 7; remaining conversion in matrix A4.6-A4.9 |
| 4.11 Follow-ups from the Nov 8 review | Status paragraph in evidence, Nov 8; actions 1-9 are matrix rows 4.11-1 to 4.11-9 |
| 5.1-5.5 G90, G91, G99, B24, G94 | Target 5; builds in evidence, Nov 7; status matrix |
| 6, 6.1 Per-ticker outcomes, read labels | Target 6, 6.1; builds in evidence, Nov 7; matrix A2.1-A2.3 |
| 7 Later steps | Target 7; matrix A2.4-B2 |
| 8 Weekend order, 8.1 Monday runbook | Runbook (rules, deploy, fallbacks, rehearsal, pre-open); 8.1's measured runs in evidence, Nov 8 |
| 9 Status after the first session, 9.4 review | Matrix (statuses and next steps); evidence, Nov 9 and Nov 10 |
