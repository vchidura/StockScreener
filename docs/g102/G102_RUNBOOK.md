# G102 rollout and rollback runbook

How G102 changes reach live and come back out. Design: `G102_TARGET_DESIGN.md`. Status: `G102_IMPLEMENTATION_MATRIX.md`.
Measurements: `G102_EVIDENCE.md`. Tools are under
`backend/.realtime-canary/v3-supervised-20261102/recovery-20261104/` (called `$R` below). Times are UTC.

## 1. Rules

- **In place, between sessions.** Changes go out after the close or before the open, never as a surprise mid-session;
  a fix found during a session goes in between windows as a single-service hotfix (user, Nov 9: troubleshoot and fix
  at any time; nothing stops automatically).
- **Needs the user's yes:** a database migration; raising a ceiling or budget; clearing the ledger; changing the
  Options universe; editing READY rows, watermarks, deadlines or clocks; commit and push (secrets scan first, the repo
  is public).
- **One heavy job at a time** on the Docker host (image builds, the full test suite, dumps, restores, volume copies);
  none while windows run.
- **Production bars only** (INV-5): no test, replay or rehearsal cohort writes bars to live. Review any journal replay
  on live before it runs.
- **Secrets** are mounted files; never print them, put them in a command line or write them to a log.

## 2. Tools

| Tool | Use |
| --- | --- |
| `$R/runner/build_candidate.sh LABEL` | Backend image on the package image; copies `realtime scripts equity options research`; refuses an image whose code differs from the worktree; appends to `rollout/candidates.txt` |
| `$R/runner/build_frontend.sh LABEL` | Frontend image; runs the frontend tests; appends to `rollout/frontend-candidates.txt` |
| `$R/deploy_candidate.ps1 -Step ...` | `preflight` (migration plan, audit), `stop-writers`, `stop-options`, `snapshot`, `g102-migrate -Image`, `g102-services -Image` (every backend service, one at a time, the hotfix way), `g102-frontend -Image`, `hotfix -Service -Image`, `status`. Refuses 14:00-21:10 on weekdays unless `-InSession` (except hotfix, status, stop-options); a hotfix in session waits for 3:00-4:15 of the 5-minute cycle |
| `$R/runner/audit_live.ps1 -File x.sql` | Read-only SQL on live |
| `$R/runner/api_smoke.py URL LABEL` | 153 GET routes, status per route |
| `$R/runner/backup_live.ps1 -Label X` | Full custom-format dump of live, checksummed |

**Always pass `-Image` to a hotfix.** Without it the tool uses the last line of `candidates.txt`, which is the newest
build, not necessarily what is live.

## 3. Code-only deploy (no migration)

1. Build: `build_candidate.sh rNN` (and `build_frontend.sh fNN`); confirm "image code == worktree" and diff the image
   against the live image's code: only the intended files may differ.
2. Tests: the offline suite on the worktree; compare failures with the previous image's code (the same failures =
   no regression).
3. Rehearse on a copy (section 6) when the change touches start-up, configuration, schema use or many services.
4. `deploy_candidate.ps1 -Step preflight` (expect no pending migrations, audit passed).
5. Save the live images: `docker ps` names and image ids into `rollout/pre-rNN-images-<date>.txt`.
6. `-Step g102-services -Image sha256:...`, then `-Step g102-frontend -Image sha256:...`.
7. Verify: every service up on the new image; no errors in the logs since the deploy (a role restarting with
   `DurableLeaseUnavailable` during its own handover is expected); worker leases renewing; `api_smoke.py` against the
   pre-deploy result (same statuses); the change's own check (feature read, log line).

**Rollback:** hotfix each service back to its image in the saved list. No data step is needed when no migration ran.

## 4. Migration deploy (as Nov 8)

`preflight`, `stop-writers`, `stop-options`, `snapshot` (dump plus the three volumes as tar files),
`g102-migrate -Image` (plan, apply, `harden_database_v2`, backfill if any, verify), `g102-services -Image`,
`g102-frontend -Image`, `status`, then the pre-open audit. A migration runs as the superuser, so a new table has no
grants until hardening runs; the services' `AUDITED` start check fails without it.

**Rollback after a migration:** migrations 049-052 work with the earlier images (they only replace a constraint or
add). After the A4 writer has written, `rt.bar_current` is stale: stop the services, run
`runner/g102_a4_rollback_fill.py --since <deploy time>` (plan, then `--apply`), then the earlier images
(`rollout/pre-g102-images-20261108.txt`).

## 5. Switches without a new image

| Piece | Switch | Effect |
| --- | --- | --- |
| O1 package selection | `REALTIME_V3_O1_PACKAGE_SELECTION=DISABLED` on realtime-v3-options-p8-window | Detections recorded as observations, no packages |
| Stock Alerts rules | `REALTIME_V2_STOCK_ALERT_RULES=PLACEHOLDER` on realtime-v2-canonical-stock-alerts | Back to the Nov 5 reference rule |
| OS detectors | `docker stop stock-screener-realtime-v2-advanced-detectors` | No OS1/OS2 rows; nothing in a window reads them |
| Breadth | Options scheduler onto the breadth-off image (rebuild on the current code first) | Screener and Flow serve published rows |

## 6. Rehearsal on a copy (service start run)

Isolated test server `g102-test-pg` on the internal network `g102-test-net` (no route to live, the host or the
internet; prove it each time with a socket probe). Script used Nov 10: scratchpad `rehearse_r63.sh` (steps `restore`,
`logins`, `volumes`, `compose`, `up`, `check`, `smoke`, `stop`, `cleanup`).

1. `backup_live.ps1 -Label <x>-rehearsal` (read-only on live).
2. Restore into `test_realtime_v2_<x>` (`pg_restore --no-owner --jobs=4`), check bar-head count and latest window.
3. Test logins with a test password, rename the copy to `stocks_realtime_v2`.
4. Run the hardening routine (`harden_database_v2`) on the renamed copy, then grant CONNECT to the two test logins:
   without hardening retention exits on `permission denied for schema rt` (Nov 10, r63); hardening resets the database
   grants, so a CONNECT granted before it is lost and every service exits on start (Nov 10, r66).
5. Volumes: reports and retention copies; journals: the stocks files and only the latest session's Options folder.
6. Compose project from `rollout/compose.stable.json` with every backend service on the image, `restart: no`, no host
   ports, test secrets (test DSNs, fake provider keys), the test network only.
7. Up, wait two minutes, check states, restarts and error lines; `api_smoke.py` on the test API and on live (same
   statuses); the change's feature checks.
   - Bar writer changes: replay stored revisions newest first through the writer in one transaction that is rolled
     back; no head may move, every head must equal the canonical winner (Nov 10: scratchpad `r66_replay.py`, run on
     the new and the previous code as an A/B). The service start run does not write bars, so this is the writer's
     only check on real data before live.
   - Bar reader changes: the converted reader against the SQL it replaced over the last session's cutoffs, with
     timings (Nov 10: scratchpad `r66_parity.py`).
8. Stop (all exit 0 expected; investigate otherwise with an A/B against the live image on the same copy), save logs,
   `down`, rename the copy back, set the test logins NOLOGIN.

## 7. Pre-open audit (13:00)

Equity members READY (25); Options preparation PREPARING at CATALOG_QUOTE with "history ready" until just after the
open; restart counts against the deploy baseline; worker leases renewing; dividend coverage valid through the close and
no split drift for every member (`coverage_after.sql`); `runner/g94_coverage_check.py SESSION`;
`runner/g102_read_flags_check.py`; start `live_watch.ps1 -DryRun` once.

**Pre-open dry run (13:30-14:10, from Nov 11; matrix R20, built after the Nov 10 close).** Read-only, live image, the
day's provider data: for every Options underlying run the CATALOG_QUOTE capture (the preparation's own query and
validation), then the combined candidate build and the CAPACITY_SHARDS allocation; one Stock Alerts batch and one ticker
workspace projection against the latest cutoff (payload sizes against the 128 KiB bound). Report per step: rows,
rejections by reason, invalid items, sizes. Anything that would fail at the open is fixed before 14:15 (the deploy tool
allows it then). Nov 10 would have shown R11 (`O:B1...` rows) and R16 (delta float noise) here, two hours early.

## 8. Manual bar correction

Only after the INV-2 fix (the full winner tuple in the upsert) is live.

1. Read the key's revisions and head (read-only) and confirm the correction would win on the tuple of target 4.3 with
   the same `bar_end`.
2. Write the corrected bar as a new revision through `persist_realtime_equity_bars_cursor` with the production cohort
   (never a direct `UPDATE` of `rt.bar_head`); one transaction.
3. Read the head back: it must name the new revision; if not, the correction lost on the tuple and nothing else changed.
4. Republish affected pages by the next window; record the correction in the evidence file.

## 9. Incidents

**Docker Desktop wedge (Nov 9 23:57).** Signs: worker leases stop renewing while containers show Up; `docker` commands
hang or return 500; the API port times out; `monitor.log` under `%LOCALAPPDATA%\Docker\log\host` stops growing.
Recovery: `docker desktop restart` (database on the host; volumes and containers come back by their restart policy);
then check leases, errors and the API. Restart the isolated test server separately (`docker start g102-test-pg`).

## 10. Public entry: alpha-screener-pro.com (Cloudflare Tunnel, since Nov 10)

- **Route:** Cloudflare dashboard (Zero Trust, Networks, Tunnels): the realtime stack's own tunnel; public hostname
  `alpha-screener-pro.com` to `http://realtime-v2-advanced-frontend:80` (the frontend's service name on
  `alpha-research-realtime-v2-application`). The frontend serves the pages and passes `/api/` to the API.
- **Login:** a Cloudflare Access application on the hostname is the portal's only login; it must stay on, with no path,
  so it covers the pages and `/api`. Never expose the frontend or the API another way.
- **Connector:** container `stock-screener-realtime-v2-cloudflared` (restart `unless-stopped`, application network, no
  host ports). The prepared definition `rollout/compose.edge.yml` (own Compose project, image pinned by digest, token
  read from a file in the secrets folder, read-only) is not in use yet: the running container was started by hand with
  `cloudflare/cloudflared:latest` and the token in its environment.
- **Never run two connectors with one token:** Cloudflare spreads requests across them. The legacy connector
  (`stock-screener-cloudflared`, AlphaResearchPro) is retired: profile `retired-legacy-tunnel`, restart `no`.
- **Check:** the connector's log shows 4 `Registered tunnel connection` lines; `curl -sI https://alpha-screener-pro.com`
  answers 302 to the Access login. After every frontend deploy, check the domain again (the route names the service, so
  a recreated frontend keeps it).
