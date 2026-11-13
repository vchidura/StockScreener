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
| Dashboard refresh (R39, r86+) | `REALTIME_V2_DASHBOARD_REFRESH=SESSION` (or `HOURLY`, the default) on realtime-v2-advanced-scheduler in `rollout/compose.stable.json`, then `hotfix -Service realtime-v2-advanced-scheduler -Image <same image>` | SESSION: the Dashboard bundle (overview, daily scanners for the strategy pages, momentum ranks) is built at the first window and the close only; its content changes only at the close. HOURLY: also every :35 window (~95 s each). Never switch it off: the strategy pages and Sector Intelligence breadth read it |
| Market-state loop (V3-1, r85+) | `REALTIME_V3_MARKET_STATE_LOOP=V3` (or `P8`, the default) on realtime-v3-options-p8-market-state in `rollout/compose.stable.json`, then recreate that one container (`hotfix -Service realtime-v3-options-p8-market-state -Image <same image>`) | V3: housekeeping on 1 s timers, one shared journal read, kept-open SQLite connection, idle backoff; P8: today's loop. Same states and checkpoints either way, so it resumes from the other's checkpoints; the fallback during a session is P8 |

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
   Then verify the copy file by file against its source, both read after page-cache eviction (Nov 12 night: a copy
   read back 16 foreign bytes from memory, R56/R57); kit `recovery-20261104/rehearsal-r99/rehearse_r99.sh` step `verify`.
6. Compose project from `rollout/compose.stable.json` with every backend service on the image, `restart: no`, no host
   ports, test secrets (test DSNs, fake provider keys), the test network only.
7. Up, wait two minutes, check states, restarts and error lines; `api_smoke.py` on the test API and on live (same
   statuses; once per night: the live API segfaulted during one such smoke on Nov 13 00:06, R57, and restarted by
   itself - compare later copies with that live result); the change's feature checks. A recreated role can meet its
   predecessor's lease and exit 1 (`restart: no` on the copy): start it again once the lease frees.
   - Bar writer changes: replay stored revisions newest first through the writer in one transaction that is rolled
     back; no head may move, every head must equal the canonical winner (Nov 10: scratchpad `r66_replay.py`, run on
     the new and the previous code as an A/B). The service start run does not write bars, so this is the writer's
     only check on real data before live.
   - Bar reader changes: the converted reader against the SQL it replaced over the last session's cutoffs, with
     timings (Nov 10: scratchpad `r66_parity.py`).
8. Stop (all exit 0 expected; investigate otherwise with an A/B against the live image on the same copy), save logs,
   `down`, rename the copy back, set the test logins NOLOGIN.

### After the close: journal check

`runner/verify_journals.sh` (read-only): every journal written in the last ~25 hours, raw payload hashes and
normalized hash format, ~20 s per Options day; a mismatch is re-read after page-cache eviction (memory vs disk).
Nov 13 00:15: Nov 12 Options and stocks journals clean.

## 7. Pre-open audit (13:00)

Equity members READY (25); Options preparation PREPARING at CATALOG_QUOTE with "history ready" until just after the
open; restart counts against the deploy baseline; worker leases renewing; dividend coverage valid through the close and
no split drift for every member (`runner/coverage_preopen.sql`, members from the desired revision; R9); `runner/g94_coverage_check.py SESSION`;
`runner/g102_read_flags_check.py`; `runner/ticker_readiness.py SESSION` (R40: per ticker, reference, history against the
screening rule, intraday holes over the last 20 sessions, earnings, fundamentals, what each page lacked at the previous
close; by-design labels are filtered; any remaining ISSUES line is a gap to fix or accept before the open); start
`live_watch.ps1 -DryRun` once.

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

**Fault journal and self-heal checks (R21, Nov 11; record-only).** One collector writes every fault the platform
reports, plus what it finds, to `data-audit/faults/faults-YYYY-MM-DD.jsonl` (one line per occurrence, UTC day) and
`faults-YYYY-MM-DD.md` (one row per fault: worst severity, count, first/last seen, recovered or not, action taken or
proposed, the matrix row it is known as, or NEW). Sources: `FAULT {json}` lines and tracebacks in every
`stock-screener-realtime-*` container's log, container restarts/health, the self-heal checks (read-only, in a backend
image that has `scripts/run_realtime_v2_self_heal.py`, r83 or later) and host checks (clock, disk, Docker answering).
- During the next sessions (watcher still monitors and fixes): run one pass with each 10-minute check,
  `python -B runnerault_collector.py --once --self-heal-image sha256:<r83+>`; the first pass of a day can take
  `--since 6h`.
- Offline: read the day's `.md`; every row not RECOVERED and marked NEW becomes a matrix row.
- Unattended (only on the user's go): `runner\install_fault_collector.ps1 -SelfHealImage sha256:<image>` registers a
  scheduled task (`--follow`, every 60 s, self-heal every 5 minutes). Remove with
  `Unregister-ScheduledTask -TaskName StockScreener-FaultCollector`.
- Acting: nothing acts until R38 (the user's go after one or two record-only sessions); WOULD_ACT rows show what would
  have been done.

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
