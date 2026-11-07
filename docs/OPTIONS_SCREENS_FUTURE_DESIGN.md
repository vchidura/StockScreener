# Screens: multi-user, one definition language, natural language (future work)

Status: recorded Nov 8, 2026 at the user's request ("multiple users should be supported; record it for future
design and implementation; implement only the foundation now; design the compute and storage completely").
Not scheduled. The foundation that exists is listed in section 1.

## 1. What exists (Nov 8)

| Piece | State |
| --- | --- |
| Built-in Options screens | Backend definitions, schema `options-screen-v1` (`backend/options/screens.py`, `backend/options/policies/option_screens_v1.json`): rule tree (all/any/not over comparisons), derived fields (day premium), per-tier parameters, sort, caps; published in the discovery catalog with id, version and hash; applied by `screen_id` |
| Custom Options screens | Browser only (`localStorage` key `options-screen-library-v1`, up to 100, AND-only numeric ranges); not shared, lost with the browser |
| Custom stock screens | The same pattern on the stock Screener page |
| Earlier definition work | `backend/research/screener_definition.py` (`screener_definition_v1`): stock and option domains, catalog hash, compile to a native query, JSON schema export, 32 KB budget. Not wired to any page |
| Evaluation | The API filters in memory: the release's published rows plus breadth rows (about 15,000 Options rows at 9 underlyings) |
| Users | None: the portal has no identity; every viewer sees the same thing |

## 2. Goals

1. Many users, each with their own screens, and screens shared by a user or published by the platform.
2. One definition language for stock and option screens (built-in, custom and generated alike).
3. A user can describe a screen in plain words and get a definition to review, explain and save.
4. Screens can later drive alerts (a saved screen evaluated each window, with notifications).
5. Cost grows with the number of distinct screens, not with the number of page views.

## 3. The definition language (DSL v2)

One schema with two domains, merging `screener_definition_v1` (domains, catalog hash, compile step, schema export)
and `options-screen-v1` (rule trees, derived fields, tiers, caps):

- **Field catalog:** every field with type, unit, source (ours, the provider's, derived), domain and availability
  (streamed, breadth, both). A definition names fields only through the catalog; the catalog's hash is recorded
  with the definition, so a catalog change asks for revalidation instead of silently changing a screen.
- **Rules:** comparisons (`>=`, `<=`, `in`, ...) joined by all / any / not; bounded depth (8) and size (64 nodes,
  32 KB).
- **Parameters:** named values by tier, DTE lane or underlying, so one screen states different minimums for ETFs
  and single stocks.
- **Derived fields:** a registered, versioned list (day premium, Vol/OI, OTM distance, IV rank once history exists).
- **Output:** sort, caps per group and in all, columns.
- **Identity:** id, version, owner, canonical hash; a saved screen is immutable per version.

Evaluation compiles a definition into a plan: in memory today; SQL over the per-window screener table (section 5)
when the row counts call for it. The same plan explains itself as plain rule lines.

## 4. Natural language to a definition

- The model gets the field catalog, the schema and examples, and must return a definition only (JSON), never SQL
  or code.
- The definition is validated by the same schema and catalog before anything runs; an invalid answer is shown as
  such, not repaired silently.
- The user sees the rule lines, a preview count and sample rows, then saves. The prompt, the model's answer and the
  saved version are kept together for review.
- Limits: the same depth, size and field limits as hand-written screens; a per-user rate limit.

## 5. Storage

New relations (one migration when built):

| Relation | Content |
| --- | --- |
| `app.users` | User identity (from the chosen sign-in provider), status |
| `app.screens` | Screen id, owner, domain, name, visibility (private / shared / published), current version |
| `app.screen_versions` | Immutable: definition (jsonb), hash, catalog hash, created by, created at |
| `app.screen_shares` | Screen, grantee, permission |
| `app.screen_generations` | Natural-language prompt, model, returned definition, validation result, user, time |
| `rt.option_screen_rows` (when needed) | Per window: the merged Screener rows (published + breadth), indexed for compiled queries; kept for the session |

Sizes: about 15,000 Options rows per window at 9 underlyings (40,000 at 23); screens and versions are small
(kilobytes each). `rt.option_screen_rows` at 23 underlyings is about 40,000 rows x 78 windows = 3 million rows per
session; kept one session, partitioned by session.

## 6. Compute

- **Today:** each page request evaluates in memory over the published and breadth rows (milliseconds at 15,000
  rows).
- **With many users:** results depend only on (screen version, release, breadth capture), which change every 5
  minutes. Evaluate each distinct screen once per window and cache by that key; page views read the cache. Cost is
  distinct screens x windows, not views.
- **Large or custom screens:** compiled to SQL over `rt.option_screen_rows` with a statement timeout and a row cap.
- **Screen alerts:** a scheduled evaluation per window for screens marked as alerting, writing hits to an outbox.
- **Natural language:** model calls only when a user asks; never in the window path.

## 7. Phases

| Phase | Work |
| --- | --- |
| P0 (done Nov 8) | Backend built-in Options screens (`options-screen-v1`) |
| P1 | DSL v2 schema and compiler for both domains; built-in stock screens moved to backend definitions; custom screens stored on the server for the single current user, with an import of browser screens |
| P2 | Users and sign-in; private, shared and published screens |
| P3 | Natural language to a definition, with preview and review log |
| P4 | Screen alerts (scheduled evaluation, outbox, notifications) |
| P5 | Scale: per-window screener table, compiled SQL, result cache |

## 8. Questions to settle before P1

- Sign-in provider and whether users are invited or self-registered.
- Whether published platform screens are versioned by the platform only, or users can propose versions.
- Retention of natural-language prompts (privacy) and which model provider is used.
