# Review triage — 2026-09-21 adversarial review (Codex)

Source: `2026-09-21-adversarial-review.md`. Dispositions below are binding:
`fixed` means implemented and tested on the current branch; `deferred` means
filed against a named artefact with an owner unit.

| Finding | Severity | Disposition |
|---|---|---|
| F01 artifacts not joined to run (engine keys ≠ scope key) | blocker | **fixed** — scoped writer's run key is authoritative; caller keys are advisory inside a scope (F01 test) |
| F02 millis are not stable execution/retry identities | major | **deferred** — opaque run tokens + retry semantics belong to the sealed-manifest amendment (SPEC-001 rev 2); scoped writer removes the facade-path collision today |
| F03 ON CONFLICT DO NOTHING is not immutability | major | **deferred** — sealed expected-artifact manifest with content digests is the SPEC-001 rev 2 design; current sink keeps first-write-wins |
| F04 all-namespaces prune ignores namespace correlation | major | **fixed** — artifact deletion correlates (namespace, run_key) to runs; namespace-scoped params verified |
| F05 admission views lack tenant isolation + unique latest | major | **partially fixed** — deterministic ROW_NUMBER dedup landed; authenticated per-tenant views belong to the control plane (ADR-009) |
| F06 catalog schema upgrade → partial persistence | major | **fixed** — catalog preflight validates target columns before any file write (F06 test) |
| F07 mixed rulesets have no certified parent plan | major | **deferred** — ruleset envelope amendment to ADR-004 (next kernel unit; review BET 1 agrees fingerprints survive) |
| F08 all-vacuous runs report "passed" | major | **deferred** — admission policy is a consumer/control-plane concern; v1 outcomes preserved per review BET 2 |
| F09 float bounds: decimalized rendering ≠ exact binary | major | **amended** — ADR-005 documents the decimalized-float contract; exact-binary semantics would be a new semantic version |
| F10 shared-sink scope overlap | minor | **fixed** — threading constraint documented on the sink; per-run construction remains the supported pattern |
| F11 closed repo unpinned dependency + schema-shape drift | major | **fixed** — closed repo pins `>=2.3.0,<3.0.0`; verify_schema checks existence/counts (shape checks deferred to TD-PROD-6 schema-stability contract) |
| F12 SQLAlchemy does not translate dialect SQL | major | **fixed** — ADR-010/SPEC-007 re-scoped to PostgreSQL-certified; arbitrary-RDBMS claim removed |
| F13 moat defensibility is an unvalidated hypothesis | major | **accepted-for-co-design** — recorded in product strategy; namespace primitives stay OSS per review; buyer validation is a business action |
| F14 marketplace compliance exceeds MVP ladder | major | **accepted** — AWS/Azure lifecycle, isolation, and audit requirements folded into MVP-2 scope (ADR-007 amendment pointer) |

Fixed this round: F01, F04, F06, F09 (amendment), F10, F11, F12.
Deferred with named artefacts: F02, F03, F07, F08, F13, F14.
