# Codex review prompt (copy-paste)

The block below is the full prompt to give Codex. Keep it verbatim.

---

You are an independent adversarial reviewer for the
`anvai-labs/data-quality-framework` project (Python/Spark data-quality
framework, recently restructured around a semantically versioned rule
kernel with an open-core product split).

**Primary input:** read `docs/review-handoff-codex.adoc` at the repo root
first — it maps the scope, the six design bets the author already
suspects, the review protocol, and the known limitations you should not
re-report. Then read the referenced artefacts: ADR-004..010
(`docs/decisions/adr/`), SPEC-001..007 (`docs/specs/`),
`docs/error-catalog.adoc`, `docs/product-strategy.adoc`, and the code in
`dq/plan.py`, `dq/spark_adapter.py`, `dq/portable_config.py`, `dq/sinks.py`,
`dq/postgres_sink.py`, `dq/dq_framework.py`. Tests live in `dq/tests/`
(see `test_postgres_sink.py`, `test_spark_adapter_groups.py`,
`test_spark_adapter_ranges.py`, `test_groups_differential.py`,
`test_ranges_differential.py`, `test_error_catalog.py`).

**Your mandate:** attack the design, not the style. The six bets to attack
first are in the handoff's "six challenges" section: (1) semantic-version
fragmentation of mixed rulesets, (2) vacuous-pass empty-input semantics,
(3) unvalidated admission-view SQL, (4) file-vs-catalog namespace
inconsistency, (5) sink thread-safety, (6) closed-repo dependency
sequencing. Also attack the open-core monetization boundary (ADR-009) and
the MVP ladder (ADR-007) as business/architecture risks.

**Rules:**
- Verify every claim against code and tests before accepting it; run
  `pytest dq/tests/ -m "not spark" -q` at minimum.
- Do not implement fixes — review only.
- Do not re-report the handoff's "known limitations" section.
- Cite artefact + line/statement for every finding.

**Required response format (exactly this structure):**

```
## Verdict per bet (1-6)
For each: BET n — SOUND | UNSOUND | RISKY-WITH-MITIGATION
+ two-sentence justification with artefact references.

## Findings
| ID | Severity (blocker/major/minor) | Artefact | Evidence | Proposed correction |

## Wrong-path assessment
Is any element a long-tail wrong path (would force rework of shipped
units)? YES/NO per element: kernel semantic versioning, ranges/v1,
groups/v1 vacuous pass, open-core split, PG evidence schema, MVP-1/2
ladder. One sentence each.

## Top 3 actions before MVP-1 build-out
Ranked, each with the artefact to change and why.
```
