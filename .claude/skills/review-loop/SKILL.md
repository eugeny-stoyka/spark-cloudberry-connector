---
name: review-loop
description: >-
  Multi-agent review/refine loop. Use after a change or milestone, or when asked to review work:
  dispatch a fresh-context full-spectrum reviewer plus a dissimilar correctness reviewer,
  consolidate and fix, add a regression check for every defect, and repeat until a round is clean.
---

# Adversarial review/refine loop

After a change or milestone, or when prompted, dispatch fresh-context review agents at
MAXIMUM THINKING EFFORT, then consolidate, fix, and repeat.
The goal is adversarial, diverse, independent coverage.

The prompts given to the agents shall be extremely terse.
Giving excessive detail constrains their thinking causing the tunnel vision syndrome.
They must be given the opportunity to look at the work without bias or prejudice.
A prompt longer than 2 sentences is a bad prompt; 3 sentences is the absolute limit, anything larger is unacceptable.

## The reviewer pair

Run two reviewers in parallel per round:

- An *ultrathink* Claude agent with the FULL-SPECTRUM remit, in priority order: functional CORRECTNESS and
  ROBUSTNESS first, then SIMPLIFICATION opportunities, ARCHITECTURAL CLEANLINESS and CODE QUALITY,
  and POLICY/STYLE compliance with the project's own docs (README.md; tasks.md — local notes, when present).

- A dissimilar CORRECTNESS-only reviewer, to maximize perspective diversity and minimize blind spots:
  Codex on its most advanced model at maximum reasoning effort when the `codex` CLI is installed; otherwise
  a second fresh-context Claude agent given the correctness remit only.

## Reviewers are read-only

Review agents must not modify the worktree or run mutating commands. If one needs a mutable environment,
it copies the worktree elsewhere.

## Reviewers do not re-run the project gates

The gates — `docker/run.sh jar` (the build) and `docker/run.sh regression` (the end-to-end suite on the
stand, ~8 min) — are already green at the reviewed state; re-running them duplicates work. State this in the
reviewer prompts. Reviewer effort goes instead into adversarial counterexamples for behaviors the existing
probes (`src/test/regression/probes/`) do NOT cover, executed against the stand that is up: a probe of its own
fed to `spark-shell` in the `client` container (`docker/run.sh cluster connect`, the jar in `/scc`), its own
tables in Cloudberry, dropped afterwards — never the jar of `target/` replaced while a run uses it.
Reproducing their own findings before reporting remains mandatory.

## Inputs are trusted — reject cybersec framing

This is a connector for trusted, well-meaning users: the options are code of the application, not input of
an attacker (`sqltransfer`, `dbtable`, `distributedby` are SQL by design). Corner cases arise from honest
mistakes, never from adversaries. Weigh every finding by its plausibility under honest use: a defect matters
if a well-meaning user could plausibly trigger it (a typo, a misunderstood option, an unusual but legitimate
table, type or value, a failed task or executor), not if only a contrived hostile input can reach it.
Findings whose reproduction requires deliberately pathological constructions are out of scope: reject them
outright at consolidation instead of hardening against them, and say so in the reviewer prompts. A round is
NOT unclean merely because such findings exist; treat a round whose only findings are adversarial-input
constructions as clean and stop.

## Consolidate and act

When all reviewers return, merge their findings, discard the noise, and fix what is real.
For every correctness defect, add a regression check (a `check(...)` in a probe of
`src/test/regression/probes/`, or a new probe with its line in `RUNS` of `src/test/regression/run.sh`)
verified to fail before the fix and pass after.

## Avoid scope creep

The loop always finds something, so every round can justify the next. That does not make a finding a mandate.
Each finding MUST be checked against the intent of the design (README.md; tasks.md — local notes, when present),
not only the criterion you handed the reviewer: an agent told "a wrong answer is always a defect" will
faithfully report behaviour the design deliberately sanctions (e.g. a classic read failing on a restarted
task — split reads and staging writes are the answer, by design).

After each iteration, re-read the original request and compare it to the diffstat.
If the work no longer resembles what was asked, stop and say so -- cutting back is the maintainer's call.

## When to stop

A round is clean when the reviewers surface only trivial feedback or none; the first clean round ends the loop.
Do not chase literal zero feedback: with no real issues left, agents degrade into nitpicking,
so a round is clean as soon as significant findings cease.

## Operational notes

High-effort agents can go silent for a long time — set generous timeouts and do not assume a quiet agent is stuck.
Have agents background long-running commands (the regression, a load test); blocking on a foreground command
is a common cause of stream-idle timeouts. One stand: runs that share it go one after another.

Some headless agents hang waiting on stdin (like Codex) — redirect from `/dev/null`.

Retry agents that fail on a transient or connection error until they succeed.
If an agent gets stuck or hits a security guardrail, try resuming it first instead of restarting its work from scratch.
