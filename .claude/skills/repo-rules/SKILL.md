---
name: repo-rules
description: >-
  Rules of a public repository: what may be committed and published, how the README and the other docs are
  written, how measurements are reported, how a change is verified and committed. Use before a commit or a push,
  when editing the README or any doc, when adding files, and when reporting measurements.
---

# Repository rules

A public repository publishes everything committed, the history included.

## Never in the repository

- Personal data of anyone: e-mail addresses (commits go under the noreply address of the hosting, set in the
  local git config of the repository before the first commit), personal names, local paths, names of machines,
  tokens, keys, real passwords (only placeholders and the credentials of a local test environment).
- Other projects and work that are not this repository: their names, environments, hosts, databases, internal
  names — in the docs, the comments and the configs alike.
- Local notes and working files: ignored by `.gitignore`, never linked from the docs.
- Build output, downloaded distributions, IDE files.

Before a push: grep the tracked files (`git ls-files | xargs grep -n -i -E "..."`) for personal names, e-mail
addresses, local paths and the names of the other projects, and check the e-mail addresses of the commits
(`git log --format='%an <%ae>'`). A slip already pushed is fixed by rewriting the history (a fresh commit and a
force push, or a new repository), not by one more commit.

## README

- What the project is, how to use it, how it works, what it measured.
- Borrowed or adapted code: one line about its origin and the license, no history of it.
- No comparisons with other products or with the project's own predecessors, unless asked for.
- Readable: a contents line at the top, short sections, examples before the reference tables.

## Measurements

- Every row says in words what happens (what is read, where it is written), not just "from → to".
- The current state only, no "before / after"; configurations side by side with their ratio.
- Only measured numbers: a value that was not measured is not written; "not profiled" is.
- The method, the environment and the noise (how many runs, the spread between runs) are stated.
- One measurement at a time on a shared environment; a stopped run is checked to be gone (its processes and
  containers) before the next one starts.

## Docs

- One language, terse, one line width; they describe what is in the repository now.
- A command, an option, a test or a configuration that changes — its doc changes in the same commit.

## Verification

- The build and the whole test suite pass before a commit; a fixed defect gets a test that fails without the fix.
- A script is not edited while it runs (a shell reads it as it goes).

## Commits

- A short subject of what changed in the language of the docs, a body when it is not obvious; under the noreply
  address.
- Commit and push only when asked.
