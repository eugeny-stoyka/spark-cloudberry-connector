---
name: comment-cleanup
description: >-
  Prune nonessential comments and Scaladoc across the codebase.
  Use when asked to remove verbose or redundant comments, tighten documentation, or make comment style uniform.
  Aggressive downsizing, but retain high-value information.
---

# Comment cleanup

Trim ALL comments/Scaladoc EXCEPT those that carry information a competent reader could not reconstruct from
names, signatures, and code in a few seconds.

## Never touch

Functional comments are not prose: `// scalastyle:off|on`, `// scalafix:off|on`, `@formatter` switches,
license headers (`src/main/resources/META-INF/services/*`). Preserve every one. Annotations are code, not
comments.

## Prove you changed nothing but comments

This safety net is what lets you cut confidently. Build before and after (`docker/run.sh jar`; keep a copy of
the first `target/classes`, the build in Docker copies only the jars — unpack them) and compare the
disassembly of every class: `javap -c -p` of each `.class`, before and after, without `-l` (comments move the
line numbers, nothing else). Equal disassembly proves only comments changed. The regression on the stand
(`docker/run.sh regression`) is the separate guard of behaviour.

## Scale with subagents, then audit

Partition by file ownership so agents never share a file, and never let an agent run a history-rewriting or revert
command — it destroys a sibling's concurrent work; undo mistakes by hand-editing. Keep the verification script out
of their writable set; they only run it. Fresh subagents apply a given policy more willingly than ones asked to
reverse an earlier, more conservative instruction.

When the cutting is done, audit every deletion once more for erased rationale and restore the genuine losses.
Run review agents to cross-check the results.
Then build and run the full regression before committing.
