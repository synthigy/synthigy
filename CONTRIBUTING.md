# Contributing to Synthigy

Issues, bug reports and pull requests are welcome.

## Before you start

**This repository is a filtered export.** Synthigy is developed in a private
monorepo and published here by an export script — tests, benchmarks, the
frontend and internal docs stay private. That has two consequences:

1. Pull requests are reviewed here and applied upstream in the private tree,
   then flow back out with the next export. Your commit may therefore reach
   `main` squashed or reworded, under your authorship.
2. Do not send changes to generated or exported-only files (`.github/`, build
   output) — they are overwritten on the next export.

For anything larger than a bug fix, open an issue first. A design that does
not fit the private half is a frustrating thing to discover after writing it.

## Contributor License Agreement

Synthigy is dual-licensed: AGPL-3.0 for everyone, and a commercial license
for those who cannot take on the AGPL's obligations. Dual licensing only
works if one party can license the whole codebase both ways, so contributors
sign a CLA — see [CLA.md](CLA.md). You keep your copyright; you grant the
right to license your contribution under both.

Open a pull request and the CLA Assistant bot will comment with a link.
One click, recorded once, and later pull requests go straight through.

## Practical notes

- Match the surrounding code. This codebase has strong conventions — read a
  neighbouring namespace before inventing a shape.
- **No private functions**: `defn`, never `defn-`.
- **Docstrings are one short sentence**, and only where they earn it — public
  entry points, protocol methods, genuinely tricky functions. No arg lists,
  no `Args:`/`Returns:` blocks.
- **No narrative or history comments in code.** Explain the mechanism in the
  commit message or the issue, not in a comment block.
- Keep a pull request to one concern.

## Reporting a security issue

Do not open a public issue. Email r.gersak@gmail.com with the details and
give a reasonable window for a fix before disclosing.
