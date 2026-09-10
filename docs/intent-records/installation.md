$id-2013260476935820
title: Lubko installation stays simple across supported environments
date: 2026/09/09
source: @ottojung
kind: requirement

Installing and running Lubko should be simple in every supported environment. This applies explicitly to current Termux and also to ordinary GNU/Linux environments such as Ubuntu.

The normal installation path must contain only requirements that are genuinely needed to install and run Lubko. Development, linting, type-checking, test, compiler, and other contributor-only tooling must not be pulled into the default installation path merely because the repository uses those tools during development or CI.

Platform-specific installation code and prerequisites should stay minimal, understandable, and justified by real platform/runtime differences. Termux support in particular must not accumulate a large compatibility layer, private obsolete toolchain, fake GNU/Linux environment, or substantial maintenance surface simply to make installation work. If Termux installation requires materially more complexity than ordinary Python application installation, that complexity should be treated as a problem to investigate and reduce, and any irreducible complexity must have a clear concrete reason.

The same standard applies to more typical GNU/Linux installation: avoid unnecessary bootstrap logic, host dependencies, special cases, and duplicated installation machinery. Prefer one small coherent installation model with only narrow platform-specific seams where the operating system genuinely requires them.
