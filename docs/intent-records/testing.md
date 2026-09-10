$id-9488662825936764
title: Canonical Python test suite stays under ten seconds
date: 2026/09/09
source: @ottojung
kind: requirement

The complete canonical Python test suite is `uv run pytest`, and it must continue to finish in strictly less than 10.0 seconds of wall-clock time once the Python development/test environment is already installed. This keeps the current fast-test requirement unchanged for pytest itself.

The ten-second budget measures the pytest execution, not the time required to provision an operating-system/container environment, install system packages, install Python dependencies, build an image, or perform a fresh Lubko installation before pytest can run.

$id-4342071557230346
title: Installation and environment validation is exempt from the pytest budget
date: 2026/09/09
source: @ottojung
kind: accepted-tradeoff

Fresh-install and environment-compatibility checks may legitimately take longer than ten seconds because they exercise provisioning and installation work that pytest deliberately does not include. This exemption is narrow: it applies to operating-system/container setup, dependency installation, Lubko installation, image construction, and similarly necessary environment-acceptance work. It must not be used to move ordinary slow Python tests out of the canonical `uv run pytest` suite or to weaken the pytest budget.

These environment checks should still run automatically in normal CI when practical; their longer wall-clock duration is an accepted cost of validating installation and portability.

$id-9335449599785359
title: Lubko installs in ordinary Linux and current Termux
date: 2026/09/09
source: @ottojung
kind: requirement

Lubko must have automated fresh-environment validation proving that its supported installation path works in both an ordinary Linux environment such as a clean Ubuntu container and a current Termux environment. The checks must exercise a real installation rather than relying only on the preconfigured development environment or mocked installer internals.

Termux compatibility failures caused by Python-version policy, native dependencies, packaging, process behavior, or other platform assumptions should be treated as product/toolchain compatibility work rather than hidden by Termux-specific skips or a fake Linux environment.

$id-2070127763914823
title: Prefer a small real runtime smoke test when it stays simple
date: 2026/09/09
source: @ottojung
kind: preference

If clean installation-environment validation can elegantly and robustly start an installed Lubko runtime and prove a small real interaction with it, prefer including that smoke test because it gives stronger evidence that the installed system actually runs. A simple PostgreSQL-backed command round trip or equivalent functional check is appropriate.

Do not make this preference force a fragile or complicated integration harness, runtime process-topology inspection, or substantial new maintenance surface. If the functional smoke test cannot remain small, deterministic, and easy to understand, successful real installation plus installed-command execution is sufficient for the environment-compatibility check.
