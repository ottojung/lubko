# Lubko

Lubko is the safe connector and safe execution transport for remote development commands. A (possibly untrusted) compatible agent submits commands through the `lubko.jobs` queue, a Lubko worker safely executes them in the requested working directory, and the worker publishes bounded output and a terminal result to the same job row.

## Getting started

Install the maintained transport commands with `lubko-install`, start Lubko, and submit commands as described in [`docs/SKILL.md`](docs/SKILL.md).

## Development

Lubko requires CPython 3.12 or later. See [`docs/TOOLCHAIN.md`](docs/TOOLCHAIN.md) and [`docs/GOVERNANCE.md`](docs/GOVERNANCE.md).

## License

Lubko is licensed under the GNU Affero General Public License version 3 only (`AGPL-3.0-only`). See [LICENSE](LICENSE).
