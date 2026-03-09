# Security Scanning

The project includes a `just security-scan` command that runs two checks:

1. **npm audit** - checks npm dependencies for known vulnerabilities
2. **semgrep** - static analysis for common security issues in source code

## Running the scan

```bash
just security-scan
```

This runs both checks and exits with a non-zero code if either fails.

## npm audit

`npm audit` runs against the root workspace and checks all transitive dependencies for known CVEs. It requires no extra tooling - it's built into npm.

To fix vulnerabilities, you can usually either upgrade the direct dependency or add an `overrides` entry in the root `package.json` to force a patched version of a transitive dependency.

## semgrep

[Semgrep](https://semgrep.dev/) is a static analysis tool that scans `packages/` and `frontend/src/` for common security issues (XSS, injection, etc.) using community-maintained rulesets.

Semgrep is **not installed by default** because it has a separate license. If you run `just security-scan` without it installed, you'll see:

```text
semgrep is not installed.
Install it with: ./scripts/setup.sh --security
```

To install it:

```bash
./scripts/setup.sh --security
```

The `--security` flag is opt-in to keep the default development setup lightweight. See the script for platform-specific installation details.
