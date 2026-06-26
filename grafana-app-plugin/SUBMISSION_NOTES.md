# Grafana Submission Notes

Decision log for the Grafana plugin submission work.

## Current status

`0.16.10` fixed the original attestation concern, but Grafana still blocked review.

What passed:

- GitHub Actions produced the plugin ZIP.
- `gh attestation verify ... --repo dmkskd/tracehouse` succeeded.
- Grafana no longer blocked on provenance.

What failed:

- Grafana scanned the repo root source tree.
- That included non-plugin files and root dependencies.
- Blocking errors:
  - `frontend/src/App.tsx:15` direct CSS import
  - `frontend/src/main.tsx:3` direct CSS import
  - root `package-lock.json` vulnerabilities for `http-proxy-middleware` / `undici`

`unsigned-plugin` is expected for a new plugin during review.

## Attempts

### 1. Custom hosted source ZIP

Source URL:

```text
https://dmkskd.github.io/tracehouse-assets/releases/vX.Y.Z/dmkskd-tracehouse-app-source.zip
```

Worked:

- Source ZIP could contain only plugin-relevant source.
- Local source-map checks looked promising.

Failed:

- Grafana could not link this source URL to the GitHub Actions attestation.

Learned:

- A generic hosted ZIP is not enough. Grafana wants a GitHub source reference for provenance.

### 2. GitHub release asset source ZIP

Source URL:

```text
https://github.com/dmkskd/tracehouse/releases/download/vX.Y.Z/dmkskd-tracehouse-app-source.zip
```

Worked:

- It is on `github.com/dmkskd/tracehouse`.
- It is a real public ZIP URL.

Failed:

- Grafana's source-code parser does not model GitHub release asset ZIPs as source archives.

Validator detail:

`pkg/repotool/repotool.go` parses GitHub source URLs with:

```go
regexp.MustCompile(`(?i)^(https:\/\/github\.com\/[^/]+\/[^/]+)(\/tree\/([^/]*)\/?(.*)$)?`)
```

This supports:

```text
https://github.com/owner/repo
https://github.com/owner/repo/tree/ref
https://github.com/owner/repo/tree/ref/subdir
```

It does not express:

```text
https://github.com/owner/repo/releases/download/ref/source.zip
```

Learned:

- A GitHub release asset URL can identify the repo, but the source-code checkout path only understands repo/tree URLs.

### 3. GitHub Pages ZIP with repo query parameter

Source URL:

```text
https://dmkskd.github.io/.../source.zip?repo=https://github.com/dmkskd/tracehouse
```

Worked:

- Nothing meaningful.

Failed:

- Grafana did not infer repository identity from the query parameter.

Learned:

- The validator uses the source URL itself, not custom query parameters.

### 4. GitHub repo root

Source URL:

```text
https://github.com/dmkskd/tracehouse/tree/v0.16.10
```

Worked:

- Provenance/attestation passed.
- Source files referenced by sourcemaps existed.

Failed:

- Grafana scanned the whole monorepo, not only sourcemap-referenced plugin files.
- It found blocking issues in unrelated source:
  - standalone frontend CSS imports
  - root lockfile vulnerable packages

Learned:

- Repo root is good for provenance but too broad for code/security scanning.

### 5. `grafana/plugin-actions/build-plugin`

Worked:

- Built a plugin ZIP.
- Created a draft GitHub release.
- Created attestation.

Failed:

- In this monorepo, the action treated repo root as the plugin root.
- GitHub Actions cannot set `working-directory` for a `uses:` action step.
- We had to add root-level shims:
  - root `package.json` build/sign
  - root `dist`
  - root `CHANGELOG.md`
  - root `.config/webpack/webpack.config.ts`

Extra failures hit while adapting:

- missing Linux optional dependency for `lightningcss`
- missing root `CHANGELOG.md`
- source-map/source-code mismatch
- `non-standard frontend build tooling`
- release creation needed `contents: write`

Learned:

- The official action is useful for standard plugin repos.
- It is awkward for this monorepo layout unless the plugin lives at repo root.
- It did not solve the source-scan scope problem.

### 6. Generated pruned source branch

Source URL idea:

```text
https://github.com/dmkskd/tracehouse/tree/grafana-source-vX.Y.Z
```

Worked:

- Would keep source on GitHub.
- Could include only plugin-relevant files from sourcemaps.

Failed / concern:

- Creates generated branches/files per release.
- Adds release-process complexity.

Learned:

- This is a possible emergency unblock, not a good long-term model.

## Validator limitation to report upstream

There are two separate URL assumptions:

1. Provenance extracts owner/repo from any GitHub-looking source URL.

`pkg/analysis/passes/provenance/provenance.go`:

```go
regexp.MustCompile(`https://github\.com\/([^/]+)/([^/]+)`)
```

2. Source-code retrieval only supports repo/tree URLs.

`pkg/repotool/repotool.go`:

```go
regexp.MustCompile(`(?i)^(https:\/\/github\.com\/[^/]+\/[^/]+)(\/tree\/([^/]*)\/?(.*)$)?`)
```

Practical issue:

- A GitHub release asset source ZIP is public and repo-scoped.
- Provenance can identify the repo from it.
- Source-code retrieval cannot treat it as a source archive.

This is worth reporting to Grafana because supporting GitHub release asset source ZIPs would help monorepos submit a precise source archive without exposing unrelated repo files.

## Preferred release direction

Use the custom plugin-level build again:

1. Build with `just grafana-plugin-build`.
2. If `GRAFANA_ACCESS_POLICY_TOKEN` is available, sign inside `grafana-app-plugin`.
3. Package `grafana-app-plugin/dist`.
4. Attest the final ZIP.
5. Upload ZIP, SHA1, HTML, and binaries to one draft release.

Signing must happen before zipping.

## Remaining decision

Before another Grafana submission, validate the exact source URL and ZIP URL locally with `grafana/plugin-validator-cli`.

Realistic choices:

- Fix the repo root so `https://github.com/dmkskd/tracehouse/tree/vX.Y.Z` passes Grafana scanning.
- Make `grafana-app-plugin` self-contained so `https://github.com/dmkskd/tracehouse/tree/vX.Y.Z/grafana-app-plugin` works.

Do not resubmit based on assumptions.
