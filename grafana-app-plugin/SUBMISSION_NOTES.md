# Grafana Submission Notes

Grafana reported:

```text
no-provenance-attestation
No provenance attestation. This plugin was built without build verification.
```

Verified attestation:

```bash
gh attestation verify dmkskd-tracehouse-app-0.16.2.zip --repo dmkskd/tracehouse
```

Result: verification succeeded for the plugin ZIP built by `.github/workflows/release.yml@refs/tags/v0.16.2`.

Observed source URL behavior:

- `tracehouse-assets` source ZIP downloads correctly and satisfies source-map checks, but provenance fails because the URL does not identify `github.com/dmkskd/tracehouse`.
- GitHub release source ZIP identifies the repo, but Grafana's validator treats `github.com/.../releases/download/...zip` as a repo path instead of downloading it as a ZIP.
- GitHub source folder identifies the repo and passes provenance, but fails source-map checks because the built bundle references files outside `grafana-app-plugin`.

Next fix:

- Use Grafana's official `grafana/plugin-actions/build-plugin` release path with `attestation: true`.
