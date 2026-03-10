# Frontend Notes

## SafeText: drei `<Text>` fallback for file:// builds

The single-file build (`vite.singlefile.config.ts`) produces a self-contained HTML
that can be opened from `file://`. In that context `window.origin` is `"null"`, and
browsers block blob-URL Web Workers.

drei's `<Text>` component uses **troika-three-text** internally, which spawns workers
via `URL.createObjectURL(new Blob(...))` for font parsing and SDF generation. When
the origin is null these blob URLs are rejected:

```
Not allowed to load local resource: blob:null/...
Cannot load blob:null/... due to access control checks.
worker module init function failed to rehydrate
```

### Solution

`@tracehouse/ui-shared` exports a `SafeText` component
(`packages/ui-shared/src/3d/SafeText.tsx`) that acts as a drop-in replacement.

- **Normal origin (http/https):** renders drei `<Text>` — full troika SDF text,
  workers enabled, no change in behavior.
- **Null origin (file://):** renders drei `<Html>` with CSS-styled text projected
  to the same 3D position. Not pixel-perfect (no occlusion, always on top) but
  functional and avoids the blob-URL errors entirely.
