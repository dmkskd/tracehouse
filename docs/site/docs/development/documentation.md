# Documentation

The documentation site is built with [Docusaurus](https://docusaurus.io/) and lives in `docs/site/`.

## Local Development

```bash
just docs-dev       # Start dev server with hot reload
just docs-build     # Build static site to docs/site/build/
just docs-serve     # Serve the built site locally
```

## Assets (Videos & Images)

Large assets (demo videos, hero video) are **not** stored in the repo. They are hosted as GitHub Release artifacts under the `docs-assets-v1` tag in a separate `tracehouse-assets` repository.

The base URL is configured in `docs/site/docusaurus.config.ts`:

```ts
customFields: {
  assetsBaseUrl: 'https://dmkskd.github.io/tracehouse-assets',
}
```

Components access videos via `siteConfig.customFields.assetsBaseUrl`:

```tsx
const assetsBaseUrl = siteConfig.customFields?.assetsBaseUrl as string;
<video src={`${assetsBaseUrl}/hero.mp4`} />
```

To add or update an asset, upload it to the `tracehouse-assets` repository and reference the filename in the component.

Static images (favicon, logos) are stored normally in `docs/site/static/img/`.

## Deployment

The site is deployed to GitHub Pages via the `deploy-docs` GitHub Actions workflow (`.github/workflows/deploy-docs.yml`).

- **Trigger**: Manual (`workflow_dispatch`). Auto-deploy on push to `main` is available but commented out.
- **Build**: `npm ci && npm run build` in `docs/site/`
- **Output**: Static files in `docs/site/build/` uploaded to GitHub Pages

## Structure

```
docs/site/
├── docs/                  # Markdown content
│   ├── getting-started.md
│   ├── architecture.md
│   ├── guides/            # How-to guides
│   ├── features/          # Feature documentation
│   ├── development/       # Development guides (this section)
│   └── reference/         # Reference material
├── src/
│   ├── pages/index.tsx    # Landing page
│   ├── components/        # React components (features, FAQ)
│   └── css/               # Theme files
├── static/                # Static assets (images, favicon)
├── docusaurus.config.ts   # Site configuration
└── sidebars.ts            # Sidebar navigation
```
