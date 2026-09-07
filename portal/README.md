# Zohelo Data portal

This directory contains the React, TypeScript, and Vite application published at [data.zohelo.com](https://data.zohelo.com/). It provides the release-bound data catalogue and a browser DuckDB workspace for exploring published Zohelo datasets.

Project-wide architecture, data boundaries, and operational status live in the repository root:

- [Repository overview](../README.md)
- [Architecture](../docs/architecture.md)
- [Development and verification](../docs/development.md)
- [Portal user guide](../docs/using-the-portal.md)
- [Shared contribution rules](../AGENTS.md)

## Local development

Use Node from `../.node-version` and the committed npm lockfile:

```bash
npm ci --ignore-scripts --no-audit --no-fund
npm run dev
```

The local Vite server runs at `http://localhost:5173`. From the repository root, the equivalent commands use `npm --prefix portal ...`.

Before submitting a portal change, run the checks relevant to it:

```bash
npm run lint
npm test
npm run build
```

`npm run format:check`, `npm run typecheck`, and `npm run test:e2e` are available for changes that affect formatting, type boundaries, or browser flows. See the root development guide for browser prerequisites and the authoritative CI path.

## Configuration and generated files

Copy `.env.example` to `.env` for local public client configuration. Never place OAuth secrets or refresh tokens in the portal environment; Vite browser variables are public.

The production portal is a static build deployed by the root repository workflows. `dist/`, generated dbt Docs under `public/docs/`, local databases, test reports, and installed packages are build outputs and must remain untracked.

## Upstream foundation

The browser workbench is derived from [Duck-UI](https://github.com/caioricciuti/duck-ui). Zohelo-specific catalogue, release, Google Drive, and product behavior is maintained in this repository. The upstream MIT copyright and permission notice are preserved in [LICENSE.md](LICENSE.md).
