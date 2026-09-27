# Contributing to the Zohelo Data portal

Read the repository-root [AGENTS.md](../AGENTS.md) and [development guide](../docs/development.md) before changing the portal. They define the current product boundaries, supported environment, and required evidence.

## Setup

Use Node from `../.node-version` and npm with the committed lockfile:

```bash
npm ci --ignore-scripts --no-audit --no-fund
npm run dev
```

Do not introduce a second package manager or lockfile. Change intentional dependencies in `package.json`, regenerate `package-lock.json` with npm, and review the resolved diff.

## Checks

Run the checks that cover the change. The ordinary portal gate is:

```bash
npm run lint
npm test
npm run build
```

Use `npm run format:check`, `npm run typecheck`, and `npm run test:e2e` when the affected code or workflow calls for them. The root [development guide](../docs/development.md) describes the authoritative CI and browser paths.

## Code conventions

- Keep TypeScript strict and avoid `any`.
- Prefer named exports.
- Keep SQL identifiers and values behind the sanitizing helpers in `src/lib/sqlSanitize.ts`.
- Add focused regression coverage for behavior changes.
- Split a component when a change would add another responsibility to an already large file.
- Preserve the release boundary: portal queries must use one verified release and must fail visibly when released data cannot be loaded.

The portal is based on Duck-UI under the MIT license. Preserve [LICENSE.md](LICENSE.md) and relevant upstream attribution when reusing or modifying upstream code.
