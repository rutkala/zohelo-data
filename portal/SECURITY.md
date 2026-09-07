# Security policy

## Supported versions

The deployed Zohelo Data portal and the current `main` branch are supported. The portal runs its SQL workspace in the browser and connects to Google APIs for authenticated release discovery.

## Reporting a vulnerability

Please use [GitHub private vulnerability reporting](https://github.com/rutkala/zohelo-data/security/advisories/new).

Do not open a public issue for a vulnerability or include credentials, tokens, private Drive identities, or private data in a report.

## Scope notes

The portal executes user-supplied SQL in the user's own browser by design. Relevant reports include cross-site scripting through results or shared links, deep links that execute without the intended confirmation, content-security-policy bypasses, credential-storage weaknesses, release-integrity failures, and cross-origin access to another session's data.
