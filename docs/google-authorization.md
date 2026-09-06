# Google authorization in Zohelo-data

Checked 6 September 2026, starting from main `482b04a`. Google Drive is the only authenticated Google data API currently used by this repository. NBP's public API does not require Google credentials. Browser sign-in, Python jobs and the ChatGPT Drive connector are separate authorization contexts; success in one does not prove the others work.

## Configuration map

| Consumer | Required configuration | Verification boundary |
| --- | --- | --- |
| Actions: ingestion, backfill, bronze, silver, folder reconciliation | Normally `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` **and** `GOOGLE_OAUTH_REFRESH_TOKEN`; all use `StorageManager` | The [read-only diagnostic job](https://github.com/rutkala/zohelo-data/actions/runs/34056318981/job/101570751331) reported `read_access_verified` at 22:36:34 UTC on 6 September 2026, using `oauth_environment`. Drive metadata and platform-folder visibility were verified. The earlier [ingestion run](https://github.com/rutkala/zohelo-data/actions/runs/34005869087) remains historical `invalid_grant` evidence. |
| Codespaces Python | Same Python credential requirements, supplied by Codespaces secrets or its environment | Owner-supplied diagnostic output on 6 September 2026 verified service-account reads at 23:06:51 UTC, then OAuth reads at 23:11:19 UTC with the legacy JSON variable omitted for that process. Both reported `read_access_verified`; writes remain untested. |
| Live portal | Public OAuth **web** client ID, authorized JavaScript origin, Drive API enabled, and the user's browser consent | Deployment maps `GOOGLE_OAUTH_CLIENT_ID` to `DUCK_UI_GOOGLE_CLIENT_ID`. The owner previously demonstrated an actual Drive-backed query. This is not proof that an old browser token is still valid. |
| Codespaces portal preview | Same browser requirements; the preview origin must be explicitly authorized | Vite now accepts the public `GOOGLE_OAUTH_CLIENT_ID` as a fallback. An explicit `DUCK_UI_GOOGLE_CLIENT_ID` still wins. Private client secrets and refresh tokens are not mapped into the bundle. Real preview-origin authorization is not inspectable here. |
| Native DuckDB through the current Python transfer path | Python downloads authorized data, then dbt/DuckDB reads local files | DuckDB does not need separate Google consent for those local files. |
| Legacy `StorageManager.setup_duckdb()` helper | Service-account JSON for the community extension, according to the existing code | Not invoked by the current pipeline. It is not a validated OAuth-compatible path and should not be used as a general access test. |

There are no current Sheets, Docs, Gmail or Calendar API consumers to authorize. Public Google STUN addresses used by optional browser collaboration are not an authenticated Google API. Separate tools such as an AI CLI may have their own account sign-in; these two repository variables do not authorize arbitrary Google products or establish API billing entitlements.

## What the secrets mean

The client ID and client secret identify the application. A user's consent produces tokens that authorize access to their data. For unattended execution, the current Python path needs a refresh token so it can obtain short-lived access tokens when the user is away. Creating an OAuth client alone does not grant access to Drive. See [Google's OAuth flow](https://developers.google.com/identity/protocols/oauth2/web-server).

The Python path asks for `https://www.googleapis.com/auth/drive`; the browser asks for `https://www.googleapis.com/auth/drive.readonly`. Browser code uses Google's token model and **never needs the client secret or a refresh token**. The new expiry/401 handling requests sign-in again through the existing user interaction rather than silently reusing rejected tokens or repeatedly opening popups. Manual/legacy tokens with unknown lifetimes are not assigned an invented expiry; Drive rejection still ends their session. See [Google's browser token lifecycle](https://developers.google.com/identity/oauth2/web/guides/use-token-model#token_expiration).

When all three OAuth environment variables are nonempty, `StorageManager` selects `oauth_environment` before reading `GCP_SERVICE_ACCOUNT_JSON`. This keeps the owner's configured OAuth identity consistent across Actions and Codespaces. A failed OAuth refresh raises an error; it never switches to another account or starts browser consent. Unused legacy JSON, including malformed JSON, does not affect this selected OAuth path.

When the OAuth trio is absent or incomplete, the existing legacy paths remain available: service-account JSON, supported typed credential JSON such as `authorized_user`, or an OAuth client JSON fallback. Malformed JSON still fails if this legacy path is selected. Supply the complete OAuth trio for the personal Drive pipeline rather than relying on legacy account selection. The diagnostic identifies the selected source when initialization succeeds. A service account's access to files and its ability to own/upload files in a consumer My Drive account are different questions; it is not a drop-in use of the owner's storage allocation.

Actions secrets and Codespaces secrets are distinct stores. Giving secrets the same names does not synchronize their contents. GitHub makes authorized Codespaces secrets available to the running environment; updating a secret can require stopping/restarting an existing Codespace. Removing explicit `remoteEnv` forwarding in the foundation change did not remove separately configured Codespaces secrets. See [GitHub's Codespaces secret guidance](https://docs.github.com/en/codespaces/managing-your-codespaces/managing-your-account-specific-secrets-for-github-codespaces).

## Read-only diagnostic

The **Check Google Drive access** workflow runs on main when its implementation changes, or manually on main. It installs the same pinned dependencies and invokes:

```bash
python scripts/check_google_access.py
```

This command is also available to the responsible agent inside a Codespace with its existing credentials. It reports only secret-name presence, a fixed credential-source/status code and permission booleans. It never prints token values, client secrets, account identities, file IDs or raw exception bodies, and never starts interactive consent.

The check refreshes/authenticates through the same `StorageManager` path, calls read-only Drive metadata endpoints and looks for the platform folder. It does not create folders, ingest data, upload, delete, rotate credentials or start downstream pipelines. Folder `canAddChildren` is metadata about capability, not proof that a write using a particular OAuth grant succeeds. A successful read check must not be reported as an upload test or proof that every dataset file is accessible.

| Report status | Meaning |
| --- | --- |
| `missing_oauth_configuration` | Required variable names are listed; two client identifiers alone are insufficient. |
| `oauth_invalid_grant` / `refresh_rejected` | Google rejected the refresh grant; reauthorization is needed after checking the cause. |
| `oauth_invalid_client` | The client identity/secret pairing needs checking. |
| `drive_http_403` | Drive access was denied; check enabled API, scope, policy and resource permissions. No single cause is inferred. |
| `platform_folder_not_visible` / `ambiguous_platform_folder` | Authentication reached Drive but the configured folder cannot be selected safely. Nothing is created or replaced. |
| `read_access_verified` | The selected credentials reached Drive and could inspect/list the platform folder; writes remain untested. |

## Remaining account checks and recovery

### Owner evidence and public application pages

Earlier owner screenshots showed an **External / Testing** app, an enabled web client, the live `https://data.zohelo.com` JavaScript origin, and incomplete branding. The owner now reports that the app is **In production**, fresh offline grants returned HTTP 200, and matching client/refresh credentials were updated in Actions and Codespaces. The earlier Testing state and retained/mismatched token were plausible causes of the historical failure, not proof of which grant was rejected. Before the OAuth-priority change, Actions selected `oauth_environment` while the owner's normal Codespaces check selected `service_account_json`; the same secret names did not establish the same effective configuration.

The portal now supplies these public pages, accessible without Google sign-in or a local profile:

| Google Branding field | Value |
| --- | --- |
| Application home page | `https://data.zohelo.com/about.html` |
| Application privacy policy link | `https://data.zohelo.com/privacy.html` |
| Application terms of service link | `https://data.zohelo.com/terms.html` |

Use the descriptive About page as Google's homepage rather than the workspace's initial profile dialog. The public pages are static files in `portal/public/`; they load no scripts and are linked from the profile dialog, workspace home and Drive privacy disclosure. Keep the privacy text synchronized with actual persistence, optional remote AI, sharing and pipeline behavior. Do not restore blanket claims that the complete app never transmits data or that disconnecting Drive deletes all retained copies.

The owner reports that the Branding settings were saved and **Audience → Publish app** now shows production. If that status changes, inspect Google's specific message; supplying these pages is not a promise of Google approval or domain verification. Personal-use verification exemptions and publishing status are separate. See [Google's branding requirements](https://support.google.com/cloud/answer/15549049?hl=en) and [personal-use exemption](https://support.google.com/cloud/answer/13464323?hl=en).

Publishing alone does not repair an expired, revoked or mismatched token. The owner reports obtaining a fresh offline grant for the intended current client and scope and storing matching values in each required runtime. Re-run the read-only authorization check in any runtime that has not been tested before production data jobs; do not use a data job as an authorization smoke test.

### Checks still requiring account access

This session cannot directly inspect or change private Google Cloud settings, secret stores or an active personal Codespace. Workflow results are the evidence for Actions; the owner supplied the two Codespaces diagnostic outputs described above. All three checks reported `drive_api_access=true`, `platform_folder_visible=true`, `can_list_children=true` and `can_add_children=true`, while `actual_writes_tested=false`. The capability flags are permission metadata, not performed uploads.

The Codespaces OAuth result came from `env -u GCP_SERVICE_ACCOUNT_JSON python scripts/check_google_access.py`, which omitted the legacy variable only for that process. It verifies the stored OAuth grant, not an updated checkout using the new default precedence. After the responsible agent updates that checkout without discarding local work, the ordinary diagnostic should select `oauth_environment` while the complete OAuth trio is present. Personal browser-preview authorization and real uploads still require their own checks; neither is proved by these metadata reads.

One common cause of refresh failure is an external OAuth app remaining in **Testing**. Google documents a seven-day refresh-token lifetime for that configuration when Drive scopes are used. Other causes exist, including revoked access and token limits, so `invalid_grant` alone does not prove Testing is the cause. See [Google's refresh-token rules](https://developers.google.com/identity/protocols/oauth2#expiration).

For future reauthorization, check the selected Cloud project's [OAuth Audience screen](https://console.cloud.google.com/auth/audience), complete Google's consent flow for the intended Drive scope and offline access, then store the resulting refresh token securely in each required runtime. User consent/account access cannot be manufactured from the client secret. Never paste tokens or client secrets into chat, issues, commits, browser bundles or public logs.

For browser access, the live origin `https://data.zohelo.com` and web client type appear in the owner's screenshot. A Codespaces preview has its own exact origin, commonly `https://<codespace-name>-5173.app.github.dev`, and localhost is a separate origin again. No Codespaces preview origin appears in the supplied list; the actual preview address and its registration still need verification. The public-ID fallback fixes application wiring, not Google's origin allowlist.
