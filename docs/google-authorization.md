# Google authorization in Zohelo-data

Checked 6 September 2026, starting from main `482b04a`. Google Drive is the only authenticated Google data API currently used by this repository. NBP's public API does not require Google credentials. Browser sign-in, Python jobs and the ChatGPT Drive connector are separate authorization contexts; success in one does not prove the others work.

## Configuration map

| Consumer | Required configuration | Verification boundary |
| --- | --- | --- |
| Actions: ingestion, backfill, bronze, silver, folder reconciliation | Normally `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET` **and** `GOOGLE_OAUTH_REFRESH_TOKEN`; all use `StorageManager` | New read-only workflow checks the currently selected credentials. The earlier [ingestion run](https://github.com/rutkala/zohelo-data/actions/runs/34005869087) failed with `invalid_grant`. |
| Codespaces Python | Same Python credential requirements, supplied by Codespaces secrets or its environment | A clean container/fixture CI pass proves setup, not access to the owner's Drive. The active personal Codespace and its secrets were not accessible from this session. |
| Live portal | Public OAuth **web** client ID, authorized JavaScript origin, Drive API enabled, and the user's browser consent | Deployment maps `GOOGLE_OAUTH_CLIENT_ID` to `DUCK_UI_GOOGLE_CLIENT_ID`. The owner previously demonstrated an actual Drive-backed query. This is not proof that an old browser token is still valid. |
| Codespaces portal preview | Same browser requirements; the preview origin must be explicitly authorized | Vite now accepts the public `GOOGLE_OAUTH_CLIENT_ID` as a fallback. An explicit `DUCK_UI_GOOGLE_CLIENT_ID` still wins. Private client secrets and refresh tokens are not mapped into the bundle. Real preview-origin authorization is not inspectable here. |
| Native DuckDB through the current Python transfer path | Python downloads authorized data, then dbt/DuckDB reads local files | DuckDB does not need separate Google consent for those local files. |
| Legacy `StorageManager.setup_duckdb()` helper | Service-account JSON for the community extension, according to the existing code | Not invoked by the current pipeline. It is not a validated OAuth-compatible path and should not be used as a general access test. |

There are no current Sheets, Docs, Gmail or Calendar API consumers to authorize. Public Google STUN addresses used by optional browser collaboration are not an authenticated Google API. Separate tools such as an AI CLI may have their own account sign-in; these two repository variables do not authorize arbitrary Google products or establish API billing entitlements.

## What the secrets mean

The client ID and client secret identify the application. A user's consent produces tokens that authorize access to their data. For unattended execution, the current Python path needs a refresh token so it can obtain short-lived access tokens when the user is away. Creating an OAuth client alone does not grant access to Drive. See [Google's OAuth flow](https://developers.google.com/identity/protocols/oauth2/web-server).

The Python path asks for `https://www.googleapis.com/auth/drive`; the browser asks for `https://www.googleapis.com/auth/drive.readonly`. Browser code uses Google's token model and **never needs the client secret or a refresh token**. The new expiry/401 handling requests sign-in again through the existing user interaction rather than silently reusing rejected tokens or repeatedly opening popups. Manual/legacy tokens with unknown lifetimes are not assigned an invented expiry; Drive rejection still ends their session. See [Google's browser token lifecycle](https://developers.google.com/identity/oauth2/web/guides/use-token-model#token_expiration).

`GCP_SERVICE_ACCOUNT_JSON`, when configured, can override the OAuth environment variables. Valid service-account JSON is selected first; supported typed credential JSON such as `authorized_user` is loaded next. Malformed JSON fails parsing rather than silently falling back. A valid OAuth client JSON object can also supply a fallback flow. The diagnostic identifies the selected source when initialization succeeds. A service account's access to files and its ability to own/upload files in a consumer My Drive account are different questions; it is not a drop-in use of the owner's storage allocation.

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

The owner subsequently supplied Google Cloud screenshots confirming an **External / Testing** app, an enabled web client, the live `https://data.zohelo.com` JavaScript origin, and an incomplete-branding banner. The owner also reported replacing the client ID and client secret while retaining the old refresh token. Testing gives Drive refresh grants a seven-day lifetime; a token issued to a different client also cannot be transferred by changing the two client variables. These are plausible causes of the observed failure, not proof of which grant was rejected. The existing `GCP_SERVICE_ACCOUNT_JSON` secret can still affect credential selection.

The portal now supplies these public pages, accessible without Google sign-in or a local profile:

| Google Branding field | Value |
| --- | --- |
| Application home page | `https://data.zohelo.com/about.html` |
| Application privacy policy link | `https://data.zohelo.com/privacy.html` |
| Application terms of service link | `https://data.zohelo.com/terms.html` |

Use the descriptive About page as Google's homepage rather than the workspace's initial profile dialog. The public pages are static files in `portal/public/`; they load no scripts and are linked from the profile dialog, workspace home and Drive privacy disclosure. Keep the privacy text synchronized with actual persistence, optional remote AI, sharing and pipeline behavior. Do not restore blanket claims that the complete app never transmits data or that disconnecting Drive deletes all retained copies.

The owner must save the Branding settings and then use **Audience → Publish app** when Google enables it. If it remains disabled, inspect Google's specific remaining message; supplying these pages is not a promise of Google approval or domain verification. Personal-use verification exemptions and publishing status are separate. See [Google's branding requirements](https://support.google.com/cloud/answer/15549049?hl=en) and [personal-use exemption](https://support.google.com/cloud/answer/13464323?hl=en).

After the publishing status changes, obtain a fresh offline grant for the intended current client and scope and store the refresh token securely in each required runtime. Publishing alone does not repair an expired, revoked or mismatched token. Re-run the read-only authorization check before production data jobs. No new refresh token or Google Cloud publication has been performed as part of adding these pages.

### Checks still requiring account access

This session cannot directly inspect or change private Google Cloud settings, secret stores or an active personal Codespace. The owner's screenshots provide the client type, shown origins and Testing status described above; enabled APIs, granted scopes and live Codespaces access remain unverified. The GitHub connector does not expose those settings, and its secret-metadata endpoints were unavailable. Workflow results are the evidence for Actions; do not infer live Codespaces success from them.

One common cause of refresh failure is an external OAuth app remaining in **Testing**. Google documents a seven-day refresh-token lifetime for that configuration when Drive scopes are used. Other causes exist, including revoked access and token limits, so `invalid_grant` alone does not prove Testing is the cause. See [Google's refresh-token rules](https://developers.google.com/identity/protocols/oauth2#expiration).

Complete the app configuration using the public pages above, then check the selected Cloud project's [OAuth Audience screen](https://console.cloud.google.com/auth/audience). Once the configuration is ready, complete Google's consent flow for the intended Drive scope and offline access, then store the resulting refresh token securely in each required runtime. The assistant handles the technical steps where access permits; user consent/account access cannot be manufactured from the client secret. Never paste tokens or client secrets into chat, issues, commits, browser bundles or public logs.

For browser access, the live origin `https://data.zohelo.com` and web client type appear in the owner's screenshot. A Codespaces preview has its own exact origin, commonly `https://<codespace-name>-5173.app.github.dev`, and localhost is a separate origin again. No Codespaces preview origin appears in the supplied list; the actual preview address and its registration still need verification. The public-ID fallback fixes application wiring, not Google's origin allowlist.
