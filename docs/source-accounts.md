# Free source accounts and API credentials

Verified against current primary provider documentation on 8 September 2026. This is an onboarding guide and credential registry, not evidence that all sources are connected, ingested, modeled, or included in the 182-product research snapshot. `AR-*` is the stable account-research ID in [`config/source-access.yaml`](../config/source-access.yaml); `inventory_id` is also recorded there when the source already has one.

No account was created and no key was acquired during this work. Only the BDL credential path is wired to a runtime adapter in this change set. WDI, Eurostat, and NBP already run without source credentials. Every other credential below prepares future onboarding and still requires its own source contract, adapter, tests, and bounded connection proof.

## Where keys go

Put source keys only in the repository's [GitHub Actions secrets settings](https://github.com/rutkala/zohelo-data/settings/secrets/actions), using the exact secret name in this guide. Enter the secret value in GitHub's **Secret** field and the listed name in **Name**. The workflow can receive a secret later without committing its value.

For the currently connected BDL key, an authenticated repository operator can instead run `python scripts/configure-source-secret.py GUS_BDL_API_KEY`. The helper prompts without echo and passes the value to `gh secret set` through standard input. It never accepts the value as a command-line argument. The manual GitHub settings page remains the canonical place to inspect the secret name and replace the value.

Do not paste keys into chat, issues, pull requests, repository files, logs, screenshots, the portal, or browser-delivered configuration. If a provider emails a key, copy it directly from that email into the Actions secret and keep the message in the account owner's mailbox. Rotate a disclosed key at the provider and replace the Actions secret. The YAML registry contains names and handling metadata only; it must never contain a secret value.

For local development, pass a needed value as a process environment variable from a local credential manager. Do not create or commit a plaintext `.env` file. A developer does not need source keys to run fixture-based checks.

## Account classes

| Class | Meaning | Sources in this review |
| --- | --- | --- |
| Free account increases limits | Anonymous or preview access exists, while a free key raises documented capacity or removes the anonymous threshold. | BDL, DBW, SDP, SMUP, UN Comtrade, U.S. Census |
| Free account required for access | The selected API or download workflow needs a free key/profile. | REGON BIR1, FRED, EIA API, OpenAQ API, NASA Earthdata authenticated downloads, Copernicus CDS API |
| No account needed | The current public integration has no key benefit to activate. | World Bank WDI, Eurostat, NBP |

An API account is separate from a licence decision. Free access does not by itself authorize every form of redistribution, and an account does not make an unimplemented adapter operational.

## Statistics Poland: highest-value first accounts

### AR-001 — BDL (`GUS_BDL_API_KEY`)

Register at the [exact BDL client registration endpoint](https://bdl.stat.gov.pl/api/v1/client?showInFrame=yes&theme=Default). Enter a valid contact email. The page says the automatically generated client key is sent to that address; it does not document a separate confirmation-link step. Store the received key as `GUS_BDL_API_KEY`.

The [official BDL documentation](https://api.stat.gov.pl/Home/BdlApi?lang=en) specifies the `X-ClientId` request header and these ceilings:

| Window | Anonymous | Registered |
| --- | ---: | ---: |
| 1 second | 5 | 10 |
| 15 minutes | 100 | 500 |
| 12 hours | 1,000 | 5,000 |
| 7 days | 10,000 | 50,000 |

These are provider ceilings, not scheduler targets. The runtime should retain headroom for retries, manual diagnostics, and concurrent jobs. The sample on the official page is UUID-like, but the provider does not promise that format as a validation contract, so treat the delivered value as an opaque secret.

### AR-002 to AR-004 — DBW, SDP, and SMUP

Use the provider registration pages for [DBW](https://api.stat.gov.pl/Home/DBWApi?lang=en), [SDP](https://api.stat.gov.pl/Home/SDPApi), and [SMUP](https://api.stat.gov.pl/Home/SMUPApi?lang=en). Each page asks for a valid email because the client key is sent there. Store the keys separately as `GUS_DBW_API_KEY`, `GUS_SDP_API_KEY`, and `GUS_SMUP_API_KEY`; do not reuse or copy the BDL key unless GUS explicitly states that a delivered key applies across products.

The current official [DBW API documentation](https://api-dbw.stat.gov.pl/apidocs/index.html?urls.primaryName=API+DBW+%28en%29), [SDP page](https://api.stat.gov.pl/Home/SDPApi), and [SMUP API documentation](https://api.smup.gov.pl/apidocs/index.html) show the same anonymous and registered ceilings as BDL: 5 versus 10 requests/second, 100 versus 500/15 minutes, 1,000 versus 5,000/12 hours, and 10,000 versus 50,000/7 days. Their adapters do not exist yet. Confirm the exact request transport from each API specification when implementing them; the authorization widget alone is not enough evidence to invent a header name.

### AR-005 — REGON BIR1 (`GUS_REGON_API_KEY`)

REGON is free but key-gated. The [official REGON page](https://api.stat.gov.pl/Home/RegonApi) instructs an ordinary commercial or non-public-body applicant to email `regon_bir@stat.gov.pl` with:

- full organization name and its REGON number;
- contact person's name, email, landline and/or mobile number;
- source IP addresses when users will connect through the software provider's access server; and
- approximate number of simultaneous end users.

This research did not send that email. The owner should supply real organization and contact details; none should be invented in code or a ticket. After GUS returns the production User Key, store it as `GUS_REGON_API_KEY`. Public bodies seeking the extended non-public scope have a different written-application path and legal basis; that is outside this general public-field onboarding.

The same page states the following local-time ceilings: 08:00–16:59, 6,000/hour, 120/minute, 3/second; 06:00–07:59 and 17:00–21:59, 8,000/hour, 150/minute, 3/second; 22:00–05:59, 10,000/hour, 200/minute, 4/second. A future adapter must use `Europe/Warsaw`, preserve headroom, and keep the approved public-field/privacy allowlist separate from credential possession.

## Additional source accounts

| ID | Source and account step | Free benefit | Actions secret | Runtime |
| --- | --- | --- | --- | --- |
| AR-006 | [FRED API keys](https://fred.stlouisfed.org/docs/api/api_key.html): create/login to a FRED account, then request a distinct key for this application. | API access is key-required. The official key page gives no numeric quota. | `FRED_API_KEY` | Future adapter |
| AR-007 | [EIA API registration](https://www.eia.gov/opendata/register.php): give first/last name, email, organization category and use reason, then accept the API terms. | API access and notices. EIA bulk downloads require no key. No numeric API allowance is stated on the registration page. | `EIA_API_KEY` | Future adapter |
| AR-008 | Register from [UN Comtrade Plus](https://comtradeplus.un.org/), then obtain the free API key described by its official registration prompt. | Up to 100,000 records/call and 500 calls/day on the advertised free account. | `UN_COMTRADE_API_KEY` | Future adapter |
| AR-009 | [Request a U.S. Census key](https://api.census.gov/data/key_signup.html) with organization name and email and accept the terms. The [current guide](https://www.census.gov/data/developers/guidance/api-user-guide.API_Key.html) says the email must end in `.com`, `.net`, `.org`, `.gov`, or `.edu` and includes both the key and a registration link. | Queries above the anonymous 500/IP/day threshold; the keyed maximum is not stated. Queries may contain up to 50 variables. | `US_CENSUS_API_KEY` | Future adapter |
| AR-010 | [Create an OpenAQ Explorer account](https://explore.openaq.org/register), then manage the key in account settings. | Required API access at the [general-use limits](https://docs.openaq.org/using-the-api/rate-limits): 60/minute and 2,000/hour. | `OPENAQ_API_KEY` | Future adapter |
| AR-011 | [Register an Earthdata Login profile](https://urs.earthdata.nasa.gov/users/new) using the real user's name, email, country, affiliation, study area, and user type. | One login for EOSDIS data centers, tools, and services; NASA says Earthdata are generally open and free, with exceptions governed by international agreements. | `NASA_EARTHDATA_USERNAME`, `NASA_EARTHDATA_PASSWORD` | Future adapter |
| AR-012 | Follow the [Copernicus CDSAPI setup](https://cds.climate.copernicus.eu/how-to-api): register/login, copy the personal access token, and manually accept the terms on every selected dataset's page. | Programmatic CDS/ADS/CEMS access. No numeric allowance is stated on the setup page. | `COPERNICUS_CDS_PERSONAL_ACCESS_TOKEN` | Future adapter |

These are free account routes, not paid trials. The reviewed forms and official setup pages do not call for a payment card. Stop if a provider redirects to a paid plan, requests billing details, or presents materially different terms; update this evidence before proceeding.

FRED documents a 32-character lowercase alphanumeric key. OpenAQ uses `X-API-Key`; BDL uses `X-ClientId`; EIA, Census, and FRED document query parameters. Keep all values opaque in runtime code and error messages.

## Sources that should remain keyless

- **World Bank WDI (`AR-013`, `GL-005`)** — the [official Indicators API documentation](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation) says API keys and other authentication methods are no longer necessary. Do not create an Actions secret for the connected `world_bank_wdi` adapter.
- **Eurostat (`AR-014`, `GL-001`)** — the connected adapter uses the public dissemination endpoints documented in the [Eurostat API introduction](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-introduction). That documentation does not advertise an API-account capacity tier, so registration would add no verified benefit here.
- **NBP (`AR-015`)** — the [NBP Web API](https://api.nbp.pl/en.html) is a public HTTPS GET API and documents no account or API key. Keep it credential-free.

## Human handoff

1. Start with BDL because it is the only account credential connected to a runtime adapter in this work. Register with the operational mailbox and add `GUS_BDL_API_KEY` in [Actions secrets](https://github.com/rutkala/zohelo-data/settings/secrets/actions).
2. Let the bounded BDL workflow prove registered operation while staying below the configured safety budgets. Never raise runtime limits directly to the provider ceilings.
3. Create another free account only when that source's contract and adapter are ready for connection proof. Add the exact listed secret name, then run a bounded read that reports only presence/status, never the value.
4. Treat REGON separately: prepare the real applicant details and approved public-field use before the owner sends the provider email.
5. Record account ownership, recovery mailbox, accepted terms/date, key rotation date, and dataset-specific approvals in the private operational record. Keep those details and all key values out of the public portal and Git repository.

This review covers the named priority products and seven additional account-bearing candidates, plus the three connected keyless sources. It does not claim that account conditions for all 182 inventoried products have been researched.
