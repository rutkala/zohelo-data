import { useId, useState } from "react";
import { ClipboardCopy, Download, ExternalLink } from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import {
  BUSINESS_REVIEW_VERSION,
  NBP_REVIEW_CHOICES,
  SOURCE_REVIEW_CHOICES,
  renderReviewResponse,
  reviewStorageKey,
  type BusinessReviewDraft,
} from "@/lib/businessReview";
import { updateBusinessReviewDraft, useBusinessReviewDraft } from "@/lib/businessReviewStore";

const repoDocs = "https://github.com/rutkala/zohelo-data/blob/main/docs/";

function ReviewForm({ storageKey }: { storageKey: string }) {
  const { draft, saved } = useBusinessReviewDraft(storageKey);
  const formId = useId();
  const nbpTitleId = `${formId}-nbp-title`;
  const sourceTitleId = `${formId}-source-title`;
  const nbpNotesId = `${formId}-nbp-notes`;
  const sourceChoiceId = `${formId}-source-choice`;
  const sourceNotesId = `${formId}-source-notes`;
  const [copyMessage, setCopyMessage] = useState("");
  const [showResponse, setShowResponse] = useState(false);
  const response = renderReviewResponse(draft);
  const answered =
    Number(Boolean(draft.nbpChoice || draft.nbpNotes.trim())) +
    Number(Boolean(draft.sourceChoice || draft.sourceNotes.trim()));

  const update = (key: keyof Omit<BusinessReviewDraft, "version">, value: string) => {
    setCopyMessage("");
    updateBusinessReviewDraft(storageKey, (current) => ({ ...current, [key]: value }));
  };

  const copyResponse = async () => {
    setShowResponse(true);
    try {
      await navigator.clipboard.writeText(response);
      setCopyMessage("Copied. Paste this response into your Zohelo-data chat to send it.");
    } catch {
      setCopyMessage(
        "Copy was unavailable. Select the response below or download it, then share it in your chat."
      );
    }
  };

  const downloadResponse = () => {
    const url = URL.createObjectURL(new Blob([response], { type: "text/plain;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "zohelo-business-input.txt";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    setCopyMessage("Downloaded. Attach the file to your Zohelo-data chat to send it.");
  };

  return (
    <section className="mx-auto max-w-4xl space-y-6 p-4 sm:p-6" aria-label="Review and decisions">
      <header className="space-y-2">
        <h1 className="text-2xl font-semibold">Review &amp; decisions</h1>
        <p className="text-sm text-muted-foreground">
          Two open items, together in one place. The source definitions are researched for you; your
          input is about what you want to analyse.
        </p>
        <p className="text-xs text-muted-foreground">
          Proposal version {BUSINESS_REVIEW_VERSION} · {answered}/2 items have a draft response
        </p>
      </header>

      {draft.version !== BUSINESS_REVIEW_VERSION && (
        <p role="alert" className="rounded border border-amber-500 p-3 text-sm">
          Your saved draft refers to proposal {draft.version}. Read the current proposal before
          updating your answer.
        </p>
      )}

      <article className="space-y-4 rounded-lg border p-4" aria-labelledby={nbpTitleId}>
        <div>
          <p className="text-xs font-medium text-amber-500">BR-001 · Waiting for your input</p>
          <h2 id={nbpTitleId} className="mt-1 text-lg font-semibold">
            First NBP metrics
          </h2>
        </div>
        <p className="text-sm">
          Proposed starting scope: published exchange-rate values from Tables A, B and C, and NBP
          gold prices. Choose a publication date, currency and source table to see the value.
        </p>
        <details className="rounded border p-3 text-sm">
          <summary className="cursor-pointer font-medium">Read the researched definitions</summary>
          <dl className="mt-3 space-y-3">
            <div>
              <dt className="font-medium">Tables A and B</dt>
              <dd>
                NBP middle exchange-rate quotations. Each observation belongs to one source table,
                currency and publication date.
              </dd>
            </div>
            <div>
              <dt className="font-medium">Table C</dt>
              <dd>
                Separate buy (bid) and sell (ask) quotations. The publication date and trading date
                are distinct fields.
              </dd>
            </div>
            <div>
              <dt className="font-medium">Gold</dt>
              <dd>
                NBP's calculated price in PLN for one gram of gold at 1000 millesimal fineness.
              </dd>
            </div>
          </dl>
          <p className="mt-3 text-muted-foreground">
            These values are not summed across dates or currencies. Missing publication days stay
            missing. Historical FX quotation units still need provider evidence; the platform
            preserves the API values unchanged.
          </p>
          <p className="mt-3 flex flex-wrap gap-3">
            <a
              className="underline"
              href="https://api.nbp.pl/en.html"
              target="_blank"
              rel="noopener noreferrer"
            >
              Official NBP definitions
            </a>
            <a
              className="underline"
              href={`${repoDocs}nbp-business-definitions.md`}
              target="_blank"
              rel="noopener noreferrer"
            >
              Full proposal and gold-model mapping
            </a>
          </p>
        </details>
        <fieldset className="space-y-2">
          <legend className="mb-2 text-sm font-medium">Which starting scope would help you?</legend>
          {NBP_REVIEW_CHOICES.map((choice) => (
            <label
              key={choice.id}
              className="flex cursor-pointer items-start gap-2 rounded border p-3 text-sm"
            >
              <input
                type="radio"
                name={`${formId}-nbp-scope`}
                value={choice.id}
                checked={draft.nbpChoice === choice.id}
                onChange={() => update("nbpChoice", choice.id)}
                className="mt-1 accent-current"
              />
              {choice.label}
            </label>
          ))}
        </fieldset>
        <p className="text-xs text-muted-foreground">
          The optional spread is ask − bid for the same Table C currency and publication. It is a
          proposed calculation, not an extra NBP-published value.
        </p>
        <div className="space-y-2">
          <label htmlFor={nbpNotesId} className="text-sm font-medium">
            Your comments or examples
          </label>
          <Textarea
            id={nbpNotesId}
            value={draft.nbpNotes}
            maxLength={8000}
            onChange={(event) => update("nbpNotes", event.target.value)}
            placeholder="For example: I want to compare PLN exchange rates over time. Write freely."
          />
        </div>
      </article>

      <article className="space-y-4 rounded-lg border p-4" aria-labelledby={sourceTitleId}>
        <div>
          <p className="text-xs font-medium text-amber-500">BR-002 · Waiting for your input</p>
          <h2 id={sourceTitleId} className="mt-1 text-lg font-semibold">
            Next data source
          </h2>
        </div>
        <p className="text-sm">
          The two researched candidates are a starting list. You can add a different idea before a
          source is selected.
        </p>
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="rounded border p-3 text-sm">
            <h3 className="font-semibold">Eurostat</h3>
            <p className="mt-1">European economic, social and demographic statistics.</p>
            <a
              className="mt-2 inline-flex items-center gap-1 underline"
              href="https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access"
              target="_blank"
              rel="noopener noreferrer"
            >
              Official access guide <ExternalLink className="h-3 w-3" />
            </a>
          </div>
          <div className="rounded border p-3 text-sm">
            <h3 className="font-semibold">World Bank WDI</h3>
            <p className="mt-1">
              Development indicators for comparisons across countries and years.
            </p>
            <a
              className="mt-2 inline-flex items-center gap-1 underline"
              href="https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators"
              target="_blank"
              rel="noopener noreferrer"
            >
              Official dataset catalogue <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        </div>
        <p className="text-sm text-muted-foreground">
          Both offer public API access. Commercial reuse and attribution must be checked for each
          selected dataset and any third-party material.
        </p>
        <a
          className="inline-block text-sm underline"
          href={`${repoDocs}source-candidates.md`}
          target="_blank"
          rel="noopener noreferrer"
        >
          Read the research, licences and exceptions
        </a>
        <div className="space-y-2">
          <label htmlFor={sourceChoiceId} className="block text-sm font-medium">
            Where would you like to start? (optional)
          </label>
          <select
            id={sourceChoiceId}
            value={draft.sourceChoice}
            onChange={(event) => update("sourceChoice", event.target.value)}
            className="w-full rounded border bg-background p-2 text-sm"
          >
            <option value="">No preference recorded</option>
            {SOURCE_REVIEW_CHOICES.map((choice) => (
              <option key={choice.id} value={choice.id}>
                {choice.label}
              </option>
            ))}
          </select>
        </div>
        <div className="space-y-2">
          <label htmlFor={sourceNotesId} className="text-sm font-medium">
            Your ideas and priorities for the next source
          </label>
          <Textarea
            id={sourceNotesId}
            value={draft.sourceNotes}
            maxLength={8000}
            onChange={(event) => update("sourceNotes", event.target.value)}
            placeholder="A topic, country, business question or source link is enough. No technical specification needed."
          />
        </div>
      </article>

      <section
        className="space-y-3 rounded-lg border bg-muted/30 p-4"
        aria-label="Share your business input"
      >
        <h2 className="font-semibold">Send your input when you are ready</h2>
        <p className="text-sm" role="status">
          {saved === null
            ? "Your answers are saved on this device as you type. Copy or download them to share."
            : saved
              ? "Draft saved on this device. It does not sync to other devices or change the platform."
              : "This browser could not save your draft. Copy or download it before leaving."}
        </p>
        <p className="text-sm text-muted-foreground">
          Copy the response into this chat, or attach the downloaded file. You can answer one item
          and leave the other for later.
        </p>
        <div className="flex flex-wrap gap-2">
          <Button onClick={copyResponse} disabled={answered === 0}>
            <ClipboardCopy className="mr-2 h-4 w-4" />
            Copy response for chat
          </Button>
          <Button variant="outline" onClick={downloadResponse} disabled={answered === 0}>
            <Download className="mr-2 h-4 w-4" />
            Download response
          </Button>
        </div>
        {copyMessage && (
          <p role="status" className="text-sm">
            {copyMessage}
          </p>
        )}
        {showResponse && (
          <Textarea
            aria-label="Response to share in chat"
            readOnly
            value={response}
            className="min-h-64"
          />
        )}
      </section>
    </section>
  );
}

export default function ReviewDecisionsTab() {
  const profileId = useDuckStore((state) => state.currentProfile?.id ?? "default");
  return (
    <div className="h-full overflow-y-auto">
      <ReviewForm key={profileId} storageKey={reviewStorageKey(profileId)} />
    </div>
  );
}
