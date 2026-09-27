import { useState } from "react";
import { ClipboardCopy, Download } from "lucide-react";
import { useDuckStore } from "@/store";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { renderReviewResponse, reviewStorageKey } from "@/lib/businessReview";
import { useBusinessReviewDraft } from "@/lib/businessReviewStore";

const repoDocs = "https://github.com/rutkala/zohelo-data/blob/main/docs/";

function SavedProjectInput({ storageKey }: { storageKey: string }) {
  const { draft, saved } = useBusinessReviewDraft(storageKey);
  const [copyMessage, setCopyMessage] = useState("");
  const response = renderReviewResponse(draft);
  const hasSavedInput = Boolean(
    draft.nbpChoice || draft.nbpNotes.trim() || draft.sourceChoice || draft.sourceNotes.trim()
  );

  const copyResponse = async () => {
    try {
      await navigator.clipboard.writeText(response);
      setCopyMessage("Copied saved project input.");
    } catch {
      setCopyMessage("Copy was unavailable. Select the saved input below or download it.");
    }
  };

  const downloadResponse = () => {
    const url = URL.createObjectURL(new Blob([response], { type: "text/plain;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "zohelo-saved-project-input.txt";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    setCopyMessage("Downloaded saved project input.");
  };

  return (
    <section className="mx-auto max-w-3xl space-y-4 p-4 sm:p-6" aria-label="Saved project input">
      <header className="space-y-2">
        <h1 className="text-2xl font-semibold">Saved project input</h1>
        <p className="text-sm text-muted-foreground">
          This recovery page keeps earlier local input available to copy or download. It is
          read-only and does not submit or change the platform.
        </p>
      </header>

      {saved === false ? (
        <p role="alert" className="rounded border border-amber-500 p-3 text-sm">
          This browser could not read local saved input.
        </p>
      ) : hasSavedInput ? (
        <p className="text-sm text-muted-foreground">Saved proposal version: {draft.version}</p>
      ) : (
        <p className="rounded border p-3 text-sm text-muted-foreground">
          No saved project input was found for this profile.
        </p>
      )}

      <Textarea
        aria-label="Saved project input response"
        readOnly
        value={response}
        className="min-h-64"
      />

      <div className="flex flex-wrap items-center gap-2">
        <Button onClick={copyResponse} disabled={!hasSavedInput}>
          <ClipboardCopy className="mr-2 h-4 w-4" />
          Copy saved input
        </Button>
        <Button variant="outline" onClick={downloadResponse} disabled={!hasSavedInput}>
          <Download className="mr-2 h-4 w-4" />
          Download saved input
        </Button>
        <a
          className="text-sm underline"
          href={`${repoDocs}business-review.md`}
          target="_blank"
          rel="noopener noreferrer"
        >
          Read the business review guide
        </a>
      </div>
      {copyMessage && (
        <p role="status" className="text-sm">
          {copyMessage}
        </p>
      )}
    </section>
  );
}

export default function ReviewDecisionsTab() {
  const profileId = useDuckStore((state) => state.currentProfile?.id ?? "default");
  return (
    <div className="h-full overflow-y-auto">
      <SavedProjectInput key={profileId} storageKey={reviewStorageKey(profileId)} />
    </div>
  );
}
