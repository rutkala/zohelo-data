/** Owner review proposals. Selections are drafts, never executable approvals. */
export const BUSINESS_REVIEW_VERSION = "2026-09-07.1";
export const NBP_REVIEW_CHOICES = [
  { id: "published", label: "Published daily values first (recommended)" },
  { id: "spread", label: "Daily values and the Table C bid–ask spread" },
  { id: "different", label: "I have a different use case" },
] as const;

export const SOURCE_REVIEW_CHOICES = [
  { id: "eurostat", label: "Eurostat" },
  { id: "wdi", label: "World Bank WDI" },
  { id: "undecided", label: "Another source or still exploring" },
] as const;

export interface BusinessReviewDraft {
  version: string;
  nbpChoice: string;
  nbpNotes: string;
  sourceChoice: string;
  sourceNotes: string;
}

export const emptyReviewDraft = (): BusinessReviewDraft => ({
  version: BUSINESS_REVIEW_VERSION,
  nbpChoice: "",
  nbpNotes: "",
  sourceChoice: "",
  sourceNotes: "",
});

export const reviewStorageKey = (profileId: string) => `zohelo:business-review:${profileId}`;

export function parseReviewDraft(raw: string | null): BusinessReviewDraft {
  const empty = emptyReviewDraft();
  if (!raw) return empty;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return empty;
    const value = parsed as Record<string, unknown>;
    // Preserve the version so an older answer is never shown as approval of a changed proposal.
    return {
      version: typeof value.version === "string" ? value.version.slice(0, 40) : "unknown",
      nbpChoice: NBP_REVIEW_CHOICES.some((choice) => choice.id === value.nbpChoice)
        ? String(value.nbpChoice)
        : "",
      nbpNotes: typeof value.nbpNotes === "string" ? value.nbpNotes.slice(0, 8000) : "",
      sourceChoice: SOURCE_REVIEW_CHOICES.some((choice) => choice.id === value.sourceChoice)
        ? String(value.sourceChoice)
        : "",
      sourceNotes: typeof value.sourceNotes === "string" ? value.sourceNotes.slice(0, 8000) : "",
    };
  } catch {
    return empty;
  }
}

export function renderReviewResponse(draft: BusinessReviewDraft): string {
  const nbp = NBP_REVIEW_CHOICES.find((choice) => choice.id === draft.nbpChoice)?.label;
  const source = SOURCE_REVIEW_CHOICES.find((choice) => choice.id === draft.sourceChoice)?.label;
  return [
    "Zohelo-data — my business input",
    `Proposal version: ${draft.version}`,
    "",
    "BR-001 — First NBP metrics",
    `My preference: ${nbp ?? "Not answered yet"}`,
    `My notes: ${draft.nbpNotes.trim() || "None"}`,
    "",
    "BR-002 — Next data source",
    `My interest: ${source ?? "Not answered yet"}`,
    `My ideas and priorities: ${draft.sourceNotes.trim() || "Not provided yet"}`,
    "",
    "Please use this input to prepare the next agreed scope. These answers have not changed the platform automatically.",
  ].join("\n");
}
