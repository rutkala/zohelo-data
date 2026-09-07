import { describe, expect, it } from "vitest";
import {
  BUSINESS_REVIEW_VERSION,
  emptyReviewDraft,
  parseReviewDraft,
  renderReviewResponse,
  reviewStorageKey,
} from "./businessReview";

describe("business review drafts", () => {
  it("does not invent answers from missing or damaged storage", () => {
    for (const raw of [null, "broken", "[]", "null", "42"]) {
      expect(parseReviewDraft(raw)).toEqual(emptyReviewDraft());
    }
    expect(parseReviewDraft('{"nbpChoice":"approve-everything"}').nbpChoice).toBe("");
  });

  it("preserves draft provenance when the proposal changes", () => {
    const draft = parseReviewDraft(
      JSON.stringify({ ...emptyReviewDraft(), version: "older-version", nbpChoice: "spread" })
    );
    expect(draft.version).not.toBe(BUSINESS_REVIEW_VERSION);
    expect(renderReviewResponse(draft)).toContain("older-version");
  });

  it("exports the owner's words and unanswered items without applying them", () => {
    const draft = { ...emptyReviewDraft(), sourceNotes: "Poland, inflation\nand wages" };
    const text = renderReviewResponse(parseReviewDraft(JSON.stringify(draft)));
    expect(text).toContain("My preference: Not answered yet");
    expect(text).toContain("Poland, inflation\nand wages");
    expect(text).toContain("have not changed the platform automatically");
    expect(reviewStorageKey("one")).not.toBe(reviewStorageKey("two"));
  });
});
