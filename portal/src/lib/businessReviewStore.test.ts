import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  getBusinessReviewSnapshot,
  subscribeToBusinessReview,
  updateBusinessReviewDraft,
} from "./businessReviewStore";

class MemoryStorage {
  values = new Map<string, string>();
  failWrites = false;

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    if (this.failWrites) throw new Error("storage full");
    this.values.set(key, value);
  }
}

const storage = new MemoryStorage();
const originalStorage = Object.getOwnPropertyDescriptor(globalThis, "localStorage");

beforeAll(() => {
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage });
});

afterAll(() => {
  if (originalStorage) Object.defineProperty(globalThis, "localStorage", originalStorage);
  else Reflect.deleteProperty(globalThis, "localStorage");
});

describe("shared business review drafts", () => {
  it("notifies every form for one profile without leaking to another profile", () => {
    const one = "review-store-test-one";
    const two = "review-store-test-two";
    let notifications = 0;
    const unsubscribe = subscribeToBusinessReview(one, () => {
      notifications += 1;
    });

    updateBusinessReviewDraft(one, (draft) => ({ ...draft, sourceNotes: "shared draft" }));

    expect(notifications).toBe(1);
    expect(getBusinessReviewSnapshot(one)).toMatchObject({
      draft: { sourceNotes: "shared draft" },
      saved: true,
    });
    expect(getBusinessReviewSnapshot(two).draft.sourceNotes).toBe("");
    unsubscribe();
  });

  it("keeps the draft in memory when local storage rejects a write", () => {
    const key = "review-store-test-storage-failure";
    storage.failWrites = true;

    updateBusinessReviewDraft(key, (draft) => ({ ...draft, nbpNotes: "retain this answer" }));

    expect(getBusinessReviewSnapshot(key)).toMatchObject({
      draft: { nbpNotes: "retain this answer" },
      saved: false,
    });
    storage.failWrites = false;
  });
});
