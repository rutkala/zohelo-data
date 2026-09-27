import { expect, test, type Page } from "@playwright/test";

async function readProfileId(pageName: string, page: Page) {
  return page.evaluate(
    async (profileName) =>
      new Promise<string>((resolve, reject) => {
        const request = indexedDB.open("duck-ui-persistence");
        request.onerror = () => reject(request.error);
        request.onsuccess = () => {
          const db = request.result;
          const transaction = db.transaction("profiles", "readonly");
          const profiles = transaction.objectStore("profiles").getAll();
          profiles.onerror = () => {
            db.close();
            reject(profiles.error);
          };
          profiles.onsuccess = () => {
            const profile = profiles.result.find((item) => item.name === profileName);
            db.close();
            if (!profile?.id) reject(new Error("Created profile was not stored in IndexedDB."));
            else resolve(profile.id);
          };
        };
      }),
    pageName
  );
}

test("legacy review link recovers saved local input as read-only", async ({ page }) => {
  const profileName = "Saved input recovery";
  await page.goto("./");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await profile.getByPlaceholder("Profile name").fill(profileName);
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(profile).toBeHidden();

  const profileId = await readProfileId(profileName, page);
  await page.evaluate(
    ({ storageKey, draft }) => localStorage.setItem(storageKey, JSON.stringify(draft)),
    {
      storageKey: `zohelo:business-review:${profileId}`,
      draft: {
        version: "2026-09-07.1",
        nbpChoice: "published",
        nbpNotes: "Compare PLN exchange rates over time.",
        sourceChoice: "eurostat",
        sourceNotes: "Research Polish regional employment data.",
      },
    }
  );

  await page.goto("./?view=review");
  const recovery = page.locator('section[aria-label="Saved project input"]:visible');
  await expect(
    recovery.getByRole("heading", { name: "Saved project input", exact: true })
  ).toBeVisible();
  const savedInput = recovery.getByRole("textbox", { name: "Saved project input response" });
  await expect(savedInput).toHaveAttribute("readonly", "");
  await expect(savedInput).toHaveValue(/Compare PLN exchange rates over time/);
  await expect(savedInput).toHaveValue(/Research Polish regional employment data/);
  await expect(recovery.getByRole("radio")).toHaveCount(0);
  await expect(recovery.getByRole("button", { name: "Copy saved input" })).toBeEnabled();
  await expect(recovery.getByRole("button", { name: "Download saved input" })).toBeEnabled();
  await expect(
    recovery.getByRole("link", { name: "Read the business review guide" })
  ).toHaveAttribute("href", /docs\/business-review.md$/);
});
