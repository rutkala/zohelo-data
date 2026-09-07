import { expect, test, type Page } from "@playwright/test";

const review = (page: Page) => page.locator('section[aria-label="Review and decisions"]:visible');

test("mobile review link retains a draft and exports input without submitting it", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./?view=review");
  const profile = page.getByRole("dialog", { name: "Create Profile" });
  await expect(profile).toBeVisible();
  await profile.getByPlaceholder("Profile name").fill("Business review");
  await profile.getByRole("button", { name: "Create Profile" }).click();
  await expect(
    review(page).getByRole("heading", { name: "Review & decisions", exact: true })
  ).toBeVisible();
  await review(page)
    .getByRole("radio", { name: "Published daily values first (recommended)", exact: true })
    .check();
  const priorities = review(page).getByLabel("Your ideas and priorities for the next source");
  await priorities.fill("I want Polish regional employment data; please also research GUS.");
  await expect(
    review(page).getByText("Draft saved on this device.", { exact: false })
  ).toBeVisible();
  await review(page).getByRole("button", { name: "Copy response for chat" }).click();
  const response = review(page).getByRole("textbox", { name: "Response to share in chat" });
  await expect(response).toHaveValue(/Polish regional employment data/);
  await expect(response).toHaveValue(/not changed the platform automatically/);

  await page.setViewportSize({ width: 1280, height: 844 });
  await expect(
    review(page).getByLabel("Your ideas and priorities for the next source")
  ).toHaveValue("I want Polish regional employment data; please also research GUS.");
  await expect(
    review(page).getByRole("radio", {
      name: "Published daily values first (recommended)",
      exact: true,
    })
  ).toBeChecked();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(
    review(page).getByLabel("Your ideas and priorities for the next source")
  ).toHaveValue("I want Polish regional employment data; please also research GUS.");
  await page.goto("./?view=review");
  await expect(
    review(page).getByRole("heading", { name: "Review & decisions", exact: true })
  ).toBeVisible();
  await expect(
    review(page).getByLabel("Your ideas and priorities for the next source")
  ).toHaveValue("I want Polish regional employment data; please also research GUS.");
  await expect(
    review(page).getByRole("radio", {
      name: "Published daily values first (recommended)",
      exact: true,
    })
  ).toBeChecked();
  await expect(
    review(page).getByRole("link", { name: "Read the research, licences and exceptions" })
  ).toHaveAttribute("href", /docs\/source-candidates.md$/);
  await page.goto("./?view=catalog");
  await expect(
    page.getByRole("heading", { name: "Business catalogue", exact: true })
  ).toBeVisible();
});
