import { describe, expect, it } from "vitest";
import { resolveGoogleClientId } from "../auth";

describe("resolveGoogleClientId", () => {
  it("prefers runtime DUCK_UI_GOOGLE_CLIENT_ID when present", () => {
    expect(
      resolveGoogleClientId(
        { DUCK_UI_GOOGLE_CLIENT_ID: "runtime-client-id" } as Partial<Window["env"]>,
        "build-client-id"
      )
    ).toBe("runtime-client-id");
  });

  it("falls back to build DUCK_UI_GOOGLE_CLIENT_ID when runtime is empty", () => {
    expect(resolveGoogleClientId({ DUCK_UI_GOOGLE_CLIENT_ID: " " } as Partial<Window["env"]>, "build-id"))
      .toBe("build-id");
  });

  it("returns empty when neither runtime nor build client ids are set", () => {
    expect(resolveGoogleClientId(undefined, "")).toBe("");
  });
});
