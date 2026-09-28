import { describe, expect, it } from "vitest";
import { canonicalBronzeName } from "../bronzeNames";

describe("canonical Bronze SQL names", () => {
  it.each([
    ["nbp_exchange_rates_table_a", "nbp", "nbp_exchange_rates_table_a"],
    ["bdl_variables", "gus_bdl", "gus_bdl_variables"],
    ["br_dbw_observations", "gus_dbw", "gus_dbw_observations"],
    ["dbw_indicators", "gus_dbw", "gus_dbw_indicators"],
    ["wdi_data", "world_bank_wdi", "world_bank_wdi_data"],
    ["eurostat_observations", "eurostat", "eurostat_observations"],
    ["br_opendata_organizations", "opendata_org", "opendata_org_organizations"],
    ["rates", "nbp", "nbp_rates"],
  ] as const)("maps %s from %s to %s", (physical, source, canonical) => {
    expect(canonicalBronzeName(physical, source)).toBe(canonical);
    expect(canonicalBronzeName(canonical, source)).toBe(canonical);
  });

  it("rejects a mismatched dbt prefix rather than claiming the wrong source", () => {
    expect(() => canonicalBronzeName("br_wdi_data", "gus_dbw")).toThrow(/does not belong/);
  });
});
