import { describe, expect, it } from "vitest";
import {
  DBT_DOCUMENTATION_LIMIT_BYTES,
  loadReleaseDbtArtifacts,
  prepareDbtManifest,
  type DbtManifest,
} from "../dbtCatalog";
import { createDriveDownloadBudget, sha256Hex } from "../releaseCatalog";
import type { ReleaseCatalogResolution } from "../types";

const encoder = new TextEncoder();
const bronzeModels = {
  nbp_exchange_rates_table_a: "model.zohelo_data.br_nbp_table_a",
  nbp_exchange_rates_table_b: "model.zohelo_data.br_nbp_table_b",
  nbp_exchange_rates_table_c: "model.zohelo_data.br_nbp_table_c",
  nbp_gold_prices: "model.zohelo_data.br_nbp_gold_prices",
};

const encode = (value: unknown) => encoder.encode(JSON.stringify(value));

function dbtManifest(invocationId = "run-1"): DbtManifest {
  return {
    metadata: {
      dbt_schema_version: "https://schemas.getdbt.com/dbt/manifest/v12.json",
      invocation_id: invocationId,
      project_name: "zohelo_data",
    },
    nodes: Object.fromEntries(
      Object.values(bronzeModels).map((id) => [
        id,
        {
          unique_id: id,
          package_name: "zohelo_data",
          meta: { layer: "02_bronze" },
          config: { meta: { layer: "02_bronze" } },
        },
      ])
    ),
    sources: {},
    docs: {
      "doc.zohelo_data.__overview__": {
        name: "__overview__",
        package_name: "zohelo_data",
        block_contents: "Existing project overview.",
      },
    },
  };
}

function dbtCatalog(invocationId = "run-1") {
  return {
    metadata: {
      dbt_schema_version: "https://schemas.getdbt.com/dbt/catalog/v1.json",
      invocation_id: invocationId,
    },
    nodes: {},
    sources: {},
  };
}

async function resolution(
  manifest: unknown = dbtManifest(),
  catalog: unknown = dbtCatalog()
): Promise<Extract<ReleaseCatalogResolution, { kind: "release" }>> {
  const manifestBytes = encode(manifest);
  const catalogBytes = encode(catalog);
  return {
    kind: "release",
    pointer: {
      format_version: 1,
      release_id: "123e4567-e89b-42d3-a456-426614174000",
      manifest_file_id: "release-manifest",
      manifest_sha256: "a".repeat(64),
      updated_at_utc: "2026-09-07T00:00:00Z",
    },
    manifestFileId: "release-manifest",
    fingerprint: "release",
    manifest: {
      format_version: 2,
      release_scope: "nbp_platform",
      release_id: "123e4567-e89b-42d3-a456-426614174000",
      status: "validated",
      code_sha: "b".repeat(40),
      created_at_utc: "2026-09-07T00:00:00Z",
      datasets: [],
      artifacts: [
        {
          id: "dbt-manifest",
          name: "manifest.json",
          size: manifestBytes.byteLength,
          sha256: await sha256Hex(manifestBytes),
        },
        {
          id: "dbt-catalog",
          name: "catalog.json",
          size: catalogBytes.byteLength,
          sha256: await sha256Hex(catalogBytes),
        },
      ],
      inputs: [],
      tests: { passed: true },
    },
    businessCatalogue: {
      format_version: 1,
      code_sha: "b".repeat(40),
      lineage: { nodes: [], edges: [] },
      metrics: [],
      sources: Object.keys(bronzeModels).map((source_id) => ({
        source_id,
        name: source_id.replace(/_/g, " "),
        description: "Released NBP source.",
        status: "published_snapshot",
        checked_through: "2026-09-06",
        latest_observation_date: "2026-09-05",
        last_successful_ingestion_at: "2026-09-07T00:00:00Z",
        last_attempt_at: "2026-09-07T00:00:00Z",
        raw_response_count: 3,
      })),
    },
  };
}

describe("release dbt artifacts", () => {
  it("loads only exact, hash-verified artifacts from the selected release", async () => {
    const selected = await resolution();
    const expected = new Map([
      ["dbt-manifest", encode(dbtManifest())],
      ["dbt-catalog", encode(dbtCatalog())],
    ]);
    const loaded = await loadReleaseDbtArtifacts(
      selected,
      "token",
      createDriveDownloadBudget(),
      async (id, token, maxBytes) => {
        expect(token).toBe("token");
        const bytes = expected.get(id)!;
        expect(maxBytes).toBe(bytes.byteLength);
        return bytes;
      }
    );
    expect(loaded.manifest.metadata.project_name).toBe("zohelo_data");
    expect(loaded.catalog.metadata.invocation_id).toBe("run-1");
  });

  it("accepts independently verified artifacts from different dbt invocations", async () => {
    const selected = await resolution(dbtManifest("build-run"), dbtCatalog("docs-run"));
    const files = new Map([
      ["dbt-manifest", encode(dbtManifest("build-run"))],
      ["dbt-catalog", encode(dbtCatalog("docs-run"))],
    ]);
    await expect(
      loadReleaseDbtArtifacts(selected, "token", createDriveDownloadBudget(), async (id) =>
        files.get(id)!
      )
    ).resolves.toMatchObject({
      manifest: { metadata: { invocation_id: "build-run" } },
      catalog: { metadata: { invocation_id: "docs-run" } },
    });
  });

  it("rejects documentation artifacts that exceed the 16 MiB browser budget", async () => {
    const selected = await resolution();
    selected.manifest.artifacts[0].size = DBT_DOCUMENTATION_LIMIT_BYTES;
    selected.manifest.artifacts[1].size = 1;
    await expect(loadReleaseDbtArtifacts(selected, "token")).rejects.toThrow(
      "16 MiB browser documentation limit"
    );
  });

  it("adds release state only to the existing bronze branches and native overview", async () => {
    const selected = await resolution();
    const original = dbtManifest();
    selected.businessCatalogue!.sources[0].provider_metadata = {
      documentation_url: "https://api.nbp.pl/en.html",
      frequency: "each working day",
      reuse_summary: "Commercial redistribution terms unresolved",
    };
    const prepared = prepareDbtManifest(original, selected);
    expect(prepared.nodes[bronzeModels.nbp_exchange_rates_table_a].meta).toMatchObject({
      documentation_url: "https://api.nbp.pl/en.html",
      frequency: "each working day",
      reuse_summary: "Commercial redistribution terms unresolved",
    });

    expect(original.nodes[bronzeModels.nbp_exchange_rates_table_a].config).toEqual({
      meta: { layer: "02_bronze" },
    });
    for (const [sourceId, modelId] of Object.entries(bronzeModels)) {
      expect(prepared.nodes[modelId].meta).toMatchObject({
        layer: "02_bronze",
        zohelo_source_id: sourceId,
        zohelo_checked_through: "2026-09-06",
      });
      expect(prepared.nodes[modelId].config).toMatchObject({
        meta: { zohelo_source_id: sourceId, zohelo_checked_through: "2026-09-06" },
      });
    }
    const overview = prepared.docs?.["doc.zohelo_data.__overview__"];
    expect(overview?.block_contents).toContain("Existing project overview.");
    expect(overview?.block_contents).toContain("123e4567-e89b-42d3-a456-426614174000");
    expect(overview?.block_contents).toContain(
      `#!/model/${encodeURIComponent(bronzeModels.nbp_gold_prices)}`
    );
    expect(overview?.block_contents).toContain("No approved metric definitions");
    expect(Object.keys(prepared.nodes)).toEqual(Object.keys(original.nodes));
  });

  it("does not claim metrics are absent when the release publishes definitions", async () => {
    const selected = await resolution();
    selected.businessCatalogue!.metrics = [{ name: "approved_metric" }];
    const overview = prepareDbtManifest(dbtManifest(), selected).docs?.[
      "doc.zohelo_data.__overview__"
    ];
    expect(overview?.block_contents).toContain("1 governed metric definition is published");
    expect(overview?.block_contents).not.toContain("No approved metric definitions");
  });

  it("does not project an incomplete source state into native docs", async () => {
    const selected = await resolution();
    selected.businessCatalogue!.sources.pop();
    const original = dbtManifest();
    expect(prepareDbtManifest(original, selected)).toEqual(original);
  });
});
