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

  it("annotates BDL bronze models and projects BDL source into overview for bdl_platform release", async () => {
    const bdlBronzeModels = [
      "model.zohelo_data.br_bdl_variables",
      "model.zohelo_data.br_bdl_subjects",
      "model.zohelo_data.br_bdl_units",
      "model.zohelo_data.br_bdl_dictionary_entries",
      "model.zohelo_data.br_bdl_years",
      "model.zohelo_data.br_bdl_observations",
    ];
    const manifest: DbtManifest = {
      metadata: {
        dbt_schema_version: "https://schemas.getdbt.com/dbt/manifest/v12.json",
        invocation_id: "bdl-run",
        project_name: "zohelo_data",
      },
      nodes: Object.fromEntries(
        bdlBronzeModels.map((id) => [
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
    const selected: Extract<ReleaseCatalogResolution, { kind: "release" }> = {
      kind: "release",
      pointer: {
        format_version: 1,
        release_id: "223e4567-e89b-42d3-a456-426614174000",
        manifest_file_id: "bdl-manifest",
        manifest_sha256: "b".repeat(64),
        updated_at_utc: "2026-09-10T00:00:00Z",
      },
      manifestFileId: "bdl-manifest",
      fingerprint: "bdl-release",
      manifest: {
        format_version: 2,
        release_scope: "bdl_platform",
        release_id: "223e4567-e89b-42d3-a456-426614174000",
        status: "validated",
        code_sha: "c".repeat(40),
        created_at_utc: "2026-09-10T00:00:00Z",
        datasets: [],
        artifacts: [
          { id: "art-1", name: "manifest.json", size: 1, sha256: "a".repeat(64) },
          { id: "art-2", name: "catalog.json", size: 1, sha256: "a".repeat(64) },
        ],
        inputs: [],
        tests: { passed: true },
      },
      businessCatalogue: {
        format_version: 1,
        code_sha: "c".repeat(40),
        lineage: { nodes: [], edges: [] },
        metrics: [{ name: "bdl_discovery_coverage_ratio" }],
        sources: [
          {
            source_id: "gus_bdl",
            name: "GUS BDL",
            description: "Statistics Poland Local Data Bank",
            status: "published",
            checked_through: "2026-09-10",
            latest_observation_date: "2026-09-10",
            last_successful_ingestion_at: "2026-09-10T00:00:00Z",
            last_attempt_at: "2026-09-10T00:00:00Z",
            raw_response_count: 10,
          },
        ],
      },
    };

    const prepared = prepareDbtManifest(manifest, selected);
    for (const modelId of bdlBronzeModels) {
      expect(prepared.nodes[modelId].meta).toMatchObject({
        zohelo_source_id: "gus_bdl",
        zohelo_checked_through: "2026-09-10",
        zohelo_ingestion_status: "published",
      });
    }
    const overview = prepared.docs?.["doc.zohelo_data.__overview__"];
    expect(overview?.block_contents).toContain("223e4567-e89b-42d3-a456-426614174000");
    expect(overview?.block_contents).toContain("GUS BDL");
    expect(overview?.block_contents).toContain("1 governed metric definition is published");
  });

  it("merges artifacts and manifests when multiple releases participate", async () => {
    const nbp = await resolution();
    const bdlResolution: Extract<ReleaseCatalogResolution, { kind: "release" }> = {
      kind: "release",
      pointer: {
        format_version: 1,
        release_id: "223e4567-e89b-42d3-a456-426614174000",
        manifest_file_id: "bdl-manifest-id",
        manifest_sha256: "c".repeat(64),
        updated_at_utc: "2026-09-10T00:00:00Z",
      },
      manifestFileId: "bdl-manifest-id",
      fingerprint: "bdl",
      manifest: {
        format_version: 2,
        release_scope: "bdl_platform",
        release_id: "223e4567-e89b-42d3-a456-426614174000",
        status: "validated",
        code_sha: "c".repeat(40),
        created_at_utc: "2026-09-10T00:00:00Z",
        datasets: [],
        artifacts: [
          { id: "bdl-dbt-manifest", name: "manifest.json", size: 10, sha256: "d".repeat(64) },
          { id: "bdl-dbt-catalog", name: "catalog.json", size: 10, sha256: "d".repeat(64) },
        ],
        inputs: [],
        tests: { passed: true },
      },
      businessCatalogue: {
        format_version: 1,
        code_sha: "c".repeat(40),
        lineage: { nodes: [], edges: [] },
        metrics: [{ name: "bdl_discovery_coverage_ratio" }],
        sources: [
          {
            source_id: "gus_bdl",
            name: "GUS BDL",
            description: "BDL",
            status: "published",
            checked_through: "2026-09-10",
            latest_observation_date: "2026-09-10",
            last_successful_ingestion_at: null,
            last_attempt_at: null,
            raw_response_count: 1,
          },
        ],
      },
    };

    const composite: Extract<ReleaseCatalogResolution, { kind: "release" }> = {
      ...nbp,
      releases: [nbp, bdlResolution],
    };

    const bdlManifestData = {
      metadata: { dbt_schema_version: "https://schemas.getdbt.com/dbt/manifest/v12.json" },
      nodes: {
        "model.zohelo_data.br_bdl_variables": {
          unique_id: "model.zohelo_data.br_bdl_variables",
          meta: { layer: "02_bronze" },
        },
      },
      sources: {},
    };
    const bdlCatalogData = {
      metadata: { dbt_schema_version: "https://schemas.getdbt.com/dbt/catalog/v1.json" },
      nodes: {
        "model.zohelo_data.br_bdl_variables": { unique_id: "model.zohelo_data.br_bdl_variables" },
      },
      sources: {},
    };

    const bdlManifestBytes = encode(bdlManifestData);
    const bdlCatalogBytes = encode(bdlCatalogData);
    bdlResolution.manifest.artifacts[0].size = bdlManifestBytes.byteLength;
    bdlResolution.manifest.artifacts[0].sha256 = await sha256Hex(bdlManifestBytes);
    bdlResolution.manifest.artifacts[1].size = bdlCatalogBytes.byteLength;
    bdlResolution.manifest.artifacts[1].sha256 = await sha256Hex(bdlCatalogBytes);

    const storeFiles = new Map([
      ["dbt-manifest", encode(dbtManifest())],
      ["dbt-catalog", encode(dbtCatalog())],
      ["bdl-dbt-manifest", bdlManifestBytes],
      ["bdl-dbt-catalog", bdlCatalogBytes],
    ]);

    const loaded = await loadReleaseDbtArtifacts(
      composite,
      "token",
      createDriveDownloadBudget(),
      async (id) => storeFiles.get(id)!
    );

    // Both NBP and BDL models are present in merged manifest
    expect(loaded.manifest.nodes["model.zohelo_data.br_nbp_table_a"]).toBeDefined();
    expect(loaded.manifest.nodes["model.zohelo_data.br_bdl_variables"]).toBeDefined();

    // Prepare manifest annotates both
    const prepared = prepareDbtManifest(loaded.manifest, composite);
    expect(prepared.nodes["model.zohelo_data.br_nbp_table_a"].meta).toMatchObject({
      zohelo_source_id: "nbp_exchange_rates_table_a",
    });
    expect(prepared.nodes["model.zohelo_data.br_bdl_variables"].meta).toMatchObject({
      zohelo_source_id: "gus_bdl",
    });
    const overview = prepared.docs?.["doc.zohelo_data.__overview__"];
    expect(overview?.block_contents).toContain("nbp exchange rates table a");
    expect(overview?.block_contents).toContain("GUS BDL");
    expect(overview?.block_contents).toContain("1 governed metric definition is published");
  });
});
