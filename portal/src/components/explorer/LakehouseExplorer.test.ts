import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LakehouseLayer, LandingCatalogResolution } from "@/services/googleDrive";
import LakehouseExplorer from "./LakehouseExplorer";

const state = {
  tabs: [],
  activeTabId: null,
  googleAuth: { token: "token", isAuthenticated: true, authSource: "manual", error: null },
  lakehouseCatalog: [] as LakehouseLayer[],
  lakehouseRelease: null,
  lakehouseLanding: null as LandingCatalogResolution | null,
  nativeLandingRoot: { id: "landing", name: "01_landing" },
  nativeLandingChildren: {
    landing: {
      loaded: true,
      loading: false,
      error: null,
      files: [
        {
          id: "bdl-one",
          name: "gus_bdl",
          parentId: "landing",
          mimeType: "application/vnd.google-apps.folder",
          version: "7",
        },
        {
          id: "bdl-two",
          name: "gus_bdl",
          parentId: "landing",
          mimeType: "application/vnd.google-apps.folder",
          version: "8",
        },
        {
          id: "unknown",
          name: "new_source",
          parentId: "landing",
          mimeType: "application/vnd.google-apps.folder",
        },
      ],
    },
  },
  lakehouseSourceInventory: {
    drive_api_pages: 2,
    entries: [
      {
        source_id: "gus_teryt",
        label: "GUS TERYT",
        state: "retained",
        fetched_at: "2026-09-21T10:00:00Z",
        stages: [
          {
            stage: "Landing",
            basis: "observed_drive_metadata",
            file_count: 3,
            byte_count: 12,
            latest_modified_time: "2026-09-20T10:00:00Z",
          },
        ],
      },
    ],
  },
  isLakehouseLoading: false,
  nativeMetadataTables: {
    "bdl-one": "gus_bdl_files__bdl-one",
    "bdl-two": "gus_bdl_files__bdl-two",
    unknown: "new_source_files",
  },
  nativeMetadataLoading: null,
  nativeMetadataError: null,
  loadNativeMetadataTable: vi.fn(),
  isSourceInventoryLoading: false,
  lakehouseStatusMessage: "",
  activeLakehouseDataset: null,
  activeLakehouseLayer: null,
  signInWithGoogle: vi.fn(),
  setManualGoogleToken: vi.fn(),
  disconnectGoogleDrive: vi.fn(),
  refreshLakehouseCatalog: vi.fn(),
  toggleLakehouseLayer: vi.fn(),
  toggleLakehouseTable: vi.fn(),
  selectLakehouseDataset: vi.fn(),
  selectLakehouseFile: vi.fn(),
  createTab: vi.fn(),
  executeQuery: vi.fn(),
};

vi.mock("@/store", () => ({
  useDuckStore: Object.assign((selector: (value: typeof state) => unknown) => selector(state), {
    getState: () => state,
  }),
}));

beforeEach(() => {
  state.lakehouseCatalog = [
    {
      type: "layer",
      name: "01_landing",
      id: "landing-index",
      expanded: true,
      loaded: true,
      children: [
        {
          type: "table",
          name: "world_bank_wdi_responses",
          id: "index",
          layer: "01_landing",
          expanded: false,
          loaded: true,
          children: [],
        },
      ],
    },
    { type: "layer", name: "02_bronze", id: "bronze", expanded: false, loaded: true, children: [] },
  ];
  state.lakehouseLanding = {
    snapshots: [{ manifest: { kind: "retained_bronze_snapshot", indicators: [] } }],
  } as unknown as LandingCatalogResolution;
});

describe("one Landing entry", () => {
  it("shows every native source as a compact row, including duplicate names and unknown providers", () => {
    const html = renderToStaticMarkup(createElement(LakehouseExplorer));

    expect(html.match(/aria-label="Landing"/g)).toHaveLength(1);
    expect(html).not.toContain("Native Landing files");
    expect(html).not.toContain("Files on Drive");
    expect(html).not.toContain(">01_landing</span>");
    expect(html).toContain('data-native-id="bdl-one"');
    expect(html).toContain('data-native-id="bdl-two"');
    expect(html).toContain('data-native-id="unknown"');
    expect(html).toContain("GUS BDL");
    expect(html).toContain("new_source");
    expect(html).not.toContain("application/vnd.google-apps.folder");
    expect(html).not.toContain("Open in Drive");
    expect(html).not.toContain("Download via Drive");
  });

  it("offers source-folder metadata without exposing legacy response indexes", () => {
    const html = renderToStaticMarkup(createElement(LakehouseExplorer));
    expect(html).toContain("Query file metadata");
    expect(html).toContain("Shortcuts are excluded");
    expect(html).not.toContain("File indexes (SQL)");
    expect(html).not.toContain("SQL indexes");
    expect(html).not.toContain("Actions for world_bank_wdi_responses");
  });

  it("shows source metadata access without a loaded legacy catalog layer", () => {
    state.lakehouseCatalog[0] = {
      ...state.lakehouseCatalog[0],
      loaded: false,
      expanded: false,
      children: [],
    };
    const html = renderToStaticMarkup(createElement(LakehouseExplorer));
    expect(html).toContain("Query file metadata");
    expect(html).not.toContain("File indexes (SQL)");
  });

  it("shows DBW search only inside expanded Bronze, after Landing", () => {
    const collapsedHtml = renderToStaticMarkup(createElement(LakehouseExplorer));
    expect(collapsedHtml).not.toContain("Search DBW indicator");
    expect(collapsedHtml).toMatch(/<button[^>]*aria-label="02_bronze" aria-expanded="false"/);
    state.lakehouseCatalog[1].expanded = true;
    const html = renderToStaticMarkup(createElement(LakehouseExplorer));
    expect(html).toContain("Search DBW indicator name or ID");
    expect(html).toMatch(/<button[^>]*aria-label="02_bronze" aria-expanded="true"/);
    expect(html.indexOf("Search DBW indicator")).toBeGreaterThan(html.indexOf(">02_bronze</span>"));
  });
});
