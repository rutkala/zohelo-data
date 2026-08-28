const LAYERS = ["01_landing", "02_bronze", "03_silver", "04_gold", "05_archive"];
const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly";
const GOOGLE_CLIENT_ID = "196210210522-9cqf8ie1nddnbpldf5e48eernuiffr9c.apps.googleusercontent.com";
const DRIVE_ROOT = "zohelo-data";

let db;
let conn;
let oauthToken = "";
let activeDatasetName = "nbp_exchange_rates_table_a";
let tokenClient;
let editor = null;
let gridApi = null;
const registeredFiles = new Set();

// Initial Lakehouse Tree state representation
const catalogTree = [
  {
    type: "layer",
    name: "01_landing",
    id: null,
    expanded: false,
    loaded: false,
    children: []
  },
  {
    type: "layer",
    name: "02_bronze",
    id: null,
    expanded: true,
    loaded: false,
    children: [
      {
        type: "table",
        name: "nbp_exchange_rates_table_a",
        id: null,
        layer: "02_bronze",
        expanded: true,
        loaded: true,
        children: [
          {
            type: "file",
            name: "data.parquet",
            id: null,
            layer: "02_bronze",
            tableName: "nbp_exchange_rates_table_a"
          }
        ]
      }
    ]
  },
  {
    type: "layer",
    name: "03_silver",
    id: null,
    expanded: false,
    loaded: false,
    children: []
  },
  {
    type: "layer",
    name: "04_gold",
    id: null,
    expanded: false,
    loaded: false,
    children: []
  },
  {
    type: "layer",
    name: "05_archive",
    id: null,
    expanded: false,
    loaded: false,
    children: []
  }
];

const ui = {
  tabs: [...document.querySelectorAll(".portal-tab, .tab")],
  panels: {
    sql: document.getElementById("tab-sql"),
    catalog: document.getElementById("tab-catalog")
  },
  lakehouseTree: document.getElementById("lakehouseTree"),
  refreshFilesBtn: document.getElementById("refreshFilesBtn"),
  fileState: document.getElementById("fileState"),
  authStatus: document.getElementById("authStatus"),
  googleSignInBtn: document.getElementById("googleSignInBtn"),
  tokenPopoverBtn: document.getElementById("tokenPopoverBtn"),
  tokenPopover: document.getElementById("tokenPopover"),
  manualTokenInput: document.getElementById("manualTokenInput"),
  useManualTokenBtn: document.getElementById("useManualTokenBtn"),
  monacoEditor: document.getElementById("monacoEditor"),
  activeLayerBadge: document.getElementById("activeLayerBadge"),
  defaultQueryBtn: document.getElementById("defaultQueryBtn"),
  runQueryBtn: document.getElementById("runQueryBtn"),
  queryMeta: document.getElementById("queryMeta"),
  resultsWrapper: document.getElementById("resultsWrapper"),
  resultsGrid: document.getElementById("resultsGrid"),
  sqlEditor: document.getElementById("sqlEditor")
};

function setStatus(el, text) {
  if (el) {
    el.textContent = text;
  }
}

function updateActiveLayerBadge(name) {
  if (ui.activeLayerBadge) {
    ui.activeLayerBadge.textContent = name;
  }
}

/* ==========================================================================
   1. Tabs Navigation
   ========================================================================== */
function initTabs() {
  ui.tabs.forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.tab;
      ui.tabs.forEach((b) => {
        const isActive = b === btn;
        b.classList.toggle("active", isActive);
        b.setAttribute("aria-selected", String(isActive));
      });
      Object.entries(ui.panels).forEach(([name, panel]) => {
        if (panel) {
          panel.classList.toggle("active", name === tab);
        }
      });
      if (tab === "sql" && editor) {
        setTimeout(() => editor.layout(), 50);
      }
    });
  });
}

/* ==========================================================================
   2. Monaco SQL Editor Initialization
   ========================================================================== */
function initMonaco() {
  return new Promise((resolve) => {
    const setupEditor = () => {
      if (!window.monaco || !ui.monacoEditor) {
        return false;
      }
      editor = window.monaco.editor.create(ui.monacoEditor, {
        value: "SELECT * FROM active_layer LIMIT 50;",
        language: "sql",
        theme: "vs-dark",
        automaticLayout: true,
        fontSize: 13,
        lineNumbers: "on",
        scrollBeyondLastLine: false,
        minimap: { enabled: false },
        padding: { top: 10, bottom: 10 },
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
        renderLineHighlight: "all"
      });

      // Keyboard shortcut (Cmd/Ctrl + Enter) to trigger query execution
      editor.addCommand(window.monaco.KeyMod.CtrlCmd | window.monaco.KeyCode.Enter, () => {
        runQuery();
      });

      resolve(editor);
      return true;
    };

    if (setupEditor()) return;

    if (window.require) {
      window.require.config({
        paths: {
          vs: "https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.45.0/min/vs"
        }
      });
      window.require(["vs/editor/editor.main"], () => {
        setupEditor();
      }, (err) => {
        console.error("Failed to load Monaco editor from CDN:", err);
        resolve(null);
      });
    } else {
      setTimeout(() => {
        if (!setupEditor()) {
          console.warn("Monaco loader unavailable, using textarea fallback if needed.");
          resolve(null);
        }
      }, 500);
    }
  });
}

function getEditorQuery() {
  if (editor) {
    return editor.getValue().trim();
  }
  if (ui.sqlEditor) {
    return ui.sqlEditor.value.trim();
  }
  return "";
}

function setEditorQuery(sql) {
  if (editor) {
    editor.setValue(sql);
  }
  if (ui.sqlEditor) {
    ui.sqlEditor.value = sql;
  }
}

/* ==========================================================================
   3. AG Grid Community Initialization
   ========================================================================== */
function initAGGrid() {
  if (!ui.resultsGrid || !window.agGrid || gridApi) return;

  const gridOptions = {
    columnDefs: [],
    rowData: [],
    defaultColDef: {
      sortable: true,
      resizable: true,
      filter: true,
      minWidth: 110,
      flex: 1
    },
    rowHeight: 30,
    headerHeight: 32,
    suppressCellFocus: true,
    overlayNoRowsTemplate: '<div class="ag-empty-state"><i class="fa-solid fa-table"></i><span>No data to display. Click a table in the Lakehouse Explorer or run a query.</span></div>',
    overlayLoadingTemplate: '<div class="ag-empty-state"><i class="fa-solid fa-spinner fa-spin"></i><span>Executing query in DuckDB-WASM...</span></div>'
  };

  gridApi = window.agGrid.createGrid(ui.resultsGrid, gridOptions);
}

function normalizeRow(row, columns) {
  let plain = {};
  if (row && typeof row.toJSON === "function") {
    plain = row.toJSON();
  } else if (Array.isArray(row)) {
    plain = Object.fromEntries(columns.map((c, idx) => [c, row[idx]]));
  } else if (row && typeof row === "object") {
    plain = { ...row };
  }

  const result = {};
  for (const col of columns) {
    let val = plain[col];
    if (typeof val === "bigint") {
      val = Number(val);
    } else if (val instanceof Date) {
      val = val.toISOString().slice(0, 10);
    }
    result[col] = val == null ? "" : val;
  }
  return result;
}

function renderResults(columns, rows) {
  if (!gridApi) return;

  const columnDefs = columns.map((col) => ({
    field: col,
    headerName: col,
    sortable: true,
    resizable: true,
    filter: true,
    minWidth: 110,
    flex: 1
  }));

  if (typeof gridApi.setGridOption === "function") {
    gridApi.setGridOption("columnDefs", columnDefs);
    gridApi.setGridOption("rowData", rows);
  } else if (typeof gridApi.updateGridOptions === "function") {
    gridApi.updateGridOptions({ columnDefs, rowData: rows });
  } else {
    if (typeof gridApi.setColumnDefs === "function") gridApi.setColumnDefs(columnDefs);
    if (typeof gridApi.setRowData === "function") gridApi.setRowData(rows);
  }
}

/* ==========================================================================
   4. DuckDB-WASM Setup & Default Data
   ========================================================================== */
async function ensureDemoTables() {
  if (!conn) return;
  try {
    await conn.query(`
      CREATE OR REPLACE TABLE demo_bronze_rates (
        table_no VARCHAR,
        effective_date DATE,
        currency VARCHAR,
        currency_code VARCHAR,
        mid_rate DOUBLE
      );
      INSERT INTO demo_bronze_rates VALUES
        ('168/A/NBP/2026', DATE '2026-08-28', 'Euro', 'EUR', 4.3250),
        ('168/A/NBP/2026', DATE '2026-08-28', 'US Dollar', 'USD', 3.9820),
        ('168/A/NBP/2026', DATE '2026-08-28', 'British Pound', 'GBP', 5.1240),
        ('168/A/NBP/2026', DATE '2026-08-28', 'Swiss Franc', 'CHF', 4.5610),
        ('168/A/NBP/2026', DATE '2026-08-28', 'Japanese Yen', 'JPY', 0.0275);

      CREATE OR REPLACE VIEW active_layer AS SELECT * FROM demo_bronze_rates;
      CREATE OR REPLACE VIEW nbp_exchange_rates_table_a AS SELECT * FROM demo_bronze_rates;
    `);
  } catch (err) {
    console.warn("Demo table initial setup note:", err);
  }
}

async function initDuckDB() {
  const duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm");
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  const workerUrl = URL.createObjectURL(new Blob([`importScripts('${bundle.mainWorker}');`], { type: "text/javascript" }));
  const worker = new Worker(workerUrl);
  db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  conn = await db.connect();
  URL.revokeObjectURL(workerUrl);

  await ensureDemoTables();
  setStatus(ui.queryMeta, "DuckDB-WASM ready. Select a table from Lakehouse Explorer or run a query.");
  setStatus(ui.fileState, "Lakehouse catalog ready. Click any table/file to preview.");
}

/* ==========================================================================
   5. Authentication & Google Drive API
   ========================================================================== */
function initAuth() {
  if (ui.googleSignInBtn) {
    ui.googleSignInBtn.addEventListener("click", () => {
      if (!window.google?.accounts?.oauth2) {
        setStatus(ui.authStatus, "Google Identity Services failed to load.");
        return;
      }

      tokenClient = window.google.accounts.oauth2.initTokenClient({
        client_id: GOOGLE_CLIENT_ID,
        scope: DRIVE_SCOPE,
        callback: (response) => {
          if (response?.access_token) {
            oauthToken = response.access_token;
            setAuthSuccess("Connected (Google Drive)");
            refreshCatalog().catch((error) => {
              setStatus(ui.fileState, `Failed to refresh Lakehouse: ${error.message}`);
            });
          }
        }
      });

      tokenClient.requestAccessToken({ prompt: "consent" });
    });
  }

  if (ui.tokenPopoverBtn && ui.tokenPopover) {
    ui.tokenPopoverBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      ui.tokenPopover.classList.toggle("hidden");
    });

    document.addEventListener("click", (e) => {
      if (!ui.tokenPopover.contains(e.target) && e.target !== ui.tokenPopoverBtn) {
        ui.tokenPopover.classList.add("hidden");
      }
    });
  }

  if (ui.useManualTokenBtn) {
    ui.useManualTokenBtn.addEventListener("click", () => {
      const token = ui.manualTokenInput.value.trim();
      if (!token) {
        setStatus(ui.authStatus, "Paste a manual access token first.");
        return;
      }
      oauthToken = token;
      if (ui.tokenPopover) ui.tokenPopover.classList.add("hidden");
      setAuthSuccess("Connected (Manual Token)");
      refreshCatalog().catch((error) => {
        setStatus(ui.fileState, `Failed to refresh Lakehouse: ${error.message}`);
      });
    });
  }
}

function setAuthSuccess(label) {
  if (ui.authStatus) {
    ui.authStatus.classList.add("authenticated");
    ui.authStatus.innerHTML = `<span class="auth-indicator-dot"></span> ${label}`;
  }
}

async function driveRequest(url) {
  if (!oauthToken) {
    throw new Error("No OAuth token available");
  }

  const response = await fetch(url, {
    headers: { Authorization: "Bearer " + oauthToken }
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Drive API error ${response.status}: ${body}`);
  }

  return response;
}

async function findFolderIdByName(name, parentId = "root") {
  if (name.includes("'")) {
    throw new Error("Folder names containing single quotes are not supported.");
  }

  const query = [
    `mimeType='application/vnd.google-apps.folder'`,
    `name='${name}'`,
    `'${parentId}' in parents`,
    "trashed=false"
  ].join(" and ");

  const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(query)}&fields=files(id,name)&pageSize=1`;
  const response = await driveRequest(url);
  const payload = await response.json();
  return payload.files?.[0]?.id || null;
}

async function resolveLayerFolderId(layerName) {
  const rootId = await findFolderIdByName(DRIVE_ROOT, "root");
  if (!rootId) {
    throw new Error(`Drive master folder '${DRIVE_ROOT}' not found under root.`);
  }
  return findFolderIdByName(layerName, rootId);
}

async function listSubfolders(folderId) {
  const folders = [];
  let pageToken = null;
  do {
    const query = `'${folderId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`;
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(query)}&fields=files(id,name),nextPageToken&pageSize=200`;
    if (pageToken) {
      url += `&pageToken=${encodeURIComponent(pageToken)}`;
    }
    const response = await driveRequest(url);
    const payload = await response.json();
    for (const item of payload.files || []) {
      folders.push(item);
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);
  return folders;
}

async function listDataFilesInFolder(folderId) {
  const files = [];
  let pageToken = null;
  do {
    const query = `'${folderId}' in parents and trashed=false`;
    let url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(query)}&fields=files(id,name,mimeType),nextPageToken&pageSize=200`;
    if (pageToken) {
      url += `&pageToken=${encodeURIComponent(pageToken)}`;
    }
    const response = await driveRequest(url);
    const payload = await response.json();
    for (const item of payload.files || []) {
      if (item.mimeType !== "application/vnd.google-apps.folder") {
        const lc = item.name.toLowerCase();
        if (lc.endsWith(".parquet") || lc.endsWith(".json")) {
          files.push(item);
        }
      }
    }
    pageToken = payload.nextPageToken || null;
  } while (pageToken);
  return files;
}

/* ==========================================================================
   6. Hierarchical Lakehouse Explorer Tree View
   ========================================================================== */
function renderTree() {
  if (!ui.lakehouseTree) return;
  ui.lakehouseTree.innerHTML = "";

  catalogTree.forEach((layerNode) => {
    const layerLi = document.createElement("li");
    layerLi.className = "tree-node";

    const layerRow = document.createElement("div");
    layerRow.className = "tree-row level-0";
    layerRow.dataset.type = "layer";
    layerRow.dataset.name = layerNode.name;

    const toggleSpan = document.createElement("span");
    toggleSpan.className = `tree-toggle ${layerNode.expanded ? "expanded" : ""}`;
    toggleSpan.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';

    const icon = document.createElement("i");
    icon.className = "tree-icon layer-icon fa-solid fa-folder";

    const label = document.createElement("span");
    label.className = "tree-label";
    label.textContent = layerNode.name;

    layerRow.appendChild(toggleSpan);
    layerRow.appendChild(icon);
    layerRow.appendChild(label);
    layerLi.appendChild(layerRow);

    // Children list for layer
    if (layerNode.expanded) {
      const childrenUl = document.createElement("ul");
      childrenUl.className = "tree-children";

      if (layerNode.children.length === 0) {
        const emptyLi = document.createElement("li");
        emptyLi.className = "tree-node-loading";
        emptyLi.textContent = layerNode.loaded ? "No datasets found" : "Click to expand & load…";
        childrenUl.appendChild(emptyLi);
      } else {
        layerNode.children.forEach((tableNode) => {
          const tableLi = document.createElement("li");
          tableLi.className = "tree-node";

          const tableRow = document.createElement("div");
          tableRow.className = `tree-row level-1 ${tableNode.name === activeDatasetName ? "active-node" : ""}`;
          tableRow.dataset.type = "table";
          tableRow.dataset.layer = layerNode.name;
          tableRow.dataset.table = tableNode.name;

          const tableToggle = document.createElement("span");
          tableToggle.className = `tree-toggle ${tableNode.expanded ? "expanded" : ""}`;
          tableToggle.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';

          const branch = document.createElement("span");
          branch.className = "tree-branch";
          branch.textContent = "↳";

          const tableIcon = document.createElement("i");
          tableIcon.className = "tree-icon table-icon fa-solid fa-folder";

          const tableLabel = document.createElement("span");
          tableLabel.className = "tree-label";
          tableLabel.textContent = tableNode.name;

          tableRow.appendChild(tableToggle);
          tableRow.appendChild(branch);
          tableRow.appendChild(tableIcon);
          tableRow.appendChild(tableLabel);
          tableLi.appendChild(tableRow);

          // Files inside dataset table
          if (tableNode.expanded && tableNode.children) {
            const filesUl = document.createElement("ul");
            filesUl.className = "tree-children";

            tableNode.children.forEach((fileNode) => {
              const fileLi = document.createElement("li");
              fileLi.className = "tree-node";

              const fileRow = document.createElement("div");
              fileRow.className = "tree-row level-2";
              fileRow.dataset.type = "file";
              fileRow.dataset.layer = layerNode.name;
              fileRow.dataset.table = tableNode.name;
              fileRow.dataset.file = fileNode.name;

              const spacer = document.createElement("span");
              spacer.className = "tree-toggle-spacer";

              const fileBranch = document.createElement("span");
              fileBranch.className = "tree-branch";
              fileBranch.textContent = "↳";

              const fileIcon = document.createElement("i");
              fileIcon.className = "tree-icon file-icon fa-solid fa-file-lines";

              const fileLabel = document.createElement("span");
              fileLabel.className = "tree-label";
              fileLabel.textContent = fileNode.name;

              fileRow.appendChild(spacer);
              fileRow.appendChild(fileBranch);
              fileRow.appendChild(fileIcon);
              fileRow.appendChild(fileLabel);
              fileLi.appendChild(fileRow);
              filesUl.appendChild(fileLi);
            });

            tableLi.appendChild(filesUl);
          }

          childrenUl.appendChild(tableLi);
        });
      }
      layerLi.appendChild(childrenUl);
    }

    ui.lakehouseTree.appendChild(layerLi);
  });
}

function initTreeEvents() {
  if (!ui.lakehouseTree) return;

  ui.lakehouseTree.addEventListener("click", async (e) => {
    const toggle = e.target.closest(".tree-toggle");
    const row = e.target.closest(".tree-row");
    if (!row) return;

    const type = row.dataset.type;

    // Handle toggle click specifically
    if (toggle) {
      e.stopPropagation();
      if (type === "layer") {
        const layer = catalogTree.find((l) => l.name === row.dataset.name);
        if (layer) {
          layer.expanded = !layer.expanded;
          if (layer.expanded && !layer.loaded && oauthToken) {
            await fetchLayerDatasets(layer);
          }
          renderTree();
        }
      } else if (type === "table") {
        const layer = catalogTree.find((l) => l.name === row.dataset.layer);
        const table = layer?.children.find((t) => t.name === row.dataset.table);
        if (table) {
          table.expanded = !table.expanded;
          if (table.expanded && !table.loaded && oauthToken && table.id) {
            await fetchTableFiles(table, layer.name);
          }
          renderTree();
        }
      }
      return;
    }

    // Handle row click (Layer, Table, or File)
    if (type === "layer") {
      const layer = catalogTree.find((l) => l.name === row.dataset.name);
      if (layer) {
        layer.expanded = !layer.expanded;
        if (layer.expanded && !layer.loaded && oauthToken) {
          await fetchLayerDatasets(layer);
        }
        renderTree();
      }
    } else if (type === "table") {
      const layer = catalogTree.find((l) => l.name === row.dataset.layer);
      const table = layer?.children.find((t) => t.name === row.dataset.table);
      if (table) {
        // Expand to show files if not expanded
        table.expanded = true;
        if (!table.loaded && oauthToken && table.id) {
          await fetchTableFiles(table, layer.name);
        }
        renderTree();
        await loadTableAndPreview(table, layer?.name || "02_bronze");
      }
    } else if (type === "file") {
      const layer = catalogTree.find((l) => l.name === row.dataset.layer);
      const table = layer?.children.find((t) => t.name === row.dataset.table);
      const file = table?.children.find((f) => f.name === row.dataset.file);
      if (file && table) {
        renderTree();
        await loadFileAndPreview(file, table.name);
      }
    }
  });
}

async function fetchLayerDatasets(layerNode) {
  if (!oauthToken) return;
  try {
    setStatus(ui.fileState, `Fetching datasets in ${layerNode.name}…`);
    const folderId = layerNode.id || (await resolveLayerFolderId(layerNode.name));
    layerNode.id = folderId;
    if (!folderId) {
      layerNode.children = [];
      layerNode.loaded = true;
      return;
    }
    const folders = await listSubfolders(folderId);
    folders.sort((a, b) => a.name.localeCompare(b.name));
    layerNode.children = folders.map((f) => ({
      type: "table",
      name: f.name,
      id: f.id,
      layer: layerNode.name,
      expanded: false,
      loaded: false,
      children: []
    }));
    layerNode.loaded = true;
    setStatus(ui.fileState, `Loaded ${folders.length} dataset(s) in ${layerNode.name}.`);
  } catch (err) {
    console.error(`Error loading layer ${layerNode.name}:`, err);
    setStatus(ui.fileState, `Failed to load ${layerNode.name}: ${err.message}`);
  }
}

async function fetchTableFiles(tableNode, layerName) {
  if (!oauthToken || !tableNode.id) return;
  try {
    const files = await listDataFilesInFolder(tableNode.id);
    tableNode.children = files.map((f) => ({
      type: "file",
      name: f.name,
      id: f.id,
      layer: layerName,
      tableName: tableNode.name
    }));
    tableNode.loaded = true;
  } catch (err) {
    console.error(`Error loading files for table ${tableNode.name}:`, err);
  }
}

async function refreshCatalog() {
  if (!oauthToken) {
    setStatus(ui.fileState, "Authenticate first to browse Google Drive datasets.");
    return;
  }

  setStatus(ui.fileState, "Refreshing Lakehouse catalog from Google Drive…");
  try {
    const rootId = await findFolderIdByName(DRIVE_ROOT, "root");
    if (!rootId) {
      throw new Error(`Master folder '${DRIVE_ROOT}' not found in Drive root.`);
    }

    for (const layerNode of catalogTree) {
      const layerFolderId = await findFolderIdByName(layerNode.name, rootId);
      layerNode.id = layerFolderId;
      if (layerFolderId && (layerNode.expanded || layerNode.name === "02_bronze")) {
        await fetchLayerDatasets(layerNode);
        for (const tableNode of layerNode.children) {
          if (tableNode.expanded || layerNode.name === "02_bronze") {
            await fetchTableFiles(tableNode, layerNode.name);
          }
        }
      }
    }
    renderTree();
    setStatus(ui.fileState, "Lakehouse catalog synchronized with Google Drive.");
  } catch (err) {
    setStatus(ui.fileState, `Failed to refresh catalog: ${err.message}`);
    console.error("Refresh catalog error:", err);
  }
}

/* ==========================================================================
   7. Dataset & File Loading with Automatic Schema Preview
   ========================================================================== */
async function loadTableAndPreview(tableNode, layerName) {
  if (!conn || !db) {
    setStatus(ui.fileState, "DuckDB-WASM is still initializing. Please retry in a moment.");
    return;
  }

  const datasetName = tableNode.name;
  activeDatasetName = datasetName;
  updateActiveLayerBadge(datasetName);
  setEditorQuery(`SELECT * FROM active_layer LIMIT 50;`);

  setStatus(ui.fileState, `Registering dataset '${datasetName}' in DuckDB-WASM…`);
  setStatus(ui.queryMeta, `Registering '${datasetName}' and inspecting schema…`);

  try {
    if (oauthToken && tableNode.id) {
      const files = tableNode.children?.length ? tableNode.children : await listDataFilesInFolder(tableNode.id);
      if (!files.length) {
        setStatus(ui.fileState, `No parquet/json files found in '${datasetName}'.`);
        return;
      }

      for (const file of files) {
        const filePath = `/${datasetName}/${file.name}`;
        if (!registeredFiles.has(filePath)) {
          const resp = await driveRequest(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file.id)}?alt=media`);
          const buffer = new Uint8Array(await resp.arrayBuffer());
          await db.registerFileBuffer(filePath, buffer);
          registeredFiles.add(filePath);
        }
      }

      await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM '/${datasetName}/**'`);
      await conn.query(`CREATE OR REPLACE VIEW ${JSON.stringify(datasetName)} AS SELECT * FROM '/${datasetName}/**'`);
    } else {
      // Fallback / demo preview
      await ensureDemoTables();
      await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM demo_bronze_rates`);
      await conn.query(`CREATE OR REPLACE VIEW ${JSON.stringify(datasetName)} AS SELECT * FROM demo_bronze_rates`);
    }

    // Run background schema preview: DESCRIBE SELECT * FROM active_layer
    const schemaResult = await conn.query("DESCRIBE SELECT * FROM active_layer");
    const columns = schemaResult.schema.fields.map((f) => f.name);
    const rows = schemaResult.toArray().map((r) => normalizeRow(r, columns));
    renderResults(columns, rows);

    setStatus(
      ui.queryMeta,
      `Schema preview for active_layer (${datasetName}): ${rows.length} columns. Press Ctrl+Enter to execute query.`
    );
    setStatus(ui.fileState, `Active layer set to '${datasetName}'.`);
  } catch (err) {
    setStatus(ui.queryMeta, `Schema preview failed: ${err.message}`);
    setStatus(ui.fileState, `Failed to load '${datasetName}': ${err.message}`);
    console.error("Load table error:", err);
  }
}

async function loadFileAndPreview(fileNode, tableName) {
  if (!conn || !db) {
    setStatus(ui.fileState, "DuckDB-WASM is still initializing. Please retry in a moment.");
    return;
  }

  const fileName = fileNode.name;
  activeDatasetName = tableName;
  updateActiveLayerBadge(`${tableName}/${fileName}`);
  setEditorQuery(`SELECT * FROM active_layer LIMIT 50;`);

  setStatus(ui.fileState, `Registering file '${fileName}' in DuckDB-WASM…`);
  setStatus(ui.queryMeta, `Registering '${fileName}' and inspecting schema…`);

  try {
    const filePath = `/${tableName}/${fileName}`;
    if (oauthToken && fileNode.id) {
      if (!registeredFiles.has(filePath)) {
        const resp = await driveRequest(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileNode.id)}?alt=media`);
        const buffer = new Uint8Array(await resp.arrayBuffer());
        await db.registerFileBuffer(filePath, buffer);
        registeredFiles.add(filePath);
      }
      await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM '${filePath}'`);
      await conn.query(`CREATE OR REPLACE VIEW ${JSON.stringify(tableName)} AS SELECT * FROM '${filePath}'`);
    } else {
      // Fallback / demo preview
      await ensureDemoTables();
      await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM demo_bronze_rates`);
      await conn.query(`CREATE OR REPLACE VIEW ${JSON.stringify(tableName)} AS SELECT * FROM demo_bronze_rates`);
    }

    // Run background schema preview: DESCRIBE SELECT * FROM active_layer
    const schemaResult = await conn.query("DESCRIBE SELECT * FROM active_layer");
    const columns = schemaResult.schema.fields.map((f) => f.name);
    const rows = schemaResult.toArray().map((r) => normalizeRow(r, columns));
    renderResults(columns, rows);

    setStatus(
      ui.queryMeta,
      `Schema preview for ${fileName} (active_layer): ${rows.length} columns. Press Ctrl+Enter to execute query.`
    );
    setStatus(ui.fileState, `Registered '${fileName}' as active_layer.`);
  } catch (err) {
    setStatus(ui.queryMeta, `Schema preview failed: ${err.message}`);
    setStatus(ui.fileState, `Failed to load '${fileName}': ${err.message}`);
    console.error("Load file error:", err);
  }
}

/* ==========================================================================
   8. Query Execution
   ========================================================================== */
async function runQuery() {
  if (!conn) {
    setStatus(ui.queryMeta, "DuckDB-WASM is still initializing. Please wait a moment.");
    return;
  }

  const sql = getEditorQuery();
  if (!sql) {
    setStatus(ui.queryMeta, "Please enter a SQL query to execute.");
    return;
  }

  setStatus(ui.queryMeta, "Executing query in DuckDB-WASM…");
  const started = performance.now();

  try {
    const result = await conn.query(sql);
    const columns = result.schema.fields.map((field) => field.name);
    const rows = result.toArray().map((row) => normalizeRow(row, columns));
    renderResults(columns, rows);

    const durationMs = Math.round(performance.now() - started);
    setStatus(
      ui.queryMeta,
      `Query complete in ${durationMs} ms. Rows: ${rows.length}.${activeDatasetName ? ` Source: ${activeDatasetName}` : ""}`
    );
  } catch (err) {
    setStatus(ui.queryMeta, `Query failed: ${err.message}`);
    console.error("Query execution error:", err);
  }
}

function initQueryActions() {
  if (ui.defaultQueryBtn) {
    ui.defaultQueryBtn.addEventListener("click", () => {
      setEditorQuery("SELECT * FROM active_layer LIMIT 50;");
    });
  }

  if (ui.refreshFilesBtn) {
    ui.refreshFilesBtn.addEventListener("click", () => {
      refreshCatalog().catch((error) => {
        setStatus(ui.fileState, `Failed to refresh Lakehouse: ${error.message}`);
      });
    });
  }

  if (ui.runQueryBtn) {
    ui.runQueryBtn.addEventListener("click", () => {
      runQuery();
    });
  }

  // Global keyboard shortcut for Ctrl/Cmd + Enter
  window.addEventListener("keydown", (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
  });
}

/* ==========================================================================
   9. Application Bootstrap
   ========================================================================== */
async function init() {
  initTabs();
  initAuth();
  initQueryActions();
  renderTree();
  initTreeEvents();

  // Initialize Monaco Editor and AG Grid in parallel
  await Promise.all([
    initMonaco().catch((err) => console.warn("Monaco init issue:", err)),
    Promise.resolve(initAGGrid())
  ]);

  await initDuckDB();
}

init().catch((error) => {
  setStatus(ui.queryMeta, `Portal initialization error: ${error.message}`);
  console.error("Portal initialization error:", error);
});

