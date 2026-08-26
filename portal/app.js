const LAYERS = ["01_landing", "02_bronze", "03_silver", "04_gold", "05_archive"];
const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly";
const DRIVE_ROOT = "zohelo-data";

let db;
let conn;
let oauthToken = "";
let activeFileName = "";
let currentFiles = [];
let tokenClient;

const ui = {
  tabs: [...document.querySelectorAll(".tab")],
  panels: {
    sql: document.getElementById("tab-sql"),
    catalog: document.getElementById("tab-catalog")
  },
  clientIdInput: document.getElementById("clientIdInput"),
  manualTokenInput: document.getElementById("manualTokenInput"),
  googleSignInBtn: document.getElementById("googleSignInBtn"),
  useManualTokenBtn: document.getElementById("useManualTokenBtn"),
  authStatus: document.getElementById("authStatus"),
  layerSelect: document.getElementById("layerSelect"),
  refreshFilesBtn: document.getElementById("refreshFilesBtn"),
  loadFileBtn: document.getElementById("loadFileBtn"),
  fileList: document.getElementById("fileList"),
  fileState: document.getElementById("fileState"),
  sqlEditor: document.getElementById("sqlEditor"),
  defaultQueryBtn: document.getElementById("defaultQueryBtn"),
  runQueryBtn: document.getElementById("runQueryBtn"),
  queryMeta: document.getElementById("queryMeta"),
  resultsWrapper: document.getElementById("resultsWrapper")
};

function setStatus(el, text) {
  el.textContent = text;
}

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
        panel.classList.toggle("active", name === tab);
      });
    });
  });
}

function escapeSqlLiteral(value) {
  return String(value).replaceAll("'", "''");
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
  setStatus(ui.queryMeta, "DuckDB-WASM ready. Load a layer file to query active_layer.");
}

function initAuth() {
  ui.googleSignInBtn.addEventListener("click", () => {
    const clientId = ui.clientIdInput.value.trim();
    if (!clientId) {
      setStatus(ui.authStatus, "Enter OAuth Client ID before signing in.");
      return;
    }

    if (!window.google?.accounts?.oauth2) {
      setStatus(ui.authStatus, "Google Identity Services failed to load.");
      return;
    }

    tokenClient = window.google.accounts.oauth2.initTokenClient({
      client_id: clientId,
      scope: DRIVE_SCOPE,
      callback: (response) => {
        if (response?.access_token) {
          oauthToken = response.access_token;
          setStatus(ui.authStatus, "Authenticated with Google token flow.");
          refreshFiles().catch((error) => {
            setStatus(ui.fileState, `Failed to refresh files: ${error.message}`);
          });
        }
      }
    });

    tokenClient.requestAccessToken({ prompt: "consent" });
  });

  ui.useManualTokenBtn.addEventListener("click", () => {
    const token = ui.manualTokenInput.value.trim();
    if (!token) {
      setStatus(ui.authStatus, "Paste a manual access token first.");
      return;
    }
    oauthToken = token;
    setStatus(ui.authStatus, "Using manual OAuth access token.");
    refreshFiles().catch((error) => {
      setStatus(ui.fileState, `Failed to refresh files: ${error.message}`);
    });
  });
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
  const query = [
    `mimeType='application/vnd.google-apps.folder'`,
    `name='${name.replaceAll("'", "\\'")}'`,
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
    throw new Error(`Drive folder '${DRIVE_ROOT}' not found under root.`);
  }
  return findFolderIdByName(layerName, rootId);
}

function renderFiles(files) {
  ui.fileList.innerHTML = "";
  if (!files.length) {
    setStatus(ui.fileState, `No files found in ${ui.layerSelect.value}.`);
    return;
  }

  const fragment = document.createDocumentFragment();
  files.forEach((file, index) => {
    const li = document.createElement("li");
    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "radio";
    input.name = "selectedFile";
    input.id = `file-${index}`;
    input.value = file.id;
    input.checked = index === 0;

    const span = document.createElement("span");
    span.textContent = file.name;

    label.appendChild(input);
    label.appendChild(span);
    li.appendChild(label);
    fragment.appendChild(li);
  });
  ui.fileList.appendChild(fragment);
  setStatus(ui.fileState, `Loaded ${files.length} file(s) from ${ui.layerSelect.value}.`);
}

async function refreshFiles() {
  if (!oauthToken) {
    setStatus(ui.fileState, "Authenticate first to browse Google Drive files.");
    return;
  }

  const layer = ui.layerSelect.value;
  const folderId = await resolveLayerFolderId(layer);
  if (!folderId) {
    currentFiles = [];
    renderFiles(currentFiles);
    return;
  }

  const query = `'${folderId}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'`;
  const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(query)}&fields=files(id,name,mimeType,size,modifiedTime)&orderBy=modifiedTime desc&pageSize=200`;
  const response = await driveRequest(url);
  const payload = await response.json();
  currentFiles = payload.files || [];
  renderFiles(currentFiles);
}

async function loadSelectedFileAsActiveLayer() {
  if (!db || !conn) {
    setStatus(ui.fileState, "DuckDB is still initializing. Please retry in a moment.");
    return;
  }

  const selected = document.querySelector("input[name='selectedFile']:checked");
  if (!selected) {
    setStatus(ui.fileState, "Select a file first.");
    return;
  }

  const file = currentFiles.find((f) => f.id === selected.value);
  if (!file) {
    setStatus(ui.fileState, "Selected file no longer exists in the current listing.");
    return;
  }

  const response = await driveRequest(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file.id)}?alt=media`);
  const buffer = new Uint8Array(await response.arrayBuffer());
  const sanitizedName = `${file.id}_${file.name}`.replaceAll("/", "_");

  await db.registerFileBuffer(sanitizedName, buffer);

  const escaped = escapeSqlLiteral(sanitizedName);
  if (file.name.toLowerCase().endsWith(".json")) {
    await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM read_json_auto('${escaped}')`);
  } else if (file.name.toLowerCase().endsWith(".csv")) {
    await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM read_csv_auto('${escaped}')`);
  } else {
    await conn.query(`CREATE OR REPLACE VIEW active_layer AS SELECT * FROM read_parquet('${escaped}')`);
  }

  activeFileName = file.name;
  setStatus(ui.fileState, `Loaded '${file.name}' into view active_layer.`);
}

function normalizeRow(row, columns) {
  if (row && typeof row.toJSON === "function") {
    return row.toJSON();
  }
  if (Array.isArray(row)) {
    return Object.fromEntries(columns.map((c, idx) => [c, row[idx]]));
  }
  return row || {};
}

function renderResults(columns, rows) {
  ui.resultsWrapper.innerHTML = "";
  if (!rows.length) {
    ui.resultsWrapper.textContent = "Query completed with no rows.";
    return;
  }

  const table = document.createElement("table");
  table.className = "results-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column];
      td.textContent = value == null ? "" : String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  ui.resultsWrapper.appendChild(table);
}

async function runQuery() {
  if (!conn) {
    setStatus(ui.queryMeta, "DuckDB is still initializing. Please retry in a moment.");
    return;
  }

  const sql = ui.sqlEditor.value.trim();
  if (!sql) {
    setStatus(ui.queryMeta, "Enter a SQL query to run.");
    return;
  }

  const started = performance.now();
  const result = await conn.query(sql);
  const columns = result.schema.fields.map((field) => field.name);
  const rows = result.toArray().map((row) => normalizeRow(row, columns));
  renderResults(columns, rows);
  const durationMs = Math.round(performance.now() - started);
  setStatus(
    ui.queryMeta,
    `Query complete in ${durationMs} ms. Rows: ${rows.length}.${activeFileName ? ` Source: ${activeFileName}` : ""}`
  );
}

function initQueryActions() {
  ui.defaultQueryBtn.addEventListener("click", () => {
    ui.sqlEditor.value = "SELECT * FROM active_layer LIMIT 50;";
  });

  ui.refreshFilesBtn.addEventListener("click", () => {
    refreshFiles().catch((error) => {
      setStatus(ui.fileState, `Failed to list files: ${error.message}`);
    });
  });

  ui.loadFileBtn.addEventListener("click", () => {
    loadSelectedFileAsActiveLayer().catch((error) => {
      setStatus(ui.fileState, `Failed to load file: ${error.message}`);
    });
  });

  ui.runQueryBtn.addEventListener("click", () => {
    runQuery().catch((error) => {
      setStatus(ui.queryMeta, `Query failed: ${error.message}`);
    });
  });

  ui.layerSelect.addEventListener("change", () => {
    refreshFiles().catch((error) => {
      setStatus(ui.fileState, `Failed to list files: ${error.message}`);
    });
  });
}

async function init() {
  initTabs();
  initAuth();
  initQueryActions();
  await initDuckDB();
  if (!LAYERS.includes(ui.layerSelect.value)) {
    ui.layerSelect.value = "02_bronze";
  }
}

init().catch((error) => {
  setStatus(ui.queryMeta, `Portal initialization failed: ${error.message}`);
});
