const LAYERS = ["01_landing", "02_bronze", "03_silver", "04_gold", "05_archive"];
const DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly";
const GOOGLE_CLIENT_ID = "196210210522-9cqf8ie1nddnbpldf5e48eernuiffr9c.apps.googleusercontent.com";
const DRIVE_ROOT = "zohelo-data";

let db;
let conn;
let oauthToken = "";
let activeDatasetName = "";
let currentDatasets = [];
let tokenClient;

const ui = {
  tabs: [...document.querySelectorAll(".tab")],
  panels: {
    sql: document.getElementById("tab-sql"),
    catalog: document.getElementById("tab-catalog")
  },
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
  setStatus(ui.queryMeta, "DuckDB-WASM ready. Load a dataset to query it by name.");
}

function initAuth() {
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
  if (name.includes("'")) {
    throw new Error("Folder names containing single quotes are not supported by this query path.");
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
    throw new Error(`Drive folder '${DRIVE_ROOT}' not found under root.`);
  }
  return findFolderIdByName(layerName, rootId);
}

function renderDatasets(datasets) {
  ui.fileList.innerHTML = "";
  if (!datasets.length) {
    setStatus(ui.fileState, `No dataset folders found in ${ui.layerSelect.value}.`);
    return;
  }

  const fragment = document.createDocumentFragment();
  datasets.forEach((dataset, index) => {
    const li = document.createElement("li");
    const label = document.createElement("label");
    const input = document.createElement("input");
    input.type = "radio";
    input.name = "selectedFile";
    input.id = `file-${index}`;
    input.value = dataset.id;
    input.checked = index === 0;

    const span = document.createElement("span");
    span.textContent = dataset.name;

    label.appendChild(input);
    label.appendChild(span);
    li.appendChild(label);
    fragment.appendChild(li);
  });
  ui.fileList.appendChild(fragment);
  setStatus(ui.fileState, `Found ${datasets.length} dataset(s) in ${ui.layerSelect.value}.`);
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

async function refreshFiles() {
  if (!oauthToken) {
    setStatus(ui.fileState, "Authenticate first to browse Google Drive datasets.");
    return;
  }

  const layer = ui.layerSelect.value;
  setStatus(ui.fileState, `Searching for datasets in ${layer}…`);
  const folderId = await resolveLayerFolderId(layer);
  if (!folderId) {
    currentDatasets = [];
    renderDatasets(currentDatasets);
    return;
  }

  currentDatasets = await listSubfolders(folderId);
  currentDatasets.sort((a, b) => a.name.localeCompare(b.name));
  renderDatasets(currentDatasets);
}

async function loadSelectedDataset() {
  if (!db || !conn) {
    setStatus(ui.fileState, "DuckDB is still initializing. Please retry in a moment.");
    return;
  }

  const selected = document.querySelector("input[name='selectedFile']:checked");
  if (!selected) {
    setStatus(ui.fileState, "Select a dataset first.");
    return;
  }

  const dataset = currentDatasets.find((d) => d.id === selected.value);
  if (!dataset) {
    setStatus(ui.fileState, "Selected dataset no longer exists in the current listing.");
    return;
  }

  setStatus(ui.fileState, `Loading files for dataset '${dataset.name}'…`);

  const files = await listDataFilesInFolder(dataset.id);
  if (!files.length) {
    setStatus(ui.fileState, `No parquet/json files found in dataset '${dataset.name}'.`);
    return;
  }

  const buffers = await Promise.all(
    files.map(async (file) => {
      const response = await driveRequest(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file.id)}?alt=media`);
      const buffer = new Uint8Array(await response.arrayBuffer());
      return { name: file.name, buffer };
    })
  );

  const datasetName = dataset.name;
  for (const { name, buffer } of buffers) {
    await db.registerFileBuffer(`/${datasetName}/${name}`, buffer);
  }

  await conn.query(`CREATE OR REPLACE VIEW ${JSON.stringify(datasetName)} AS SELECT * FROM '/${datasetName}/**'`);

  activeDatasetName = datasetName;
  ui.sqlEditor.value = `SELECT * FROM ${JSON.stringify(datasetName)} LIMIT 50;`;
  setStatus(ui.fileState, `Loaded ${buffers.length} file(s) into view '${datasetName}'.`);
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

  ui.resultsWrapper.innerHTML = "";
  const started = performance.now();
  const result = await conn.query(sql);
  const columns = result.schema.fields.map((field) => field.name);
  const rows = result.toArray().map((row) => normalizeRow(row, columns));
  renderResults(columns, rows);
  const durationMs = Math.round(performance.now() - started);
  setStatus(
    ui.queryMeta,
    `Query complete in ${durationMs} ms. Rows: ${rows.length}.${activeDatasetName ? ` Source: ${activeDatasetName}` : ""}`
  );
}

function initQueryActions() {
  ui.defaultQueryBtn.addEventListener("click", () => {
    ui.sqlEditor.value = activeDatasetName
      ? `SELECT * FROM ${JSON.stringify(activeDatasetName)} LIMIT 50;`
      : "SELECT * FROM active_layer LIMIT 50;";
  });

  ui.refreshFilesBtn.addEventListener("click", () => {
    refreshFiles().catch((error) => {
      setStatus(ui.fileState, `Failed to list files: ${error.message}`);
    });
  });

  ui.loadFileBtn.addEventListener("click", () => {
    loadSelectedDataset().catch((error) => {
      setStatus(ui.fileState, `Failed to load dataset: ${error.message}`);
    });
  });

  ui.runQueryBtn.addEventListener("click", () => {
    runQuery().catch((error) => {
      setStatus(ui.queryMeta, `Query failed: ${error.message}`);
      ui.resultsWrapper.textContent = `Query failed: ${error.message}`;
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
