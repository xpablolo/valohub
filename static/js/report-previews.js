(() => {
  const MAX_PREVIEW_ROWS = 25;
  const GID_PLACEHOLDER = "__GID_PLACEHOLDER__";
  const stateMap = new WeakMap();

  const inferGidFromUrl = (url) => {
    if (!url) return null;
    try {
      const parsed = new URL(url);
      const gid = parsed.searchParams.get("gid");
      if (gid) return gid;
    } catch {
      // ignore invalid URL parsing
    }
    const match = /gid=([0-9]+)/i.exec(url);
    return match ? match[1] : null;
  };

  const deriveTemplates = (csvUrl) => {
    if (!csvUrl) {
      return { csvTemplate: null, imageTemplate: null };
    }
    try {
      const csvTarget = new URL(csvUrl);
      csvTarget.searchParams.set("gid", GID_PLACEHOLDER);
      const csvTemplate = csvTarget.toString().replace(GID_PLACEHOLDER, "{gid}");

      const imageTarget = new URL(csvTarget.toString());
      if (imageTarget.searchParams.has("output")) {
        imageTarget.searchParams.set("output", "png");
      } else {
        imageTarget.searchParams.append("output", "png");
      }
      const imageTemplate = imageTarget.toString().replace(GID_PLACEHOLDER, "{gid}");

      return { csvTemplate, imageTemplate };
    } catch {
      let csvTemplate = csvUrl;
      if (csvTemplate.includes("gid=")) {
        csvTemplate = csvTemplate.replace(/gid=[^&]+/i, "gid={gid}");
      } else {
        const joiner = csvTemplate.includes("?") ? "&" : "?";
        csvTemplate = `${csvTemplate}${joiner}gid={gid}`;
      }

      let imageTemplate = csvTemplate;
      if (imageTemplate.includes("output=")) {
        imageTemplate = imageTemplate.replace(/output=[^&]+/i, "output=png");
      } else {
        const joiner = imageTemplate.includes("?") ? "&" : "?";
        imageTemplate = `${imageTemplate}${joiner}output=png`;
      }

      return { csvTemplate, imageTemplate };
    }
  };

  const getState = (container) => {
    let state = stateMap.get(container);
    if (!state) {
      const { csvTemplate, imageTemplate } = deriveTemplates(container.dataset.csvUrl || "");
      state = {
        tabs: [],
        defaultGid: null,
        activeGid: null,
        csvTemplate,
        imageTemplate,
        dataCache: new Map(),
        imageCache: new Map(),
        metadataLoaded: false,
        loadingTab: null,
      };
      stateMap.set(container, state);
    }
    return state;
  };

  const textToRows = (text) => {
    const rows = [];
    let current = "";
    let row = [];
    let inQuotes = false;

    for (let i = 0; i < text.length; i += 1) {
      const char = text[i];

      if (inQuotes) {
        if (char === '"') {
          const lookahead = text[i + 1];
          if (lookahead === '"') {
            current += '"';
            i += 1;
          } else {
            inQuotes = false;
          }
        } else {
          current += char;
        }
        continue;
      }

      if (char === '"') {
        inQuotes = true;
        continue;
      }

      if (char === ",") {
        row.push(current);
        current = "";
        continue;
      }

      if (char === "\r") {
        continue;
      }

      if (char === "\n") {
        row.push(current);
        rows.push(row);
        row = [];
        current = "";
        continue;
      }

      current += char;
    }

    row.push(current);
    const hasContent = row.some((cell) => cell.trim() !== "");
    if (row.length && hasContent) {
      rows.push(row);
    }

    return rows.map((cells) => cells.map((cell) => cell.trim()));
  };

  const normaliseRows = (rows, width) =>
    rows.map((row) => {
      if (row.length === width) {
        return row;
      }
      const clone = row.slice(0, width);
      while (clone.length < width) {
        clone.push("");
      }
      return clone;
    });

  const extractImageUrl = (value) => {
    if (!value) return null;
    const match = /^=?(?:IMAGE)\((.+)\)$/i.exec(value);
    if (!match) return null;
    const args = match[1];
    const doubleQuote = /"([^"]+)"/.exec(args);
    if (doubleQuote && doubleQuote[1]) {
      return doubleQuote[1];
    }
    const singleQuote = /'([^']+)'/.exec(args);
    return singleQuote && singleQuote[1] ? singleQuote[1] : null;
  };

  const createCellNode = (value) => {
    const trimmed = (value || "").trim();
    if (!trimmed) {
      return document.createTextNode("");
    }

    const imageUrl = extractImageUrl(trimmed);
    if (imageUrl) {
      const img = document.createElement("img");
      img.src = imageUrl;
      img.alt = "Sheet image";
      img.loading = "lazy";
      img.decoding = "async";
      img.referrerPolicy = "no-referrer";
      return img;
    }

    if (/^https?:\/\//i.test(trimmed)) {
      const link = document.createElement("a");
      link.href = trimmed;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = trimmed;
      link.className = "report-preview-cell-link";
      return link;
    }

    return document.createTextNode(trimmed);
  };

  const buildTableData = (rows) => {
    if (!rows.length) {
      return {
        headers: ["Value"],
        rows: [],
        totalRows: 0,
      };
    }
    const rawHeaders = rows[0];
    const headers = rawHeaders.length
      ? rawHeaders.map((cell, index) => cell || `Column ${index + 1}`)
      : ["Value"];

    const dataRows = rows.slice(1).filter((row) => row.some((cell) => (cell || "").trim() !== ""));
    const limitedRows = normaliseRows(dataRows.slice(0, MAX_PREVIEW_ROWS), headers.length);

    return {
      headers,
      rows: limitedRows,
      totalRows: dataRows.length,
    };
  };

  const renderTable = (container, data) => {
    const host = container.querySelector("[data-report-table-body]");
    if (!host) return;

    host.innerHTML = "";

    const table = document.createElement("table");
    table.className = "report-preview-table";

    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    data.headers.forEach((headerLabel, index) => {
      const th = document.createElement("th");
      th.textContent = headerLabel || `Column ${index + 1}`;
      headRow.appendChild(th);
    });
    head.appendChild(headRow);
    table.appendChild(head);

    const tbody = document.createElement("tbody");
    if (!data.rows.length) {
      const singleRow = document.createElement("tr");
      const singleCell = document.createElement("td");
      singleCell.colSpan = Math.max(data.headers.length, 1);
      singleCell.textContent = "No tabular rows found for this sheet.";
      singleCell.style.fontStyle = "italic";
      singleCell.style.color = "#64748b";
      singleRow.appendChild(singleCell);
      tbody.appendChild(singleRow);
    } else {
      data.rows.forEach((row) => {
        const tr = document.createElement("tr");
        row.forEach((cellValue) => {
          const td = document.createElement("td");
          td.appendChild(createCellNode(cellValue));
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }
    table.appendChild(tbody);
    host.appendChild(table);

    const meta = container.querySelector("[data-report-table-meta]");
    if (meta) {
      meta.textContent =
        data.totalRows > data.rows.length
          ? `Showing ${data.rows.length} of ${data.totalRows} rows`
          : `Showing ${data.rows.length} rows`;
    }
  };

  const setLoading = (container, message) => {
    const loadingEl = container.querySelector("[data-report-loading]");
    const errorEl = container.querySelector("[data-report-error]");
    const metaEl = container.querySelector("[data-report-table-meta]");

    if (loadingEl) {
      loadingEl.textContent = message || "Loading preview…";
      loadingEl.classList.remove("hidden");
    }
    if (errorEl) {
      errorEl.classList.add("hidden");
    }
    if (metaEl) {
      metaEl.textContent = "Fetching rows…";
    }
  };

  const setIdle = (container) => {
    const loadingEl = container.querySelector("[data-report-loading]");
    const errorEl = container.querySelector("[data-report-error]");

    if (loadingEl) {
      loadingEl.classList.add("hidden");
    }
    if (errorEl) {
      errorEl.classList.add("hidden");
    }
  };

  const setError = (container, message) => {
    const loadingEl = container.querySelector("[data-report-loading]");
    const errorEl = container.querySelector("[data-report-error]");
    const metaEl = container.querySelector("[data-report-table-meta]");
    const tableHost = container.querySelector("[data-report-table-body]");
    const visual = container.querySelector("[data-report-visual]");

    if (loadingEl) {
      loadingEl.classList.add("hidden");
    }
    if (tableHost) {
      tableHost.innerHTML = "";
    }
    if (errorEl) {
      errorEl.textContent = message || "Unable to load the report preview.";
      errorEl.classList.remove("hidden");
    }
    if (metaEl) {
      metaEl.textContent = "Preview unavailable";
    }
    if (visual) {
      visual.classList.add("hidden");
    }
  };

  const loadImage = (url) =>
    new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(url);
      image.onerror = () => reject(new Error("Image failed to load"));
      image.referrerPolicy = "no-referrer";
      image.src = url;
    });

  const applyImage = (container, url) => {
    const visual = container.querySelector("[data-report-visual]");
    if (!visual) return;

    visual.innerHTML = "";
    if (!url) {
      visual.classList.add("hidden");
      return;
    }

    const img = document.createElement("img");
    img.src = url;
    img.alt = "Sheet snapshot";
    img.loading = "lazy";
    img.decoding = "async";
    img.referrerPolicy = "no-referrer";
    visual.appendChild(img);
    visual.classList.remove("hidden");
  };

  const ensureSheetImage = async (container, gid) => {
    const state = getState(container);
    if (!state.imageTemplate) {
      applyImage(container, null);
      return;
    }

    if (state.imageCache.has(gid)) {
      applyImage(container, state.imageCache.get(gid));
      return;
    }

    const imageUrl = state.imageTemplate.replace("{gid}", gid);
    try {
      await loadImage(imageUrl);
      state.imageCache.set(gid, imageUrl);
      if (state.activeGid === gid) {
        applyImage(container, imageUrl);
      }
    } catch (error) {
      console.warn("Sheet snapshot unavailable", error);
      state.imageCache.set(gid, null);
      if (state.activeGid === gid) {
        applyImage(container, null);
      }
    }
  };

  const ensureMetadata = async (container) => {
    const state = getState(container);
    if (state.metadataLoaded) {
      return state.tabs;
    }

    const fallbackGid = inferGidFromUrl(container.dataset.csvUrl) || "0";
    state.tabs = [];
    state.defaultGid = fallbackGid;

    const metaUrl = container.dataset.metaUrl;
    if (metaUrl) {
      try {
        const response = await fetch(metaUrl, { cache: "no-store" });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(body.error || `HTTP ${response.status}`);
        }
        const receivedTabs = Array.isArray(body.sheets) ? body.sheets : [];
        state.tabs = receivedTabs
          .map((sheet, index) => ({
            gid: sheet.gid || "",
            title: sheet.title || `Sheet ${index + 1}`,
          }))
          .filter((sheet) => sheet.gid);
        if (body.default_gid) {
          state.defaultGid = body.default_gid;
        } else if (state.tabs.length) {
          state.defaultGid = state.tabs[0].gid;
        }
      } catch (error) {
        console.warn("Failed to load sheet metadata", error);
      }
    }

    if (!state.tabs.length) {
      state.tabs = [{ gid: state.defaultGid, title: "Overview" }];
    }

    state.metadataLoaded = true;
    return state.tabs;
  };

  const renderTabs = (container) => {
    const state = getState(container);
    const tabsRoot = container.querySelector("[data-report-tabs]");
    if (!tabsRoot) return;

    tabsRoot.innerHTML = "";

    if (!state.tabs.length || state.tabs.length === 1) {
      tabsRoot.classList.add("hidden");
      return;
    }

    state.tabs.forEach((tab, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "report-preview-tab";
      button.dataset.gid = tab.gid;
      button.textContent = tab.title || `Sheet ${index + 1}`;
      if (tab.gid === state.activeGid) {
        button.classList.add("is-active");
      }
      button.addEventListener("click", () => {
        if (state.activeGid === tab.gid) {
          return;
        }
        setActiveTab(container, tab.gid);
      });
      tabsRoot.appendChild(button);
    });

    tabsRoot.classList.remove("hidden");
  };

  const loadTabContent = async (container, gid) => {
    const state = getState(container);
    const cached = state.dataCache.get(gid);
    if (cached) {
      renderTable(container, cached);
      setIdle(container);
      await ensureSheetImage(container, gid);
      return;
    }

    if (!state.csvTemplate) {
      setError(container, "No CSV export is available for this sheet. Try the full report link.");
      await ensureSheetImage(container, gid);
      return;
    }

    setLoading(container);
    try {
      const csvUrl = state.csvTemplate.replace("{gid}", gid);
      const response = await fetch(csvUrl, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const raw = await response.text();
      const rows = textToRows(raw);
      const tableData = buildTableData(rows);
      state.dataCache.set(gid, tableData);

      if (state.activeGid !== gid) {
        return;
      }

      renderTable(container, tableData);
      setIdle(container);
    } catch (error) {
      console.error("Failed to load CSV preview", error);
      if (state.activeGid === gid) {
        setError(container, "Could not fetch this page. Open the full sheet for the latest data.");
      }
    }

    await ensureSheetImage(container, gid);
  };

  const setActiveTab = async (container, gid) => {
    const state = getState(container);
    if (!gid) return;

    state.activeGid = gid;
    state.loadingTab = gid;
    renderTabs(container);
    await loadTabContent(container, gid);
    if (state.loadingTab === gid) {
      state.loadingTab = null;
    }
  };

  const openPreview = async (container) => {
    const state = getState(container);

    try {
      await ensureMetadata(container);
      const initialGid =
        state.activeGid ||
        state.defaultGid ||
        (state.tabs.length ? state.tabs[0].gid : inferGidFromUrl(container.dataset.csvUrl) || "0");
      await setActiveTab(container, initialGid);
    } catch (error) {
      console.error("Failed to initialise preview", error);
      setError(container, "Unable to load this report preview. Try again later.");
    }
  };

  const toggleContainer = (container, button, shouldShow) => {
    container.classList.toggle("hidden", !shouldShow);
    if (button) {
      button.textContent = shouldShow ? "Hide Table" : "Show Table";
    }
  };

  const initReportPreviews = (options = {}) => {
    const buttonSelector = options.buttonSelector || "[data-toggle-target]";
    const items = document.querySelectorAll("[data-report-item]");

    items.forEach((item) => {
      const button = item.querySelector(buttonSelector);
      if (!button) return;

      const targetId = button.getAttribute("data-toggle-target");
      if (!targetId) return;

      const container = document.getElementById(`sheet-container-${targetId}`);
      if (!container) return;

      button.addEventListener("click", () => {
        const isHidden = container.classList.contains("hidden");
        toggleContainer(container, button, isHidden);
        if (isHidden) {
          openPreview(container);
        }
      });
    });
  };

  window.initReportPreviews = initReportPreviews;
})();
