(() => {
  const stateMap = new WeakMap();

  const AGENT_IMAGE_MAP = {
    Astra: "astra.png",
    Breach: "breach.png",
    Brimstone: "brimstone.png",
    Chamber: "chamber.png",
    Cypher: "cypher.png",
    Deadlock: "deadlock.png",
    Fade: "fade.png",
    Gekko: "geeko.png",
    Harbor: "tejo.png",
    Iso: "iso.png",
    Jett: "jett.png",
    "KAY/O": "kayo.png",
    Killjoy: "kj.png",
    Neon: "neon.png",
    Omen: "omen.png",
    Raze: "raze.png",
    Sage: "sage.png",
    Skye: "skye.png",
    Sova: "sova.png",
    Viper: "viper.png",
    Vyse: "vyse.png",
    Yoru: "yoru.png",
    Waylay: "waylay.png",
  };

  const AGENT_ALIASES = {
    kj: "Killjoy",
    killjoy: "Killjoy",
    "kay/o": "KAY/O",
    kayo: "KAY/O",
    "kay-o": "KAY/O",
    "kay/o": "KAY/O",
    harbour: "Harbor",
    tejo: "Harbor",
    gekko: "Gekko",
    geeko: "Gekko",
    vyse: "Vyse",
  };

  const parseISODate = (value) => {
    if (!value) return null;
    const raw = String(value).trim();
    if (!raw) return null;
    let parsed = new Date(raw);
    if (!Number.isNaN(parsed.getTime())) return parsed;
    const normalised = raw.replace(/\.\d+/, (match) => match.slice(0, 4));
    parsed = new Date(normalised);
    if (!Number.isNaN(parsed.getTime())) return parsed;
    return null;
  };

  const DATE_FORMATTER = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });

  const DATE_TIME_FORMATTER = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });

  const formatDate = (isoString) => {
    const dt = parseISODate(isoString);
    if (!dt) return "—";
    return DATE_FORMATTER.format(dt);
  };

  const formatDateTime = (isoString) => {
    const dt = parseISODate(isoString);
    if (!dt) return "—";
    return DATE_TIME_FORMATTER.format(dt);
  };

  const normaliseAgentName = (name) => {
    if (!name) return null;
    const raw = String(name).trim();
    if (!raw) return null;
    const lower = raw.toLowerCase();
    if (AGENT_ALIASES[lower]) return AGENT_ALIASES[lower];
    const capitalised = raw[0].toUpperCase() + raw.slice(1).toLowerCase();
    if (AGENT_IMAGE_MAP[capitalised]) return capitalised;
    return raw;
  };

  const createAgentChips = (agents, playerAgentsMap) => {
    const stack = document.createElement("div");
    stack.className = "report-agent-stack";

    const agentPlayers = new Map();
    if (playerAgentsMap && typeof playerAgentsMap === "object") {
      Object.entries(playerAgentsMap).forEach(([playerName, agentName]) => {
        const agent = normaliseAgentName(agentName);
        if (!agent) return;
        const players = agentPlayers.get(agent) || [];
        players.push(playerName);
        agentPlayers.set(agent, players);
      });
    }

    agents.forEach((entry) => {
      const agent = normaliseAgentName(entry);
      if (!agent) return;
      const chip = document.createElement("div");
      chip.className = "report-agent-chip";
      const head = document.createElement("div");
      head.className = "report-agent-chip-head";
      const filename = AGENT_IMAGE_MAP[agent];
      if (filename) {
        const img = document.createElement("img");
        img.src = `/static/images/${filename}`;
        img.alt = agent;
        img.loading = "lazy";
        img.referrerPolicy = "no-referrer";
        head.appendChild(img);
      }
      const label = document.createElement("span");
      label.textContent = agent;
      head.appendChild(label);
      chip.appendChild(head);
      const players = agentPlayers.get(agent);
      if (players && players.length) {
        const playersLabel = document.createElement("small");
        playersLabel.className = "report-agent-player";
        playersLabel.textContent = players.join(", ");
        chip.appendChild(playersLabel);
      }
      stack.appendChild(chip);
    });
    return stack;
  };

  const createRateNode = (rate) => {
    const span = document.createElement("span");
    span.className = "report-rate";
    if (!rate || rate.value === null || rate.value === undefined || !rate.total) {
      span.classList.add("is-neutral");
      span.textContent = "—";
      return span;
    }
    span.textContent = `${rate.value}% (${rate.won}/${rate.total})`;
    if (rate.value >= 55) {
      span.classList.add("is-positive");
    } else if (rate.value <= 45) {
      span.classList.add("is-negative");
    } else {
      span.classList.add("is-neutral");
    }
    return span;
  };

  const createCountShareNode = (entry) => {
    const span = document.createElement("span");
    span.className = "report-count";
    if (!entry) {
      span.textContent = "—";
      return span;
    }
    const count = entry.count ?? 0;
    const share = entry.share_pct;
    span.textContent = share === null || share === undefined ? `${count}` : `${count} (${share}%)`;
    return span;
  };

  const createTable = (columns, rows, options = {}) => {
    const wrapper = document.createElement("div");
    wrapper.className = "report-scroll";
    const table = document.createElement("table");
    table.className = "report-table";
    if (options.tableClass) {
      table.classList.add(options.tableClass);
    }
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    columns.forEach((col) => {
      const th = document.createElement("th");
      th.textContent = col;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
   table.appendChild(thead);
    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      let cells = row;
      let className = "";
      let dataset = null;
      if (row && typeof row === "object" && !Array.isArray(row)) {
        cells = row.cells || [];
        className = row.className || "";
        dataset = row.dataset || null;
      }
      const tr = document.createElement("tr");
      if (className) {
        tr.className = className;
      }
      if (dataset) {
        Object.entries(dataset).forEach(([key, value]) => {
          if (value !== undefined && value !== null) {
            tr.dataset[key] = value;
          }
        });
      }
      cells.forEach((value) => {
        const td = document.createElement("td");
        if (value instanceof Node) {
          td.appendChild(value);
        } else {
          td.textContent = value;
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    wrapper.appendChild(table);
    return wrapper;
  };

  let lightboxRoot = null;

  const closeLightbox = () => {
    if (!lightboxRoot) return;
    lightboxRoot.classList.remove("is-open");
  };

  const ensureLightbox = () => {
    if (lightboxRoot) return lightboxRoot;
    const overlay = document.createElement("div");
    overlay.className = "report-lightbox";
    overlay.setAttribute("role", "dialog");
    overlay.setAttribute("aria-modal", "true");
    overlay.innerHTML = `
      <div class="report-lightbox-backdrop" data-lightbox-dismiss></div>
      <div class="report-lightbox-content">
        <button type="button" class="report-lightbox-close" data-lightbox-dismiss aria-label="Close image preview">
          <span aria-hidden="true">&times;</span>
        </button>
        <img class="report-lightbox-image" alt="" />
        <p class="report-lightbox-caption"></p>
      </div>
    `;
    overlay.querySelectorAll("[data-lightbox-dismiss]").forEach((el) => {
      el.addEventListener("click", () => closeLightbox());
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeLightbox();
      }
    });
    document.body.appendChild(overlay);
    lightboxRoot = overlay;
    return overlay;
  };

  const openLightbox = (url, captionText) => {
    const overlay = ensureLightbox();
    const imageEl = overlay.querySelector(".report-lightbox-image");
    const captionEl = overlay.querySelector(".report-lightbox-caption");
    imageEl.src = url;
    imageEl.alt = captionText || "Report visual";
    captionEl.textContent = captionText || "";
    requestAnimationFrame(() => {
      overlay.classList.add("is-open");
    });
  };

  const getState = (container) => {
    let state = stateMap.get(container);
    if (!state) {
      const raw = container.dataset.reportPayload;
      let payload = null;
      if (raw) {
        try {
          payload = JSON.parse(raw);
        } catch (error) {
          console.warn("Failed to parse report payload", error);
        }
      }
      const initialMap =
        payload && Array.isArray(payload.maps) && payload.maps.length ? payload.maps[0].name : null;
      state = {
        payload,
        activeMap: initialMap,
        rendered: false,
      };
      stateMap.set(container, state);
    }
    return state;
  };

  const clearElement = (el) => {
    if (!el) return;
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  };

  const applyTrendClass = (card, rate) => {
    if (!rate || rate.value === null || rate.value === undefined) {
      card.classList.add("is-neutral");
      return;
    }
    if (rate.value >= 55) {
      card.classList.add("is-positive");
    } else if (rate.value <= 45) {
      card.classList.add("is-negative");
    } else {
      card.classList.add("is-neutral");
    }
  };

  const renderOverview = (container, state) => {
    const root = container.querySelector("[data-report-overview]");
    if (!root) return;
    clearElement(root);

    const { payload } = state;
    if (!payload) {
      const empty = document.createElement("div");
      empty.className = "report-detail-loading";
      empty.textContent = "Preview unavailable for this report.";
      root.appendChild(empty);
      return;
    }

    const card = document.createElement("div");
    card.className = "report-overview-card";

    const brand = document.createElement("div");
    brand.className = "report-overview-brand";

    if (payload.team && payload.team.image_url) {
      const logo = document.createElement("img");
      logo.className = "report-overview-logo";
      logo.src = payload.team.image_url;
      logo.alt = `${payload.team.name} crest`;
      logo.loading = "lazy";
      logo.referrerPolicy = "no-referrer";
      brand.appendChild(logo);
    }

    const titles = document.createElement("div");
    titles.className = "report-overview-titles";

    const title = document.createElement("h3");
    title.className = "text-xl font-semibold text-gray-900 dark-mode:text-gray-100";
    const teamName = payload.team ? payload.team.name : container.dataset.teamName || "Unknown team";
    const tag = payload.team && payload.team.tag ? payload.team.tag : container.dataset.teamTag;
    title.textContent = tag ? `${teamName} • ${tag}` : teamName;
    titles.appendChild(title);

    const meta = document.createElement("div");
    meta.className = "report-overview-meta";
    if (payload.summary && payload.summary.match_count) {
      const item = document.createElement("span");
      item.textContent = `${payload.summary.match_count} matches processed`;
      meta.appendChild(item);
    }
    if (payload.summary && payload.summary.map_count) {
      const item = document.createElement("span");
      item.textContent = `${payload.summary.map_count} maps analysed`;
      meta.appendChild(item);
    }
    titles.appendChild(meta);
    brand.appendChild(titles);

    card.appendChild(brand);

    const statsGroup = document.createElement("div");
    statsGroup.className = "report-stat-group";

    const record = (payload.summary && payload.summary.record) || { wins: 0, losses: 0 };
    const wins = Number(record.wins ?? 0);
    const losses = Number(record.losses ?? 0);
    const totalMatches = wins + losses;
    const winrate = totalMatches > 0 ? Math.round((wins / totalMatches) * 100) : null;

    const statItems = [
      {
        label: "Record",
        primary: `${wins}-${losses}`,
        detail: payload.summary && payload.summary.match_count ? `${payload.summary.match_count} matches` : "",
      },
      {
        label: "Maps reviewed",
        primary: payload.summary && payload.summary.map_count ? String(payload.summary.map_count) : "—",
        detail: "Included in this report",
      },
      {
        label: "Winrate",
        primary: winrate !== null ? `${winrate}%` : "—",
        detail: "Across all processed matches",
        winrate,
      },
    ];

    statItems.forEach((stat) => {
      const statCard = document.createElement("div");
      statCard.className = "report-stat-card";

      const label = document.createElement("span");
      label.textContent = stat.label;
      statCard.appendChild(label);

      const primary = document.createElement("strong");
      primary.textContent = stat.primary;
      statCard.appendChild(primary);

      if (stat.detail) {
        const detail = document.createElement("small");
        detail.textContent = stat.detail;
        statCard.appendChild(detail);
      }

      if (stat.winrate !== undefined) {
        applyTrendClass(statCard, { value: stat.winrate });
      }

      statsGroup.appendChild(statCard);
    });

    card.appendChild(statsGroup);

    const matchesSection = document.createElement("div");
    matchesSection.className = "report-matches-section";
    const matchesHeading = document.createElement("h5");
    matchesHeading.className = "report-subtitle";
    matchesHeading.textContent = "All maps played";
    matchesSection.appendChild(matchesHeading);

    if (Array.isArray(payload.matches) && payload.matches.length) {
      const wrapper = document.createElement("div");
      wrapper.className = "report-scroll";
      const table = document.createElement("table");
      table.className = "report-matches-table";

      const thead = document.createElement("thead");
      const headRow = document.createElement("tr");
      ["Map", "Opponent", "Result", "Score", "DEF", "ATK", "Started"].forEach((label) => {
        const th = document.createElement("th");
        th.textContent = label;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      payload.matches.forEach((match) => {
        const tr = document.createElement("tr");
        const resultLabel = (match.result || "").toLowerCase();
        if (resultLabel.includes("win")) {
          tr.classList.add("is-win");
          tr.dataset.result = "win";
        } else if (resultLabel.includes("loss") || resultLabel.includes("lose")) {
          tr.classList.add("is-loss");
          tr.dataset.result = "loss";
        } else if (resultLabel) {
          tr.dataset.result = resultLabel;
        }

        const mapTd = document.createElement("td");
        mapTd.textContent = match.map || "—";
        tr.appendChild(mapTd);

        const opponentTd = document.createElement("td");
        opponentTd.textContent = match.opponent || "—";
        tr.appendChild(opponentTd);

        const resultTd = document.createElement("td");
        resultTd.textContent = match.result || "—";
        tr.appendChild(resultTd);

        const scoreTd = document.createElement("td");
        scoreTd.textContent = match.score ? `${match.score.team}-${match.score.opponent}` : "—";
        tr.appendChild(scoreTd);

        const defenceTd = document.createElement("td");
        defenceTd.appendChild(createRateNode(match.defence));
        tr.appendChild(defenceTd);

        const attackTd = document.createElement("td");
        attackTd.appendChild(createRateNode(match.attack));
        tr.appendChild(attackTd);

        const startedTd = document.createElement("td");
        startedTd.textContent = formatDate(match.started_at);
        tr.appendChild(startedTd);

        tbody.appendChild(tr);
      });

      table.appendChild(tbody);
      wrapper.appendChild(table);
      matchesSection.appendChild(wrapper);
    } else {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No matches available.";
      matchesSection.appendChild(placeholder);
    }

    card.appendChild(matchesSection);

    const performanceHost = document.createElement("div");
    performanceHost.className = "report-performance-section";
    performanceHost.setAttribute("data-report-performance", "");
    card.appendChild(performanceHost);
    renderPerformanceOverview(container, state);

    if (container.dataset.openUrl) {
      const links = document.createElement("div");
      links.className = "report-overview-links";
      const anchor = document.createElement("a");
      anchor.href = container.dataset.openUrl;
      anchor.target = "_blank";
      anchor.rel = "noopener";
      anchor.textContent = "Open full Google Sheet →";
      links.appendChild(anchor);
      card.appendChild(links);
    }

    root.appendChild(card);
  };

  const renderCompositions = (map) => {
    const compositions = Array.isArray(map.compositions) ? map.compositions : [];
    const wrapper = document.createElement("div");
    const heading = document.createElement("h5");
    heading.className = "report-subtitle";
    heading.textContent = "Agent compositions";
    wrapper.appendChild(heading);

    if (!compositions.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No composition data available for this map.";
      wrapper.appendChild(placeholder);
      return wrapper;
    }

    const rows = compositions.map((entry) => {
      const played = typeof entry.played === "number" ? entry.played : "—";
      const winrate = entry.winrate !== null && entry.winrate !== undefined ? `${entry.winrate}%` : "—";
      const record = `${entry.wins ?? "—"}-${entry.losses ?? "—"}`;
      const chips = createAgentChips(
        Array.isArray(entry.agents) ? entry.agents : [],
        entry.player_agents,
      );
      return [played, winrate, record, chips];
    });

    wrapper.appendChild(createTable(["Played", "Winrate", "Record", "Agents"], rows));
    return wrapper;
  };

  const renderVisuals = (map) => {
    const section = document.createElement("div");
    const heading = document.createElement("h5");
    heading.className = "report-subtitle";
    heading.textContent = "Visual breakdown";
    section.appendChild(heading);

    const visuals = map.visuals || {};
    const cards = [];

    const pushPositionCards = (labelPrefix, entries) => {
      if (!Array.isArray(entries)) return;
      entries.forEach((entry) => {
        if (!entry || !entry.image_url) return;
        cards.push({
          label: `${labelPrefix} • ${entry.seconds}s`,
          url: entry.image_url,
        });
      });
    };

    pushPositionCards("Defence", visuals.def_positions);
    pushPositionCards("Attack", visuals.atk_positions);

    if (visuals.sniper && visuals.sniper.defence) {
      cards.push({ label: "Defence sniper heatmap", url: visuals.sniper.defence });
    }
    if (visuals.sniper && visuals.sniper.attack) {
      cards.push({ label: "Attack sniper heatmap", url: visuals.sniper.attack });
    }

    if (!cards.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No visual assets generated for this map.";
      section.appendChild(placeholder);
      return section;
    }

    const grid = document.createElement("div");
    grid.className = "report-visual-grid";
    cards.forEach((card) => {
      const figure = document.createElement("div");
      figure.className = "report-visual-card";
      const label = document.createElement("span");
      label.textContent = card.label;
      figure.appendChild(label);
      const trigger = document.createElement("button");
      trigger.type = "button";
      trigger.className = "report-visual-trigger";
      trigger.setAttribute("aria-label", `Expand ${card.label}`);
      const img = document.createElement("img");
      img.src = card.url;
      img.alt = card.label;
      img.loading = "lazy";
      img.referrerPolicy = "no-referrer";
      trigger.appendChild(img);
      trigger.addEventListener("click", () => openLightbox(card.url, card.label));
      figure.appendChild(trigger);
      grid.appendChild(figure);
    });
    section.appendChild(grid);
    return section;
  };

  const buildMapPerformanceSection = (container, state) => {
    const maps = (state.payload && state.payload.maps) || [];
    const section = document.createElement("div");
    section.className = "report-map-performance";

    if (!maps.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No map data available.";
      section.appendChild(placeholder);
      return section;
    }

    const heading = document.createElement("div");
    heading.className = "report-map-performance-head";
    const title = document.createElement("h4");
    title.className = "report-subtitle text-gray-700 dark-mode:text-gray-200";
    title.textContent = "Performance by map";
    heading.appendChild(title);
    section.appendChild(heading);

    const rowsData = maps.map((map) => {
      const wins = Number(map.wins ?? 0);
      const losses = Number(map.losses ?? 0);
      const totalMatches = wins + losses;
      const computedWinrate =
        map.winrate !== null && map.winrate !== undefined
          ? map.winrate
          : totalMatches
            ? Math.round((wins / totalMatches) * 100)
            : null;
      let lastPlayedRaw = null;
      if (Array.isArray(map.matches)) {
        map.matches.forEach((match) => {
          if (!match || !match.started_at) return;
          const dt = parseISODate(match.started_at);
          if (!dt) return;
          if (!lastPlayedRaw) {
            lastPlayedRaw = match.started_at;
            return;
          }
          const currentBest = parseISODate(lastPlayedRaw);
          if (!currentBest || dt > currentBest) {
            lastPlayedRaw = match.started_at;
          }
        });
      }
      const matchesCount = Array.isArray(map.matches) ? map.matches.length : totalMatches;
      return {
        name: map.name || "Unknown map",
        wins,
        losses,
        totalMatches,
        winrate: computedWinrate,
        defence: map.defence,
        attack: map.attack,
        matchesCount,
        lastPlayedRaw,
      };
    });

    rowsData.sort((a, b) => {
      const aWin = a.winrate ?? -1;
      const bWin = b.winrate ?? -1;
      if (bWin !== aWin) return bWin - aWin;
      return (b.matchesCount || 0) - (a.matchesCount || 0);
    });

    const rows = rowsData.map((entry) => {
      const trendClass =
        entry.winrate === null || entry.winrate === undefined
          ? "is-neutral"
          : entry.winrate >= 55
            ? "is-positive"
            : entry.winrate <= 45
              ? "is-negative"
              : "is-neutral";
      const classNames = ["report-map-performance-row", trendClass];
      const cells = [];

      const mapCell = document.createElement("span");
      mapCell.className = "report-map-name";
      mapCell.textContent = entry.name;
      if (entry.name === state.activeMap) {
        const marker = document.createElement("span");
        marker.className = "report-map-active-indicator";
        marker.setAttribute("aria-hidden", "true");
        marker.textContent = ">";
        mapCell.classList.add("is-active");
        mapCell.prepend(marker);
      }

      return {
        className: classNames.join(" "),
        dataset: { map: entry.name },
        cells: [
          mapCell,
          String(entry.matchesCount || 0),
          createRateNode({
            value: entry.winrate,
            won: entry.wins,
            total: entry.totalMatches,
          }),
          `${entry.wins}-${entry.losses}`,
          createRateNode(entry.defence),
          createRateNode(entry.attack),
          formatDateTime(entry.lastPlayedRaw),
        ],
      };
    });

    const tableWrapper = createTable(
      ["Map", "Matches", "Winrate", "Record", "DEF WR", "ATK WR", "Last played"],
      rows,
      { tableClass: "report-table--map-performance" },
    );
    section.appendChild(tableWrapper);
    const table = tableWrapper.querySelector("table");
    if (table) {
      table.querySelectorAll("tbody tr").forEach((row) => {
        row.addEventListener("click", () => {
          const mapName = row.dataset.map;
          if (!mapName || state.activeMap === mapName) return;
          state.activeMap = mapName;
          renderMapTabs(container, state);
          renderMapPanel(container, state);
          renderPerformanceOverview(container, state);
        });
      });
    }
    return section;
  };

  const renderPerformanceOverview = (container, state) => {
    const host = container.querySelector("[data-report-performance]");
    if (!host) return;
    clearElement(host);
    host.appendChild(buildMapPerformanceSection(container, state));
  };

  const renderMapTabs = (container, state) => {
    const tabsRoot = container.querySelector(".report-map-tabs");
    if (!tabsRoot) return;
    clearElement(tabsRoot);
    const maps = (state.payload && state.payload.maps) || [];
    if (!maps.length) {
      const placeholder = document.createElement("span");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No map data available.";
      tabsRoot.appendChild(placeholder);
      return;
    }

    maps.forEach((map) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "report-map-tab";
      if (map.name === state.activeMap) {
        button.classList.add("is-active");
      }
      button.textContent = map.name;
      button.setAttribute("aria-pressed", map.name === state.activeMap ? "true" : "false");
      button.addEventListener("click", () => {
        if (state.activeMap === map.name) return;
        state.activeMap = map.name;
        renderMapTabs(container, state);
        renderMapPanel(container, state);
        renderPerformanceOverview(container, state);
      });
      tabsRoot.appendChild(button);
    });
  };

  const renderPostPlantCombined = (map) => {
    const section = document.createElement("div");
    const heading = document.createElement("h5");
    heading.className = "report-subtitle";
    heading.textContent = "Post-plant performance";
    section.appendChild(heading);

    const general = map.post_plants || {};
    const pistol = map.pistol_plants || {};

    const mapBySite = (items) => {
      const store = new Map();
      (Array.isArray(items) ? items : []).forEach((site) => {
        if (!site) return;
        const name = site.site || "—";
        if (!store.has(name)) {
          store.set(name, site);
        }
      });
      return store;
    };

    const generalSites = mapBySite(general.sites);
    const pistolSites = mapBySite(pistol.sites);
    const siteOrder = [];

    const pushSite = (name) => {
      if (name && !siteOrder.includes(name)) {
        siteOrder.push(name);
      }
    };

    generalSites.forEach((_, name) => pushSite(name));
    pistolSites.forEach((_, name) => pushSite(name));

    const rows = [];
    rows.push({
      className: "report-postplant-row",
      cells: [
        "All",
        createCountShareNode(general.overall ? general.overall.team_plants : null),
        createRateNode(general.overall ? general.overall.post_plant : null),
        createCountShareNode(general.overall ? general.overall.opponent_plants : null),
        createRateNode(general.overall ? general.overall.retake_win : null),
        createCountShareNode(pistol.overall ? pistol.overall.team_plants : null),
        createRateNode(pistol.overall ? pistol.overall.post_plant : null),
        createCountShareNode(pistol.overall ? pistol.overall.opponent_plants : null),
        createRateNode(pistol.overall ? pistol.overall.retake_win : null),
      ],
    });

    siteOrder.forEach((name) => {
      rows.push({
        className: "report-postplant-row",
        cells: [
          name,
          createCountShareNode(generalSites.get(name)?.team_plants),
          createRateNode(generalSites.get(name)?.post_plant),
          createCountShareNode(generalSites.get(name)?.opponent_plants),
          createRateNode(generalSites.get(name)?.retake_win),
          createCountShareNode(pistolSites.get(name)?.team_plants),
          createRateNode(pistolSites.get(name)?.post_plant),
          createCountShareNode(pistolSites.get(name)?.opponent_plants),
          createRateNode(pistolSites.get(name)?.retake_win),
        ],
      });
    });

    const tableWrapper = createTable(
      [
        "Site",
        "Team plants",
        "Post-plant WR",
        "Opp plants",
        "Retake WR",
        "Pistol plants",
        "Pistol WR",
        "Opp pistol",
        "Pistol retake WR",
      ],
      rows,
      { tableClass: "report-postplant-table" },
    );
    section.appendChild(tableWrapper);
    return section;
  };

  const renderMapPanel = (container, state) => {
    const panel = container.querySelector("[data-report-map-panel]");
    if (!panel) return;
    clearElement(panel);

    const maps = (state.payload && state.payload.maps) || [];
    if (!maps.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "No map data available.";
      panel.appendChild(placeholder);
      return;
    }

    const current = maps.find((map) => map.name === state.activeMap) || maps[0];
    if (!current) {
      const placeholder = document.createElement("div");
      placeholder.className = "report-detail-loading";
      placeholder.textContent = "Select a map to view its breakdown.";
      panel.appendChild(placeholder);
      return;
    }

    const statsGroup = document.createElement("div");
    statsGroup.className = "report-stat-group";

    const wins = Number(current.wins ?? 0);
    const losses = Number(current.losses ?? 0);
    const total = wins + losses;
    const computedWinrate =
      current.winrate !== null && current.winrate !== undefined
        ? current.winrate
        : total > 0
          ? Math.round((wins / total) * 100)
          : null;

    const overviewCard = document.createElement("div");
    overviewCard.className = "report-stat-card report-stat-card--overall";
    const overviewLabel = document.createElement("span");
    overviewLabel.textContent = `${current.name} overview`;
    overviewCard.appendChild(overviewLabel);
    const overviewPrimary = document.createElement("strong");
    overviewPrimary.textContent = `${wins}-${losses}`;
    overviewCard.appendChild(overviewPrimary);
    const overviewDetail = document.createElement("small");
    overviewDetail.textContent =
      computedWinrate !== null && computedWinrate !== undefined ? `${computedWinrate}% winrate` : "No rounds recorded";
    overviewCard.appendChild(overviewDetail);
    applyTrendClass(overviewCard, { value: computedWinrate });
    statsGroup.appendChild(overviewCard);

    const defenceCard = document.createElement("div");
    defenceCard.className = "report-stat-card report-stat-card--defence";
    const defenceLabel = document.createElement("span");
    defenceLabel.textContent = "Defence";
    defenceCard.appendChild(defenceLabel);
    const defencePrimary = document.createElement("strong");
    defencePrimary.textContent = current.defence && current.defence.value !== null ? `${current.defence.value}%` : "—";
    defenceCard.appendChild(defencePrimary);
    const defenceDetail = document.createElement("small");
    defenceDetail.textContent = current.defence ? `${current.defence.won}/${current.defence.total} rounds` : "No rounds";
    defenceCard.appendChild(defenceDetail);
    applyTrendClass(defenceCard, current.defence);
    statsGroup.appendChild(defenceCard);

    const attackCard = document.createElement("div");
    attackCard.className = "report-stat-card report-stat-card--attack";
    const attackLabel = document.createElement("span");
    attackLabel.textContent = "Attack";
    attackCard.appendChild(attackLabel);
    const attackPrimary = document.createElement("strong");
    attackPrimary.textContent = current.attack && current.attack.value !== null ? `${current.attack.value}%` : "—";
    attackCard.appendChild(attackPrimary);
    const attackDetail = document.createElement("small");
    attackDetail.textContent = current.attack ? `${current.attack.won}/${current.attack.total} rounds` : "No rounds";
    attackCard.appendChild(attackDetail);
    applyTrendClass(attackCard, current.attack);
    statsGroup.appendChild(attackCard);

    panel.appendChild(statsGroup);
    panel.appendChild(renderCompositions(current));
    panel.appendChild(renderPostPlantCombined(current));
    panel.appendChild(renderVisuals(current));
  };

  const renderReport = (container, state) => {
    renderOverview(container, state);
    renderMapTabs(container, state);
    renderMapPanel(container, state);
    renderPerformanceOverview(container, state);
    state.rendered = true;
  };

  const toggleContainer = (container, button, shouldShow) => {
    container.classList.toggle("hidden", !shouldShow);
    if (button) {
      button.textContent = shouldShow ? "Hide report" : "View report";
    }
  };

  const initReportPreviews = () => {
    const items = document.querySelectorAll("[data-report-item]");
    items.forEach((item) => {
      const button = item.querySelector("[data-toggle-target]");
      if (!button) return;
      const targetId = button.getAttribute("data-toggle-target");
      if (!targetId) return;
      const container = document.getElementById(`report-panel-${targetId}`);
      if (!container) return;

      button.addEventListener("click", () => {
        const isHidden = container.classList.contains("hidden");
        toggleContainer(container, button, isHidden);
        if (isHidden && container.hasAttribute("data-report-card")) {
          const state = getState(container);
          if (state.payload && !state.rendered) {
            renderReport(container, state);
          } else if (!state.payload) {
            const overview = container.querySelector("[data-report-overview]");
            if (overview) {
              clearElement(overview);
              const placeholder = document.createElement("div");
              placeholder.className = "report-detail-loading";
              placeholder.textContent = "Preview unavailable for this report.";
              overview.appendChild(placeholder);
            }
          }
        }
      });
    });
  };

  window.initReportPreviews = initReportPreviews;
})();
