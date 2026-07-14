(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const elements = {
    scenario: byId("scenario-select"),
    status: byId("playback-status"),
    position: byId("playback-position"),
    progress: byId("playback-progress"),
    time: byId("current-time"),
    states: byId("state-grid"),
    timeline: byId("timeline"),
    graph: byId("graph"),
    graphSummary: byId("graph-summary"),
    graphNodeList: byId("graph-node-list"),
    graphRelationshipList: byId("graph-relationship-list"),
    insights: byId("insights"),
    context: byId("context"),
    connection: byId("connection-status"),
    connectionDot: byId("connection-dot"),
    scenarioStatus: byId("scenario-status"),
    toast: byId("toast"),
  };

  let currentContext = {};
  let stream;
  let reconnectDelay = 1000;
  let refreshPending = false;
  let refreshQueued = false;
  const mutationQueue = PerceptionControl.createIntentQueue(
    async ({ path, body }) => {
      await request(path, {
        method: "POST",
        body: JSON.stringify(body ?? {}),
      });
      return request("/api/context/current");
    },
    (context) => render(context),
    setMutationPending,
  );

  async function request(path, options = {}) {
    const response = await fetch(path, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error?.message || `Request failed (${response.status})`);
    }
    return data;
  }

  function showToast(message, error = false) {
    elements.toast.textContent = message;
    elements.toast.className = `toast visible${error ? " error" : ""}`;
    window.setTimeout(() => { elements.toast.className = "toast"; }, 2600);
  }

  function setMutationPending(active) {
    [
      "scenario-select",
      "play-button",
      "pause-button",
      "step-button",
      "reset-button",
      "speed-select",
      "rebuild-position",
      "rebuild-button",
    ].forEach((id) => {
      byId(id).disabled = active;
    });
    byId("controls").setAttribute("aria-busy", String(active));
  }

  function empty(container, message) {
    const node = document.createElement("p");
    node.className = "empty";
    node.textContent = message;
    container.replaceChildren(node);
  }

  function renderPlayback(playback) {
    elements.status.textContent = playback.status.toUpperCase();
    elements.position.textContent = `${playback.cursor} / ${playback.total_events}`;
    elements.progress.textContent = `${Math.round(playback.progress * 100)}%`;
    elements.time.textContent = playback.current_time
      ? new Date(playback.current_time).toLocaleTimeString()
      : "—";
    elements.scenarioStatus.textContent = playback.scenario_id;
    elements.scenario.value = playback.scenario_id;
    byId("speed-select").value = String(playback.speed);
    byId("rebuild-position").max = String(playback.total_events);
  }

  function renderState(items) {
    const findFact = (key, entityPrefix = "") => {
      const fact = [...items].reverse().find((item) => (
        item.key === key && item.entity_id.startsWith(entityPrefix)
      ));
      return fact?.value;
    };
    const display = (value, fallback = "Not observed") => {
      if (value === undefined || value === null || value === "") return fallback;
      if (typeof value === "number") return String(value);
      return String(value).replaceAll("_", " ");
    };
    const setFact = (id, value, fallback, suffix = "") => {
      const target = byId(id);
      target.textContent = `${display(value, fallback)}${value == null ? "" : suffix}`;
      target.closest(".state-card").classList.toggle(
        "observed", value !== undefined && value !== null,
      );
    };

    byId("state-count").textContent = `${items.length} facts`;
    setFact("state-build-status", findFact("build_status"), "Not observed");
    setFact(
      "state-release-status",
      findFact("status", "deployment:"),
      "Not observed",
    );
    setFact(
      "state-release-version",
      findFact("current_version", "service:"),
      "Not observed",
    );
    setFact(
      "state-service-status",
      findFact("status", "service:"),
      "Not observed",
    );
    setFact(
      "state-service-latency",
      findFact("metric:latency_ms", "service:"),
      "Not observed",
      " ms",
    );
    const errorRate = findFact("metric:error_rate", "service:");
    setFact(
      "state-service-errors",
      errorRate == null ? errorRate : `${(Number(errorRate) * 100).toFixed(1)}%`,
      "Not observed",
    );
    setFact(
      "state-incident-status",
      findFact("incident_status", "service:")
        ?? findFact("status", "incident:"),
      "No active incident",
    );
    setFact(
      "state-secret-status",
      findFact("secret_status"),
      "Not observed",
    );
    setFact(
      "state-gate-status",
      findFact("gate_status"),
      "Not observed",
    );
    if (!items.length) {
      elements.states.querySelectorAll(".state-card").forEach((card) => {
        card.classList.remove("observed");
      });
    }
  }

  function renderTimeline(events) {
    byId("timeline-count").textContent = String(events.length);
    if (!events.length) {
      empty(elements.timeline, "No events perceived yet.");
      return;
    }
    const rows = events.map((event) => {
      const row = document.createElement("li");
      const details = document.createElement("details");
      const summary = document.createElement("summary");
      const type = document.createElement("span");
      type.className = "event-type";
      type.textContent = event.type;
      const meta = document.createElement("div");
      meta.className = "event-meta";
      meta.textContent = [
        `${event.subject.type}:${event.subject.id}`,
        `actor ${event.actor.type}:${event.actor.id}`,
        `source ${event.source}`,
        new Date(event.occurred_at).toLocaleTimeString(),
      ].join(" · ");
      summary.append(type, meta);
      const identifiers = document.createElement("div");
      identifiers.className = "event-identifiers";
      identifiers.textContent = [
        `event ${event.id}`,
        `correlation ${event.correlation_id}`,
        `trace ${event.trace_id}`,
      ].join(" · ");
      const payloadLabel = document.createElement("strong");
      payloadLabel.className = "payload-label";
      payloadLabel.textContent = "Full payload";
      const payload = document.createElement("pre");
      payload.className = "event-payload";
      payload.textContent = JSON.stringify(event.payload, null, 2);
      details.append(summary, identifiers, payloadLabel, payload);
      row.append(details);
      row.title = event.id;
      return row;
    });
    elements.timeline.replaceChildren(...rows);
    elements.timeline.lastElementChild?.scrollIntoView({ block: "nearest" });
  }

  function svgNode(name, attributes = {}) {
    const node = document.createElementNS("http://www.w3.org/2000/svg", name);
    Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
    return node;
  }

  function renderGraph(graph) {
    const { nodes, edges } = graph;
    byId("graph-count").textContent = `${nodes.length}N · ${edges.length}E`;
    elements.graph.querySelectorAll(":scope > g").forEach((node) => node.remove());
    elements.graphSummary.textContent = nodes.length
      ? `${nodes.length} nodes and ${edges.length} directed relationships.`
      : "No graph nodes or relationships perceived yet.";
    const nodeDescriptions = nodes.map((node) => {
      const item = document.createElement("li");
      const name = node.data?.name ? ` (${node.data.name})` : "";
      item.textContent = `${node.kind}: ${node.id}${name}`;
      return item;
    });
    const relationshipDescriptions = edges.map((edge) => {
      const item = document.createElement("li");
      item.textContent = `${edge.source} —${edge.relation}→ ${edge.target}`;
      return item;
    });
    if (!nodeDescriptions.length) {
      const emptyNode = document.createElement("li");
      emptyNode.textContent = "None";
      nodeDescriptions.push(emptyNode);
    }
    if (!relationshipDescriptions.length) {
      const emptyRelationship = document.createElement("li");
      emptyRelationship.textContent = "None";
      relationshipDescriptions.push(emptyRelationship);
    }
    elements.graphNodeList.replaceChildren(...nodeDescriptions);
    elements.graphRelationshipList.replaceChildren(...relationshipDescriptions);
    if (!nodes.length) return;

    const kinds = Array.from(new Set(nodes.map((node) => node.kind))).sort();
    const columns = Math.max(kinds.length, 1);
    const positions = new Map();
    kinds.forEach((kind, column) => {
      const inKind = nodes.filter((node) => node.kind === kind);
      inKind.forEach((node, row) => {
        positions.set(node.id, {
          x: 75 + column * (750 / Math.max(columns - 1, 1)),
          y: 65 + row * Math.min(90, 390 / Math.max(inKind.length, 1)),
        });
      });
    });

    const edgeLayer = svgNode("g", { "aria-hidden": "true" });
    edges.forEach((edge) => {
      const source = positions.get(edge.source);
      const target = positions.get(edge.target);
      if (!source || !target) return;
      edgeLayer.append(svgNode("line", {
        class: "edge",
        "marker-end": "url(#arrow)",
        x1: source.x,
        y1: source.y,
        x2: target.x,
        y2: target.y,
      }));
      const label = svgNode("text", {
        class: "edge-label",
        x: (source.x + target.x) / 2,
        y: (source.y + target.y) / 2 - 5,
      });
      label.textContent = edge.relation;
      edgeLayer.append(label);
    });

    const nodeLayer = svgNode("g", { "aria-hidden": "true" });
    nodes.forEach((node) => {
      const position = positions.get(node.id);
      const group = svgNode("g");
      group.append(svgNode("rect", {
        class: `node ${node.kind}`,
        x: position.x - 58,
        y: position.y - 20,
        width: 116,
        height: 40,
        rx: 7,
      }));
      const label = svgNode("text", {
        class: "node-label",
        x: position.x,
        y: position.y + 4,
      });
      const shortLabel = node.id.length > 20 ? `…${node.id.slice(-19)}` : node.id;
      label.textContent = shortLabel;
      group.append(label);
      nodeLayer.append(group);
    });
    elements.graph.append(edgeLayer, nodeLayer);
  }

  function renderInsights(insights, timeline) {
    byId("insight-count").textContent = String(insights.length);
    if (!insights.length) {
      empty(elements.insights, "No rule-based insights at this position.");
      return;
    }
    const timelineById = new Map(timeline.map((event) => [event.id, event]));
    const cards = [...insights].reverse().map((insight) => {
      const card = document.createElement("article");
      card.className = `insight ${insight.severity}`;
      const header = document.createElement("div");
      header.className = "insight-header";
      const title = document.createElement("strong");
      title.textContent = insight.title;
      const severity = document.createElement("span");
      severity.className = "severity";
      severity.textContent = `${insight.severity} · ${insight.status}`;
      header.append(title, severity);
      const summary = document.createElement("p");
      summary.textContent = insight.summary;
      const evidence = document.createElement("div");
      evidence.className = "evidence";
      if (!insight.evidence_event_ids.length) {
        evidence.textContent = "Evidence: none";
      } else {
        insight.evidence_event_ids.forEach((eventId) => {
          const event = timelineById.get(eventId);
          const details = document.createElement("details");
          details.className = "evidence-detail";
          const evidenceSummary = document.createElement("summary");
          evidenceSummary.textContent = event
            ? `${event.type} · ${event.id}`
            : `Unavailable event · ${eventId}`;
          details.append(evidenceSummary);
          if (event) {
            const metadata = document.createElement("div");
            metadata.className = "evidence-meta";
            metadata.textContent = [
              `actor ${event.actor.type}:${event.actor.id}`,
              `source ${event.source}`,
              new Date(event.occurred_at).toLocaleTimeString(),
            ].join(" · ");
            const payload = document.createElement("pre");
            payload.className = "evidence-payload";
            payload.textContent = JSON.stringify(event.payload, null, 2);
            details.append(metadata, payload);
          }
          evidence.append(details);
        });
      }
      card.append(header, summary, evidence);
      return card;
    });
    elements.insights.replaceChildren(...cards);
  }

  function render(context) {
    currentContext = context;
    renderPlayback(context.playback);
    renderState(context.state);
    renderTimeline(context.timeline);
    renderGraph(context.graph);
    renderInsights(context.insights, context.timeline);
    elements.context.textContent = JSON.stringify(context, null, 2);
  }

  async function refresh() {
    if (refreshPending) {
      refreshQueued = true;
      return;
    }
    refreshPending = true;
    try {
      render(await request("/api/context/current"));
    } catch (error) {
      showToast(error.message, true);
    } finally {
      refreshPending = false;
      if (refreshQueued) {
        refreshQueued = false;
        await refresh();
      }
    }
  }

  async function loadScenarios() {
    const scenarios = await request("/api/scenarios");
    const options = scenarios.map((scenario) => {
      const option = document.createElement("option");
      option.value = scenario.id;
      option.textContent = `${scenario.name} · ${scenario.event_count} events`;
      option.title = scenario.description;
      return option;
    });
    elements.scenario.replaceChildren(...options);
  }

  async function control(path, body) {
    try {
      await mutationQueue.run({ path, body });
    } catch (error) {
      showToast(error.message, true);
    }
  }

  function connectStream() {
    stream?.close();
    elements.connection.textContent = "Connecting";
    elements.connectionDot.className = "status-dot pending";
    stream = new EventSource("/api/stream");
    stream.addEventListener("open", () => {
      reconnectDelay = 1000;
      elements.connection.textContent = "Live";
      elements.connectionDot.className = "status-dot live";
    });
    stream.addEventListener("perception.updated", refresh);
    stream.addEventListener("error", () => {
      stream.close();
      elements.connection.textContent = "Reconnecting";
      elements.connectionDot.className = "status-dot";
      window.setTimeout(connectStream, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 15000);
    });
  }

  elements.scenario.addEventListener("change", async () => {
    await control(`/api/scenarios/${encodeURIComponent(elements.scenario.value)}/load`);
  });
  byId("play-button").addEventListener("click", () => {
    control("/api/playback/play", { speed: Number(byId("speed-select").value) });
  });
  byId("pause-button").addEventListener("click", () => control("/api/playback/pause"));
  byId("step-button").addEventListener("click", () => control("/api/playback/step"));
  byId("reset-button").addEventListener("click", () => control("/api/playback/reset"));
  byId("rebuild-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const raw = byId("rebuild-position").value;
    control("/api/playback/rebuild", raw === "" ? {} : { position: Number(raw) });
  });
  byId("copy-context").addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(currentContext, null, 2));
      showToast("Context copied");
    } catch {
      showToast("Clipboard unavailable", true);
    }
  });
  document.addEventListener("keydown", (event) => {
    const editing = ["INPUT", "SELECT", "TEXTAREA", "BUTTON"].includes(document.activeElement?.tagName);
    if (editing || event.altKey || event.ctrlKey || event.metaKey) return;
    if (event.key.toLowerCase() === "p") byId("play-button").click();
    if (event.code === "Space") {
      event.preventDefault();
      byId("step-button").click();
    }
  });

  Promise.all([loadScenarios(), refresh()])
    .then(connectStream)
    .catch((error) => showToast(error.message, true));
})();
