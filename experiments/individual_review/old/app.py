"""
Tiny local review app for the individual donor review experiment.

Run:
    python experiments/individual_review/app.py

Then open:
    http://127.0.0.1:8765/
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


EXPERIMENT_DIR = Path(__file__).resolve().parent
QUEUE_PATH = EXPERIMENT_DIR / "review_queue.csv"
DECISIONS_PATH = EXPERIMENT_DIR / "curated_individuals.csv"
ALIAS_DECISIONS_PATH = EXPERIMENT_DIR / "cluster_alias_decisions.csv"

DECISION_COLUMNS = [
    "review_key",
    "contributor_name",
    "decision_status",
    "identity_id",
    "consolidated_name",
    "is_tech_figure",
    "tech_company",
    "confidence",
    "notes",
    "decided_at",
]

ALIAS_DECISION_COLUMNS = [
    "canonical_identity_id",
    "canonical_name",
    "alias_review_key",
    "alias_name",
    "alias_decision",
    "notes",
    "decided_at",
]


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Individual Donor Review</title>
  <style>
    :root {
      --ink: #17202a;
      --muted: #607080;
      --line: #d8e0e8;
      --background: #FCF9EA;
      --soft: #f4efd6;
      --panel: #fffdf2;
      --field: #f7f0d9;
      --accent: #146c94;
      --good: #237a57;
      --warn: #966a10;
      --bad: #9c2f2f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--background);
    }
    header {
      height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 0 20px;
      border-bottom: 1px solid var(--line);
      background: var(--background);
      position: sticky;
      top: 0;
      z-index: 3;
    }
    h1 { font-size: 18px; margin: 0; font-weight: 700; letter-spacing: 0; }
    .status { color: var(--muted); font-size: 13px; white-space: nowrap; }
    main {
      display: grid;
      grid-template-columns: minmax(280px, 35vw) 1fr;
      min-height: calc(100vh - 58px);
    }
    aside { border-right: 1px solid var(--line); min-width: 0; background: var(--soft); }
    .tabs {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px;
      padding: 10px 12px 0;
      background: var(--background);
    }
    .tab {
      padding: 8px 6px;
      border-color: #c7baa0;
      background: var(--panel);
      font-size: 13px;
    }
    .tab.active {
      background: var(--ink);
      color: var(--background);
      border-color: var(--ink);
      font-weight: 700;
    }
    .filters {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 8px;
      padding: 12px;
      border-bottom: 1px solid var(--line);
      background: var(--background);
    }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      background: var(--field);
      color: var(--ink);
    }
    button {
      border: 1px solid var(--line);
      background: var(--field);
      color: var(--ink);
      border-radius: 6px;
      padding: 9px 11px;
      font: inherit;
      cursor: pointer;
    }
    button.primary { border-color: var(--accent); background: var(--accent); color: #fff; font-weight: 700; }
    .list { height: calc(100vh - 164px); overflow: auto; }
    .row {
      width: 100%;
      text-align: left;
      border: 0;
      border-bottom: 1px solid var(--line);
      border-radius: 0;
      background: var(--panel);
      padding: 12px;
      display: grid;
      gap: 6px;
    }
    .row.active { background: #eaf4f8; box-shadow: inset 4px 0 0 var(--accent); }
    .row-title { font-weight: 750; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .row-meta { display: flex; flex-wrap: wrap; gap: 6px; font-size: 12px; color: var(--muted); }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--muted);
      font-size: 12px;
      max-width: 100%;
    }
    .pill.good { border-color: #9bd0bc; color: var(--good); }
    .pill.warn { border-color: #dfc78e; color: var(--warn); }
    .pill.bad { border-color: #d7aaa5; color: var(--bad); }
    .detail { padding: 22px; min-width: 0; }
    .empty { padding: 40px; color: var(--muted); }
    .topline {
      display: flex;
      justify-content: space-between;
      align-items: start;
      gap: 20px;
      margin-bottom: 18px;
    }
    h2 { margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }
    .subtle { color: var(--muted); }
    .money { font-size: 24px; font-weight: 800; text-align: right; white-space: nowrap; }
    .flow { display: grid; gap: 16px; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
    .summary-strip { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .panel { border: 1px solid var(--line); border-radius: 8px; padding: 14px; background: var(--panel); min-width: 0; }
    .panel h3 {
      margin: 0 0 14px;
      font-size: 19px;
      color: var(--ink);
      font-weight: 850;
      text-transform: none;
      letter-spacing: 0;
    }
    .support-grid h4 {
      margin: 0 0 8px;
      font-size: 13px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .evidence { line-height: 1.45; white-space: pre-wrap; overflow-wrap: anywhere; }
    .decision { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .decision label { display: grid; gap: 5px; font-size: 13px; color: var(--muted); }
    .decision .wide { grid-column: 1 / -1; }
    .actions { display: flex; justify-content: flex-end; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    .final-actions {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 12px;
    }
    .final-actions button {
      min-height: 46px;
      font-weight: 800;
    }
    .final-actions button.active {
      outline: 3px solid var(--accent);
      outline-offset: 2px;
    }
    .final-actions .tech { background: #dceede; border-color: #8cc19b; }
    .final-actions .not-tech { background: #f3ded8; border-color: #c99285; }
    .final-actions .research { background: #eee3c2; border-color: #c9b16e; }
    .tuple-list {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      line-height: 1.5;
      max-height: 280px;
      overflow: auto;
      white-space: pre-wrap;
    }
    .alias-stack {
      display: grid;
      gap: 12px;
    }
    .alias-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 12px;
      display: grid;
      gap: 10px;
    }
    .alias-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
    }
    .alias-name {
      font-size: 18px;
      font-weight: 800;
    }
    .choice-row {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }
    .choice-row button {
      font-weight: 700;
      background: var(--field);
    }
    .choice-row button.yes.active { background: #dceede; border-color: #8cc19b; }
    .choice-row button.no.active { background: #f3ded8; border-color: #c99285; }
    .choice-row button.research.active { background: #eee3c2; border-color: #c9b16e; }
    .canonical-box {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .support-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-top: 16px;
    }
    @media (max-width: 860px) {
      main { grid-template-columns: 1fr; }
      aside { border-right: 0; border-bottom: 1px solid var(--line); }
      .list { height: 38vh; }
      .grid, .decision { grid-template-columns: 1fr; }
      .money { text-align: left; }
      .topline { display: grid; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Individual Donor Review</h1>
    <div id="status" class="status">Loading...</div>
  </header>
  <main>
    <aside>
      <div class="tabs">
        <button class="tab active" data-tab="needs_review">Needs review</button>
        <button class="tab" data-tab="needs_research">Needs research</button>
        <button class="tab" data-tab="reviewed">Reviewed</button>
      </div>
      <div class="filters">
        <input id="search" placeholder="Search names, employers, companies">
        <select id="reason">
          <option value="">All</option>
          <option value="major_donor">Major donors</option>
          <option value="tech_linked_employer">Tech-linked</option>
          <option value="possible_tech_context">Possible tech</option>
        </select>
      </div>
      <div id="list" class="list"></div>
    </aside>
    <section id="detail" class="detail">
      <div class="empty">Select a donor to review.</div>
    </section>
  </main>
  <script>
    let rows = [];
    let decisions = [];
    let aliasDecisions = [];
    let active = null;
    let currentTab = "needs_review";
    const finalStatuses = new Set(["reviewed_not_tech", "tech_figure", "same_identity", "not_individual"]);

    const money = value => {
      const n = Number(value || 0);
      return n.toLocaleString(undefined, {style: "currency", currency: "USD", maximumFractionDigits: 0});
    };
    const text = value => (value || "").toString();
    const slug = value => text(value).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    const decisionLabel = status => ({
      reviewed_not_tech: "Reviewed: not tech",
      tech_figure: "Reviewed: tech figure",
      same_identity: "Same identity",
      needs_research: "Needs research",
      needs_split: "Needs split",
      not_individual: "Not individual"
    }[status] || status || "");

    async function loadRows() {
      const [queueResponse, decisionResponse, aliasDecisionResponse] = await Promise.all([
        fetch("/api/queue"),
        fetch("/api/decisions"),
        fetch("/api/alias-decisions")
      ]);
      rows = (await queueResponse.json()).rows;
      decisions = (await decisionResponse.json()).rows;
      aliasDecisions = (await aliasDecisionResponse.json()).rows;
      document.getElementById("status").textContent = statusText();
      renderList();
    }

    function statusText() {
      const visible = filteredRows();
      const label = currentTab.replace("_", " ");
      return `${visible.length.toLocaleString()} ${label}`;
    }

    function decisionForKey(reviewKey) {
      return decisions.find(decision => decision.review_key === reviewKey);
    }

    function candidateForDecision(decision) {
      const row = rows.find(candidate => candidate.review_key === decision.review_key) || {};
      return {...row, ...decision, from_decision: true};
    }

    function canonicalIdentity() {
      return {
        id: slug(document.getElementById("canonical-id")?.value || active?.display_name_suggestion || active?.contributor_name || ""),
        name: document.getElementById("canonical-name")?.value || active?.display_name_suggestion || active?.contributor_name || ""
      };
    }

    function canonicalFromRow(row, fallbackId = "") {
      const name = row?.consolidated_name || row?.display_name_suggestion || row?.contributor_name || "";
      const id = row?.identity_id || fallbackId || slug(name);
      return {id: slug(id), name};
    }

    function aliasDecisionFor(canonicalId, aliasKey) {
      return aliasDecisions.find(decision =>
        decision.canonical_identity_id === canonicalId &&
        decision.alias_review_key === aliasKey
      );
    }

    function filteredRows() {
      const q = document.getElementById("search").value.trim().toLowerCase();
      const reason = document.getElementById("reason").value;
      let baseRows = [];
      if (currentTab === "reviewed") {
        baseRows = decisions
          .filter(decision => finalStatuses.has(decision.decision_status))
          .map(candidateForDecision);
      } else if (currentTab === "needs_research") {
        baseRows = decisions
          .filter(decision => ["needs_research", "needs_split"].includes(decision.decision_status))
          .map(candidateForDecision);
      } else {
        baseRows = rows.filter(row => {
          const decision = decisionForKey(row.review_key);
          return !decision || !decision.decision_status;
        });
      }
      return baseRows.filter(row => {
        const hay = [
          row.contributor_name, row.employers, row.occupations,
          row.tech_companies, row.suggested_cluster_members,
          row.consolidated_name, row.identity_id, row.notes
        ].join(" ").toLowerCase();
        return (!q || hay.includes(q)) && (!reason || text(row.review_reasons).includes(reason));
      });
    }

    function renderList() {
      const list = document.getElementById("list");
      const visible = filteredRows();
      list.innerHTML = "";
      for (const row of visible) {
        const button = document.createElement("button");
        button.className = "row" + (active && active.review_key === row.review_key ? " active" : "");
        button.innerHTML = `
          <div class="row-title">${row.contributor_name}</div>
          <div class="row-meta">
            ${text(row.net_total) ? `<span class="pill warn">${money(row.net_total)}</span>` : ""}
            ${text(row.tech_companies) ? `<span class="pill good">${row.tech_companies}</span>` : ""}
            ${Number(row.suggested_cluster_size || 1) > 1 ? `<span class="pill">${row.suggested_cluster_size} name variants</span>` : ""}
            ${row.decision_status ? `<span class="pill bad">${decisionLabel(row.decision_status)}</span>` : ""}
          </div>
          <div class="row-meta">${row.review_reasons || ""}</div>
        `;
        button.addEventListener("click", () => {
          active = row;
          renderList();
          renderDetail();
        });
        list.appendChild(button);
      }
      if (!visible.length) {
        list.innerHTML = `<div class="empty">No candidates match this filter.</div>`;
      }
      document.getElementById("status").textContent = statusText();
    }

    function clusterHtml(row) {
      const members = text(row.suggested_cluster_members)
        .split(";")
        .map(name => name.trim())
        .filter(Boolean);
      if (!members.length) members.push(row.contributor_name);
      const canonical = canonicalFromRow(row);
      return `<div class="alias-stack">` + members.map(name => {
        const member = rows.find(candidate => candidate.contributor_name === name) || {};
        const aliasKey = member.review_key || name;
        const existing = aliasDecisionFor(canonical.id, aliasKey) || {};
        const state = existing.alias_decision || "";
        return `<div class="alias-card" data-alias-key="${aliasKey}" data-alias-name="${name}">
          <div class="alias-head">
            <div class="alias-name">${name}</div>
            ${member.net_total ? `<div class="subtle">${money(member.net_total)}</div>` : ""}
          </div>
          <div class="tuple-list">${member.variant_evidence || "No tuple evidence available."}</div>
          <div class="choice-row">
            <button class="alias-choice yes ${state === "yes" ? "active" : ""}" data-choice="yes">Yes, this is ${canonical.name}</button>
            <button class="alias-choice no ${state === "no" ? "active" : ""}" data-choice="no">No</button>
            <button class="alias-choice research ${state === "research" ? "active" : ""}" data-choice="research">Research</button>
          </div>
        </div>`;
      }).join("") + `</div>`;
    }

    function renderDetail() {
      const detail = document.getElementById("detail");
      if (!active) return;
      const identity = slug(active.display_name_suggestion || active.contributor_name);
      const canonical = canonicalFromRow(active, identity);
      detail.innerHTML = `
        <div class="topline">
          <div>
            <h2>${active.contributor_name}</h2>
            <div class="subtle">${active.display_name_suggestion || ""}</div>
          </div>
          <div class="money">${money(active.net_total)}</div>
        </div>
        <div class="flow">
          <div class="panel">
            <h3>1. Canonical Target</h3>
            <div class="canonical-box">
              <label>Canonical identity
                <input id="canonical-name" value="${canonical.name}">
              </label>
              <label>Identity ID
                <input id="canonical-id" value="${canonical.id}">
              </label>
            </div>
            <div class="summary-strip">
              <span class="pill warn">${money(active.net_total)}</span>
              <span class="pill">${active.states || "No state"}</span>
              <span class="pill">${active.n_contributions || "0"} contributions</span>
              <span class="pill">${active.variant_count || "0"} tuples</span>
              <span class="pill">${active.n_committees || "0"} committees</span>
              ${text(active.tech_companies) ? `<span class="pill good">${active.tech_companies}</span>` : ""}
            </div>
            <h4 style="margin-top: 16px;">Evidence for selected candidate</h4>
            <div class="support-grid">
              <div>
                <h4>Employers</h4>
                <div class="evidence">${active.employers || ""}</div>
              </div>
              <div>
                <h4>Occupations</h4>
                <div class="evidence">${active.occupations || ""}</div>
              </div>
            </div>
            <h4 style="margin-top: 16px;">Selected Candidate Tuples</h4>
            <div class="tuple-list">${active.variant_evidence || "Regenerate candidates to see tuple-level evidence here."}</div>
          </div>

          <div class="panel">
            <h3>2. Alias Adjudication</h3>
            <div class="evidence">${clusterHtml(active)}</div>
          </div>

          <div class="panel">
            <h3>3. Final Person Decision</h3>
            <input type="hidden" id="selected-final-status" value="${active.decision_status || ""}">
            <form id="decision-form" class="decision">
              <label>Confidence
                <select name="confidence">
                  <option value="high">High</option>
                  <option value="medium">Medium</option>
                  <option value="low">Low</option>
                </select>
              </label>
              <label>Tech Company / Role
                <input name="tech_company" value="${active.tech_companies || ""}">
              </label>
              <label class="wide">Notes
                <textarea name="notes" rows="3"></textarea>
              </label>
            </form>
            <div class="final-actions">
              <button class="tech" data-final-status="tech_figure">Tech figure</button>
              <button class="not-tech" data-final-status="reviewed_not_tech">Not tech figure</button>
              <button class="research" data-final-status="needs_research">Needs research</button>
              <button data-final-status="not_individual">Not individual</button>
            </div>
            <div class="actions">
              ${active.from_decision ? `<button id="revert">Revert decision</button>` : ""}
              <button class="primary" id="save-move">Save and move on</button>
            </div>
          </div>
        </div>
      `;
      document.querySelectorAll(".alias-choice").forEach(button => {
        button.addEventListener("click", event => {
          event.preventDefault();
          saveAliasDecision(button);
        });
      });
      document.querySelectorAll("#canonical-name, #canonical-id").forEach(input => {
        input.addEventListener("change", () => {
          active.consolidated_name = document.getElementById("canonical-name").value;
          active.identity_id = document.getElementById("canonical-id").value;
          renderDetail();
        });
      });
      if (active.confidence) {
        document.querySelector("[name=confidence]").value = active.confidence;
      }
      if (active.tech_company) {
        document.querySelector("[name=tech_company]").value = active.tech_company;
      }
      if (active.notes) {
        document.querySelector("[name=notes]").value = active.notes;
      }
      document.querySelectorAll("[data-final-status]").forEach(button => {
        button.classList.toggle("active", button.dataset.finalStatus === (active.decision_status || ""));
        button.addEventListener("click", event => {
          event.preventDefault();
          document.getElementById("selected-final-status").value = button.dataset.finalStatus;
          document.querySelectorAll("[data-final-status]").forEach(choice => choice.classList.remove("active"));
          button.classList.add("active");
        });
      });
      document.getElementById("save-move").addEventListener("click", event => {
        event.preventDefault();
        const status = document.getElementById("selected-final-status").value;
        if (!status) {
          alert("Choose a final person decision first.");
          return;
        }
        saveDecision(status);
      });
      const revert = document.getElementById("revert");
      if (revert) revert.addEventListener("click", revertDecision);
    }

    function techFlagForStatus(status) {
      if (!status) return "";
      if (status === "tech_figure") return "TRUE";
      if (status === "same_identity") return "INHERIT";
      if (["needs_research", "needs_split"].includes(status)) return "UNSURE";
      return "FALSE";
    }

    async function saveDecision(status = "", advance = true) {
      const form = new FormData(document.getElementById("decision-form"));
      const payload = Object.fromEntries(form.entries());
      const canonical = canonicalIdentity();
      payload.review_key = active.review_key;
      payload.contributor_name = active.contributor_name;
      payload.decision_status = status;
      payload.identity_id = canonical.id;
      payload.consolidated_name = canonical.name;
      payload.is_tech_figure = techFlagForStatus(payload.decision_status);
      const response = await fetch("/api/decision", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        alert("Save failed.");
        return;
      }
      await loadRows();
      active = advance ? (filteredRows()[0] || null) : active;
      if (active) renderDetail();
      else document.getElementById("detail").innerHTML = `<div class="empty">Queue complete for this tab.</div>`;
    }

    async function saveAliasDecision(button) {
      const card = button.closest(".alias-card");
      const canonical = canonicalIdentity();
      const payload = {
        canonical_identity_id: canonical.id,
        canonical_name: canonical.name,
        alias_review_key: card.dataset.aliasKey,
        alias_name: card.dataset.aliasName,
        alias_decision: button.dataset.choice,
        notes: ""
      };
      const response = await fetch("/api/alias-decision", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        alert("Alias decision failed.");
        return;
      }
      aliasDecisions = (await response.json()).rows;
      card.querySelectorAll(".alias-choice").forEach(choice => choice.classList.remove("active"));
      button.classList.add("active");
    }

    async function revertDecision() {
      const response = await fetch("/api/revert", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({review_key: active.review_key})
      });
      if (!response.ok) {
        alert("Revert failed.");
        return;
      }
      await loadRows();
      currentTab = "needs_review";
      setActiveTab();
      active = rows.find(row => row.review_key === active.review_key) || filteredRows()[0] || null;
      if (active) renderDetail();
      else document.getElementById("detail").innerHTML = `<div class="empty">Decision reverted.</div>`;
    }

    function setActiveTab() {
      document.querySelectorAll(".tab").forEach(button => {
        button.classList.toggle("active", button.dataset.tab === currentTab);
      });
      renderList();
    }

    document.querySelectorAll(".tab").forEach(button => {
      button.addEventListener("click", () => {
        currentTab = button.dataset.tab;
        active = null;
        setActiveTab();
        document.getElementById("detail").innerHTML = `<div class="empty">Select a donor to review.</div>`;
      });
    });
    document.getElementById("search").addEventListener("input", renderList);
    document.getElementById("reason").addEventListener("change", renderList);
    loadRows();
  </script>
</body>
</html>
"""


def ensure_decisions_file() -> None:
    if DECISIONS_PATH.exists():
        return
    with DECISIONS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_COLUMNS)
        writer.writeheader()


def ensure_alias_decisions_file() -> None:
    if ALIAS_DECISIONS_PATH.exists():
        return
    with ALIAS_DECISIONS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALIAS_DECISION_COLUMNS)
        writer.writeheader()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_decision(decision: dict[str, str]) -> None:
    ensure_decisions_file()
    rows = read_csv_rows(DECISIONS_PATH)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    clean = {column: str(decision.get(column, "")).strip() for column in DECISION_COLUMNS}
    clean["decided_at"] = now

    replaced = False
    for i, row in enumerate(rows):
        if row.get("review_key") == clean["review_key"]:
            rows[i] = clean
            replaced = True
            break
    if not replaced:
        rows.append(clean)

    with DECISIONS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_alias_decision(decision: dict[str, str]) -> list[dict[str, str]]:
    ensure_alias_decisions_file()
    rows = read_csv_rows(ALIAS_DECISIONS_PATH)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    clean = {column: str(decision.get(column, "")).strip() for column in ALIAS_DECISION_COLUMNS}
    clean["decided_at"] = now

    replaced = False
    for i, row in enumerate(rows):
        same_canonical = row.get("canonical_identity_id") == clean["canonical_identity_id"]
        same_alias = row.get("alias_review_key") == clean["alias_review_key"]
        if same_canonical and same_alias:
            rows[i] = clean
            replaced = True
            break
    if not replaced:
        rows.append(clean)

    with ALIAS_DECISIONS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALIAS_DECISION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def revert_decision(review_key: str) -> bool:
    ensure_decisions_file()
    rows = read_csv_rows(DECISIONS_PATH)
    kept = [row for row in rows if row.get("review_key") != review_key]
    if len(kept) == len(rows):
        return False
    with DECISIONS_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DECISION_COLUMNS)
        writer.writeheader()
        writer.writerows(kept)
    return True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/queue":
            params = parse_qs(parsed.query)
            limit = int(params.get("limit", ["0"])[0] or 0)
            rows = read_csv_rows(QUEUE_PATH)
            if limit > 0:
                rows = rows[:limit]
            self.send_json({"rows": rows})
            return
        if parsed.path == "/api/decisions":
            self.send_json({"rows": read_csv_rows(DECISIONS_PATH)})
            return
        if parsed.path == "/api/alias-decisions":
            ensure_alias_decisions_file()
            self.send_json({"rows": read_csv_rows(ALIAS_DECISIONS_PATH)})
            return
        self.send_error(404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in {"/api/decision", "/api/revert", "/api/alias-decision"}:
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self.send_json({"error": "Invalid JSON"}, status=400)
            return
        if not payload.get("review_key"):
            if parsed.path == "/api/alias-decision" and payload.get("alias_review_key"):
                rows = write_alias_decision(payload)
                self.send_json({"ok": True, "rows": rows})
                return
            self.send_json({"error": "Missing review_key"}, status=400)
            return
        if parsed.path == "/api/revert":
            reverted = revert_decision(str(payload.get("review_key", "")).strip())
            self.send_json({"ok": True, "reverted": reverted})
            return
        write_decision(payload)
        self.send_json({"ok": True})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--watchlist", action="store_true", help="Review evidence groups for the prominent-person pilot.")
    args = parser.parse_args()

    if args.watchlist:
        from watchlist import serve
        serve(args.port)
        return 0

    if not QUEUE_PATH.exists():
        print(f"Missing {QUEUE_PATH.name}. Run build_candidates.py first.")
        return 1
    ensure_decisions_file()
    ensure_alias_decisions_file()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving individual review app at http://{args.host}:{args.port}/")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
