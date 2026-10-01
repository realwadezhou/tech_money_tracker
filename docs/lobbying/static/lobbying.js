(function (root) {
  "use strict";

  function eligibleMatches(item, filters) {
    return item.matches.filter(function (match) {
      if (filters.topic && match.topic_id !== filters.topic) return false;
      var rejected = match.decision === "rejected" || match.dependency_rejected;
      if (filters.evidence === "rejected") return rejected;
      if (rejected) return false;
      if (filters.evidence === "accepted") return match.decision === "accepted";
      var prerequisite = item.matches.find(function (m) { return m.topic_id === match.requires; });
      var explicit = match.decision === "accepted" || (match.decision !== "uncertain" &&
        (match.kind === "direct" || (prerequisite && prerequisite.decision !== "rejected" &&
          prerequisite.decision !== "uncertain" && (prerequisite.kind === "direct" || prerequisite.decision === "accepted"))));
      if (filters.evidence === "explicit") return explicit;
      if (filters.evidence === "ambiguous") return !explicit;
      return true;
    });
  }

  function filterRows(activities, filters, watchlists) {
    var watchlist = watchlists.find(function (w) { return w.id === filters.watchlist; });
    var query = (filters.query || "").trim().toLowerCase().split(/\s+/).filter(Boolean);
    var client = (filters.client || "").trim().toLowerCase();
    return activities.filter(function (item) {
      if (filters.year && String(item.year) !== filters.year) return false;
      if (filters.quarter && String(item.quarter) !== filters.quarter) return false;
      if (watchlist && !watchlist.organization_ids.includes(item.organization_id)) return false;
      if (client && !(item.client_name + " " + (item.organization_name || "")).toLowerCase().includes(client)) return false;
      if (!eligibleMatches(item, filters).length) return false;
      if (!query.length) return true;
      var haystack = [item.description, item.client_name, item.organization_name || "", item.registrant_name,
        item.issue_label].concat(item.lobbyists.map(function (p) { return p.name; }),
        item.government_entities.map(function (e) { return e.name; })).join(" ").toLowerCase();
      return query.every(function (word) { return haystack.includes(word); });
    });
  }

  function summarize(items) {
    return {activities: items.length, reports: new Set(items.map(function (r) { return r.filing_uuid; })).size,
      clients: new Set(items.map(function (r) { return r.client_source_key; })).size};
  }

  function governmentEntityLabel(scope) {
    if (scope === "filing") return "Government entities listed for the whole filing (not this issue entry)";
    if (scope === "issue_entry") return "Government entities listed for this issue entry";
    return "Government entities listed (scope unverified)";
  }

  function csvCell(value) {
    var text = String(value == null ? "" : value);
    // Spreadsheet-safe export of untrusted filing text; JSON retains exact source text.
    if (/^[\s]*[=+@-]/.test(text) || /^[\t\r\n]/.test(text)) text = "'" + text;
    return '"' + text.replace(/"/g, '""') + '"';
  }

  function asCSV(items, filters, version) {
    var fields = ["activity_id", "filing_uuid", "year", "quarter", "client_source_key", "client_name", "organization_name",
      "organization_id", "organization_status", "registrant_name", "issue_label", "description", "description_sha256",
      "topic_id", "match_kind", "review_decision", "dependency_rejected", "matched_phrases",
      "requires", "prerequisite_match_kind", "prerequisite_review_decision", "prerequisite_matched_phrases",
      "lobbyists", "government_entities", "government_entity_scope", "filing_url", "rules_version"];
    var lines = [fields.map(csvCell).join(",")];
    items.forEach(function (item) {
      eligibleMatches(item, filters).forEach(function (match) {
        var prerequisite = item.matches.find(function (m) { return m.topic_id === match.requires; });
        var row = Object.assign({}, item, {topic_id: match.topic_id, match_kind: match.kind,
          review_decision: match.decision, matched_phrases: match.evidence.map(function (e) { return e.text; }).join(" | "),
          dependency_rejected: Boolean(match.dependency_rejected), requires: match.requires || "",
          prerequisite_match_kind: prerequisite ? prerequisite.kind : "",
          prerequisite_review_decision: prerequisite ? prerequisite.decision : "",
          prerequisite_matched_phrases: prerequisite ? prerequisite.evidence.map(function (e) { return e.text; }).join(" | ") : "",
          lobbyists: item.lobbyists.map(function (p) { return p.name; }).join(" | "),
          government_entities: item.government_entities.map(function (e) { return e.name; }).join(" | "), rules_version: version});
        lines.push(fields.map(function (field) { return csvCell(row[field]); }).join(","));
      });
    });
    return "\ufeff" + lines.join("\r\n") + "\r\n";
  }

  function highlightSegments(text, evidence) {
    var spans = evidence.map(function (e) { return [e.start, e.end]; }).sort(function (a, b) { return a[0] - b[0]; });
    var merged = [];
    spans.forEach(function (span) {
      var last = merged[merged.length - 1];
      if (last && span[0] <= last[1]) last[1] = Math.max(last[1], span[1]);
      else merged.push(span.slice());
    });
    // Python offsets count Unicode code points; JS string slices count UTF-16 units.
    var chars = Array.from(text), output = [], position = 0;
    merged.forEach(function (span) {
      if (span[0] > position) output.push({text: chars.slice(position, span[0]).join(""), matched: false});
      output.push({text: chars.slice(span[0], span[1]).join(""), matched: true});
      position = span[1];
    });
    if (position < chars.length) output.push({text: chars.slice(position).join(""), matched: false});
    return output;
  }

  var api = {eligibleMatches: eligibleMatches, filterRows: filterRows, summarize: summarize,
    asCSV: asCSV, highlightSegments: highlightSegments, governmentEntityLabel: governmentEntityLabel};
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  root.TechMoneyLobbying = api;
  if (typeof document === "undefined" || !document.getElementById("lobbying-explorer")) return;

  function node(tag, text, className) {
    var element = document.createElement(tag);
    if (text != null) element.textContent = text;
    if (className) element.className = className;
    return element;
  }

  var data = root.TechMoneyLobbyingData;
  var status = document.getElementById("result-counts");
  if (!data) {
    status.textContent = "The saved index could not load. Reload this page or use the CSV download below.";
    status.setAttribute("role", "alert");
    document.querySelectorAll("#lobbying-filters input, #lobbying-filters select, #lobbying-filters button").forEach(function (el) { el.disabled = true; });
    return;
  }
  var keys = ["year", "quarter", "watchlist", "topic", "client", "evidence", "query"];
  var controls = {};
  keys.forEach(function (key) { controls[key] = document.getElementById("filter-" + key); });
  function option(select, value, label) { var el = node("option", label); el.value = value; select.appendChild(el); }
  data.metadata.sources.slice().sort(function (a, b) { return b.year - a.year; }).forEach(function (source) {
    option(controls.year, String(source.year), String(source.year) + " · as of " + (source.snapshot_at || "unknown").slice(0, 10));
  });
  data.watchlists.forEach(function (w) { option(controls.watchlist, w.id, w.name); });
  option(controls.topic, "", "Any indexed topic");
  data.topics.forEach(function (topic) { option(controls.topic, topic.id, topic.label); });
  var suggestions = document.getElementById("client-suggestions");
  Array.from(new Set(data.activities.map(function (item) { return item.organization_name || item.client_name; }))).sort().forEach(function (name) { option(suggestions, name, name); });
  var topicById = {};
  data.topics.forEach(function (topic) { topicById[topic.id] = topic; });
  var defaults = {year: String(Math.max.apply(null, data.metadata.sources.map(function (s) { return s.year; }))), topic: data.topics[0].id,
    watchlist: data.watchlists.some(function (w) { return w.id === "selected"; }) ? "selected" : ""};
  var params = new URLSearchParams(root.location.search);
  keys.forEach(function (key) { controls[key].value = params.has(key) ? params.get(key) : (defaults[key] || ""); });
  if (!controls.year.value) controls.year.value = defaults.year;
  var currentPage = 0, pageSize = 20, filtered = [];
  var results = document.getElementById("lobbying-results");
  var previous = document.getElementById("previous-page"), next = document.getElementById("next-page");
  function quantity(count, singular, plural) { return count.toLocaleString() + " " + (count === 1 ? singular : plural); }
  function getFilters() { var f = {}; keys.forEach(function (key) { f[key] = controls[key].value; }); return f; }
  function renderCard(item, filters) {
    var article = node("article", null, "lobbying-result");
    article.appendChild(node("h2", item.organization_name || item.client_name));
    article.appendChild(node("p", item.year + " Q" + item.quarter + " · " + item.issue_label, "result-meta"));
    article.appendChild(node("p", "Reported client: " + item.client_name + " · Filed by: " + item.registrant_name));
    var selectedMatches = eligibleMatches(item, filters);
    var evidence = [];
    selectedMatches.forEach(function (match) {
      evidence = evidence.concat(match.evidence);
      if (match.requires) {
        var dependency = item.matches.find(function (m) { return m.topic_id === match.requires; });
        if (dependency) evidence = evidence.concat(dependency.evidence);
      }
    });
    function passageNode(text, spans) {
      var passage = node("p", null, "passage");
      highlightSegments(text, spans).forEach(function (part) {
        passage.appendChild(node(part.matched ? "mark" : "span", part.text));
      });
      return passage;
    }
    var chars = Array.from(item.description);
    if (chars.length > 900) {
      var first = evidence.length ? Math.min.apply(null, evidence.map(function (e) { return e.start; })) : 0;
      var start = Math.max(0, first - 160), end = Math.min(chars.length, start + 750);
      var offset = start > 0 ? 1 : 0;
      var excerpt = (start > 0 ? "…" : "") + chars.slice(start, end).join("") + (end < chars.length ? "…" : "");
      var clipped = evidence.filter(function (e) { return e.start < end && e.end > start; }).map(function (e) {
        return {start: Math.max(e.start, start) - start + offset, end: Math.min(e.end, end) - start + offset};
      });
      article.appendChild(passageNode(excerpt, clipped));
      var full = node("details"); full.appendChild(node("summary", "Read full issue description"));
      full.appendChild(passageNode(item.description, evidence)); article.appendChild(full);
    } else article.appendChild(passageNode(item.description, evidence));
    var labels = node("div", null, "match-labels");
    selectedMatches.forEach(function (match) {
      var kind = {direct: "explicit phrase", ambiguous: "abbreviation · check context", context: "co-occurring language", manual: "reviewer added"}[match.kind];
      var decision = match.decision === "unreviewed" ? "not reviewed" : match.decision;
      labels.appendChild(node("span", topicById[match.topic_id].label + " · " + kind + " · " + decision +
        (match.dependency_rejected ? " · prerequisite rejected or missing" : ""), "match-label"));
    });
    article.appendChild(labels);
    var detail = node("details");
    detail.appendChild(node("summary", "People, government entities, and match evidence"));
    detail.appendChild(node("p", "Lobbyists listed: " + (item.lobbyists.map(function (p) { return p.name; }).join("; ") || "None listed in this issue entry")));
    detail.appendChild(node("p", governmentEntityLabel(item.government_entity_scope) + ": " +
      (item.government_entities.map(function (e) { return e.name; }).join("; ") || "None listed")));
    detail.appendChild(node("p", "Company mapping: " + ({name_seed: "initial exact-name seed; source-ID review pending", reviewed: "reviewed source-ID mapping", unmapped: "reported name only; organization identity not consolidated", rejected_mapping: "suggested mapping rejected; reported name retained"}[item.organization_status])));
    var list = node("ul", null, "evidence-list");
    selectedMatches.forEach(function (match) {
      match.evidence.forEach(function (e) { list.appendChild(node("li", '“' + e.text + '” · rule ' + e.rule_id)); });
      if (match.review) list.appendChild(node("li", "Review: " + match.decision + " by " + match.review.reviewer + " (" + match.review.reviewed_at + "). " + match.review.notes));
    });
    detail.appendChild(list);
    detail.appendChild(node("p", "Issue ID: " + item.activity_id + " · " + item.revision_count + " saved report version(s). Latest selected version posted " + item.posted_at.slice(0, 10) + ".", "result-meta"));
    article.appendChild(detail);
    var link = node("a", "Read original filing ↗");
    // Source URLs are validated by the exporter; retain a browser-side scheme check.
    if (item.filing_url.startsWith("https://lda.gov/")) link.href = item.filing_url;
    link.target = "_blank"; link.rel = "noopener noreferrer";
    article.appendChild(link);
    return article;
  }
  function render() {
    var filters = getFilters();
    filtered = filterRows(data.activities, filters, data.watchlists);
    var totals = summarize(filtered), pages = Math.max(1, Math.ceil(filtered.length / pageSize));
    currentPage = Math.min(currentPage, pages - 1);
    status.textContent = quantity(totals.activities, "issue entry", "issue entries") + " · " +
      quantity(totals.reports, "distinct report", "distinct reports") + " · " + quantity(totals.clients, "client source record", "client source records");
    document.getElementById("topic-note").textContent = filters.topic ? topicById[filters.topic].description : "Results match at least one saved topic definition. Search operates within this index.";
    var watchlist = data.watchlists.find(function (w) { return w.id === filters.watchlist; });
    document.getElementById("watchlist-note").textContent = watchlist ? "Selected watchlist: " + data.organizations.filter(function (o) {
      return watchlist.organization_ids.includes(o.id);
    }).map(function (o) { return o.name; }).join(", ") + ". Exact-name seeds may miss subsidiaries and intermediaries." : "All sectors are included. Watchlists use initial exact-name mappings; unmapped clients retain their reported names.";
    var quarters = document.getElementById("quarter-counts"); quarters.replaceChildren();
    var acrossQuarters = filterRows(data.activities, Object.assign({}, filters, {quarter:""}), data.watchlists);
    var source = data.metadata.sources.find(function (s) { return String(s.year) === filters.year; });
    for (var q = 1; q <= 4; q++) {
      var count = summarize(acrossQuarters.filter(function (r) { return r.quarter === q; })).reports;
      var label = source && !source.periods_with_reports.includes(q) ? "no quarterly reports in snapshot" : quantity(count, "matching report", "matching reports");
      var period = source && (source.quarters || []).find(function (p) { return p.quarter === q; });
      if (period && period.status === "not_started") label = "period has not started";
      if (period && period.status === "in_progress") label += " · quarter still in progress";
      quarters.appendChild(node("span", "Q" + q + ": " + label));
    }
    results.replaceChildren();
    if (!filtered.length) results.appendChild(node("p", "No indexed passages match these filters. Try a broader client or topic selection. This is not evidence that no lobbying occurred.", "lobbying-empty"));
    filtered.slice(currentPage * pageSize, (currentPage + 1) * pageSize).forEach(function (item) { results.appendChild(renderCard(item, filters)); });
    previous.disabled = currentPage === 0; next.disabled = currentPage >= pages - 1;
    document.getElementById("page-count").textContent = "Page " + (currentPage + 1) + " of " + pages;
    document.getElementById("download-results").disabled = !filtered.length;
    var url = new URL(root.location.href);
    keys.forEach(function (key) { if (filters[key]) url.searchParams.set(key, filters[key]); else url.searchParams.delete(key); });
    try { root.history.replaceState(null, "", url); } catch (_) { /* Local-file previews may restrict history. */ }
  }
  var timer;
  document.getElementById("lobbying-filters").addEventListener("submit", function (event) { event.preventDefault(); });
  keys.forEach(function (key) {
    controls[key].addEventListener("input", function () { clearTimeout(timer); currentPage = 0; timer = setTimeout(render, 100); });
  });
  document.getElementById("lobbying-filters").addEventListener("reset", function (event) {
    event.preventDefault(); clearTimeout(timer);
    keys.forEach(function (key) { controls[key].value = defaults[key] || ""; }); currentPage = 0; render();
  });
  previous.addEventListener("click", function () { currentPage--; render(); results.scrollIntoView({block: "start"}); });
  next.addEventListener("click", function () { currentPage++; render(); results.scrollIntoView({block: "start"}); });
  document.getElementById("download-results").addEventListener("click", function () {
    clearTimeout(timer); render();
    var blob = new Blob([asCSV(filtered, getFilters(), data.metadata.rules_version)], {type: "text/csv;charset=utf-8"});
    var url = URL.createObjectURL(blob), anchor = node("a");
    anchor.href = url; anchor.download = "lobbying-matches-" + controls.year.value + ".csv";
    document.body.appendChild(anchor); anchor.click(); anchor.remove();
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
  });
  render();
})(typeof window === "undefined" ? globalThis : window);
