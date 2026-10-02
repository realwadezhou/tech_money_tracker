(function (root, factory) {
  "use strict";
  var api = factory();
  if (typeof module === "object" && module.exports) { module.exports = api; }
  else { root.TechMoneyLobbyingTopics = api; }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";

  // Complete quarters only. measure "tracked": tracked companies naming the topic;
  // "share": percent of all lobbying clients naming it.
  function series(data, topic, measure) {
    return data.quarters.map(function (quarter, index) {
      if (!quarter.complete) return null;
      var clients = data.all_clients[index];
      var value = measure === "share"
        ? (clients ? 100 * topic.all_clients_mentioning[index] / clients : 0)
        : topic.tracked_companies_mentioning[index];
      return {label: quarter.id, year: quarter.year, quarter: quarter.quarter, value: value,
        detail: measure === "share"
          ? topic.all_clients_mentioning[index].toLocaleString("en-US") + " of " + clients.toLocaleString("en-US") + " clients"
          : value + " of " + data.tracked_companies_active[index] + " tracked companies filing that quarter"};
    }).filter(Boolean);
  }

  // Shade level 0-4 for a company's number of issue entries in a quarter.
  function level(entries) {
    if (!entries) return 0;
    if (entries === 1) return 1;
    if (entries <= 3) return 2;
    if (entries <= 6) return 3;
    return 4;
  }

  // Companies ordered by how recently and how often they named the topic.
  function rankCompanies(topic, quarters) {
    var complete = quarters.map(function (q, i) { return q.complete ? i : -1; }).filter(function (i) { return i >= 0; });
    var recent = complete.slice(-4);
    var sum = function (company, indexes) {
      return indexes.reduce(function (total, i) { return total + company.entries[i]; }, 0);
    };
    return topic.companies.filter(function (c) { return sum(c, complete) > 0; }).map(function (c) {
      var quartersNamed = complete.filter(function (i) { return c.entries[i] > 0; });
      return {company: c, recent: sum(c, recent), quarters: quartersNamed.length,
        first: quarters[quartersNamed[0]].id};
    }).sort(function (a, b) {
      return b.recent - a.recent || b.quarters - a.quarters || a.company.name.localeCompare(b.company.name);
    });
  }

  function axisTop(maxValue) {
    var raw = (maxValue || 1) / 4;
    var magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
    var step = [1, 1.25, 1.5, 2, 2.5, 5, 10].map(function (f) { return f * magnitude; })
      .filter(function (s) { return s >= raw; })[0];
    return step * 4;
  }

  function svgNode(name, attrs, text) {
    var element = document.createElementNS(SVG_NS, name);
    Object.keys(attrs || {}).forEach(function (key) { element.setAttribute(key, String(attrs[key])); });
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function drawChart(svg, points, measure) {
    var width = Math.max(svg.clientWidth || 860, 300), height = width < 520 ? 240 : 320;
    var left = 46, right = 10, top = 14, bottom = 34;
    var peak = Math.max.apply(null, points.map(function (p) { return p.value; }).concat([0]));
    // Whole-number axis for company counts; readable steps for percentages.
    var max = measure === "share" ? axisTop(peak) : Math.max(4, Math.ceil(peak / 4) * 4);
    var slot = (width - left - right) / Math.max(points.length, 1);
    var y = function (value) { return top + (height - top - bottom) * (1 - value / max); };
    var label = function (value) { return measure === "share" ? Number(value.toFixed(2)) + "%" : String(Math.round(value)); };
    svg.setAttribute("viewBox", "0 0 " + width + " " + height);
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    for (var tick = 0; tick <= 4; tick++) {
      var value = max * tick / 4;
      svg.appendChild(svgNode("line", {"class": "spending-grid", x1: left, x2: width - right, y1: y(value), y2: y(value)}));
      svg.appendChild(svgNode("text", {x: left - 8, y: y(value) + 4, "text-anchor": "end"}, label(value)));
    }
    points.forEach(function (point, index) {
      var x = left + index * slot;
      var bar = svgNode("rect", {"class": "spending-bar", x: x + slot * 0.15, width: slot * 0.7,
        y: y(point.value), height: Math.max(0, y(0) - y(point.value))});
      bar.appendChild(svgNode("title", {}, point.label + ": " + point.detail));
      svg.appendChild(bar);
      if (point.quarter === 1 && (slot * 4 >= 44 || point.year % 2 === 0)) {
        svg.appendChild(svgNode("text", {x: x + slot / 2, y: height - 12, "text-anchor": "middle"}, String(point.year)));
      }
    });
  }

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function drawGrid(table, data, topic) {
    while (table.firstChild) table.removeChild(table.firstChild);
    var shown = data.quarters.map(function (q, i) { return q.complete ? i : -1; }).filter(function (i) { return i >= 0; });
    var head = table.appendChild(element("thead")).appendChild(element("tr"));
    head.appendChild(element("th", "topic-grid-name", "Company"));
    shown.forEach(function (i) {
      var q = data.quarters[i];
      var cell = element("th", "topic-grid-quarter", q.quarter === 1 ? "'" + String(q.year).slice(2) : "");
      cell.title = q.id;
      head.appendChild(cell);
    });
    head.appendChild(element("th", "topic-grid-count", "First named"));
    var body = table.appendChild(element("tbody"));
    var ranked = rankCompanies(topic, data.quarters);
    ranked.forEach(function (row) {
      var tr = body.appendChild(element("tr"));
      var name = tr.appendChild(element("th", "topic-grid-name", row.company.name));
      name.scope = "row";
      name.title = data.sector_labels[row.company.sector] || "";
      shown.forEach(function (i) {
        var entries = row.company.entries[i];
        var cell = tr.appendChild(element("td", "topic-cell topic-level-" + level(entries)));
        cell.title = row.company.name + ", " + data.quarters[i].id + ": " +
          (entries ? entries + (entries === 1 ? " issue entry" : " issue entries") : "not named");
      });
      tr.appendChild(element("td", "topic-grid-count", row.first));
    });
    return ranked;
  }

  function drawExamples(holder, ranked) {
    while (holder.firstChild) holder.removeChild(holder.firstChild);
    ranked.slice(0, 12).forEach(function (row) {
      var example = row.company.example;
      var block = holder.appendChild(element("div", "topic-example"));
      var heading = block.appendChild(element("p", "topic-example-meta"));
      heading.appendChild(element("strong", "", row.company.name));
      heading.appendChild(document.createTextNode(" · " + example.quarter + " · filed by " + example.filer + " · "));
      var link = heading.appendChild(element("a", "", "original filing"));
      link.href = example.url;
      link.rel = "noopener";
      block.appendChild(element("p", "topic-example-text", example.text));
    });
  }

  function start() {
    var holder = document.getElementById("topic-data");
    var select = document.getElementById("topic-select");
    var measure = document.getElementById("topic-measure");
    if (!holder || !select || !measure) return;
    var data = JSON.parse(holder.textContent);
    data.topics.forEach(function (topic) {
      var option = element("option", "", topic.label);
      option.value = topic.id;
      select.appendChild(option);
    });
    var requested = new URLSearchParams(window.location.search).get("topic");
    if (data.topics.some(function (t) { return t.id === requested; })) select.value = requested;

    var update = function () {
      var topic = data.topics.filter(function (t) { return t.id === select.value; })[0];
      var points = series(data, topic, measure.value);
      drawChart(document.getElementById("topic-chart"), points, measure.value);
      document.getElementById("topic-phrases").textContent = "Counts reports whose issue description contains " + topic.phrases + ".";
      document.getElementById("topic-chart-caption").textContent = measure.value === "share"
        ? "Percent of all lobbying clients, in every industry, with a report naming " + topic.label.toLowerCase() + ", by quarter."
        : "Number of tracked tech companies with a report naming " + topic.label.toLowerCase() + ", by quarter.";
      var ranked = drawGrid(document.getElementById("topic-grid"), data, topic);
      document.getElementById("topic-grid-title").textContent = "By company: " + ranked.length + " tracked companies have named it";
      drawExamples(document.getElementById("topic-examples"), ranked);
    };
    select.addEventListener("change", function () {
      window.history.replaceState(null, "", "?topic=" + encodeURIComponent(select.value) + "#explore");
      update();
    });
    measure.addEventListener("change", update);
    window.addEventListener("resize", update);
    update();
  }

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
  }

  return {series: series, level: level, rankCompanies: rankCompanies, axisTop: axisTop};
});
