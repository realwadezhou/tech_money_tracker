(function (root, factory) {
  "use strict";
  var api = factory();
  if (typeof module === "object" && module.exports) { module.exports = api; }
  else { root.TechMoneyLobbyingSpending = api; }
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";

  // Complete quarters only, as {label, year, quarter, total} for one company or all of them.
  function series(data, companyId) {
    var companies = data.companies.filter(function (c) { return !companyId || c.id === companyId; });
    return data.quarters.map(function (quarter, index) {
      if (!quarter.complete) return null;
      var total = 0;
      companies.forEach(function (company) {
        var cell = company.quarters[index];
        if (cell) total += cell.total;
      });
      return {label: quarter.id, year: quarter.year, quarter: quarter.quarter, total: total};
    }).filter(Boolean);
  }

  function formatMoneyShort(value) {
    if (value >= 1000000) return "$" + (value / 1000000).toFixed(1) + "M";
    if (value >= 1000) return "$" + Math.round(value / 1000) + "K";
    return "$" + Math.round(value);
  }

  // Round the axis top up to the next readable step without leaving the chart half empty.
  function axisTop(maxValue) {
    var raw = (maxValue || 1) / 4;
    var magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
    var step = [1, 1.25, 1.5, 2, 2.5, 5, 10].map(function (f) { return f * magnitude; })
      .filter(function (s) { return s >= raw; })[0];
    return step * 4;
  }

  function node(name, attrs, text) {
    var element = document.createElementNS(SVG_NS, name);
    Object.keys(attrs || {}).forEach(function (key) { element.setAttribute(key, String(attrs[key])); });
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function draw(svg, points) {
    // Draw at the element's real width so labels stay a readable size on phones.
    var width = Math.max(svg.clientWidth || 860, 300), height = width < 520 ? 240 : 320;
    var left = 58, right = 10, top = 14, bottom = 34;
    var max = axisTop(Math.max.apply(null, points.map(function (p) { return p.total; }).concat([0])));
    var slot = (width - left - right) / Math.max(points.length, 1);
    var y = function (value) { return top + (height - top - bottom) * (1 - value / max); };
    svg.setAttribute("viewBox", "0 0 " + width + " " + height);
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    for (var tick = 0; tick <= 4; tick++) {
      var value = max * tick / 4;
      svg.appendChild(node("line", {"class": "spending-grid", x1: left, x2: width - right, y1: y(value), y2: y(value)}));
      svg.appendChild(node("text", {x: left - 8, y: y(value) + 4, "text-anchor": "end"}, formatMoneyShort(value)));
    }
    points.forEach(function (point, index) {
      var x = left + index * slot;
      var bar = node("rect", {"class": "spending-bar", x: x + slot * 0.15, width: slot * 0.7,
        y: y(point.total), height: Math.max(0, y(0) - y(point.total))});
      bar.appendChild(node("title", {}, point.label + ": $" + Math.round(point.total).toLocaleString("en-US")));
      svg.appendChild(bar);
      // On narrow screens there is room for every other year only.
      if (point.quarter === 1 && (slot * 4 >= 44 || point.year % 2 === 0)) {
        svg.appendChild(node("text", {x: x + slot / 2, y: height - 12, "text-anchor": "middle"}, String(point.year)));
      }
    });
  }

  function start() {
    var holder = document.getElementById("spending-data");
    var svg = document.getElementById("spending-chart");
    var select = document.getElementById("spending-company");
    if (!holder || !svg || !select) return;
    var data = JSON.parse(holder.textContent);
    data.companies.forEach(function (company) {
      var option = document.createElement("option");
      option.value = company.id;
      option.textContent = company.name;
      select.appendChild(option);
    });
    var update = function () { draw(svg, series(data, select.value)); };
    select.addEventListener("change", update);
    window.addEventListener("resize", update);
    update();
  }

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
  }

  return {series: series, formatMoneyShort: formatMoneyShort, axisTop: axisTop};
});
