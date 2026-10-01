const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function element(name, text = "") {
  return {
    name, textContent: text, children: [], attrs: {}, events: {},
    setAttribute(key, value) { this.attrs[key] = String(value); },
    getAttribute(key) { return this.attrs[key] ?? null; },
    hasAttribute(key) { return key in this.attrs; },
    appendChild(child) { this.children.push(child); },
    addEventListener(event, callback) { this.events[event] = callback; },
  };
}

function loadAsset(name, document, globals = {}) {
  const context = { document, window: {}, ...globals };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../frontend/assets", name), "utf8"), context);
  return context.window;
}

function chart(rows, options, width) {
  const target = element("div");
  target.clientWidth = width;
  const document = {
    getElementById: () => target,
    createElementNS: (_namespace, name) => element(name),
  };
  loadAsset("charts.js", document).TechMoneyCharts.renderWeeklyChart("chart", rows, options);
  return target.children[0];
}

test("weekly chart preserves missing weeks and carries the cumulative balance through them", () => {
  const svg = chart([
    { week_end: "2026-03-22", net_total: 20, cumulative_net_total: 130 },
    { week_end: "2026-03-01", net_total: 10, cumulative_net_total: 110 },
  ], { displayStart: "2026-01-01" });
  const bars = svg.children.filter(node => node.name === "rect");
  assert.equal(bars.length, 4);
  assert.equal(Number(bars[1].attrs.height), 0);
  assert.equal(Number(bars[2].attrs.height), 0);
  const points = svg.children.find(node => node.name === "polyline").attrs.points.split(" ").map(p => p.split(",").map(Number));
  assert.equal(points[0][1], points[1][1]);
  assert.equal(points[1][1], points[2][1]);
  assert.ok(points[3][1] < points[2][1]);
  assert.ok(Math.abs((points[3][0] - points[0][0]) / (points[1][0] - points[0][0]) - 3) < 1e-9);
});

test("refund-heavy cumulative lines remain inside the plot", () => {
  const svg = chart([
    { week_end: "2026-01-04", net_total: -100, cumulative_net_total: -100 },
    { week_end: "2026-01-11", net_total: 25, cumulative_net_total: -75 },
    { week_end: "2026-01-18", net_total: 100, cumulative_net_total: 25 },
  ]);
  const points = svg.children.find(node => node.name === "polyline").attrs.points.split(" ").map(p => p.split(",").map(Number));
  points.forEach(([, y]) => assert.ok(y >= 20 && y <= 302, `Cumulative y=${y} is outside the plot`));
  assert.ok(points[0][1] > points[1][1]);
  assert.ok(points[1][1] > points[2][1]);
});

test("one zero-activity week draws no positive bar and has a visible cumulative point", () => {
  const svg = chart([{ week_end: "2026-01-04", net_total: 0, cumulative_net_total: 0 }]);
  assert.equal(Number(svg.children.find(node => node.name === "rect").attrs.height), 0);
  assert.ok(svg.children.some(node => node.name === "circle"));
  assert.ok(svg.children.filter(node => node.name === "text").every(node => !node.textContent.includes("$-")));
});

test("chart display start excludes early transactions without resetting the cumulative balance", () => {
  const svg = chart([
    { week_end: "2024-12-29", net_total: 500, cumulative_net_total: 500 },
    { week_end: "2025-01-05", net_total: 100, cumulative_net_total: 600 },
  ], { displayStart: "2025-01-01" });
  assert.equal(svg.children.filter(node => node.name === "rect").length, 1);
  assert.ok(svg.children.some(node => node.name === "text" && node.textContent === "$600"));
});

test("the final chart date does not overlap the preceding regular date label", () => {
  const rows = Array.from({ length: 83 }, (_, index) => ({
    week_end: new Date(Date.UTC(2025, 0, 5 + index * 7)).toISOString().slice(0, 10),
    net_total: 1, cumulative_net_total: index + 1,
  }));
  const svg = chart(rows);
  const labels = svg.children.filter(node => node.name === "text" && Number(node.attrs.y) === 320);
  assert.ok(labels.length > 2);
  for (let index = 1; index < labels.length; index++) {
    assert.ok(Number(labels[index].attrs.x) - Number(labels[index - 1].attrs.x) >= 50);
  }
});

test("phone-width charts retain readable labels without dropping weekly data", () => {
  const rows = Array.from({ length: 83 }, (_, index) => ({
    week_end: new Date(Date.UTC(2025, 0, 5 + index * 7)).toISOString().slice(0, 10),
    net_total: 1, cumulative_net_total: index + 1,
  }));
  for (const width of [260, 320, 390]) {
    const svg = chart(rows, {}, width);
    assert.equal(svg.attrs.viewBox, `0 0 ${width} 360`);
    assert.equal(svg.children.filter(node => node.name === "rect").length, rows.length);
    const labels = svg.children.filter(node => node.name === "text");
    labels.forEach(node => {
      assert.ok(Number(node.attrs["font-size"]) >= 12);
      assert.ok(Number(node.attrs.x) >= 0 && Number(node.attrs.x) <= width);
    });
    const dates = labels.filter(node => Number(node.attrs.y) === 320);
    assert.ok(dates.length >= 2);
    for (let index = 1; index < dates.length; index++) {
      assert.ok(Number(dates[index].attrs.x) - Number(dates[index - 1].attrs.x) >= 50);
    }
    const points = svg.children.find(node => node.name === "polyline").attrs.points.split(" ").map(point => point.split(",").map(Number));
    assert.equal(points.length, rows.length);
    points.forEach(([x, y]) => assert.ok(x >= 0 && x <= width && y >= 20 && y <= 302));
  }
});

test("chart resizing redraws cached data once when the available width changes", async () => {
  const target = element("div");
  target.clientWidth = 320;
  Object.defineProperty(target, "innerHTML", { set() { this.children = []; } });
  const document = {
    getElementById: () => target,
    createElementNS: (_namespace, name) => element(name),
  };
  let fetchCount = 0;
  let onResize;
  let observedTarget;
  const rows = [{ week_end: "2026-01-04", net_total: 25, cumulative_net_total: 125 }];
  const api = loadAsset("charts.js", document, {
    fetch: async () => { fetchCount++; return { ok: true, json: async () => rows }; },
    ResizeObserver: class {
      constructor(callback) { onResize = callback; }
      observe(node) { observedTarget = node; }
    },
  }).TechMoneyCharts;
  api.loadWeeklyChart("chart", "data/weekly.json");
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(observedTarget, target);
  const original = target.children[0];
  assert.equal(original.attrs.viewBox, "0 0 320 360");
  onResize();
  assert.equal(target.children[0], original);
  target.clientWidth = 680;
  onResize();
  assert.equal(target.children.length, 1);
  assert.notEqual(target.children[0], original);
  assert.equal(target.children[0].attrs.viewBox, "0 0 680 360");
  assert.ok(target.children[0].children.some(node => node.name === "text" && node.textContent === "$125"));
  assert.equal(fetchCount, 1);
});

test("table search spans cell boundaries and survives numeric sorting", () => {
  const filter = element("input");
  filter.value = "";
  const headers = ["Name", "State", "Total"].map(text => {
    const header = element("th", text);
    const button = element("button", text);
    header.querySelector = () => button;
    return header;
  });
  const rows = [["Alice Smith", "California", "$2,000"], ["Bob", "New York", "$500"], ["Carol", "Virginia", "$10,000"]].map(values => ({
    cells: values.map(value => element("td", value)),
    textContent: values.join(""),
  }));
  const body = {
    rows,
    appendChild(row) { this.rows.splice(this.rows.indexOf(row), 1); this.rows.push(row); },
  };
  const table = {
    tHead: { rows: [{ cells: headers }] }, tBodies: [body],
    parentElement: { previousElementSibling: { querySelector: () => filter } },
  };
  const document = {
    querySelectorAll: () => [table],
    addEventListener(_name, callback) { this.ready = callback; },
  };
  loadAsset("tables.js", document);
  document.ready();
  filter.value = "smith california";
  filter.events.input();
  assert.deepEqual(body.rows.map(row => row.hidden), [false, true, true]);
  headers[2].events.click();
  assert.deepEqual(body.rows.map(row => row.cells[2].textContent), ["$500", "$2,000", "$10,000"]);
  assert.deepEqual(body.rows.map(row => row.hidden), [true, false, true]);
  headers[2].events.click();
  assert.deepEqual(body.rows.map(row => row.cells[2].textContent), ["$10,000", "$2,000", "$500"]);
  filter.value = "";
  filter.events.input();
  assert.ok(body.rows.every(row => !row.hidden));
});
