(function () {
  "use strict";

  var SVG_NS = "http://www.w3.org/2000/svg";
  var TEXT_COLOR = "#566170";
  var LINK_COLOR = "#315f9b";
  var BAR_COLOR = "#b7c9df";

  function createSvgNode(name, attrs) {
    var node = document.createElementNS(SVG_NS, name);
    Object.keys(attrs || {}).forEach(function (key) {
      node.setAttribute(key, String(attrs[key]));
    });
    return node;
  }

  function formatMoneyShort(value) {
    var abs = Math.abs(value);
    if (abs >= 1000000000) {
      return "$" + (value / 1000000000).toFixed(1) + "B";
    }
    if (abs >= 1000000) {
      return "$" + (value / 1000000).toFixed(1) + "M";
    }
    if (abs >= 1000) {
      return "$" + (value / 1000).toFixed(0) + "K";
    }
    return "$" + Math.round(value);
  }

  function formatDateLabel(value) {
    var date = new Date(value + "T00:00:00");
    if (Number.isNaN(date.getTime())) {
      return value;
    }
    return date.toLocaleDateString("en-US", {
      month: "short",
      year: "2-digit"
    });
  }

  function completeWeeklySeries(rows, displayStart) {
    var weekMs = 7 * 24 * 60 * 60 * 1000;
    var start = displayStart ? Date.parse(displayStart + "T00:00:00Z") : -Infinity;
    var datedRows = (rows || []).filter(function (row) {
      var date = Date.parse(row.week_end + "T00:00:00Z");
      return Number.isFinite(date) && date >= start;
    }).slice().sort(function (a, b) {
      return a.week_end.localeCompare(b.week_end);
    });
    var series = [];

    datedRows.forEach(function (row) {
      var previous = series[series.length - 1];
      if (previous) {
        var nextWeek = Date.parse(previous.week_end + "T00:00:00Z") + weekMs;
        var rowDate = Date.parse(row.week_end + "T00:00:00Z");
        while (nextWeek < rowDate) {
          series.push({
            week_end: new Date(nextWeek).toISOString().slice(0, 10),
            net_total: 0,
            cumulative_net_total: previous.cumulative_net_total
          });
          nextWeek += weekMs;
        }
      }
      series.push(row);
    });
    return series;
  }

  function renderWeeklyChart(targetId, rows, options) {
    var target = document.getElementById(targetId);
    if (!target) {
      return;
    }

    var series = completeWeeklySeries(rows, options && options.displayStart);

    if (!series.length) {
      target.textContent = "No chart data.";
      return;
    }

    var width = Math.max(260, target.clientWidth || 920);
    var height = 360;
    var margin = { top: 20, right: width < 500 ? 64 : 80, bottom: 58, left: width < 500 ? 64 : 72 };
    var innerWidth = width - margin.left - margin.right;
    var innerHeight = height - margin.top - margin.bottom;

    var weeklyValues = series.map(function (row) { return Number(row.net_total) || 0; });
    var cumulativeValues = series.map(function (row) { return Number(row.cumulative_net_total) || 0; });

    var weeklyMax = Math.max.apply(null, weeklyValues.concat([0]));
    var weeklyMin = Math.min.apply(null, weeklyValues.concat([0]));
    var cumulativeMax = Math.max.apply(null, cumulativeValues.concat([0]));
    var cumulativeMin = Math.min.apply(null, cumulativeValues.concat([0]));
    if (weeklyMax === weeklyMin) {
      weeklyMax = weeklyMin + 1;
    }
    if (cumulativeMax === cumulativeMin) {
      cumulativeMax = cumulativeMin + 1;
    }
    var weeklyRange = weeklyMax - weeklyMin;
    var cumulativeRange = cumulativeMax - cumulativeMin;
    var zeroY = margin.top + ((weeklyMax / weeklyRange) * innerHeight);
    var barStep = innerWidth / series.length;
    var barWidth = Math.max(2, barStep - 1);

    function x(index) {
      return margin.left + (index * barStep);
    }

    function yWeekly(value) {
      return margin.top + ((weeklyMax - value) / weeklyRange) * innerHeight;
    }

    function yCumulative(value) {
      return margin.top + ((cumulativeMax - value) / cumulativeRange) * innerHeight;
    }

    var svg = createSvgNode("svg", {
      viewBox: "0 0 " + width + " " + height,
      class: "chart-svg",
      role: "img",
      "aria-label": "Weekly tech-linked giving chart"
    });

    svg.appendChild(createSvgNode("line", {
      x1: margin.left,
      y1: zeroY,
      x2: margin.left + innerWidth,
      y2: zeroY,
      stroke: TEXT_COLOR,
      "stroke-width": 1
    }));

    svg.appendChild(createSvgNode("line", {
      x1: margin.left,
      y1: margin.top,
      x2: margin.left,
      y2: margin.top + innerHeight,
      stroke: TEXT_COLOR,
      "stroke-width": 1
    }));

    svg.appendChild(createSvgNode("line", {
      x1: margin.left + innerWidth,
      y1: margin.top,
      x2: margin.left + innerWidth,
      y2: margin.top + innerHeight,
      stroke: TEXT_COLOR,
      "stroke-width": 1
    }));

    [0, 0.25, 0.5, 0.75, 1].forEach(function (fraction) {
      var weeklyTick = weeklyMax - (weeklyRange * fraction);
      var weeklyTickY = yWeekly(weeklyTick);

      svg.appendChild(createSvgNode("line", {
        x1: margin.left - 4,
        y1: weeklyTickY,
        x2: margin.left,
        y2: weeklyTickY,
        stroke: TEXT_COLOR,
        "stroke-width": 1
      }));

      var leftLabel = createSvgNode("text", {
        x: margin.left - 8,
        y: weeklyTickY + 4,
        "text-anchor": "end",
        "font-size": 12
      });
      leftLabel.textContent = formatMoneyShort(weeklyTick);
      svg.appendChild(leftLabel);

      var cumulativeTick = cumulativeMax - (cumulativeRange * fraction);
      var rightTickY = yCumulative(cumulativeTick);

      svg.appendChild(createSvgNode("line", {
        x1: margin.left + innerWidth,
        y1: rightTickY,
        x2: margin.left + innerWidth + 4,
        y2: rightTickY,
        stroke: TEXT_COLOR,
        "stroke-width": 1
      }));

      var rightLabel = createSvgNode("text", {
        x: margin.left + innerWidth + 8,
        y: rightTickY + 4,
        "text-anchor": "start",
        "font-size": 12
      });
      rightLabel.textContent = formatMoneyShort(cumulativeTick);
      svg.appendChild(rightLabel);
    });

    series.forEach(function (row, index) {
      var weeklyValue = Number(row.net_total) || 0;
      var y = yWeekly(Math.max(weeklyValue, 0));
      var heightValue = Math.abs(yWeekly(weeklyValue) - zeroY);
      var rect = createSvgNode("rect", {
        x: x(index),
        y: weeklyValue >= 0 ? y : zeroY,
        width: barWidth,
        height: heightValue,
        fill: BAR_COLOR
      });
      svg.appendChild(rect);
    });

    var linePoints = series.map(function (row, index) {
      return (x(index) + (barWidth / 2)) + "," + yCumulative(Number(row.cumulative_net_total) || 0);
    }).join(" ");

    svg.appendChild(createSvgNode("polyline", {
      points: linePoints,
      fill: "none",
      stroke: LINK_COLOR,
      "stroke-width": 2
    }));
    if (series.length === 1) {
      svg.appendChild(createSvgNode("circle", {
        cx: x(0) + (barWidth / 2),
        cy: yCumulative(cumulativeValues[0]),
        r: 3,
        fill: LINK_COLOR
      }));
    }

    var xTickEvery = Math.max(1, Math.ceil(series.length / Math.max(2, Math.floor(innerWidth / 85))));
    series.forEach(function (row, index) {
      if (index % xTickEvery !== 0 && index !== series.length - 1) {
        return;
      }
      var tickX = x(index) + (barWidth / 2);
      // The final week is always labeled. Omit a nearby regular tick so
      // its month label does not collide with that endpoint label.
      if (index > 0 && index < series.length - 1 &&
          x(series.length - 1) + (barWidth / 2) - tickX < 50) {
        return;
      }
      svg.appendChild(createSvgNode("line", {
        x1: tickX,
        y1: margin.top + innerHeight,
        x2: tickX,
        y2: margin.top + innerHeight + 4,
        stroke: TEXT_COLOR,
        "stroke-width": 1
      }));

      var label = createSvgNode("text", {
        x: tickX,
        y: margin.top + innerHeight + 18,
        "text-anchor": "middle",
        "font-size": 12
      });
      label.textContent = formatDateLabel(row.week_end);
      svg.appendChild(label);
    });

    var leftAxisLabel = createSvgNode("text", {
      x: margin.left,
      y: margin.top - 6,
      "text-anchor": "start",
      "font-size": 13
    });
    leftAxisLabel.textContent = "Weekly";
    svg.appendChild(leftAxisLabel);

    var rightAxisLabel = createSvgNode("text", {
      x: margin.left + innerWidth,
      y: margin.top - 6,
      "text-anchor": "end",
      "font-size": 13
    });
    rightAxisLabel.textContent = "Cumulative";
    svg.appendChild(rightAxisLabel);

    target.innerHTML = "";
    target.appendChild(svg);
  }

  function loadWeeklyChart(targetId, url, options) {
    var target = document.getElementById(targetId);
    if (!target) {
      return;
    }

    fetch(url, { cache: "no-store" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Failed to load chart data");
        }
        return response.json();
      })
      .then(function (rows) {
        renderWeeklyChart(targetId, rows, options || {});
        if (typeof ResizeObserver !== "undefined") {
          var previousWidth = target.clientWidth;
          var observer = new ResizeObserver(function () {
            if (target.clientWidth !== previousWidth) {
              previousWidth = target.clientWidth;
              renderWeeklyChart(targetId, rows, options || {});
            }
          });
          observer.observe(target);
        }
      })
      .catch(function () {
        target.textContent = "Chart data unavailable.";
      });
  }

  window.TechMoneyCharts = {
    renderWeeklyChart: renderWeeklyChart,
    loadWeeklyChart: loadWeeklyChart
  };
})();
