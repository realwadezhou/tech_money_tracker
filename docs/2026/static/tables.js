(function () {
  "use strict";

  function cellText(row, index) {
    var cell = row.cells[index];
    return cell ? cell.textContent.trim() : "";
  }

  function numericValue(value) {
    var cleaned = value.replace(/[$,%]/g, "").replace(/,/g, "").trim();
    if (cleaned === "" || cleaned === "-") {
      return null;
    }
    var compact = cleaned.match(/^(-?\d+(?:\.\d+)?)([kmb])$/i);
    if (compact) {
      var multiplier = { k: 1000, m: 1000000, b: 1000000000 }[compact[2].toLowerCase()];
      return Number(compact[1]) * multiplier;
    }
    var parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
  }

  var INITIAL_SORT_HEADERS = [
    "tech receipts",
    "total ($)",
    "total",
    "tech-linked giving",
    "total receipts",
    "tech donors",
    "donors",
    "committees"
  ];

  function normalizedHeader(header) {
    return header.textContent.trim().toLowerCase();
  }

  function numericColumnValues(table, index) {
    var rows = Array.prototype.slice.call(table.tBodies[0].rows);
    if (rows.length < 3) {
      return [];
    }

    return rows.map(function (row) {
      return numericValue(cellText(row, index));
    });
  }

  function monotonicDirection(values) {
    if (values.length < 3 || values.some(function (value) { return value === null; })) {
      return null;
    }

    var ascending = true;
    var descending = true;
    var changed = false;

    for (var index = 1; index < values.length; index += 1) {
      if (values[index] < values[index - 1]) {
        ascending = false;
      }
      if (values[index] > values[index - 1]) {
        descending = false;
      }
      if (values[index] !== values[index - 1]) {
        changed = true;
      }
    }

    if (!changed) {
      return null;
    }
    if (descending) {
      return "descending";
    }
    return ascending ? "ascending" : null;
  }

  function markInitialSort(table) {
    var headers = Array.prototype.slice.call(table.tHead.rows[0].cells);
    var hasSort = headers.some(function (header) {
      var value = header.getAttribute("aria-sort");
      return value === "ascending" || value === "descending";
    });

    if (hasSort) {
      return;
    }

    INITIAL_SORT_HEADERS.some(function (target) {
      var headerIndex = headers.findIndex(function (header) {
        return normalizedHeader(header) === target;
      });

      if (headerIndex === -1) {
        return false;
      }

      var direction = monotonicDirection(numericColumnValues(table, headerIndex));
      if (!direction) {
        return false;
      }

      headers[headerIndex].setAttribute("aria-sort", direction);
      return true;
    });
  }

  function compareValues(a, b, direction) {
    var aNumber = numericValue(a);
    var bNumber = numericValue(b);
    var result;

    if (aNumber !== null && bNumber !== null) {
      result = aNumber - bNumber;
    } else {
      result = a.localeCompare(b, undefined, {
        numeric: true,
        sensitivity: "base"
      });
    }

    return direction === "desc" ? -result : result;
  }

  function filterTable(table, query) {
    var needle = query.trim().toLowerCase();
    Array.prototype.forEach.call(table.tBodies[0].rows, function (row) {
      var haystack = row.textContent.toLowerCase();
      row.hidden = needle !== "" && haystack.indexOf(needle) === -1;
    });
  }

  function enhanceTable(table) {
    var wrapper = table.parentElement;
    var control = wrapper ? wrapper.previousElementSibling : table.previousElementSibling;
    var filter = control ? control.querySelector(".table-filter") : null;

    function applyFilter() {
      filterTable(table, filter ? filter.value : "");
    }

    if (filter) {
      filter.addEventListener("input", applyFilter);
    }

    Array.prototype.forEach.call(table.tHead.rows[0].cells, function (header, index) {
      var button = header.querySelector(".sort-button");
      if (!button) {
        return;
      }

      button.setAttribute("aria-label", "Sort by " + button.textContent.trim());
      if (!header.hasAttribute("aria-sort")) {
        header.setAttribute("aria-sort", "none");
      }

      header.addEventListener("click", function () {
        var current = header.getAttribute("aria-sort");
        var direction = current === "ascending" ? "desc" : "asc";
        var rows = Array.prototype.slice.call(table.tBodies[0].rows);

        Array.prototype.forEach.call(table.tHead.rows[0].cells, function (cell) {
          cell.setAttribute("aria-sort", "none");
        });

        rows.sort(function (rowA, rowB) {
          return compareValues(cellText(rowA, index), cellText(rowB, index), direction);
        });

        rows.forEach(function (row) {
          table.tBodies[0].appendChild(row);
        });

        header.setAttribute("aria-sort", direction === "asc" ? "ascending" : "descending");
        applyFilter();
      });
    });

    markInitialSort(table);
  }

  document.addEventListener("DOMContentLoaded", function () {
    Array.prototype.forEach.call(document.querySelectorAll(".data-table"), enhanceTable);
  });
})();
