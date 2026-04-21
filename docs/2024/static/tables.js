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
    var parsed = Number(cleaned);
    return Number.isFinite(parsed) ? parsed : null;
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
      header.setAttribute("aria-sort", "none");

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
  }

  document.addEventListener("DOMContentLoaded", function () {
    Array.prototype.forEach.call(document.querySelectorAll(".data-table"), enhanceTable);
  });
})();
