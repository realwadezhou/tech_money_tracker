const assert = require('node:assert/strict');
const test = require('node:test');
const { series, formatMoneyShort, axisTop } = require('../frontend/assets/lobbying_spending.js');

const cell = total => ({ total, basis: 'in_house', in_house_expenses: total, outside_firm_income: 0 });
const data = {
  quarters: [
    { id: '2025 Q1', year: 2025, quarter: 1, complete: true },
    { id: '2025 Q2', year: 2025, quarter: 2, complete: true },
    { id: '2025 Q3', year: 2025, quarter: 3, complete: false },
  ],
  companies: [
    { id: 'openai', name: 'OpenAI', quarters: [cell(100), cell(70), cell(5)] },
    { id: 'google', name: 'Google', quarters: [cell(560), null, null] },
  ],
};

test('chart series adds companies and leaves out incomplete quarters', () => {
  assert.deepEqual(series(data, '').map(p => [p.label, p.total]), [['2025 Q1', 660], ['2025 Q2', 70]]);
  assert.deepEqual(series(data, 'google').map(p => p.total), [560, 0]);
});

test('axis and money labels are readable', () => {
  assert.equal(axisTop(46690000), 50000000);
  assert.equal(axisTop(1970000), 2000000);
  assert.equal(axisTop(0), 1);
  assert.equal(formatMoneyShort(46690000), '$46.7M');
  assert.equal(formatMoneyShort(620000), '$620K');
});
