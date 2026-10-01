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
    { id: 'openai', name: 'OpenAI', sector: 'ai', quarters: [cell(100), cell(70), cell(5)] },
    { id: 'google', name: 'Google', sector: 'tech_giant', quarters: [cell(560), null, null] },
    { id: 'anthropic', name: 'Anthropic', sector: 'ai', quarters: [cell(1), cell(2), null] },
  ],
};

test('chart series adds companies and leaves out incomplete quarters', () => {
  assert.deepEqual(series(data, '').map(p => [p.label, p.total]), [['2025 Q1', 661], ['2025 Q2', 72]]);
  assert.deepEqual(series(data, 'sector:ai').map(p => p.total), [101, 72]);
  assert.deepEqual(series(data, 'google').map(p => p.total), [560, 0]);
});

test('axis and money labels are readable', () => {
  assert.equal(axisTop(46690000), 50000000);
  assert.equal(axisTop(1970000), 2000000);
  assert.equal(axisTop(0), 1);
  assert.equal(formatMoneyShort(46690000), '$46.69M');
  assert.equal(formatMoneyShort(3750000), '$3.75M');
  assert.equal(formatMoneyShort(5000000), '$5M');
  assert.equal(formatMoneyShort(620000), '$620K');
});
