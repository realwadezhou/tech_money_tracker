const assert = require('node:assert/strict');
const test = require('node:test');
const { series, level, rankCompanies } = require('../frontend/assets/lobbying_topics.js');

const quarters = [
  { id: '2025 Q1', year: 2025, quarter: 1, complete: true },
  { id: '2025 Q2', year: 2025, quarter: 2, complete: true },
  { id: '2025 Q3', year: 2025, quarter: 3, complete: false },
];
const topic = {
  id: 'ai', label: 'Artificial intelligence',
  all_clients_mentioning: [50, 100, 10], tracked_companies_mentioning: [2, 1, 1],
  companies: [
    { id: 'old', name: 'Old Co', entries: [3, 0, 0] },
    { id: 'new', name: 'New Co', entries: [1, 4, 9] },
    { id: 'late', name: 'Late Co', entries: [0, 0, 2] },
  ],
};
const data = { quarters, all_clients: [1000, 2000, 500], tracked_companies_active: [10, 12, 3], topics: [topic] };

test('series leaves out incomplete quarters and computes the share of all clients', () => {
  assert.deepEqual(series(data, topic, 'tracked').map(p => [p.label, p.value]), [['2025 Q1', 2], ['2025 Q2', 1]]);
  assert.deepEqual(series(data, topic, 'share').map(p => p.value), [5, 5]);
  assert.equal(series(data, topic, 'tracked')[0].detail, '2 of 10 tracked companies filing that quarter');
});

test('companies are ranked by recent mentions and incomplete quarters are ignored', () => {
  const ranked = rankCompanies(topic, quarters);
  assert.deepEqual(ranked.map(r => r.company.id), ['new', 'old']);
  assert.deepEqual([ranked[0].recent, ranked[0].quarters, ranked[0].first], [5, 2, '2025 Q1']);
});

test('shade levels step up with the number of entries', () => {
  assert.deepEqual([0, 1, 2, 3, 4, 6, 7, 40].map(level), [0, 1, 2, 2, 3, 3, 4, 4]);
});
