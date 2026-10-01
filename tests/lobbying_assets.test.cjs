const assert = require('node:assert/strict');
const test = require('node:test');
const { filterRows, summarize, asCSV, highlightSegments, governmentEntityLabel } = require('../frontend/assets/lobbying.js');

const ai = { topic_id: 'ai', kind: 'direct', decision: 'unreviewed', evidence: [{start: 0, end: 23, text: 'Artificial intelligence'}] };
const copyright = { topic_id: 'copyright', requires: 'ai', kind: 'context', decision: 'unreviewed', evidence: [] };
function row(id, changes = {}) {
  return { activity_id: id, filing_uuid: 'report-a', year: 2025, quarter: 1, client_source_key: '1:2',
    client_name: 'Example University', organization_name: null, organization_id: null, registrant_name: 'Example Firm',
    description: 'Artificial intelligence and copyright', issue_label: 'Science',
    lobbyists: [{id: '3', name: 'Example Person'}], government_entities: [{id:'4', name: 'Example Agency'}],
    filing_url: 'https://lda.gov/filings/public/filing/a/print/', matches: [ai, copyright], ...changes };
}

test('organization and topic filters remain independent and counts dedupe reports', () => {
  const items = [row('a'), row('b'), row('c', {organization_id:'openai', organization_name:'OpenAI',
    client_name:'OPENAI OPCO, LLC', client_source_key:'1:3', filing_uuid:'report-b'})];
  const lists = [{id:'selected', organization_ids:['openai']}];
  assert.equal(filterRows(items, {topic:'ai', year:'2025'}, lists).length, 3);
  assert.deepEqual(summarize(items), {activities:3, reports:2, clients:2});
  const filtered = filterRows(items, {topic:'copyright', watchlist:'selected', client:'openai', query:'agency'}, lists);
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].activity_id, 'c');
  assert.equal(filterRows(items, {quarter:'2'}, lists).length, 0);
});

test('rejected prerequisites, uncertain reviews, and ambiguous matches have honest filters', () => {
  const items = [row('explicit'), row('abbreviation', {matches:[{...ai, kind:'ambiguous'}]}),
    row('uncertain', {matches:[{...ai, decision:'uncertain'}]}),
    row('rejected', {matches:[{...ai, decision:'rejected'}, {...copyright, dependency_rejected:true}]})];
  assert.deepEqual(filterRows(items, {evidence:'explicit'}, []).map(r=>r.activity_id), ['explicit']);
  assert.deepEqual(filterRows(items, {evidence:'ambiguous'}, []).map(r=>r.activity_id), ['abbreviation','uncertain']);
  assert.deepEqual(filterRows(items, {evidence:'rejected', topic:'copyright'}, []).map(r=>r.activity_id), ['rejected']);
  assert.equal(filterRows(items, {evidence:'accepted'}, []).length, 0);
  assert.equal(filterRows(items, {}, []).length, 3);
});

test('highlights use Unicode code points and combine overlapping matches', () => {
  const text = '🧠 AI and more';
  const segments = highlightSegments(text, [{start:2,end:4}, {start:2,end:4}]);
  assert.equal(segments.map(s=>s.text).join(''), text);
  assert.deepEqual(segments.filter(s=>s.matched), [{text:'AI',matched:true}]);
});

test('filtered CSV exports eligible matches, full evidence, and spreadsheet-safe text', () => {
  const csv = asCSV([row('a', {client_name:'=HYPERLINK("x")', description:'Line one\nLine "two"'})],
    {topic:'ai'}, 'rules-v1');
  assert.ok(csv.includes('"\'=HYPERLINK(""x"")"'));
  assert.ok(csv.includes('"Line one\nLine ""two"""'));
  assert.ok(csv.includes('Example Person'));
  assert.ok(csv.includes('rules-v1'));
  assert.ok(!csv.includes('"copyright","context"'));
});

test('filtered subtopic CSV preserves mapping uncertainty and rejected prerequisite evidence', () => {
  const item = row('a', {organization_id:'example', organization_name:'Example', organization_status:'name_seed',
    description_sha256:'passage-hash', government_entity_scope:'filing',
    matches:[{...ai, decision:'rejected'}, {...copyright, dependency_rejected:true}]});
  const csv = asCSV([item], {topic:'copyright', evidence:'rejected'}, 'rules-v1');
  const [header, values] = csv.trim().replace(/^\ufeff/, '').split('\r\n').map(line => line.slice(1, -1).split('","'));
  const exported = Object.fromEntries(header.map((key, index) => [key, values[index]]));
  assert.equal(exported.client_source_key, '1:2');
  assert.equal(exported.organization_status, 'name_seed');
  assert.equal(exported.description_sha256, 'passage-hash');
  assert.equal(exported.government_entity_scope, 'filing');
  assert.equal(exported.review_decision, 'unreviewed');
  assert.equal(exported.dependency_rejected, 'true');
  assert.equal(exported.requires, 'ai');
  assert.equal(exported.prerequisite_review_decision, 'rejected');
  assert.equal(exported.prerequisite_matched_phrases, 'Artificial intelligence');
});

test('government entity labels distinguish old filing scope and unverified scope', () => {
  assert.ok(governmentEntityLabel('filing').includes('whole filing (not this issue entry)'));
  assert.ok(governmentEntityLabel('issue_entry').includes('for this issue entry'));
  assert.ok(governmentEntityLabel('unknown').includes('scope unverified'));
  assert.ok(governmentEntityLabel(undefined).includes('scope unverified'));
});
