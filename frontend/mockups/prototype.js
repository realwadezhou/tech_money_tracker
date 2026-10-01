(() => {
  'use strict';
  const root = document.getElementById('tm-prototypes');
  const data = window.TECH_MONEY_DATA;
  if (!root || !data) return;
  const q = s => root.querySelector(s);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = n => n == null ? 'Unavailable' : Math.round(n).toLocaleString('en-US');
  const money = n => n == null ? 'Unavailable' : new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0}).format(n);
  const compact = n => n == null ? 'Unavailable' : '$' + (Math.abs(n)>=1e6?(n/1e6).toFixed(1)+'m':Math.abs(n)>=1e3?(n/1e3).toFixed(0)+'k':number(n));
  const titleCase = s => String(s || '').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase()).replace(/\b(Ai|Vc)\b/g,s=>s.toUpperCase());
  const concepts = {
    reference: {name:'Reference', description:'A quiet research library. Persistent navigation, linked records, and source context beside the data.'},
    atlas: {name:'Atlas', description:'A compact research workspace. Clear sections, dense comparisons, and a consistent path from totals to evidence.'},
    ledger: {name:'Ledger', description:'An editorial public record. Strong typography and open tables, with definitions and sources close at hand.'},
    index: {name:'Index', description:'A directory built for repeated use. Compact navigation and tables put connected records within reach.'}
  };
  const pages = {overview:'Overview',companies:'Giving by employer',company:'Company profile',candidates:'Candidates & races',candidate:'Candidate accounts',donors:'Donors',donor:'Donor record',committees:'Committees',committee:'Committee profile',lobbying:'Federal lobbying',methodology:'Methodology',data:'Data & sources',about:'About'};
  let state = {design:'reference',cycle:'2026',page:'companies',id:'',search:'',filter:'all',sort:'total',direction:-1,pageNum:1};
  let currentRows = [];
  let exportRows = [];
  const cycle = () => data.cycles[state.cycle];
  const url = (page,id='') => '#' + [state.design,state.cycle,page,id].filter(Boolean).map(encodeURIComponent).join('/');
  const link = (page,text,id='',cls='') => `<a href="${url(page,id)}" data-page="${page}" data-id="${esc(id)}"${cls?` class="${cls}"`:''}>${text}</a>`;
  const action = (page,text,id='') => link(page,text,id,'action');
  const currentSection = () => ({company:'companies',candidate:'candidates',donor:'donors',committee:'committees'}[state.page] || state.page);
  const rowLink = (page,item) => link(page,esc(item.name),item.slug || item.id || item.name);
  const partyName = p => ({D:'Democratic',R:'Republican',DEM:'Democratic',REP:'Republican',Mixed:'Mixed',Unknown:'Unknown',UNK:'Unknown'}[p] || p || 'Unclassified');
  function partyBar(row) {
    const sum = Math.max(1,row.dem + row.rep + row.other);
    return `<div class="split-bar" role="img" aria-label="Democratic ${money(row.dem)}, Republican ${money(row.rep)}, other or unclassified ${money(row.other)}"><span class="split-dem" style="width:${Math.max(0,row.dem)/sum*100}%"></span><span class="split-rep" style="width:${Math.max(0,row.rep)/sum*100}%"></span><span class="split-other" style="width:${Math.max(0,row.other)/sum*100}%"></span></div>`;
  }
  const legend = () => '<div class="legend"><span><i class="split-dem"></i>Democratic</span><span><i class="split-rep"></i>Republican</span><span><i class="split-other"></i>Other / unclassified</span></div>';
  const stats = items => `<div class="stats">${items.map(([label,value,note])=>`<div class="stat"><div class="stat-label">${label}</div><div class="stat-value">${value}</div><div class="stat-note">${note}</div></div>`).join('')}</div>`;
  const section = (id,title,content,after='') => `<section class="section" id="tm-${id}"><div class="section-head"><h2>${title}</h2>${after}</div>${content}</section>`;
  function table(headings,rows,caption='') {
    return `<div class="table-wrap"><table class="data-table">${caption?`<caption class="sr-only">${caption}</caption>`:''}<thead><tr>${headings.map(h=>`<th${h.num?' class="num"':''}${h.key?` aria-sort="${state.sort===h.key?(state.direction===1?'ascending':'descending'):'none'}"`:''}>${h.key?`<button type="button" data-sort="${h.key}">${h.label}${state.sort===h.key?(state.direction===1?' ↑':' ↓'):''}</button>`:h.label}</th>`).join('')}</tr></thead><tbody>${rows.length?rows.join(''):`<tr><td colspan="${headings.length}" class="empty-state">No matching records. Try a broader search or clear the filters.</td></tr>`}</tbody></table></div>`;
  }
  const cell = (content,num=false) => `<td${num?' class="num"':''}>${content}</td>`;
  const companyHeadings = [{label:'Employer',key:'name'},{label:'Net contributions',key:'total',num:true},{label:'Donor groups',key:'donors',num:true},{label:'Recipient mix'},{label:'Records',key:'contributions',num:true}];
  const companyTable = rows => table(companyHeadings,rows.map(r=>'<tr>'+cell(rowLink('company',r)+`<span class="subtext">${esc(r.sectors.filter(Boolean).map(titleCase).join(' · ') || 'Uncategorized')}</span>`)+cell(money(r.total),true)+cell(number(r.donors),true)+cell(partyBar(r))+cell(number(r.contributions),true)+'</tr>'),'Employer-matched contributions');
  function toolbar(placeholder,options=[],label='Filter records') {
    return `<div class="table-tools"><input type="search" id="record-search" placeholder="${placeholder}" aria-label="${placeholder}" value="${esc(state.search)}">${options.length?`<select id="record-filter" aria-label="${label}"><option value="all">${label}</option>${options.map(([value,text])=>`<option value="${esc(value)}"${state.filter===value?' selected':''}>${esc(text)}</option>`).join('')}</select>`:''}<button type="button" class="action" data-export>Export CSV ↓</button></div><div class="result-count" aria-live="polite"></div>`;
  }
  function heading(title,eyebrow,lede,actions='') {
    q('.page-heading').innerHTML = `<div class="eyebrow">${eyebrow}</div><h1>${title}</h1><p class="lede">${lede}</p>${actions?`<div class="page-actions">${actions}</div>`:''}`;
  }
  function sourceNote() {
    return `<div class="notice"><strong>What these numbers measure.</strong> Contributions matched to a donor’s reported employer; they are not spending directed by the company. ${link('methodology','Read the definitions →')}</div>`;
  }
  function nav() {
    const c = cycle();
    const active = currentSection();
    const navLink = (page,label,count='') => link(page,`${label}${count?`<span class="nav-count">${count}</span>`:''}`).replace('<a ','<a '+(page===active?'aria-current="page" ':''));
    const primaryPages=[['overview','Overview'],['companies','Employers'],['candidates','Candidates'],...(state.design==='ledger'?[['donors','Donors'],['committees','Committees']]:[]),['lobbying','Lobbying'],['data','Sources']];
    q('.primary-nav').innerHTML = primaryPages.map(([p,l])=>navLink(p,l)).join('');
    q('.sidebar').innerHTML = `<div class="nav-group"><div class="nav-caption">Explore</div>${navLink('overview','Overview')}${navLink('companies','Giving by employer',c.companies.length)}${navLink('donors','Donors')}${navLink('candidates','Candidates & races')}${navLink('committees','Committees')}</div><div class="nav-group"><div class="nav-caption">Federal lobbying</div>${navLink('lobbying','Lobbying explorer')}</div><div class="nav-group"><div class="nav-caption">Understand the data</div>${navLink('methodology','Methodology')}${navLink('data','Data & sources')}${navLink('about','About this project')}</div><div class="side-note">Federal campaign finance<br><strong>${state.cycle} election cycle</strong><br>Transaction dates through<br>${esc(c.metadata.dataAsOf)}</div>`;
    const company = state.page==='company' ? c.companies.find(r=>r.slug===state.id) : null;
    q('.breadcrumbs').innerHTML = `${link('overview','Tech Money')}<span>/</span><span>${state.page==='lobbying'?'Federal lobbying':state.cycle}</span>${state.page!=='overview'?`<span>/</span>${company?link('companies','Employers')+'<span>/</span>'+esc(company.name):esc(pages[state.page])}`:''}`;
    q('#cycle-picker').value = state.cycle;
    q('.cycle-label').style.display = state.page==='lobbying' ? 'none' : '';
    root.querySelectorAll('.brand,.footer [data-page]').forEach(a=>a.setAttribute('href',url(a.dataset.page)));
  }
  function context() {
    const headings = [...q('.page-content').querySelectorAll('section[id] h2')];
    q('.context').innerHTML = `<div class="context-label">On this page</div><div class="context-links">${headings.map(h=>`<a href="#${h.closest('section').id}" data-jump="${h.closest('section').id}">${esc(h.textContent)}</a>`).join('')||`<span>${esc(pages[state.page])}</span>`}</div><div class="context-note"><strong>${state.page==='lobbying'?'Calendar-year records':'Data coverage'}</strong><p>${state.page==='lobbying'?'Reporting years '+data.lobbying.years.join(', '):`Cycle ${state.cycle}<br>Transaction dates through<br>${esc(cycle().metadata.dataAsOf)}`}</p>${link('data','Sources & downloads →')}</div><div class="context-note"><strong>Read with context</strong><p>${state.page==='lobbying'?'A matching passage identifies reported lobbying activity, not a position taken or AI-specific spending.':'Reported employers and recipient classifications are analytical groupings.'}</p>${link('methodology','How to use these figures →')}</div>`;
  }
  function overview() {
    const c = cycle();
    heading('Technology and political money','Federal campaign finance / '+state.cycle,'Explore the people, employers, and political organizations connected by public campaign-finance records. Follow each figure back to its context.',action('companies','Explore employers →')+action('lobbying','Explore federal lobbying'));
    const totals = c.companies.reduce((a,r)=>({dem:a.dem+r.dem,rep:a.rep+r.rep,other:a.other+r.other}),{dem:0,rep:0,other:0});
    const ranks = `<div class="rank-list">${c.companies.slice(0,6).map(r=>`<div class="rank-row"><span class="rank-name">${rowLink('company',r)}</span><div class="rank-track"><div class="rank-fill" style="width:${r.total/c.companies[0].total*100}%"></div></div><span class="rank-value">${compact(r.total)}</span></div>`).join('')}</div><div class="meta-line">Net employer-matched contributions · USD</div>`;
    const mix = partyBar(totals)+legend()+[['Democratic',totals.dem],['Republican',totals.rep],['Other / unclassified',totals.other]].map(([name,value])=>`<div class="party-row"><span>${name}</span><strong>${compact(value)}</strong></div>`).join('')+'<div class="meta-line">Grouped by recipient committee classification.</div>';
    currentRows = c.companies.slice(0,6);
    exportRows = currentRows;
    return `<div class="overview-content">${stats([['Employer-matched giving',compact(c.headline.total),'Net contributions'],['Donor groups',number(c.headline.donors),'Reported-name groups'],['Tracked employers',number(c.headline.companies),'Curated employer matches'],['Recipient committees',number(c.headline.committees),'Across the selected cycle']])}<div class="overview-grid">${section('largest','Largest employer groups',ranks,link('companies','View all →'))}${section('recipient-mix','Where the money goes',mix)}</div>${section('directory','Explore the employer directory',`<div id="table-results">${companyTable(currentRows)}</div>`,link('companies',`All ${c.companies.length} employers →`))}${sourceNote()}<div class="explore-links"><div>${link('candidates','Candidates & races →')}<p>Explore account receipts by candidate, state, and office.</p></div><div>${link('lobbying','Federal lobbying →')}<p>Search reported issues and read original filing passages.</p></div><div>${link('methodology','Understand the records →')}<p>Employer matching, transaction treatment, and limitations.</p></div></div></div>`;
  }
  function companies() {
    heading('Giving by employer','Directory / '+state.cycle,'Compare contributions associated with tracked technology employers. Select an employer to explore its recipients and source context.');
    currentRows = cycle().companies;
    return toolbar('Search employers…',[...new Set(currentRows.flatMap(r=>r.sectors).filter(Boolean))].sort().map(s=>[s,titleCase(s)]),'All sectors')+`<section class="section" id="tm-employers"><h2 class="sr-only">Employer directory</h2><div id="table-results"></div></section>`+legend()+sourceNote();
  }
  function company() {
    const c = cycle().companies.find(r=>r.slug===state.id);
    if (!c) return unavailable('This employer is not tracked in the selected cycle.','companies');
    heading(esc(c.name),'Employer profile / '+state.cycle,`Contributions associated with reported employers mapped to ${esc(c.name)}. These figures describe donor records, not company-directed political spending.`,action('companies','← All employers')+`<button class="action" type="button" data-export>Export summary ↓</button>`);
    exportRows = [c];
    const recipients = c.recipients || [];
    return `<div class="section-nav"><a class="active" href="#tm-summary" data-jump="tm-summary">Overview</a><a href="#tm-recipients" data-jump="tm-recipients">Recipients</a>${link('donors','Donors')}<a href="#tm-source" data-jump="tm-source">Source context</a></div>${stats([['Net contributions',compact(c.total),'Employer-matched records'],['Donor groups',number(c.donors),'Reported-name groups'],['Contributions',number(c.contributions),'Selected FEC records'],['Committees',number(c.committees),'Recipient organizations']])}${section('summary','Recipient distribution',partyBar(c)+legend()+`<div class="party-row"><span>Democratic committees</span><strong>${money(c.dem)}</strong></div><div class="party-row"><span>Republican committees</span><strong>${money(c.rep)}</strong></div><div class="party-row"><span>Other / unclassified committees</span><strong>${money(c.other)}</strong></div>`)}${section('recipients','Leading recipients',recipients.length?table([{label:'Recipient committee'},{label:'Classification'},{label:'Net amount',num:true}],recipients.map(r=>'<tr>'+cell(link('committee',esc(r.name),r.id+'~'+c.slug))+cell(esc(partyName(r.party)))+cell(money(r.total),true)+'</tr>'))+`<div class="meta-line">Showing ${recipients.length} leading recipients of ${number(c.committees)}. Prototype includes a subset of the full record.</div>`:`<p class="meta-line">Recipient rows for this employer are outside the prototype snapshot.</p>${sourceLink(`companies/${c.slug}/`,'Open full employer record ↗')}`)}${section('source','Source context',sourceNote()+`<p class="meta-line">Employer labels combine manually reviewed aliases. Net values reflect the project’s transaction-selection rules.</p>${link('methodology','Employer matching and transaction treatment →')}`)}`;
  }
  function candidates() {
    heading('Candidates & races','Federal elections / '+state.cycle,'Explore tech-linked receipts in candidate-associated accounts. Geographic and office groupings are not a verified ballot or list of nominees.');
    currentRows=cycle().candidates;
    return toolbar('Search candidates or states…',[['H','House'],['S','Senate'],['P','President']],'All offices')+`<section id="tm-candidates" class="section"><h2 class="sr-only">Candidate accounts</h2><div id="table-results"></div></section><div class="notice"><strong>Account context matters.</strong> Current campaign accounts and former accounts converted to PACs are shown separately on each candidate record. ${link('methodology','About attribution →')}</div>`;
  }
  function candidate() {
    const c = cycle().candidates.find(r=>r.id===state.id);
    if (!c) return unavailable('This candidate is outside the prototype snapshot.','candidates');
    heading(esc(c.name),'Candidate accounts / '+state.cycle,`${esc(partyName(c.party))} · ${{H:'House',S:'Senate',P:'President'}[c.office]}${c.state?' · '+esc(c.state):''}${c.office==='H' && c.district?(c.district==='00'?' · At-large':' · District '+esc(c.district)):''}`,action('candidates','← Candidate directory'));
    exportRows=[c];
    return stats([['Tech-linked account receipts',compact(c.total),'Associated account records'],['Donor groups',number(c.donors),'Reported-name groups'],['Current campaign accounts',compact(c.currentCampaignTotal),'Current designation'],['Former campaign accounts',compact(c.formerCampaignTotal),'Converted to PACs']])+section('accounts','Account scope',`<div class="notice">Tech-linked account receipts are an exploration measure. They are not a fully reconciled total of unique gifts to this candidate.</div><div class="source-row"><div><strong>Candidate identifier</strong><p class="mono">${esc(c.id)}</p></div><a href="https://www.fec.gov/data/candidate/${encodeURIComponent(c.id)}/?cycle=${state.cycle}" target="_blank" rel="noopener">FEC candidate record ↗</a></div><div class="source-row"><div><strong>Share of selected net itemized receipts</strong><p>${c.share == null ? 'Unavailable while attribution or memo reconciliation remains unresolved.' : Number(c.share).toFixed(2)+'% · as published in the source export.'}</p></div></div>`)+section('attribution','Interpretation & attribution',`<div class="prose"><p>Candidate-associated accounts can include current campaign committees and former committees. Transfers, memo entries, and adjustments can describe overlapping economic events; they need source-level interpretation.</p></div>${link('methodology','Read the methodology →')}`);
  }
  function entities(kind) {
    const isDonor = kind==='donors';
    currentRows=cycle()[kind];
    heading(isDonor?'Donors':'Recipient committees',(isDonor?'Reported-name groups':'Political organizations')+' / '+state.cycle,isDonor?'Explore leading reported donor groups. Name grouping and employer matching do not verify unique individual identities.':'Explore the committees receiving tech-linked contributions, including campaign accounts, parties, and other political organizations.');
    return toolbar(isDonor?'Search donors or employers…':'Search committees…')+`<section class="section" id="tm-records"><h2 class="sr-only">${isDonor?'Donor':'Committee'} records</h2><div id="table-results"></div></section><div class="meta-line">This interactive prototype contains the leading ${currentRows.length} exported records.</div>`;
  }
  function entity(kind) {
    const plural = kind==='donor'?'donors':'committees';
    const [recordId,employerSlug] = kind==='committee' ? state.id.split('~') : [state.id];
    let c = cycle()[plural].find(r=>(r.id||r.name)===recordId);
    let employer=null;
    if(kind==='committee' && employerSlug) {
      employer=cycle().companies.find(r=>r.slug===employerSlug);
      c=employer?.recipients?.find(r=>r.id===recordId);
    } else if (!c && kind==='committee') {
      for (const company of cycle().companies) {
        c=company.recipients?.find(r=>r.id===recordId);
        if(c) { employer=company; break; }
      }
    }
    if (!c) return unavailable('This record is outside the prototype snapshot.',plural);
    heading(esc(c.name),(kind==='donor'?'Donor group':'Committee record')+' / '+state.cycle,kind==='donor'?'A reported-name group in the project’s employer-matched contribution records.':'A recipient organization in federal campaign-finance records.',action(plural,'← Back to directory'));
    exportRows=[employer?{...c,employer:employer.name,scope:'Employer-matched recipient subtotal'}:c];
    const fields = [[employer?'Employer-matched subtotal':kind==='committee'?'Tech-linked account receipts':'Employer-matched contributions',money(c.total)],[kind==='donor'?'Donor lean':'Recipient lean',esc(partyName(c.party))],['Contribution records',c.contributions==null?'Not included in snapshot':number(c.contributions)],['Employer group',employer?rowLink('company',employer):esc(c.company||'All tracked employers')]];
    if(kind==='donor' && c.sourceName) fields.push(['Reported donor name',esc(c.sourceName)]);
    const scope = employer?`<div class="notice">This amount includes only records matched to ${esc(employer.name)}. It is not the committee’s total receipts. ${cycle().committees.some(r=>r.id===c.id)?link('committee','View receipts across tracked employers →',c.id):'The total across tracked employers is outside this prototype snapshot.'}</div>`:kind==='donor'?'<div class="notice">Donor lean uses all selected records under the reported name, including unmatched employers. The contribution amount above includes employer-matched records only.</div>':'';
    return section('record','Record summary',table([{label:'Field'},{label:'Value'}],fields.map(([a,b])=>'<tr>'+cell(a)+cell(b)+'</tr>'))+scope)+section('evidence','Source evidence',c.id?`<a href="https://www.fec.gov/data/committee/${encodeURIComponent(c.id)}/?cycle=${state.cycle}" target="_blank" rel="noopener">Open FEC committee ${esc(c.id)} ↗</a>`:`<p class="meta-line">Grouped from reported FEC donor names and manually mapped employer strings.</p>${sourceLink('donors/','Open full donor directory ↗')}`)+sourceNote();
  }
  function lobbying() {
    const l=data.lobbying;
    heading('Federal lobbying','Reported issues & filing evidence','Search the language of federal lobbying reports. Start with a client or topic, then examine the matching passage in context.');
    currentRows=l.records;
    return `<div class="meta-line">${number(l.activities)} indexed issue entries · ${number(l.reports)} reports · ${l.years[0]}–${l.years[l.years.length-1]}</div>`+toolbar('Search clients, topics, or filing text…',l.years.map(y=>[String(y),String(y)]),'All reporting years')+`<section class="section" id="tm-filings"><div class="section-head"><h2>Filing passages</h2><span class="result-count"></span></div><div id="table-results"></div></section><div class="notice"><strong>Read the passage, not just the match.</strong> Topic matches are a way to find reports; they do not establish a client’s position or the amount spent on that topic.</div>`;
  }
  function methodology() {
    heading('Understanding the data','Methods & definitions','Public records are detailed, but interpreting the same dollar across people, employers, and political committees requires care.');
    return `<div class="prose">${section('employer-matching','Employer matching','<p>FEC employer fields are free text. A manually reviewed lookup maps reported variants to tracked employer groups. Employer-matched totals describe reported donor records associated with these labels; they do not establish company direction or sponsorship.</p>')}${section('transactions','Transactions & adjustments','<p>Receipts, transfers, memo attributions, refunds, and adjustments can describe overlapping economic events. The correct selection depends on the question. These totals support exploration; they are not a universally reconciled answer to every employee-to-candidate question.</p>')}${section('classification','Recipient classifications','<p>Party categories refer to recipient committee classifications. “Other / unclassified” keeps mixed and unknown categories visible. A Democratic share, when used, divides Democratic amounts by Democratic plus Republican amounts; it excludes the other categories.</p>')}${section('candidate-accounts','Candidate accounts','<p>Candidate-linked data distinguish current campaign accounts from former accounts converted to PACs. Unavailable percentages remain unavailable where source reconciliation does not support a meaningful denominator. Geographic pages do not verify ballot status.</p>')}${section('coverage','Dates & coverage',`<p>The latest transaction date in this cycle is ${esc(cycle().metadata.dataAsOf)}. Transaction dates are not filing-completeness dates. Late filings and amendments can change earlier totals. Lobbying uses calendar-year reporting periods and separate coverage snapshots.</p>`)}${section('lobbying-method','Lobbying topic matches','<p>The lobbying explorer indexes reported issue descriptions with curated company mappings and versioned topic rules. A match points to a passage and its filing. It does not measure topic-specific spending, meetings, or policy impact.</p>')}</div>`;
  }
  function sourceLink(path,label) { return `<a href="https://realwadezhou.github.io/tech_money_tracker/${state.cycle}/${path}" target="_blank" rel="noopener">${label}</a>`; }
  function dataPage() {
    heading('Data & sources','Download / inspect / reproduce','Explore the local snapshot behind these prototypes and the public records that feed the project.');
    exportRows=cycle().companies;
    return section('downloads','Snapshot downloads',[['companies','Employer summaries','All tracked employers in the selected election cycle.'],['candidates','Candidate accounts','Leading 12 candidate-associated account summaries.'],['donors','Donor groups','Leading 12 reported-name donor groups.'],['committees','Recipient committees','Leading 12 recipient committee summaries.'],['lobbying','Lobbying passages','Eight original filing passages included in this prototype.']].map(([type,title,description])=>`<div class="source-row"><div><strong>${title}</strong><p>${description}</p></div><button type="button" class="action" data-export="${type}">CSV ↓</button></div>`).join(''))+section('provenance','Snapshot provenance',table([{label:'Field'},{label:'Value'}],[['Election cycle',state.cycle],['Latest matched transaction',esc(cycle().metadata.dataAsOf)],['Local build',esc(cycle().metadata.builtAt)],['Source freshness status',esc(cycle().metadata.sourceStatus || 'Unknown')],['Latest installed bulk release',esc(cycle().metadata.latestBulkRelease || 'Unavailable')]].map(([k,v])=>'<tr>'+cell(k)+cell(v)+'</tr>')))+section('primary-sources','Primary sources',`<div class="source-row"><div><strong>Federal Election Commission</strong><p>Bulk contribution data, committees, candidates, and original filings.</p></div><a href="https://www.fec.gov/data/" target="_blank" rel="noopener">FEC data ↗</a></div><div class="source-row"><div><strong>Federal lobbying disclosures</strong><p>Original reports and reported issue descriptions.</p></div><a href="https://lda.senate.gov/" target="_blank" rel="noopener">Senate LDA ↗</a></div><div class="source-row"><div><strong>Full published project data</strong><p>Existing exports and documentation for this election cycle.</p></div>${sourceLink('data/','Browse data ↗')}</div>`);
  }
  function about() {
    heading('A public record of tech and politics','About Tech Money','Tech Money makes the technology sector’s connections to US federal politics easier to explore.');
    return `<div class="prose">${section('project','The project','<p>The site brings together employer-matched campaign-finance records and federal lobbying disclosures. It is organized around the questions readers ask: who gave, which accounts received the money, and what lobbying activity was reported?</p>')}${section('principles','Follow the evidence','<p>Tables and linked records are the core of the site. Definitions sit close to the measures they describe, source context stays visible, and downloadable data support independent investigation.</p>')}${section('prototype','About this design study','<p>All four design directions share connected page types, cycle selection, directory search, sorting, profile navigation, filing passages, and downloads. The prototype uses a local data snapshot with selected detail records. A production rebuild would connect these page templates to the existing static generator.</p>')}</div><div class="mobile-browse">${link('donors','Donors')}${link('committees','Committees')}${link('methodology','Methodology')}</div>`;
  }
  function unavailable(message,parent) { heading('Record not in this snapshot','Data coverage',message,action(parent,'Back to directory')); return '<div class="notice">The prototype includes all tracked employers and selected detail records. The full published dataset remains available from Data & sources.</div>'; }
  function renderTable() {
    const target=q('#table-results');
    if(!target) return;
    const search=state.search.toLowerCase();
    let rows=currentRows.filter(r=>JSON.stringify(r).toLowerCase().includes(search));
    if(state.filter!=='all') rows=rows.filter(r=>state.page==='companies'?r.sectors.includes(state.filter):state.page==='candidates'?r.office===state.filter:state.page==='lobbying'?String(r.year)===state.filter:true);
    rows.sort((a,b)=>{const x=a[state.sort],y=b[state.sort]; return state.direction * (typeof x==='string'?x.localeCompare(y):Number(x||0)-Number(y||0));});
    exportRows=rows;
    const filteredCount=rows.length;
    const pageCount=Math.max(1,Math.ceil(filteredCount/12));
    state.pageNum=Math.min(state.pageNum||1,pageCount);
    rows=rows.slice((state.pageNum-1)*12,state.pageNum*12);
    root.querySelectorAll('.result-count').forEach(el=>el.textContent=`${filteredCount} of ${currentRows.length} ${state.page==='lobbying'?'sample passages':state.page==='companies'?'employers':'sample records'}${pageCount>1?' · page '+state.pageNum+' of '+pageCount:''}`);
    if(state.page==='companies' || state.page==='overview') target.innerHTML=companyTable(rows);
    else if(state.page==='candidates') target.innerHTML=table([{label:'Candidate',key:'name'},{label:'Office'},{label:'Party'},{label:'Tech-linked receipts',key:'total',num:true},{label:'Donor groups',key:'donors',num:true}],rows.map(r=>'<tr>'+cell(rowLink('candidate',r))+cell(esc((r.state?r.state+' · ':'')+({H:'House',S:'Senate',P:'President'}[r.office]))+`<span class="subtext">${r.office==='H' && r.district?(r.district==='00'?'At-large':'District '+esc(r.district)):''}</span>`)+cell(esc(partyName(r.party)))+cell(money(r.total),true)+cell(number(r.donors),true)+'</tr>'));
    else if(state.page==='donors') target.innerHTML=table([{label:'Donor group',key:'name'},{label:'Employer'},{label:'Net contributions',key:'total',num:true},{label:'Records',key:'contributions',num:true}],rows.map(r=>'<tr>'+cell(rowLink('donor',r))+cell(esc(r.company))+cell(money(r.total),true)+cell(number(r.contributions),true)+'</tr>'));
    else if(state.page==='committees') target.innerHTML=table([{label:'Recipient committee',key:'name'},{label:'Classification'},{label:'Tech-linked receipts',key:'total',num:true},{label:'Donor groups',key:'donors',num:true}],rows.map(r=>'<tr>'+cell(rowLink('committee',r)+`<span class="subtext mono">${esc(r.id)}</span>`)+cell(esc(partyName(r.party)))+cell(money(r.total),true)+cell(number(r.donors),true)+'</tr>'));
    else if(state.page==='lobbying') target.innerHTML=rows.length?rows.map(r=>`<article class="filing"><div class="filing-head"><span class="filing-title">${esc(r.client)}</span><span class="topic-tag">${r.year} · ${esc(r.period)}</span></div><div class="meta-line">${esc(r.organization)} · ${esc(r.issue)}${r.topics?.length?' · '+r.topics.map(esc).join(' / '):''}</div><p>${esc(r.description.length>235?r.description.slice(0,235)+'…':r.description)}</p><details><summary>Read full reported passage</summary><p>${esc(r.description)}</p><span>Posted ${esc(r.postedAt || 'date unavailable')}</span></details><div class="meta-line"><a href="${esc(r.source)}" target="_blank" rel="noopener">Original filing ↗</a></div></article>`).join(''):'<div class="empty-state">No matching passages in this eight-record snapshot. Try a broader search or another reporting year.</div>';
    if(pageCount>1) target.insertAdjacentHTML('beforeend',`<nav class="pagination" aria-label="Results pages"><span>Showing ${(state.pageNum-1)*12+1}–${Math.min(state.pageNum*12,filteredCount)} of ${filteredCount}</span><div><button class="action" type="button" data-page-step="-1"${state.pageNum===1?' disabled':''}>← Previous</button><button class="action" type="button" data-page-step="1"${state.pageNum===pageCount?' disabled':''}>Next →</button></div></nav>`);
  }
  function readRoute() {
    const parts=window.location.hash.slice(1).split('/').map(v=>{try{return decodeURIComponent(v);}catch{return v;}});
    if(concepts[parts[0]]) state.design=parts[0];
    if(data.cycles[parts[1]]) state.cycle=parts[1];
    if(pages[parts[2]]) {state.page=parts[2];state.id=parts[3]||'';}
    else if(pages[parts[0]]) {state.page=parts[0];state.id='';}
  }
  function remember() {
    if(window.openai?.setWidgetState) window.openai.setWidgetState({modelContent:{design:concepts[state.design].name,page:pages[state.page],cycle:state.cycle},privateContent:{design:state.design,page:state.page,cycle:state.cycle,id:state.id}}).catch(()=>{});
  }
  function render() {
    root.dataset.design=state.design;
    q('#concept-summary').textContent=concepts[state.design].description;
    root.querySelectorAll('[data-concept]').forEach(el=>el.setAttribute('aria-pressed',String(el.dataset.concept===state.design)));
    nav(); currentRows=[];exportRows=[];
    const renderers={overview,companies,company,candidates,candidate,donors:()=>entities('donors'),donor:()=>entity('donor'),committees:()=>entities('committees'),committee:()=>entity('committee'),lobbying,methodology,data:dataPage,about};
    q('.page-content').innerHTML=renderers[state.page]();
    q('.page-content').insertAdjacentHTML('beforeend',`<nav class="mobile-browse" aria-label="More sections">${link('donors','Donors')}${link('committees','Committees')}${link('methodology','Methodology')}${link('about','About')}</nav>`);
    renderTable();context();
    document.title=`${pages[state.page]} · ${concepts[state.design].name} · Tech Money`;
  }
  function navigate(page,id='') {
    state.page=page;state.id=id;state.search='';state.filter='all';state.sort='total';state.direction=-1;
    const newHash=url(page,id);
    if(window.location.hash!==newHash) window.location.hash=newHash; else render();
    remember();
  }
  function download(type='') {
    const rows=type==='lobbying'?data.lobbying.records:type?cycle()[type]:exportRows;
    if(!rows?.length) {q('.live-status').textContent='No records match these filters.';return;}
    const fields=Object.keys(rows[0]).filter(k=>!['recipients','weekly','description'].includes(k));
    if(type==='lobbying'||state.page==='lobbying') fields.push('description');
    const csvField=v=>'"'+String(Array.isArray(v)?v.join('; '):v??'').replace(/"/g,'""')+'"';
    const content=[fields.join(','),...rows.map(r=>fields.map(k=>csvField(r[k])).join(','))].join('\r\n');
    const objectURL=URL.createObjectURL(new Blob([content],{type:'text/csv;charset=utf-8;'}));
    const a=document.createElement('a');a.href=objectURL;a.download=`tech-money-${state.cycle}-${type||state.page}-snapshot.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(objectURL),1000);
    q('.live-status').textContent=`Downloaded ${rows.length} snapshot records.`;
  }
  root.addEventListener('click',event=>{
    const step=event.target.closest('[data-page-step]');
    if(step){state.pageNum+=Number(step.dataset.pageStep);renderTable();return;}
    const concept=event.target.closest('[data-concept]');
    if(concept) {state.design=concept.dataset.concept;window.location.hash=url(state.page,state.id);render();remember();return;}
    const page=event.target.closest('[data-page]');
    if(page && !event.ctrlKey && !event.metaKey) {event.preventDefault();navigate(page.dataset.page,page.dataset.id||'');return;}
    const jump=event.target.closest('[data-jump]');
    if(jump) {event.preventDefault();document.getElementById(jump.dataset.jump)?.scrollIntoView({block:'start',behavior:'auto'});return;}
    const sort=event.target.closest('[data-sort]');
    if(sort) {state.pageNum=1;state.direction=state.sort===sort.dataset.sort?-state.direction:(sort.dataset.sort==='name'?1:-1);state.sort=sort.dataset.sort;renderTable();return;}
    const exportButton=event.target.closest('[data-export]');
    if(exportButton) download(exportButton.dataset.export);
  });
  root.addEventListener('input',event=>{if(event.target.id==='record-search'){state.pageNum=1;state.search=event.target.value;renderTable();}});
  root.addEventListener('change',event=>{
    if(event.target.id==='preview-appearance') root.style.colorScheme=event.target.value;
    if(event.target.id==='cycle-picker') {state.cycle=event.target.value;state.search='';state.filter='all';window.location.hash=url(state.page,state.id);remember();}
    if(event.target.id==='record-filter'){state.pageNum=1;state.filter=event.target.value;renderTable();}
  });
  window.addEventListener('hashchange',()=>{readRoute();state.pageNum=1;state.search='';state.filter='all';state.sort='total';state.direction=-1;render();});
  const saved=window.openai?.widgetState?.privateContent;
  if(saved && concepts[saved.design] && pages[saved.page] && data.cycles[saved.cycle]) Object.assign(state,saved);
  readRoute();render();
  window.addEventListener('openai:set_globals',event=>{const saved=event.detail?.globals?.widgetState?.privateContent;if(saved&&concepts[saved.design]&&pages[saved.page]&&data.cycles[saved.cycle]){Object.assign(state,saved);render();}});
  if(globalThis.Tweak) {
    const tweaks={density:'Comfortable'};
    const tweak=new Tweak({container:root,onChange:()=>{root.dataset.density=tweaks.density.toLowerCase();}});
    tweak.addSelect(tweaks,'density',{label:'Table density',options:['Comfortable','Compact']});
  }
})();
