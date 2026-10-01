"""Local evidence-first donor review. Use --help for commands."""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
import csv
from datetime import datetime,timezone
import hashlib
from http.server import BaseHTTPRequestHandler,HTTPServer
import json
from pathlib import Path
import re
import shutil
import subprocess

if __package__:
    from .model import COLS,MODEL_VERSION,POLICY,case_signature,company,digest,norm,parse_name,retrieve,score
else:
    from model import COLS,MODEL_VERSION,POLICY,case_signature,company,digest,norm,parse_name,retrieve,score

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
DATA=HERE/'data'
OUT=HERE/'output'
STATE=HERE/'state'

def model_manifest():
    return dict(policy=POLICY,implementation_sha256=hashlib.sha256((HERE/'model.py').read_bytes()).hexdigest())

def model_hash():return digest(model_manifest())

def cohort_hash(cases,pid):
    return digest(sorted((c['case_id'],c['evidence_sha256']) for c in cases if c['person_id']==pid))

def read(path):return json.loads(path.read_text(encoding='utf-8'))
def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    tmp.replace(path)
def stamp():return datetime.now(timezone.utc).isoformat()
def profiles():return read(DATA/'profiles.json')['people']
def jsonlines(path):
    if not path.exists():return []
    with path.open(encoding='utf-8') as f:return [json.loads(line) for line in f if line.strip()]
def retrieval_hash(people):return digest([{k:p[k] for k in ['person_id','retrieval_surnames','retrieval_given_names','given_names']} for p in people])
def csv_write(path,rows,fields):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)

def scan(cycles):
    rg=shutil.which('rg')
    if not rg:raise ValueError('ripgrep is required for scanning raw bulk inputs')
    surnames=sorted({s for p in profiles() for s in p['retrieval_surnames']})
    pattern=r'\b(?:'+'|'.join(re.escape(s) for s in surnames)+r')\b'
    sources=[];STATE.mkdir(exist_ok=True)
    for cycle in cycles:
        path=ROOT/f'data/fec/interim/{cycle}/indiv{str(cycle)[-2:]}/itcont.txt'
        before=path.stat();target=STATE/f'surname_records_{cycle}.txt';temp=target.with_suffix('.tmp')
        with temp.open('wb') as f:
            result=subprocess.run([rg,'--text','--no-heading','--no-filename','-i',pattern,str(path)],stdout=f,stderr=subprocess.PIPE)
        if result.returncode not in (0,1):raise ValueError(result.stderr.decode(errors='replace'))
        after=path.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ValueError('Source changed during scan')
        temp.replace(target)
        sources.append(dict(cycle=cycle,source_path=path.relative_to(ROOT).as_posix(),source_size=before.st_size,
                            source_mtime_ns=before.st_mtime_ns,extracted_at=stamp(),retrieval_surnames=surnames,
                            extract_sha256=hashlib.sha256(target.read_bytes()).hexdigest()))
    write(STATE/'scan_manifest.json',dict(cycles=cycles,sources=sources))
    import_extracts(STATE)

def import_extracts(directory):
    people=profiles();manifest=read(directory/'scan_manifest.json')
    required={s for p in people for s in p['retrieval_surnames']}
    seen={};rows=[];duplicates=0
    for source in manifest['sources']:
        if not required.issubset(source['retrieval_surnames']):raise ValueError('Extract does not cover this watchlist; run scan')
        path=directory/f"surname_records_{source['cycle']}.txt"
        if hashlib.sha256(path.read_bytes()).hexdigest()!=source['extract_sha256']:raise ValueError('Extract checksum mismatch')
        with path.open(encoding='utf-8',errors='strict') as f:
            for line in f:
                fields=line.rstrip('\r\n').split('|')
                if len(fields)!=len(COLS):raise ValueError('Malformed FEC extract')
                row=dict(zip(COLS,fields));row['cycle']=source['cycle']
                if not any(retrieve(row['name'],p) for p in people):continue
                if not row['sub_id']:raise ValueError('Missing candidate SUB_ID')
                rid=f"{row['cycle']}:{row['sub_id']}";sha=digest({k:row[k] for k in COLS})
                if rid in seen:
                    if seen[rid]!=sha:raise ValueError('Conflicting source contents for '+rid)
                    duplicates+=1;continue
                seen[rid]=sha;rows.append(dict(row,record_id=rid,row_sha256=sha))
    DATA.mkdir(exist_ok=True)
    with (DATA/'records.jsonl').open('w',encoding='utf-8',newline='\n') as f:
        for row in sorted(rows,key=lambda r:r['record_id']):f.write(json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n')
    write(DATA/'provenance.json',dict(source_manifests=manifest,imported_at=stamp(),
        retrieval_hash=retrieval_hash(people),records_sha256=hashlib.sha256((DATA/'records.jsonl').read_bytes()).hexdigest(),
        candidate_records=len(rows),duplicate_source_occurrences=duplicates,
        note='Verified raw extracts only; previous decisions and profiles were not imported as labels.'))
    print(f'Imported {len(rows):,} unique candidate records; {duplicates} repeated source occurrences.')

def load_records():
    metadata=read(DATA/'provenance.json');path=DATA/'records.jsonl'
    if hashlib.sha256(path.read_bytes()).hexdigest()!=metadata['records_sha256']:raise ValueError('Raw snapshot changed; reimport explicitly')
    if retrieval_hash(profiles())!=metadata['retrieval_hash']:raise ValueError('Retrieval profile changed; reimport or scan')
    records=jsonlines(path)
    ids=set()
    for row in records:
        if row['record_id'] in ids:raise ValueError('Duplicate record ID in snapshot')
        ids.add(row['record_id'])
        if digest({k:row[k] for k in COLS})!=row['row_sha256']:raise ValueError('Row checksum mismatch')
    return records

def make_cases(records,people):
    cases={};membership=[];scorer_hash=model_hash()
    for row in records:
        for p in people:
            if not retrieve(row['name'],p):continue
            evidence=score(row,p);signature=case_signature(row,p,evidence)
            cid='c_'+digest(signature)[:20]
            case=cases.setdefault(cid,dict(case_id=cid,person_id=p['person_id'],context_sha256=digest(signature),
                profile_sha256=digest(p),model_sha256=scorer_hash,evidence=evidence,
                names=set(),locations=set(),careers=set(),cycles=set(),dates=[],record_ids=[],row_hashes=[]))
            case['names'].add(row['name']);case['locations'].add(', '.join(x for x in [row['city'],row['state']] if x))
            case['careers'].add((row['employer'],row['occupation']));case['cycles'].add(row['cycle'])
            try:case['dates'].append(datetime.strptime(row['transaction_dt'],'%m%d%Y').date().isoformat())
            except ValueError:pass
            case['record_ids'].append(row['record_id']);case['row_hashes'].append((row['record_id'],row['row_sha256']))
            membership.append(dict(case_id=cid,person_id=p['person_id'],record_id=row['record_id']))
    for case in cases.values():
        for key in ['names','locations','careers','cycles']:case[key]=sorted(case[key])
        case['date_min']=min(case['dates']) if case['dates'] else ''
        case['date_max']=max(case['dates']) if case['dates'] else ''
        del case['dates']
        case['record_ids'].sort();case['record_count']=len(case['record_ids'])
        case['evidence_sha256']=digest(sorted(case.pop('row_hashes')))
    result=sorted(cases.values(),key=lambda c:(c['person_id'],-c['evidence']['score'],c['case_id']))
    rows={r['record_id']:r for r in records}
    # Observational context for review, never an extra score or an automatic link.
    anchors=defaultdict(list)
    for c in result:
        if c['evidence']['recommendation']=='accept' and c['evidence']['signals']['career']['level']=='affiliation':
            anchors[c['person_id']].append(c)
    def postal(r):return norm(r['city']),norm(r['state']),re.sub(r'\D','',r['zip_code'])
    for c in result:
        if c['evidence']['signals']['career']['level']=='affiliation' and c['evidence']['recommendation']=='accept':continue
        anchor_locations=defaultdict(set)
        for a in anchors[c['person_id']]:
            for rid in a['record_ids']:anchor_locations[postal(rows[rid])].add(a['case_id'])
        five=defaultdict(set)
        for (city,state,code),ids in anchor_locations.items():
            if len(code)>=5:five[city,state,code[:5]].update(ids)
        exact_count=nine_count=five_count=0;supporting=set()
        for rid in c['record_ids']:
            city,state,code=postal(rows[rid]);exact=anchor_locations.get((city,state,code),set()) if code else set()
            broad=five.get((city,state,code[:5]),set()) if len(code)>=5 else set()
            exact_count+=bool(exact);nine_count+=bool(exact) and len(code)==9;five_count+=bool(broad);supporting.update(broad)
        if supporting:c['cohort_context']=dict(exact_postal_records=exact_count,zip9_records=nine_count,zip5_records=five_count,
            supporting_case_ids=sorted(supporting),note='Same city/state/postal code as strong affiliation cases; not independent identity proof or a residence claim.')
    return result,membership

def review_lookup(cases,history):
    by_id={c['case_id']:c for c in cases};latest={}
    for review in history:
        for judgment in review['judgments']:
            for cid in judgment['case_ids']:
                case=by_id.get(cid)
                if not case:continue
                valid=(review['person_id']==case['person_id'] and review['profile_sha256']==case['profile_sha256']
                       and review['model_sha256']==case['model_sha256']
                       and review['case_contexts'].get(cid)==case['context_sha256'])
                if review['scope']=='snapshot':valid=valid and review['case_evidence'].get(cid)==case['evidence_sha256']
                if review.get('cohort_sha256'):valid=valid and review['cohort_sha256']==cohort_hash(cases,review['person_id'])
                latest[cid]=dict(decision=judgment['decision'] if valid else 'stale',
                                review_id=review['review_id'],reason=judgment['reason'],note=judgment['note'],
                                person_note=review['person_note'],reviewer=review['reviewer'],reviewed_at=review['reviewed_at'],scope=review['scope'])
    return latest

def assignments(cases,history,records):
    latest=review_lookup(cases,history);rows={r['record_id']:r for r in records};links={}
    for case in cases:
        review=latest.get(case['case_id'])
        if not review or review['decision']!='accept':continue
        for rid in case['record_ids']:
            if rows[rid]['entity_tp']!='IND':raise ValueError('Non-individual accepted')
            if rid in links and links[rid]['person_id']!=case['person_id']:raise ValueError('Two accepted identities for '+rid)
            links[rid]=dict(record_id=rid,person_id=case['person_id'],case_id=case['case_id'],
                            row_sha256=rows[rid]['row_sha256'],review_id=review['review_id'],
                            score=case['evidence']['score'],evidence_sha256=case['evidence_sha256'])
    return sorted(links.values(),key=lambda l:l['record_id'])

def annotate(records,links):
    lookup={l['record_id']:l for l in links}
    if len(lookup)!=len(links):raise ValueError('Duplicate assignment')
    result=[]
    for row in records:
        rid=f"{row['cycle']}:{row['sub_id']}";link=lookup.get(rid)
        if link and digest({k:row[k] for k in COLS})!=link['row_sha256']:raise ValueError('Assigned row changed')
        result.append(dict(row,person_id=link['person_id'] if link else None,identity_review_id=link['review_id'] if link else None))
    return result

def append_reviews(incoming):
    records=load_records();cases,_=make_cases(records,profiles());by_id={c['case_id']:c for c in cases}
    history=jsonlines(DATA/'reviews.jsonl')
    for review in incoming:
        if review['scope'] not in {'snapshot','context_policy'}:raise ValueError('Invalid review scope')
        for field in ['reviewer','reviewed_at','person_note','profile_sha256','model_sha256']:
            if not review.get(field):raise ValueError('Missing '+field)
        seen=set()
        if review.get('cohort_sha256') and review['cohort_sha256']!=cohort_hash(cases,review['person_id']):raise ValueError('Supporting cohort changed')
        for judgment in review['judgments']:
            if judgment['decision'] not in {'accept','reject','unresolved'} or not judgment.get('note'):raise ValueError('Invalid judgment')
            for cid in judgment['case_ids']:
                if cid in seen:raise ValueError('Duplicate case in review')
                seen.add(cid);case=by_id[cid]
                if case['person_id']!=review['person_id'] or case['profile_sha256']!=review['profile_sha256'] or case['model_sha256']!=review['model_sha256']:
                    raise ValueError('Review is stale or names wrong person')
                if review['case_contexts'].get(cid)!=case['context_sha256']:raise ValueError('Context changed')
                if review['case_evidence'].get(cid)!=case['evidence_sha256']:raise ValueError('Review the current evidence')
        review.pop('review_id',None);review['review_id']='r_'+digest(review)[:20]
    assignments(cases,history+incoming,records)
    # Store the precise profiles and score policy referenced by historical reviews.
    for p in profiles():write(DATA/'versions'/(digest(p)+'.json'),p)
    write(DATA/'versions'/(model_hash()+'.json'),model_manifest())
    (DATA/'versions'/(model_hash()+'.py')).write_bytes((HERE/'model.py').read_bytes())
    with (DATA/'reviews.jsonl').open('a',encoding='utf-8',newline='\n') as f:
        for review in incoming:f.write(json.dumps(review,ensure_ascii=False,separators=(',',':'))+'\n')

def calibrate(cases,history,records):
    labels=read(DATA/'calibration_labels.json') if (DATA/'calibration_labels.json').exists() else []
    latest=review_lookup(cases,history)
    lookup={(c['person_id'],rid):c for c in cases for rid in c['record_ids']}
    rows={r['record_id']:r for r in records};result=[]
    for label in labels:
        baseline=Counter();reviewed=Counter();score_values=[];case_ids=set();not_retrieved=0
        for rid in label['record_ids']:
            if rid not in rows or rows[rid]['row_sha256']!=label['row_hashes'][rid]:
                not_retrieved+=1;continue
            case=lookup.get((label['person_id'],rid))
            if not case:not_retrieved+=1;continue
            case_ids.add(case['case_id']);score_values.append(case['evidence']['score'])
            baseline[case['evidence']['recommendation']]+=1
            reviewed[latest.get(case['case_id'],{}).get('decision','unreviewed')]+=1
        result.append(dict(label_id=label['label_id'],person_id=label['person_id'],expected=label['expected'],split=label['split'],
                           label_basis=label['label_basis'],records=len(label['record_ids']),distinct_cases=len(case_ids),
                           baseline=dict(baseline),reviewed=dict(reviewed),not_retrieved=not_retrieved,
                           scores=sorted(set(score_values))))
    return dict(score_is_probability=False,holdout_available=False,
                warning='Development anchors only: Musk and the Venable namesake informed profile design. No unbiased precision/recall claim.',
                label_sets=result,threshold_sweep=[dict(threshold=t,**{
                    expected:sum(1 for l in labels if l['expected']==expected for rid in l['record_ids']
                        if (l['person_id'],rid) in lookup and lookup[l['person_id'],rid]['evidence']['score']>=t)
                    for expected in ['accept','reject']}) for t in range(4,13)])

def build():
    people=profiles();records=load_records();cases,membership=make_cases(records,people)
    history=jsonlines(DATA/'reviews.jsonl');latest=review_lookup(cases,history);links=assignments(cases,history,records)
    annotated=annotate(records,links)
    assert len(annotated)==len(records) and all(all(a[k]==v for k,v in b.items()) for a,b in zip(annotated,records))
    summary=[]
    for p in people:
        subset=[c for c in cases if c['person_id']==p['person_id']]
        summary.append(dict(person_id=p['person_id'],name=p['display_name'],cases=len(subset),records=sum(c['record_count'] for c in subset),
            baseline=dict(Counter(c['evidence']['recommendation'] for c in subset)),
            reviewed=dict(Counter(latest.get(c['case_id'],{}).get('decision','unreviewed') for c in subset)),
            accepted_records=sum(c['record_count'] for c in subset if latest.get(c['case_id'],{}).get('decision')=='accept')))
    OUT.mkdir(exist_ok=True)
    write(OUT/'cases.json',cases);write(OUT/'summary.json',summary);write(OUT/'calibration.json',calibrate(cases,history,records))
    csv_write(OUT/'membership.csv',membership,['case_id','person_id','record_id'])
    csv_write(OUT/'assignments.csv',links,['record_id','person_id','case_id','row_sha256','review_id','score','evidence_sha256'])
    write(OUT/'validation.json',dict(model_version=MODEL_VERSION,model_sha256=model_hash(),raw_records=len(records),
          candidate_cases=len(cases),accepted_record_links=len(links),no_cross_person_collisions=True,
          source_fields_and_row_order_preserved=True,production_integration=False,
          records_sha256=read(DATA/'provenance.json')['records_sha256'],case_snapshot_sha256=digest(cases),
          reviewers=sorted({r['reviewer'] for r in history})))
    # The model packet excludes individual transactions, money, hashes, and repeated boilerplate.
    packets=[]
    for p in people:
        packet=dict(person_id=p['person_id'],name=p['display_name'],profile_note=p['profile_note'],
            profile={k:p[k] for k in ['given_names','middle_names','middle_initials','suffixes','distinctiveness_points',
                'plausible_locations','compatible_roles','contradictory_roles','known_namesakes']},
            affiliations=[a['label'] for a in p['affiliations']],source_ids=p['source_ids'],cases=[])
        for c in cases:
            if c['person_id']!=p['person_id']:continue
            e=c['evidence']
            packet['cases'].append(dict(id=c['case_id'],ref=f"{p['person_id']}:{len(packet['cases'])+1}",
                review_status=latest.get(c['case_id'],{}).get('decision','unreviewed'),
                coverage=[e['support_count'],e['observed_count'],e['missing_count'],e['conflict_count']],
                weights={k:s['points'] for k,s in e['signals'].items()},score=e['score'],baseline=e['recommendation'],n=c['record_count'],
                names=c['names'],places=c['locations'],work=c['careers'],flags=e['flags'],conflicts=e['conflicts'],
                cohort=c.get('cohort_context')))
        packets.append(packet)
    write(OUT/'review_packets.json',packets)
    (OUT/'review_packets.txt').write_text(compact_packet(packets),encoding='utf-8')
    # Compact pages carry case summaries; raw rows are fetched only on expansion.
    ui_cases=[{k:v for k,v in c.items() if k not in {'record_ids','evidence_sha256','context_sha256','model_sha256','profile_sha256'}} for c in cases]
    payload=dict(people=people,sources=read(DATA/'sources.json'),summary=summary,cases=ui_cases,reviews=latest,
                 calibration=read(OUT/'calibration.json'),version=MODEL_VERSION)
    template=(HERE/'review.html').read_text(encoding='utf-8')
    (OUT/'review.html').write_text(template.replace('/*DATA*/{}',json.dumps(payload,ensure_ascii=False,separators=(',',':')).replace('<','\\u003c')),encoding='utf-8')
    lines=['# Evidence-first review','', 'Scores rank evidence; they are not probabilities. Full records remain separate.','',
           '| Person | Cases | Source records | Accepted records |','|---|---:|---:|---:|']
    for s in summary:lines.append(f"| {s['name']} | {s['cases']} | {s['records']} | {s['accepted_records']} |")
    lines+=['','[Interactive review](review.html) · [Calibration](calibration.json) · [Assignments](assignments.csv)']
    (OUT/'README.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(json.dumps(dict(records=len(records),cases=len(cases),links=len(links),packet_bytes=(OUT/'review_packets.txt').stat().st_size)))


def compact_packet(packets):
    lines=['Fields: ref | score/baseline | support/observed + missing/conflicts | records | names | places | employer/occupation | flags; conflicts | postal context.',
           'Scores are not probabilities. A case groups equivalent review questions; counts are not independent evidence.',
           'Postal context is observed co-occurrence with strong affiliation cases, not a residence or identity assertion.',
           'Refs map to stable case IDs in review_packets.json. Full cited profiles: data/profiles.json and data/sources.json.']
    for p in packets:
        lines+=['',p['person_id']+' '+p['name'],p['profile_note']]
        profile=p['profile']
        lines+=['Names: '+','.join(profile['given_names'])+'; middle: '+','.join(profile['middle_names'])+
            '; initials: '+','.join(profile['middle_initials'])+'; suffixes: '+','.join(profile['suffixes'])+
            '; distinctiveness hypothesis: '+str(profile['distinctiveness_points']),
            'Affiliations: '+', '.join(p['affiliations']),
            'Explicit contrary occupations: '+', '.join(profile['contradictory_roles'])]
        for c in p['cases']:
            context=c['cohort'] or {};postal=f"zip9:{context.get('zip9_records',0)},zip5:{context.get('zip5_records',0)}" if context else ''
            sup,observed,missing,conflicts=c['coverage']
            lines.append(' | '.join([c['ref'],f"{c['score']}/{c['baseline']}",f'{sup}/{observed} +{missing}missing/{conflicts}conflicts',str(c['n']),';'.join(c['names']),
                ';'.join(c['places']),';'.join('/'.join(x) for x in c['work']),','.join(c['flags']+c['conflicts']),postal]))
    return '\n'.join(lines)+'\n'

def serve(port):
    records={r['record_id']:r for r in load_records()}
    cases={c['case_id']:c for c in read(OUT/'cases.json')}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.split('?')[0]=='/':content=(OUT/'review.html').read_bytes();kind='text/html; charset=utf-8'
            elif self.path.startswith('/api/case/'):
                cid=self.path.split('/')[-1]
                if cid not in cases:self.send_error(404);return
                content=json.dumps([records[rid] for rid in cases[cid]['record_ids']]).encode();kind='application/json'
            else:self.send_error(404);return
            self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(content)))
            self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(content)
    class Server(HTTPServer):allow_reuse_address=False
    server=Server(('127.0.0.1',port),Handler)
    print(f'Review: http://127.0.0.1:{port}/',flush=True);server.serve_forever()

def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    s=sub.add_parser('scan');s.add_argument('--cycle',type=int,action='append',required=True)
    i=sub.add_parser('import-extracts');i.add_argument('--directory',type=Path,required=True)
    sub.add_parser('build')
    p=sub.add_parser('packet');p.add_argument('--person',help='Stable person ID, e.g. p0001')
    p.add_argument('--pending',action='store_true',help='Only new or stale cases; do not re-review unchanged decisions')
    a=sub.add_parser('apply-reviews');a.add_argument('--file',type=Path,required=True)
    v=sub.add_parser('serve');v.add_argument('--port',type=int,default=18768)
    args=parser.parse_args()
    if args.command=='scan':scan(args.cycle)
    elif args.command=='import-extracts':import_extracts(args.directory)
    elif args.command=='build':build()
    elif args.command=='packet':
        packets=read(OUT/'review_packets.json')
        if args.pending:
            for p in packets:p['cases']=[c for c in p['cases'] if c['review_status'] in {'unreviewed','stale'}]
            packets=[p for p in packets if p['cases']]
        selected=[p for p in packets if not args.person or p['person_id']==args.person]
        print(compact_packet(selected) if selected else 'No matching new or stale cases.' if args.pending else 'No matching person.')
    elif args.command=='apply-reviews':append_reviews(read(args.file));build()
    elif args.command=='serve':serve(args.port)

if __name__=='__main__':main()
