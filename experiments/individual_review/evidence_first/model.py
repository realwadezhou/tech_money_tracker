"""Deterministic retrieval and explanatory scores; no LLM or old decisions."""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from difflib import SequenceMatcher

MODEL_VERSION = 'evidence-first-1'
POLICY = {
    'version': MODEL_VERSION, 'accept_at': 8,
    'name_exact_or_known': 4, 'name_initial': 0, 'name_unapproved': -4,
    'surname_typo_penalty': -1, 'middle_verified': 1, 'middle_conflict': -4,
    'suffix_supported': 1, 'suffix_conflict': -5,
    'city': 2, 'state': 1, 'career_affiliation': 5,
    'career_swapped': 4, 'career_fuzzy': 3, 'career_role_only': 1,
    'career_conflict': -8, 'known_namesake': -12,
    'probability_calibrated': False,
}
COLS = ['cmte_id','amndt_ind','rpt_tp','transaction_pgi','image_num','transaction_tp',
        'entity_tp','name','city','state','zip_code','employer','occupation',
        'transaction_dt','transaction_amt','other_id','tran_id','file_num','memo_cd','memo_text','sub_id']
TITLES = {'MR','MRS','MS','MISS','DR','MD','PHD','ESQ'}
SUFFIXES = {'JR','SR','II','III','IV','V'}
NEUTRAL = {'','NONE','N A','NA','NOT EMPLOYED','UNEMPLOYED','RETIRED','SELF','SELF EMPLOYED',
           'INFORMATION REQUESTED','INFORMATION REQUESTED PER BEST EFFORTS'}

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()

def norm(value):
    return ' '.join(re.findall(r'[A-Z0-9]+',unicodedata.normalize('NFKD',str(value).upper())))

def company(value):
    tokens=norm(value).split()
    while tokens and tokens[-1] in {'INC','INCORPORATED','LLC','LLP','LP','LTD','CORP','CORPORATION'}:
        tokens.pop()
    return ' '.join(tokens)

def parse_name(value):
    # Known filer annotation; do not erase arbitrary parenthetical name evidence.
    value=re.sub(r'\(\s*IN\s*KIND\s*\)','',value,flags=re.I)
    parts=value.split(',',1)
    if len(parts)==2:
        last,given=norm(parts[0]).split(),norm(parts[1]).split()
    else:
        tokens=[t for t in norm(value).split() if t not in TITLES|SUFFIXES]
        last,given=tokens[-1:],tokens[:-1]
    suffixes=[t for t in norm(value).split() if t in SUFFIXES]
    last=[t for t in last if t not in TITLES|SUFFIXES]
    given=[t for t in given if t not in TITLES|SUFFIXES]
    return dict(last=' '.join(last),first=given[0] if given else '',middle=given[1:],
                suffixes=suffixes,joint=('AND' in given))

def one_edit(a,b):
    if abs(len(a)-len(b))>1:return False
    if a==b:return False
    if len(a)==len(b):
        different=[i for i,(x,y) in enumerate(zip(a,b)) if x!=y]
        return len(different)==1 or (len(different)==2 and different[1]==different[0]+1
                                     and a[different[0]]==b[different[1]] and a[different[1]]==b[different[0]])
    short,long=sorted((a,b),key=len)
    return any(long[:i]+long[i+1:]==short for i in range(len(long)))

def retrieve(name,p):
    n=parse_name(name)
    if n['last'] not in p['retrieval_surnames']:return None
    if n['first'] in p['retrieval_given_names'] or n['first'] in p['given_names']:
        return 'exact_surname' if n['last']==p['surname'] else 'listed_surname_variant'
    if len(n['first'])==1 and any(g.startswith(n['first']) for g in p['given_names']):return 'first_initial'
    if len(n['first'])>=4 and any(one_edit(n['first'],g) for g in p['given_names']):return 'given_name_typo'
    return None

def role_match(value,patterns):
    return any(re.search(r'\b'+re.escape(p)+r'\w*\b',value) for p in patterns)

def affiliation(value,p):
    value=company(value)
    if value in NEUTRAL:return None,None
    for a in p['affiliations']:
        if value in {company(x) for x in a['aliases']}:return a,'exact'
    # Long distinctive company strings may be misspelled. Their matches always
    # reach contextual review rather than silently becoming new accepted aliases.
    choices=[]
    for a in p['affiliations']:
        for raw in a['aliases']:
            alias=company(raw)
            if min(len(alias),len(value))>=8:
                ratio=SequenceMatcher(None,value,alias).ratio()
                if ratio>=0.88:choices.append((ratio,a))
    if choices:return max(choices,key=lambda x:x[0])[1],'fuzzy'
    return None,None

def score(row,p):
    n=parse_name(row['name']); flags=[]; conflicts=[]; reasons=[]
    route=retrieve(row['name'],p)
    name_points=POLICY['name_exact_or_known'] if n['first'] in p['given_names'] else (
        POLICY['name_initial'] if len(n['first'])==1 else POLICY['name_unapproved'])
    name_level='compatible' if n['first'] in p['given_names'] else 'initial' if len(n['first'])==1 else 'unapproved'
    if name_level!='compatible':flags.append('name_needs_review')
    name_points+=p['distinctiveness_points'] if name_level=='compatible' else 0
    if n['last']!=p['surname']:
        name_points+=POLICY['surname_typo_penalty'];flags.append('surname_variant')
    if n['joint']:flags.append('joint_name')
    middle_level='missing'
    if n['middle']:
        full=' '.join(n['middle'])
        if full in p['middle_names']:
            middle_level='verified';name_points+=POLICY['middle_verified']
        elif all(m[0] in p['middle_initials'] for m in n['middle']):
            middle_level='compatible_initial'
            if any(len(m)>1 for m in n['middle']):flags.append('unverified_full_middle')
        else:
            middle_level='conflict';name_points+=POLICY['middle_conflict'];conflicts.append('middle_name');flags.append('name_needs_review')
    suffix_level='missing'
    if n['suffixes']:
        if all(s in p['suffixes'] for s in n['suffixes']):
            suffix_level='supported';name_points+=POLICY['suffix_supported']
        else:
            suffix_level='conflict';name_points+=POLICY['suffix_conflict'];conflicts.append('suffix');flags.append('name_needs_review')
    name_signal=dict(level=name_level,points=name_points,middle=middle_level,suffix=suffix_level,
                     surname='exact' if n['last']==p['surname'] else 'listed_variant',
                     distinctiveness_points=p['distinctiveness_points'])
    geo_points=0;geo_level='unknown';geo_sources=[]
    for loc in p['plausible_locations']:
        state,city=norm(row['state']),norm(row['city'])
        points=POLICY['city'] if city and city in loc['cities'].get(state,[]) else POLICY['state'] if state in loc['states'] else 0
        if points>geo_points:geo_points=points;geo_level='known_city' if points==POLICY['city'] else 'plausible_state';geo_sources=loc['source_ids']
    if not row['city'] and not row['state']:geo_level='missing'
    geography=dict(level=geo_level,points=geo_points,source_ids=geo_sources)
    employer,occupation=company(row['employer']),norm(row['occupation'])
    known,kind=affiliation(row['employer'],p)
    swapped,swapped_kind=affiliation(row['occupation'],p) if not known else (None,None)
    career_level='unknown';career_points=0;career_sources=[];affiliation_label=''
    if employer in NEUTRAL and occupation in NEUTRAL:
        career_level='missing_or_generic'
    elif role_match(occupation,p['contradictory_roles']):
        career_level='professional_conflict';career_points=POLICY['career_conflict'];conflicts.append('profession')
    elif known:
        affiliation_label=known['label'];career_sources=known['source_ids']
        career_level='affiliation' if kind=='exact' else 'affiliation_typo'
        career_points=POLICY['career_affiliation'] if kind=='exact' else POLICY['career_fuzzy']
        if kind=='fuzzy':flags.append('affiliation_typo')
        if occupation not in NEUTRAL and not role_match(occupation,p['compatible_roles']):
            flags.append('unexplained_role')
    elif swapped:
        affiliation_label=swapped['label'];career_sources=swapped['source_ids'];career_level='swapped_fields'
        career_points=POLICY['career_swapped'];flags.append('swapped_fields')
    elif role_match(occupation,p['compatible_roles']):
        career_level='compatible_role';career_points=POLICY['career_role_only']
    career=dict(level=career_level,points=career_points,affiliation=affiliation_label,source_ids=career_sources)
    namesake=None
    for other in p['known_namesakes']:
        if employer in {company(e) for e in other['employers']} and re.search(other['role_pattern'],occupation):
            namesake=other;break
    collision=dict(level='documented_namesake' if namesake else 'none_found',
                   points=POLICY['known_namesake'] if namesake else 0,
                   source_ids=[namesake['source_id']] if namesake else [])
    if namesake:conflicts.append('documented_namesake')
    if row['entity_tp']!='IND':flags.append('not_explicitly_individual')
    total=name_points+geo_points+career_points+collision['points']
    observed=[True,geography['level']!='missing',career_level!='missing_or_generic']
    supports=[name_points>0 and not any(c in conflicts for c in ('middle_name','suffix')),
              geo_points>0,career_points>0]
    recommendation='review'
    if namesake or career_level=='professional_conflict' or row['entity_tp'] in {'ORG','COM','PAC','CCM','PTY'}:
        recommendation='reject'
    elif total>=POLICY['accept_at'] and not flags and not conflicts:recommendation='accept'
    return dict(version=MODEL_VERSION,score=total,recommendation=recommendation,
                support_count=sum(supports),observed_count=sum(observed),family_count=3,
                missing_count=3-sum(observed),conflict_count=len(conflicts),
                signals=dict(name=name_signal,geography=geography,career=career,collision=collision),
                flags=sorted(set(flags)),conflicts=sorted(set(conflicts)),retrieval_route=route)

def case_signature(row,p,evidence):
    """Group equivalent review questions, not claims that unknown donors are one person."""
    sig=dict(person_id=p['person_id'],signals=evidence['signals'],flags=evidence['flags'],
             conflicts=evidence['conflicts'],entity=row['entity_tp'])
    # Never hide different unknown employers/professions behind a generic score.
    if evidence['signals']['career']['level'] in {'unknown','compatible_role','professional_conflict','affiliation_typo','swapped_fields'} or 'unexplained_role' in evidence['flags']:
        sig['career_context']=[company(row['employer']),norm(row['occupation'])]
    if (evidence['signals']['geography']['level']=='unknown'
            and evidence['signals']['career']['level'] not in {'affiliation','affiliation_typo','swapped_fields','professional_conflict'}
            and evidence['signals']['collision']['level']!='documented_namesake'):
        sig['location']=[norm(row['city']),norm(row['state'])]
    if evidence['signals']['name']['level']!='compatible' or 'unverified_full_middle' in evidence['flags'] or any(c in evidence['conflicts'] for c in ['middle_name','suffix']):
        sig['name_context']=parse_name(row['name'])
    return sig
