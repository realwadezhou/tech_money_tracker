"""One-time migration of cited facts only. No old decisions or matching rules."""
from pathlib import Path
import json

HERE = Path(__file__).resolve().parent
OLD = HERE.parent / 'old' / 'pilot'

# These are explicitly authored profile hypotheses. They are not learned from
# accepted/rejected labels, and distinctiveness is not a population probability.
SPECS = [
 ('MUSK',['ELON'],['R'],[],2,[('SpaceX',['SPACEX','SPACE X','SPACE EXPLORATION TECHNOLOGIES CORP','SPACE EXPLORATION TECHNOLOGIES CORPORA','SPACE EXPLORATION TECHNOLOGIES CORPORATION']),('Tesla',['TESLA','TESLA MOTORS','SPACE X TESLA MOTORS'])]),
 ('ANDREESSEN',['MARC','MARK'],['L'],[],2,[('a16z',['A16Z','ANDREESSEN HOROWITZ','AH CAPITAL MANAGEMENT','A16Z CAPITAL MANAGEMENT'])]),
 ('HOROWITZ',['BEN','BENJAMIN'],['A'],[],0,[('a16z',['A16Z','ANDREESSEN HOROWITZ','AH CAPITAL MANAGEMENT','A16Z CAPITAL MANAGEMENT'])]),
 ('HOFFMAN',['REID'],['G'],[],2,[('Greylock',['GREYLOCK','GREYLOCK PARTNER','GREYLOCK PARTNERS','GRAYLOCK']),('LinkedIn',['LINKEDIN'])]),
 ('MOSKOVITZ',['DUSTIN'],['A'],[],2,[('Asana',['ASANA'])]),
 ('THIEL',['PETER'],['A'],[],0,[('Founders Fund',['FOUNDERS FUND']),('Thiel Capital',['THIEL CAPITAL'])]),
 ('SACKS',['DAVID'],['O'],[],0,[('Craft Ventures',['CRAFT VENTURES','CRAFT VENTURES MANAGEMENT']),('Yammer',['YAMMER'])]),
 ('ALTMAN',['SAM','SAMUEL'],['H'],[],1,[('OpenAI',['OPENAI','OPEN AI'])]),
 ('LUCKEY',['PALMER'],['F'],[],2,[('Anduril',['ANDURIL','ANDURIL INDUSTRIES']),('Oculus',['OCULUS','OCULUS VR'])]),
 ('KARP',['ALEX','ALEXANDER'],['C'],[],2,[('Palantir',['PALANTIR','PALANTIR TECHNOLOGIES','PALANTIR TECHNOLOGY'])]),
 ('LONSDALE',['JOE','JOSEPH'],['T'],[],2,[('8VC',['8VC'])]),
 ('SCHMIDT',['ERIC'],['E'],[],0,[('Google / Alphabet',['GOOGLE','ALPHABET']),('Hillspire',['HILLSPIRE']),('Schmidt philanthropy',['SCHMIDT FUTURES','SCHMIDT FAMILY FOUNDATION']),('Relativity Space',['RELATIVITY SPACE'])]),
 ('PARKER',['SEAN'],['N'],[],0,[('Parker Foundation',['PARKER FOUNDATION','THE PARKER FOUNDATION','SEAN PARKER FOUNDATION','SEAN N PARKER FOUNDATION'])]),
 ('KHOSLA',['VINOD'],[],[],2,[('Khosla Ventures',['KHOSLA VENTURES'])]),
 ('CONWAY',['RON','RONALD'],['C'],[],0,[('SV Angel',['SV ANGEL','SVANGEL','SV ANGEL MANAGEMENT'])]),
 ('BALLMER',['STEVE','STEVEN'],['A'],[],2,[('Microsoft (historical)',['MICROSOFT']),('Ballmer Group',['BALLMER GROUP'])]),
 ('GATES',['BILL','WILLIAM'],['H'],['III'],0,[('Microsoft (historical)',['MICROSOFT']),('Gates Foundation',['GATES FOUNDATION','BILL MELINDA GATES FOUNDATION','BILL AND MELINDA GATES FOUNDATION','BILL MELINDA GATES FOUNDATIO']),('Breakthrough Energy',['BREAKTHROUGH ENERGY'])]),
 ('DELL',['MICHAEL'],['S'],[],0,[('Dell',['DELL','DELL TECHNOLOGIES'])]),
 ('ELLISON',['LARRY','LAWRENCE'],['J'],[],0,[('Oracle',['ORACLE','ORACLE CORPORATION','ORACLE AMERICA'])]),
 ('SANDBERG',['SHERYL'],['K'],[],2,[('Meta / Facebook (historical)',['META','FACEBOOK','META PLATFORMS']),('Lean In',['LEAN IN','LEANIN'])]),
]

def main():
    old = json.loads((OLD / 'people.json').read_text(encoding='utf-8'))
    sources = {s['source_id']:s for p in old['people'] for s in p['sources']}
    sources['user-musk-review'] = dict(source_id='user-musk-review',title='User review of the Musk cohort',
        kind='user_feedback',accessed='2026-09-20',
        claim='The user identified all 140 displayed Musk records as the famous Elon Musk; confirmed Reeve and compatible California/Texas locations. This is a development label, not independent holdout validation.')
    people=[]
    for original,spec in zip(old['people'],SPECS):
        surname,given,middle,suffix,distinct,affiliations=spec
        refs=[s['source_id'] for s in original['sources'] if s.get('purpose')!='collision']
        p=dict(person_id=original['person_id'],display_name=original['display_name'],surname=surname,
               given_names=given,middle_initials=middle,middle_names=[],suffixes=suffix,
               retrieval_surnames=original['search_surnames'],retrieval_given_names=original['search_given_names'],
               distinctiveness_points=distinct,
               distinctiveness_basis='Authored pilot hypothesis about name ambiguity; not a measured population frequency.',
               middle_basis='Candidate name variants retained for contextual review; initial agreement alone is weak evidence.',
               affiliations=[dict(label=label,aliases=aliases,source_ids=refs) for label,aliases in affiliations],
               plausible_locations=[],
               compatible_roles=['CEO','CHIEF','FOUNDER','OWNER','PARTNER','INVESTOR','VENTURE','EXECUTIVE','CHAIR','PHILANTHROP','ENTREPRENEUR','MANAGING','PRINCIPAL','PRESIDENT','ADVISOR','ADVISER'],
               contradictory_roles=[],known_namesakes=[],source_ids=refs,
               profile_note='Affiliations include career history. Missing occupation and unlisted locations are neutral; a filing title is not an independently verified employment claim.')
        people.append(p)
    by_id={p['person_id']:p for p in people}
    musk=by_id['p0001']
    musk['middle_names']=['REEVE']
    musk['middle_basis']='Reeve confirmed by the user; the R initial is compatible.'
    musk['source_ids'].append('user-musk-review')
    musk['plausible_locations']=[dict(states=['CA','TX'],cities={'TX':['AUSTIN','LAKEWAY']},source_ids=['user-musk-review'],meaning='Compatible reported geography, not proof of residence.')]
    musk['compatible_roles'].append('ENGINEER')
    musk['contradictory_roles']=['ATTORNEY','LAWYER','PHYSICIAN','SURGEON','DENTIST','DOCTOR']
    musk['profile_note']='Elon / Elon Reeve, California and Texas are compatible. Austin/Lakeway corroborates sparse records. Blank, unemployed and self-employed descriptions are neutral. Practising law or medicine contradicts this profile.'
    by_id['p0003']['contradictory_roles']=['ATTORNEY','LAWYER']
    by_id['p0012']['contradictory_roles']=['SOFTWARE ENGINEER','DEVELOPER PROGRAMS ENGINEER']
    # Alternative professional profiles are independent sources, not old outcomes.
    for pid,employers,role,source_id in [
        ('p0003',['VENABLE'],'ATTORNEY','collision-ben-venable'),
        ('p0007',['DUANE MORRIS'],'ATTORNEY|COUNSEL','collision-sacks-duane'),
        ('p0007',['NIH'],'SCIENTIST','collision-sacks-nih'),
        ('p0007',['PACE UNIVERSITY'],'PROFESSOR','collision-sachs-pace'),
        ('p0007',['SELF','SELF EMPLOYED'],'DISPUTE RESOLUTION','collision-sacks-mediator'),
        ('p0012',['GLUE UP'],'CEO|EXECUTIVE','collision-schmidt-glueup'),
        ('p0018',['COMPLETE BUSINESS SERVICES'],'CPA','collision-dell-cpa'),
    ]:
        by_id[pid]['known_namesakes'].append(dict(employers=employers,role_pattern=role,source_id=source_id))
        by_id[pid]['source_ids'].append(source_id)
    # Record this location only where the cited biography explicitly supports it.
    lonsdale_refs=by_id['p0011']['source_ids']
    by_id['p0011']['plausible_locations']=[dict(states=['TX'],cities={'TX':['AUSTIN']},source_ids=lonsdale_refs,meaning='The 8VC biography places his firm in Austin; work context, not residential proof.')]
    data=HERE/'data'
    data.mkdir(exist_ok=True)
    for name,value in [('profiles.json',dict(schema_version=1,people=people)),('sources.json',sources)]:
        (data/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f'Created {len(people)} new profiles; imported no decisions.')

if __name__=='__main__':
    main()
