"""Checks for missing evidence, discriminating conflicts and reproducible review."""
import copy
import json
from pathlib import Path
import unittest
from experiments.individual_review.evidence_first.model import COLS,POLICY,case_signature,digest,parse_name,retrieve,score
from experiments.individual_review.evidence_first.run import make_cases,assignments,annotate,review_lookup,model_hash,cohort_hash
from experiments.individual_review.evidence_first import run

DATA=Path(__file__).resolve().parents[1]/'experiments/individual_review/evidence_first/data'

def profile(pid='p0001'):
    return next(p for p in json.loads((DATA/'profiles.json').read_text(encoding='utf-8'))['people'] if p['person_id']==pid)

def row(sub='1',**kw):
    r={k:'' for k in COLS}
    r.update(cycle=2024,sub_id=sub,name='MUSK, ELON',city='AUSTIN',state='TX',zip_code='78734',
             entity_tp='IND',transaction_dt='08082024',transaction_amt='10')
    r.update(kw)
    return dict(r,record_id=f"{r['cycle']}:{r['sub_id']}",row_sha256=digest({k:r[k] for k in COLS}))

def review(cases,p,decision='accept',scope='context_policy'):
    return dict(review_id='review-test',person_id=p['person_id'],profile_sha256=digest(p),model_sha256=model_hash(),
                person_note='Test review',reviewer='Test reviewer',reviewed_at='2026-09-20',scope=scope,
                case_contexts={c['case_id']:c['context_sha256'] for c in cases},
                case_evidence={c['case_id']:c['evidence_sha256'] for c in cases},
                judgments=[dict(decision=decision,reason='test',note='Test evidence.',case_ids=[c['case_id'] for c in cases])])

class EvidenceFirstTests(unittest.TestCase):
    def test_musk_sparse_fields_are_neutral_and_reeve_supports(self):
        p=profile();sparse=score(row(),p)
        self.assertEqual(sparse['recommendation'],'accept')
        self.assertEqual(sparse['signals']['career']['points'],0)
        self.assertEqual(sparse['missing_count'],1)
        self.assertEqual(score(row(name='MUSK, ELON REEVE'),p)['score'],sparse['score']+1)
        for description in ['UNEMPLOYED','NOT EMPLOYED','SELF-EMPLOYED','RETIRED']:
            e=score(row(employer=description,occupation=description),p)
            self.assertEqual(e['signals']['career']['points'],0)
            self.assertEqual(e['recommendation'],'accept')

    def test_multiple_plausible_places_and_unlisted_location(self):
        p=profile()
        self.assertGreater(score(row(state='CA',city='HAWTHORNE'),p)['signals']['geography']['points'],0)
        self.assertGreater(score(row(state='TX',city='LAKEWAY'),p)['signals']['geography']['points'],0)
        unseen=score(row(state='NY',city='NEW YORK'),p)
        self.assertEqual(unseen['signals']['geography']['points'],0)
        self.assertNotEqual(unseen['recommendation'],'reject')
        self.assertEqual(score(row(zip_code='787340032'),p)['score'],score(row(),p)['score'])

    def test_professional_contradiction_overcomes_positive_name_and_city(self):
        for role in ['ATTORNEY','PHYSICIAN','DOCTOR']:
            e=score(row(employer='SELF',occupation=role),profile())
            self.assertEqual(e['recommendation'],'reject')
            self.assertIn('profession',e['conflicts'])
        self.assertNotIn('profession',score(row(name='MUSK, ELON DR.'),profile())['conflicts'])

    def test_matching_affiliation_is_stronger_than_generic_role(self):
        p=profile()
        known=score(row(employer='SPACEX',occupation='CEO'),p)
        generic=score(row(employer='SELF',occupation='CEO'),p)
        self.assertGreater(known['score'],generic['score'])
        self.assertEqual(known['support_count'],3)

    def test_common_name_and_missing_career_do_not_get_full_confidence(self):
        e=score(row(name='SCHMIDT, ERIC',city='',state=''),profile('p0012'))
        self.assertEqual(e['support_count'],1)
        self.assertEqual(e['observed_count'],1)
        self.assertEqual(e['recommendation'],'review')

    def test_namesakes_and_employee_roles_are_explicit_negative_evidence(self):
        for pid,changes in [('p0003',dict(name='HOROWITZ, BENJAMIN E.',employer='VENABLE LLP',occupation='ATTORNEY')),
                            ('p0012',dict(name='SCHMIDT, ERIC',employer='GOOGLE LLC',occupation='DEVELOPER PROGRAMS ENGINEER'))]:
            e=score(row(**changes),profile(pid))
            self.assertEqual(e['recommendation'],'reject')
            self.assertGreater(e['conflict_count'],0)

    def test_surname_gate_preserves_suffix_and_routes_typos_to_review(self):
        p=profile('p0002')
        r=row(name='ANDREESEN, MARC',employer='A16Z',occupation='FOUNDER')
        self.assertTrue(retrieve(r['name'],p))
        self.assertEqual(score(r,p)['recommendation'],'review')
        self.assertIsNone(retrieve('UNRELATED, MARC',p))
        parsed=parse_name('GATES III, WILLIAM H. MR.')
        self.assertEqual(parsed['last'],'GATES');self.assertEqual(parsed['suffixes'],['III'])
        self.assertEqual(parse_name('William H Gates III')['suffixes'],['III'])

    def test_fields_swapped_are_visible_and_require_contextual_review(self):
        r=row(name='HOROWITZ, BEN',employer='FOUNDER',occupation='ANDREESSEN HOROWITZ')
        e=score(r,profile('p0003'))
        self.assertEqual(e['signals']['career']['level'],'swapped_fields')
        self.assertIn('swapped_fields',e['flags']);self.assertEqual(e['recommendation'],'review')

    def test_equivalent_records_share_case_but_unknown_professions_do_not(self):
        p=profile()
        cases,_=make_cases([row(),row('2',cycle=2026,zip_code='787340032')],[p])
        self.assertEqual(len(cases),1)
        a=row(employer='UNFAMILIAR A',occupation='EXECUTIVE')
        b=row(employer='UNFAMILIAR B',occupation='EXECUTIVE')
        self.assertNotEqual(case_signature(a,p,score(a,p)),case_signature(b,p,score(b,p)))

    def test_only_recorded_reviews_emit_assignments(self):
        p=profile();records=[row()];cases,_=make_cases(records,[p])
        self.assertEqual(assignments(cases,[],records),[])
        self.assertEqual(len(assignments(cases,[review(cases,p)],records)),1)

    def test_context_policy_accepts_repeats_but_snapshot_requires_new_review(self):
        p=profile();a=[row()];old,_=make_cases(a,[p]);new_rows=a+[row('2')];new,_=make_cases(new_rows,[p])
        self.assertEqual(len(assignments(new,[review(old,p)],new_rows)),2)
        self.assertEqual(assignments(new,[review(old,p,scope='snapshot')],new_rows),[])
        changed=copy.deepcopy(p);changed['contradictory_roles'].append('ENGINEER')
        newer,_=make_cases(new_rows,[changed])
        self.assertEqual(assignments(newer,[review(old,p)],new_rows),[])

    def test_new_identity_context_is_not_covered_by_existing_review(self):
        p=profile();old,_=make_cases([row()],[p]);records=[row('2',occupation='PHYSICIAN')]
        changed,_=make_cases(records,[p])
        self.assertEqual(assignments(changed,[review(old,p)],records),[])

    def test_unverified_middle_expansion_is_not_a_verified_name(self):
        p=profile('p0002');r=row(name='ANDREESSEN, MARC LOUIS',employer='A16Z',occupation='FOUNDER')
        e=score(r,p)
        self.assertEqual(e['recommendation'],'review')
        self.assertIn('unverified_full_middle',e['flags'])
        r2=dict(r,name='ANDREESSEN, MARC LOWELL')
        self.assertNotEqual(case_signature(r,p,e),case_signature(r2,p,score(r2,p)))

    def test_cohort_support_is_visible_without_increasing_score_and_can_expire_review(self):
        p=profile();a=row(employer='SPACEX',occupation='CEO',zip_code='787340032')
        b=row('2',zip_code='787340032');cases,_=make_cases([a,b],[p])
        sparse=next(c for c in cases if c['record_ids']==[b['record_id']])
        self.assertEqual(sparse['evidence'],score(b,p))
        self.assertEqual(sparse['cohort_context']['zip9_records'],1)
        r=review([sparse],p,scope='snapshot');r['cohort_sha256']=cohort_hash(cases,p['person_id'])
        self.assertEqual(len(assignments(cases,[r],[a,b])),1)
        changed,_=make_cases([b],[p])
        self.assertEqual(assignments(changed,[r],[b]),[])

    def test_cross_person_conflicts_fail_and_annotation_preserves_raw_data(self):
        p=profile();q=copy.deepcopy(p);q['person_id']='other'
        records=[row(),row('2',transaction_amt='-10',memo_cd='X'),row()]
        # The annotation preserves repeated occurrences in the caller's data.
        cases,_=make_cases(records[:2],[p]);links=assignments(cases,[review(cases,p)],records[:2])
        result=annotate(records,links)
        self.assertEqual(len(result),len(records))
        self.assertTrue(all(all(after[k]==v for k,v in before.items()) for before,after in zip(records,result)))
        with self.assertRaisesRegex(ValueError,'Assigned row changed'):annotate([row(transaction_amt='11')],links)
        both,_=make_cases(records[:1],[p,q])
        with self.assertRaisesRegex(ValueError,'Two accepted identities'):
            assignments(both,[review([c for c in both if c['person_id']==x['person_id']],x) for x in [p,q]],records[:1])

    def test_saved_pilot_is_reproducible_and_development_labels_are_covered(self):
        records=run.load_records();cases,membership=make_cases(records,run.profiles())
        history=run.jsonlines(DATA/'reviews.jsonl');latest=review_lookup(cases,history)
        self.assertEqual(digest(cases),digest(run.read(run.OUT/'cases.json')))
        self.assertEqual(len(latest),len(cases))
        self.assertFalse(any(r['decision']=='stale' for r in latest.values()))
        self.assertEqual({r['record_id'] for r in records},{m['record_id'] for m in membership})
        links=assignments(cases,history,records)
        self.assertEqual(len(links),len({l['record_id'] for l in links}))
        annotated=annotate(records,links)
        self.assertEqual([{k:r[k] for k in COLS} for r in records],[{k:r[k] for k in COLS} for r in annotated])
        calibration=run.calibrate(cases,history,records)
        self.assertFalse(calibration['holdout_available'])
        for label in calibration['label_sets']:
            self.assertEqual(label['not_retrieved'],0)
            self.assertEqual(label['reviewed'],{label['expected']:label['records']})

if __name__=='__main__':unittest.main()
