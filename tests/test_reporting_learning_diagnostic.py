from datetime import date

import pytest
import sqlglot

from tl.evolution import diagnostic as d


def population():
    return [dict(month=date(2026,7,31),lens=lens,status='defined',nrr=0.95) for lens in sorted(d.LENSES)]


def test_frozen_oracle_and_offline_candidate_agree_on_each_named_failure():
    original=population(); cases=d.cases()['cases']
    actual={r['case_id']:r for r in d.offline(original,cases).to_pylist()}
    for case in cases:
        expected=d.oracle(d.scenario(original,case['scenario']),case['requested_date'],case.get('population','nrr'))
        assert expected['status']==case['expected']
        assert actual[case['case_id']]==dict(case_id=case['case_id'],**expected)
    assert actual['wrong-grain']['selected_rows']==0
    assert actual['wrong-grain']['suggested_date']=='2026-07-31'
    assert actual['unavailable']['unavailable_rows']==4
    assert population()==original


def test_native_query_is_a_read_only_select_and_parses_without_materialization():
    sql=d.query("SELECT DATE '2026-07-31' AS month,'base_t12m' AS lens,'defined' AS status,0.95 AS nrr",d.cases()['cases'])
    statements=sqlglot.parse(sql,read='bigquery')
    assert len(statements)==1 and isinstance(statements[0],sqlglot.exp.Select)
    assert not list(statements[0].find_all(sqlglot.exp.Filter))
    assert not any(isinstance(node,(sqlglot.exp.Create,sqlglot.exp.Insert,sqlglot.exp.Update,sqlglot.exp.Delete))
                   for node in statements[0].walk())


@pytest.mark.parametrize('case',[
    dict(case_id='bad',requested_date='2026-07-01',scenario='substitute_date'),
    dict(case_id='bad',requested_date="2026-07-01'; DROP TABLE source --"),
])
def test_unknown_scenario_and_sql_date_injection_rejected(case):
    with pytest.raises(ValueError):
        d.query('SELECT 1',[case])


def test_recursive_native_report_keeps_recursion_at_statement_scope():
    report="""WITH RECURSIVE hierarchy AS (SELECT 1 AS depth UNION ALL
        SELECT depth+1 FROM hierarchy WHERE depth<2)
        SELECT DATE '2026-07-31' AS month,'base_t12m' AS lens,'defined' AS status,0.95 AS nrr
        FROM hierarchy WHERE depth=2"""
    parsed=sqlglot.parse_one(d.query(report,d.cases()['cases']),read='bigquery')
    assert list(parsed.find_all(sqlglot.exp.With))==[parsed.args['with_']]
    assert parsed.args['with_'].args['recursive'] is True
    assert parsed.args['with_'].expressions[0].alias=='hierarchy'
    assert parsed.args['with_'].expressions[-2].alias=='classified'


def test_report_cte_cannot_shadow_diagnostic_scope():
    with pytest.raises(ValueError,match='collides'):
        d.query('WITH requests AS (SELECT 1) SELECT * FROM requests',d.cases()['cases'])
    with pytest.raises(ValueError,match='collides'):
        d.query('WITH inspected AS (SELECT 1) SELECT * FROM inspected',d.cases()['cases'])
