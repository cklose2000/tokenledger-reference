import copy
import hashlib
import io
import json
import socket
import zipfile

from click.testing import CliRunner
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tl.bigquery.hashing import fingerprint
from tl.cli import cli
from tl.compare.matching import compare_tables
from tl.public_evidence import load_manifest, measured_summary, offline, verify_archive
from tl.stream import ValidationError


def fixture(tmp_path, *, changed_baseline=False):
    manifest = dict(schema_version='tokenledger-public-cloud-projection/v1',
                    files={}, pairs=[], inputs={'input_row_range':[1,2]},
                    outputs={'yield':{'key':['id'],'columns':['id','net_cents','ratio']}}, archive={})
    archive=tmp_path/'populations.zip'
    with zipfile.ZipFile(archive,'w') as z:
        for index,case in enumerate(('warmup','measured1','measured2','measured3')):
            pair=dict(case=case,role='warmup' if index==0 else 'measured',jobs=[],outputs={},
                      accounted_wall_seconds={'baseline':10+index,'native':9+index})
            refs={}
            for side in ('baseline','native'):
                receipt='mr-'+hashlib.sha256((case+side).encode()).hexdigest()[:32]
                table=pa.table(dict(id=['a','b'],net_cents=[100,200],ratio=[None,1.25],receipt_id=[receipt,receipt]))
                if changed_baseline and index==2 and side=='baseline':
                    table=table.set_column(1,'net_cents',pa.array([101,200]))
                buffer=io.BytesIO();pq.write_table(table,buffer);raw=buffer.getvalue()
                name=f'{case}/{side}/yield.parquet';z.writestr(name,raw)
                manifest['files'][name]=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),
                    original_receipt_id=receipt,population=fingerprint(table.drop(['receipt_id']),['id']))
                refs[side]=name
                pair['jobs'].append(dict(key=case+side,side=side,state='DONE',cache_hit=False,
                                        billed_bytes=200 if side=='baseline' else 100,
                                        slot_ms=10 if side=='baseline' else 150))
            expected=pa.table(dict(id=['a','b'],net_cents=[100,200],ratio=[None,1.25]))
            pair['outputs']['yield']=dict(**refs,comparison=compare_tables(expected,expected,['id']))
            manifest['pairs'].append(pair)
    raw=archive.read_bytes();manifest['archive']=dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
    path=tmp_path/'manifest.json';path.write_text(json.dumps(manifest))
    return path,archive,manifest


def test_recalculates_population_and_adverse_resource_ratio(tmp_path):
    manifest,archive,_=fixture(tmp_path)
    result=verify_archive(manifest,archive)
    assert result['comparisons']==4 and result['population_fingerprints']==8
    assert result['measurements']['measured_pairs']==3
    assert result['measurements']['all_jobs']==8
    assert result['measurements']['native_billed_bytes_reduction_pct']==50
    assert result['measurements']['native_slot_time_ratio']==15
    assert result['cloud_sql_recomputed'] is False and result['source_stream_recomputed'] is False


def test_one_cent_difference_fails_even_when_file_and_population_hashes_match(tmp_path):
    manifest,archive,_=fixture(tmp_path,changed_baseline=True)
    with pytest.raises(ValidationError,match='comparison differs'):
        verify_archive(manifest,archive)


def test_altered_archive_fails_before_population_work(tmp_path):
    manifest,archive,_=fixture(tmp_path)
    with archive.open('ab') as f:f.write(b'changed')
    with pytest.raises(ValidationError,match='release-pinned'):
        verify_archive(manifest,archive)


def test_wrong_receipt_identity_fails(tmp_path):
    path,archive,manifest=fixture(tmp_path)
    manifest['files'][next(iter(manifest['files']))]['original_receipt_id']='mr-'+'f'*32
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValidationError,match='receipt identity'):
        verify_archive(path,archive)


def test_duplicate_populations_and_job_attribution_fail(tmp_path):
    path,_,manifest=fixture(tmp_path)
    bad=copy.deepcopy(manifest)
    bad['pairs'][1]['outputs']['yield']['native']=bad['pairs'][0]['outputs']['yield']['native']
    path.write_text(json.dumps(bad))
    with pytest.raises(ValidationError,match='exactly one'):
        load_manifest(path)
    manifest['pairs'][1]['jobs'][0]['key']=manifest['pairs'][0]['jobs'][0]['key']
    with pytest.raises(ValidationError,match='duplicate'):
        measured_summary(manifest)


def test_unknown_billed_bytes_are_not_zero(tmp_path):
    _,_,manifest=fixture(tmp_path)
    manifest['pairs'][0]['jobs'][0]['billed_bytes']=None
    with pytest.raises(ValidationError,match='unobserved'):
        measured_summary(manifest)


@pytest.mark.parametrize('wall', [None, True, 0, -1, float('nan'), float('inf')])
def test_unknown_or_invalid_wall_cannot_enter_a_ratio(tmp_path, wall):
    _, _, manifest = fixture(tmp_path)
    manifest['pairs'][1]['accounted_wall_seconds']['baseline'] = wall
    with pytest.raises(ValidationError, match='wall time'):
        measured_summary(manifest)


def test_job_with_unknown_side_is_not_silently_dropped(tmp_path):
    _, _, manifest = fixture(tmp_path)
    manifest['pairs'][0]['jobs'][0]['side'] = 'unknown'
    with pytest.raises(ValidationError, match='unknown architecture'):
        measured_summary(manifest)


def test_offline_boundary_restores_network_functions():
    previous=socket.create_connection
    with offline(),pytest.raises(ValidationError,match='forbids network'):
        socket.create_connection(('example.invalid',443))
    assert socket.create_connection is previous


def test_cli_uses_shared_observation_receipt_and_replay(tmp_path):
    manifest,archive,_=fixture(tmp_path)
    destination=tmp_path/'review'
    runner=CliRunner()
    result=runner.invoke(cli,['release','evidence','verify','--archive',str(archive),
                              '--manifest',str(manifest),'--output',str(destination),'--json'])
    assert result.exit_code==0,result.output
    payload=json.loads(result.stdout.strip().splitlines()[-1])
    replay=runner.invoke(cli,['--artifact-root',str(destination),'receipt',payload['receipt_id'],'--json'])
    assert replay.exit_code==0,replay.output
    observation=json.loads(replay.stdout)
    assert observation['verified'] is True and observation['recomputed'] is False
    assert observation['result']['observations']['comparisons']==4
