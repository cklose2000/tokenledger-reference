"""Bounded, usage-only local source readiness. No client instrumentation."""

from datetime import date,datetime
import hashlib
import json
from pathlib import Path
import uuid

from tl.stream.events import UTC,ValidationError,canonical,iso,timestamp

TOKEN_FIELDS = ('total_tokens','input_tokens','cached_input_tokens','cache_write_input_tokens',
                'output_tokens','reasoning_output_tokens')


def token_sample(application, *, source_root, day, workspace, client_version, account):
    """Inspect only a matching workspace's session on the explicitly selected day.

    The original file is never copied. Evidence is a strict projection with
    a source-file hash and explicit scope, not a full-account completeness claim.
    """
    application.validate()
    if not account or not client_version:
        raise ValidationError('source account alias and client version are required')
    with application.reader().connect() as conn:
        registered=conn.execute("""SELECT count(*) FROM stream.activity
            WHERE activity='application_inventory' AND customer=?
              AND json_extract_string(feature_json,'$.provider')='openai'
              AND json_extract_string(feature_json,'$.scope')='laptop'
              AND json_extract_string(feature_json,'$.ownership')='personally_owned'""",[account]).fetchone()[0]
    if not registered:
        raise ValidationError('confirm the personal OpenAI laptop account in the private inventory before collection')
    selected_day = date.fromisoformat(day)
    directory = Path(source_root)/selected_day.strftime('%Y/%m/%d')
    wanted = str(Path(workspace).resolve()).casefold()
    found = None
    for source in sorted(directory.glob('*.jsonl'),reverse=True)[:30]:
        if source.stat().st_size > 64*1024*1024:
            continue
        with source.open(encoding='utf-8') as handle:
            try:
                header = json.loads(handle.readline(1024*1024))
            except (ValueError,UnicodeError):
                continue
        meta = header.get('payload',{}) if header.get('type') == 'session_meta' else {}
        if not isinstance(meta,dict) or not isinstance(meta.get('cwd'),str):
            continue
        if str(Path(meta['cwd']).resolve()).casefold() != wanted:
            continue
        # The safe header confirms the requested personal workspace before body scan.
        original_hash = hashlib.sha256()
        observations = []
        with source.open('rb') as handle:
            for number,raw in enumerate(handle,1):
                original_hash.update(raw)
                if len(raw)>4*1024*1024:
                    continue
                try:
                    event = json.loads(raw)
                except (ValueError,UnicodeError):
                    continue
                payload = event.get('payload',{}) if isinstance(event,dict) else {}
                if (event.get('type')!='event_msg' or not isinstance(payload,dict)
                        or payload.get('type')!='token_count' or not isinstance(payload.get('info'),dict)):
                    continue
                info = payload['info']
                counters = {}
                for kind in ('total_token_usage','last_token_usage'):
                    values = info.get(kind)
                    if not isinstance(values,dict):
                        continue
                    selected = {key:values.get(key) for key in TOKEN_FIELDS}
                    if any(value is not None and (type(value) is not int or value<0) for value in selected.values()):
                        raise ValidationError('source token sample contains invalid quantities')
                    if any(value is not None for value in selected.values()):
                        counters[kind]=selected
                if counters:
                    observations.append(dict(source_line=number,ts=iso(timestamp(event['timestamp'])),
                        session_id=str(meta.get('id') or source.stem),counters=counters))
        if observations:
            found = source,original_hash.hexdigest(),observations
            break
    if found is None:
        raise ValidationError('no bounded token sample found for the selected day and workspace; source readiness remains pending')
    source,source_hash,observations=found
    target=application.path('evidence/source-readiness/'+uuid.uuid4().hex)
    target.mkdir(parents=True,exist_ok=False)
    content=''.join(canonical(row)+'\n' for row in observations).encode()
    (target/'observations.jsonl').write_bytes(content)
    manifest=dict(schema_version='tokenledger-source-readiness/v1',source='codex-local-token-count',
        parser_version='v1',client_version=client_version,account_alias=account,
        account_basis='operator-confirmed personally owned laptop scope; not an authenticated account claim',
        source_file=str(source.resolve()),source_sha256=source_hash,workspace_scope=str(Path(workspace).resolve()),
        workspace_basis='session initial workspace; per-token project attribution unverified',
        collected_at=iso(datetime.now(UTC)),selected_day=day,
        first_observed_at=observations[0]['ts'],last_observed_at=observations[-1]['ts'],
        observations_sha256=hashlib.sha256(content).hexdigest(),
        coverage='partial',interpretation='raw cumulative and last counters; not summed consumption',
        gaps=['Other local sessions','Cloud and mobile activity','Account-wide totals','Billed charges'],
        persistence='Only allowlisted usage metadata retained; original content was not copied',
        status='sample_ready_not_imported')
    (target/'manifest.json').write_text(canonical(manifest)+'\n',encoding='utf-8',newline='\n')
    return dict(status='sample_ready_not_imported',evidence_directory=str(target))
