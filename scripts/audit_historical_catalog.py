#!/usr/bin/env python3
"""Offline reconciliation of retained psql exports; never opens market data.

Exit 1: unapproved differences; exit 2: invalid input. Output is deterministic.
Exchange identity comes from retained format documentation (Identity tables),
not SQL insertion order. Numeric exchange IDs must resolve consistently across
all exported format codes. The exports have no exchange-name table.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
GATE_EVIDENCE = 'https://www.gate.com/uk/announcements/article/21688/fiat%20500e%20rose%20gold'


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON object key: {key}')
        result[key] = value
    return result


def export_rows(path, raw):
    columns = ('id exchange_id format_code data_type instrument_type market_category container_format compression record_format'.split()
               if raw else 'id raw_format_id target_schema'.split())
    rows = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not re.match(r'^\s*\d+\s*\|', line):
            continue
        pieces = line.split('|', len(columns))
        if len(pieces) != len(columns) + 1:
            raise ValueError(f'{path.name}:{number}: malformed record')
        row = dict(zip(columns, (x.strip() for x in pieces[:-1])))
        rest = pieces[-1].lstrip()
        value, end = json.JSONDecoder(object_pairs_hook=unique_object).raw_decode(rest)
        # Split from the right so notes may contain literal pipe characters.
        tail = rest[end:].strip()
        if not tail.startswith('|'):
            raise ValueError(f'{path.name}:{number}: missing notes separator')
        notes, created, updated = tail[1:].rsplit('|', 2)
        row.update(notes=notes.strip(), created_at=created.strip(), updated_at=updated.strip())
        row['schema_json' if raw else 'rules_json'] = value
        for key in ('id', 'exchange_id', 'raw_format_id'):
            if key in row:
                row[key] = int(row[key])
        rows.append(row)
    if not rows:
        raise ValueError(f'{path}: no records')
    return sorted(rows, key=lambda r: r['id'])


def differences(a, b, path=''):
    """Presence is distinct from null; arrays and scalar types are significant."""
    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(a.keys() | b.keys()):
            p = f'{path}.{key}' if path else key
            if key not in a or key not in b:
                yield dict(field=p, database=a.get(key), current=b.get(key),
                           difference='additional' if key not in a else 'missing')
            else:
                yield from differences(a[key], b[key], p)
    elif isinstance(a, list) and isinstance(b, list):
        for i in range(max(len(a), len(b))):
            p = f'{path}[{i}]'
            if i >= len(a) or i >= len(b):
                yield dict(field=p, database=a[i] if i < len(a) else None,
                           current=b[i] if i < len(b) else None,
                           difference='additional' if i >= len(a) else 'missing')
            else:
                yield from differences(a[i], b[i], p)
    elif type(a) is not type(b) or a != b:
        yield dict(field=path, database=a, current=b, difference='value')


def format_exchanges(repo):
    result = {}
    aliases = {'Gate.io': 'gateio', 'Bybit': 'bybit', 'Binance': 'binance', 'OKX': 'okx', 'Bitget': 'bitget'}
    for path in sorted((repo / 'docs/tofixlater/raw-formats').glob('*.md')):
        text = path.read_text()
        exchange = re.search(r'\|\s*Exchange\s*\|\s*([^|]+)\|', text)
        code = re.search(r'\|\s*Format ID\s*\|\s*`([^`]+)`', text)
        if exchange and code:
            name = aliases[exchange[1].strip()]
            key = code[1]
            if key in result and result[key] != name:
                raise ValueError(f'contradictory documented exchange for {key}')
            result[key] = name
    return result


def audit(repo, root):
    raw = export_rows(root / 'raw formats.txt', True)
    rules = export_rows(root / 'normalization rules.txt', False)
    report = {k: [] for k in ('missing_records', 'unexpected_records', 'duplicate_identities',
        'mismatched_rules', 'mismatched_raw_schemas', 'unresolved_mappings',
        'unsupported_profiles', 'missing_historical_metadata', 'invalid_documents')}
    provenance = dict(schema_version=1, sources={}, raw_formats=[], normalization_rules=[])
    for name in ('raw formats.txt', 'normalization rules.txt'):
        provenance['sources'][name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    def load(tree):
        docs = {}
        for path in sorted((root / tree).rglob('*.toml')):
            doc = tomllib.loads(path.read_text())
            identity = doc['id']
            if identity in docs:
                report['duplicate_identities'].append(dict(kind=tree, id=identity))
            docs[identity] = (doc, path.relative_to(root).as_posix())
        return docs
    profiles, schemas = load('profiles'), load('raw_schemas')
    for kind, rows in [('raw_format_id', raw), ('normalization_rule_id', rules)]:
        for key, count in sorted(Counter(r['id'] for r in rows).items()):
            if count > 1:
                report['duplicate_identities'].append(dict(kind=kind, id=key))
    identities = defaultdict(list)
    for pid, (p, path) in profiles.items():
        data_type = {'trades': 'trade', 'depth': 'l2'}.get(Path(path).parts[1])
        identity = tuple(p[k] for k in ('exchange', 'market_category', 'instrument_type', 'format_code')) + (data_type,)
        identities[identity].append(pid)
        expected = '.'.join((*identity[:3], p['format_code'].lower(), 'v1'))
        if pid != expected or p.get('schema_version') != 1 or p.get('kind') != 'profile':
            report['invalid_documents'].append(dict(profile=pid, reason='declared identity/version/kind'))
        if p['physical'] not in schemas:
            report['unresolved_mappings'].append(dict(profile=pid, physical=p['physical']))
        if p.get('unsupported_reason'):
            report['unsupported_profiles'].append(dict(profile=pid, reason=p['unsupported_reason']))
        for target, count in Counter(r['target'] for r in p['rules']).items():
            if count > 1:
                report['duplicate_identities'].append(dict(profile=pid, target=target))
    for identity, ids in identities.items():
        if len(ids) > 1:
            report['duplicate_identities'].append(dict(identity=identity, profiles=ids))
    documented = format_exchanges(repo)
    exchange_ids = defaultdict(set)
    for r in raw:
        if r['format_code'] in documented:
            exchange_ids[r['exchange_id']].add(documented[r['format_code']])
    report['exchange_identity_evidence'] = 'docs/tofixlater/raw-formats/*.md Identity tables: explicit Format ID and Exchange; exported numeric IDs cross-checked across all definitions. No SQL row-order assumption.'
    report['exchange_resolution'] = {str(k): sorted(v) for k, v in sorted(exchange_ids.items())}
    mapped, used_profiles, used_schemas = {}, set(), set()
    exact_raw = 0
    for r in raw:
        names = exchange_ids[r['exchange_id']]
        candidates = []
        if len(names) == 1:
            identity = (next(iter(names)), r['market_category'], r['instrument_type'], r['format_code'], r['data_type'])
            candidates = identities.get(identity, [])
        if len(candidates) != 1:
            report['unresolved_mappings'].append(dict(raw_format_id=r['id'], candidates=candidates))
            report['missing_records'].append(dict(raw_format_id=r['id']))
            provenance['raw_formats'].append(dict(original=r, profile=None, raw_schema=None))
            continue
        pid = candidates[0]
        p = profiles[pid][0]
        mapped[r['id']] = pid
        used_profiles.add(pid)
        used_schemas.add(p['physical'])
        provenance['raw_formats'].append(dict(original=r, profile=pid, raw_schema=p['physical']))
        if p['physical'] not in schemas:
            continue
        s = schemas[p['physical']][0]
        expected = {k: r[k] for k in ('container_format', 'record_format')}
        expected.update(supported_containers=[{'tar.gz': 'tar_gzip'}.get(r['compression'], r['compression'])], raw_schema=r['schema_json'])
        current = {k: s[k] for k in expected}
        diffs = list(differences(expected, current))
        exact_raw += not diffs
        report['mismatched_raw_schemas'].extend(dict(profile=pid, raw_format_id=r['id'], **d) for d in diffs)
    used_rules = set()
    exact_rules = 0
    for r in rules:
        pid = mapped.get(r['raw_format_id'])
        p = profiles[pid][0] if pid else None
        provenance['normalization_rules'].append(dict(original=r, profile=pid, raw_schema=p['physical'] if p else None, format_code=next((x['format_code'] for x in raw if x['id'] == r['raw_format_id']), None), target=r['target_schema']))
        matches = [x for x in p['rules'] if x['target'] == r['target_schema']] if p else []
        if len(matches) != 1:
            report['missing_records'].append(dict(normalization_rule_id=r['id'], profile=pid, target=r['target_schema']))
            continue
        key = (pid, r['target_schema'])
        if key in used_rules:
            report['duplicate_identities'].append(dict(normalization_rule_id=r['id'], profile=pid, target=r['target_schema']))
        used_rules.add(key)
        diffs = list(differences(r['rules_json'], matches[0]['rules'], 'rules'))
        exact_rules += not diffs
        # Approve only the complete, precisely known old/new map for these two records.
        approved = (pid == 'gateio.spot.spot.gate-b1.v1' and r['id'] in (24, 25)
                    and r['target_schema'] == {24: 'l2_snapshot', 25: 'l2_update'}[r['id']]
                    and r['rules_json'].get('side', {}).get('values') == {'0': 'bid', '1': 'ask'}
                    and matches[0]['rules'].get('side', {}).get('values') == {'1': 'ask', '2': 'bid'})
        for d in diffs:
            accepted = approved and d['field'] in ('rules.side.values.0', 'rules.side.values.2')
            report['mismatched_rules'].append(dict(profile=pid, raw_format_id=r['raw_format_id'], normalization_rule_id=r['id'], target=r['target_schema'], **d, approved=accepted, evidence=GATE_EVIDENCE if accepted else None))
    for pid, (p, _) in profiles.items():
        if pid not in used_profiles:
            report['unexpected_records'].append(dict(profile=pid))
        for r in p['rules']:
            if (pid, r['target']) not in used_rules:
                report['unexpected_records'].append(dict(profile=pid, target=r['target']))
    for sid in sorted(schemas.keys() - used_schemas):
        report['unexpected_records'].append(dict(raw_schema=sid))
    report['shared_schema_records'] = {sid: sorted(x['original']['id'] for x in provenance['raw_formats'] if x['raw_schema'] == sid) for sid in sorted(schemas)}
    report['counts'] = dict(database_raw_formats=len(raw), database_normalization_rules=len(rules), profiles=len(profiles), physical_schemas=len(schemas), normalization_rules=sum(len(p['rules']) for p, _ in profiles.values()), exact_raw_formats=exact_raw, exact_normalization_rules=exact_rules, mismatched_normalization_records=len({d['normalization_rule_id'] for d in report['mismatched_rules']}))
    # Notes/timestamps are preserved verbatim in the companion provenance output.
    report['source_sha256'] = provenance['sources']
    report['catalog_sha256'] = {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for _, path in sorted([*profiles.values(), *schemas.values()], key=lambda x: x[1])}
    report['historical_metadata'] = 'Preserved per original record in provenance; intentionally absent from runtime TOML.'
    report['correctness_limit'] = 'Document equality and declared invariants only; no runtime normalization or source archive verification.'
    unapproved = sum(len(report[k]) for k in ('missing_records', 'unexpected_records', 'duplicate_identities', 'mismatched_raw_schemas', 'unresolved_mappings', 'missing_historical_metadata', 'invalid_documents'))
    unapproved += sum(not d['approved'] for d in report['mismatched_rules'])
    report['unapproved_discrepancies'] = unapproved
    return report, provenance


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT / 'data/.config/historical')
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--provenance', type=Path, required=True)
    args = parser.parse_args()
    try:
        outputs = [args.report.resolve(), args.provenance.resolve()]
        protected = [args.root.resolve(), (ROOT / 'docs/tofixlater/raw-formats').resolve()]
        if outputs[0] == outputs[1] or any(p == base or base in p.parents for p in outputs for base in protected):
            raise ValueError('outputs must be distinct and outside source configuration/evidence trees')
        report, provenance = audit(ROOT, args.root)
        for path, value in ((args.report, report), (args.provenance, provenance)):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + '\n')
        print(json.dumps(report['counts'], sort_keys=True))
        for key in ('missing_records', 'unexpected_records', 'duplicate_identities', 'mismatched_rules', 'mismatched_raw_schemas', 'unresolved_mappings', 'unsupported_profiles', 'missing_historical_metadata'):
            print(f'{key}: {len(report[key])}')
        for d in report['mismatched_rules']:
            print(f"{d['profile']} / {d['target']} / {d['field']}: database={json.dumps(d['database'])} current={json.dumps(d['current'])} approved={d['approved']}")
        print(f"Unapproved discrepancies: {report['unapproved_discrepancies']}")
        return bool(report['unapproved_discrepancies'])
    except (ValueError, OSError, KeyError) as e:
        print(f'audit input error: {e}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
