"""Validate documentation examples without dispatching commands or opening services."""
from pathlib import Path
import contextlib
import io
import json
import re
import shlex
import subprocess
import sys
import tomllib

from forge.cli.main import build_parser
from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.sources.exchanges import ExchangeSource
from forge.historical.imports.models import ImportConfig
from forge.markets.ranking.policy import RankingPolicy
from forge.shared.config.models import AppSettings

root = Path('docs/content')
files = [*root.glob('workflows/acquisition/*.md'), *root.glob('workflows/markets/*.md'), root/'configuration/acquisition.md', root/'configuration/markets.md', *root.glob('reference/cli/*.md')]
parser = build_parser()
commands = []
configs = []
for path in files:
    text = path.read_text()
    for block in re.findall(r'```bash\n(.*?)```', text, re.S):
        for line in block.splitlines():
            if not line.strip() or line.lstrip().startswith('#'):
                continue
            tokens = shlex.split(line)
            assert tokens[0] in ('marketforge', 'mf'), (path, tokens)
            # Parsing only: never call main(), a service handler, or a provider operation.
            args = parser.parse_args(tokens[1:])
            assert callable(getattr(args, 'service_handler', None)) or args.command == 'acquisition'
            commands.append({'page': str(path), 'command': line})
    for n, block in enumerate(re.findall(r'```toml\n(.*?)```', text, re.S), 1):
        document = tomllib.loads(block)
        if 'requests' in document:
            model = AcquisitionConfig.model_validate(document)
            for request in model.requests:
                source = object.__new__(ExchangeSource)
                source.exchange = request.exchange
                source.validate(request)  # Pure capability validation; no client or request.
            kind = 'AcquisitionConfig + adapter request support'
        elif 'source_key' in document:
            ImportConfig.model_validate(document)
            kind = 'ImportConfig (schema only; illustrative files not read)'
        elif 'metric' in document:
            RankingPolicy.model_validate(document)
            kind = 'RankingPolicy (schema only; no input observations read)'
        else:
            AppSettings.model_validate(document)
            kind = 'AppSettings'
        configs.append({'page': str(path), 'block': n, 'validation': kind})

# Verify every exposed related hierarchy through non-mutating help.
help_paths = [
 ['acquisition'], *[['acquisition', a] for a in ('new','edit','validate','list','map','download','plan')],
 ['acquisition','import'], *[['acquisition','import',a] for a in ('new','inspect','validate','plan','run')],
 ['acquisition','import','format','new'],
 ['markets'], *[['markets',a] for a in ('sync','refresh','list','show')],
 ['markets','rank'], *[['markets','rank',a] for a in ('new','validate','build','list','show','update','delete')],
 ['storage'], *[['storage',a] for a in ('coverage','list','show','verify','reconcile','prune','delete')],
 *[['runs',a] for a in ('list','show','logs','report')],
 ['database','status'], ['database','migrate'], ['doctor'],
]
help_results = []
for path in help_paths:
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        try:
            parser.parse_args([*path,'--help'])
        except SystemExit as error:
            assert error.code == 0
        else:
            raise AssertionError('Help did not exit')
    help_results.append({'command': 'marketforge '+' '.join(path)+' --help', 'help': output.getvalue()})

result = {'commands': commands, 'toml_examples': configs, 'help': help_results, 'operational_execution': False}
Path('.agents/drafts/docdev/acquisition-markets-verification.json').write_text(json.dumps(result,indent=2))
print(f'PASS: {len(commands)} shell examples parsed; {len(configs)} complete TOML examples validated; {len(help_results)} command help paths checked. No handlers executed.')
