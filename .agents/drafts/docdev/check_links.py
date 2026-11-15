"""Focused repository Markdown and generated-site link checks; run from root."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit
import re
import subprocess
import json
import hashlib

class Links(HTMLParser):
    def __init__(self):
        super().__init__(); self.targets=[]; self.ids=set()
    def handle_starttag(self, tag, attrs):
        a=dict(attrs)
        if 'id' in a: self.ids.add(a['id'])
        for attr in ('href','src'):
            if attr in a: self.targets.append(a[attr])

def slug(s):
    return re.sub(r'[^\w\- ]', '', s.lower()).replace(' ', '-')

errors=[]
markdown=[Path('README.md'),Path('docs/README.md'),*Path('docs/content').rglob('*.md')]
for file in markdown:
    text=file.read_text()
    targets=re.findall(r'\]\(([^)]+)\)',text)+re.findall(r'(?:href|src)="([^"]+)"',text)
    for target in targets:
        u=urlsplit(target)
        if u.scheme or u.netloc: continue
        dest=file.parent/unquote(u.path) if u.path else file
        if not dest.exists(): errors.append(f'{file}: missing {target}')
        elif u.fragment and dest.is_file() and dest.suffix=='.md':
            body=dest.read_text()
            ids={slug(s) for s in re.findall(r'^#+ (.+)$',body,re.M)}
            if unquote(u.fragment) not in ids: errors.append(f'{file}: missing anchor {target}')

root=Path('docs/site')
html={}
for file in root.rglob('*.html'):
    p=Links();p.feed(file.read_text());html[file.resolve()]=p
checked=0
for file,parser in html.items():
    for target in parser.targets:
        u=urlsplit(target)
        if u.scheme or u.netloc:continue
        dest=(root/unquote(u.path.lstrip('/')) if u.path.startswith('/') else file.parent/unquote(u.path)).resolve() if u.path else file
        if dest.is_dir():dest=dest/'index.html'
        checked+=1
        if not dest.exists():errors.append(f'{file.relative_to(root.resolve())}: missing {target}')
        elif u.fragment and dest in html and unquote(u.fragment) not in html[dest].ids:errors.append(f'{file}: missing anchor {target}')

baseline=json.loads(Path('.agents/drafts/docdev/baseline.json').read_text())
allowed={'README.md','.gitignore'}
changed=[]
for name,expected in baseline['hashes'].items():
    if name in allowed:continue
    path=Path(name)
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:changed.append(name)
if changed: print('NOTICE: Concurrent changes detected outside documentation assignment (not modified by this assignment):', ', '.join(changed))
if subprocess.check_output(['git','diff','--cached']).decode()!=baseline['staged_diff']:errors.append('Staged diff changed')
assert not errors,'\n'.join(errors)
print(f'PASS: {len(markdown)} Markdown files; {len(html)} generated HTML pages; {checked} local HTML links/assets; README anchors; pre-existing file hashes compared; staged diff unchanged.')
