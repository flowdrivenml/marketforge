import json,zipfile,csv,io,pathlib
j=json.load(open('data/tests/historical/normalize/full-run/20261115-185513/staging/dataset-1335781940318399008/processing-job.json'))
a=[]
for t in j['tasks']:
 if t['stream_id']!='bitget.linear.perpetual.BTCUSDT.trades': continue
 n=missing=reg=0; lo=hi=prev=None
 with zipfile.ZipFile(t['input_path']) as z:
  for name in z.namelist():
   with z.open(name) as f:
    for r in csv.DictReader(io.TextIOWrapper(f)):
     ts=int(r['timestamp']); n+=1; missing+=not bool(r['trade_id']); reg+=prev is not None and ts<prev; prev=ts
     lo=ts if lo is None else min(lo,ts); hi=ts if hi is None else max(hi,ts)
 a.append(dict(task=t['task_id'],path=t['input_path'],rows=n,min_ms=lo,max_ms=hi,missing_ids=missing,regressions=reg))
pairs=[]
for i,x in enumerate(a):
 for y in a[i+1:]:
  if x['min_ms']<=y['max_ms'] and y['min_ms']<=x['max_ms']:
   lo=max(x['min_ms'],y['min_ms']);hi=min(x['max_ms'],y['max_ms']); sets=[]
   for s in (x,y):
    d={}
    with zipfile.ZipFile(s['path']) as z:
     for name in z.namelist():
      with z.open(name) as f:
       for r in csv.DictReader(io.TextIOWrapper(f)):
        if lo<=int(r['timestamp'])<=hi: d[r['trade_id']]=r
    sets.append(d)
   common=sets[0].keys()&sets[1].keys()
   pairs.append(dict(tasks=[x['task'],y['task']],min_ms=lo,max_ms=hi,rows_in_intersection=list(map(len,sets)),common_ids=len(common),conflicts=sum(sets[0][k]!=sets[1][k] for k in common)))
json.dump(dict(sources=a,overlaps=pairs),open('.agents/reports/full-batch-trade-evidence.json','w'),indent=2)
print(json.dumps(pairs,indent=2));print('rows',sum(s['rows'] for s in a))
