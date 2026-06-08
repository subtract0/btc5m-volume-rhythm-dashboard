#!/usr/bin/env python3
import json, math, os, time, gzip, statistics, re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import requests

ROOT = Path('/Users/am/Code/btc5m-volume-rhythm-dashboard')
OUT = ROOT / 'index.html'
DATA_JSON = ROOT / 'data_snapshot.json'
CACHE = ROOT / '.pm_file_cache.json'
DAYS = int(os.environ.get('DAYS', '60'))
NOW_MS = int(time.time() * 1000)
START_MS = NOW_MS - DAYS * 24 * 3600 * 1000
PM_LOOKBACK_DAYS = int(os.environ.get('PM_LOOKBACK_DAYS', '21'))
PM_START_MS = NOW_MS - PM_LOOKBACK_DAYS * 24 * 3600 * 1000
INCLUDE_RUNNING_PM = os.environ.get('INCLUDE_RUNNING_PM', '1') != '0'
WD = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
UA = {'User-Agent': 'studio1-polymarket-research-dashboard/1.1'}
ART = Path('/Users/am/Code/autonomous-polymarket-trader-openspec/artifacts')


def get_json(url, params, timeout=30, retries=3):
    for i in range(retries):
        r = requests.get(url, params=params, headers=UA, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if i == retries - 1:
            raise RuntimeError(f'{url} {r.status_code} {r.text[:200]}')
        time.sleep(1 + i)


def fetch_klines(base, path, symbol='BTCUSDT', interval='1h'):
    rows = []
    start = START_MS
    while start < NOW_MS:
        batch = get_json(base + path, {
            'symbol': symbol, 'interval': interval, 'startTime': start,
            'endTime': NOW_MS, 'limit': 1000,
        })
        if not batch:
            break
        rows.extend(batch)
        start = int(batch[-1][0]) + 3600 * 1000
        if len(batch) < 1000:
            break
        time.sleep(0.08)
    return rows


def fetch_futures_data(endpoint, period='1h'):
    rows = []
    start = START_MS
    while start < NOW_MS:
        end = min(start + 499 * 3600 * 1000, NOW_MS)
        try:
            batch = get_json('https://fapi.binance.com' + endpoint, {
                'symbol': 'BTCUSDT', 'period': period, 'startTime': start,
                'endTime': end, 'limit': 500,
            })
        except Exception:
            break
        if batch:
            rows.extend(batch)
        start = end + 3600 * 1000
        time.sleep(0.08)
    d = {int(x['timestamp']): x for x in rows if 'timestamp' in x}
    return [d[k] for k in sorted(d)]


def hourkey(ms):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.weekday(), dt.hour, dt.strftime('%Y-%m-%dT%H:00:00Z')


def mean(xs):
    xs = [x for x in xs if x is not None and math.isfinite(x)]
    return sum(xs) / len(xs) if xs else None


def aggregate(records, metrics):
    cells = {(d, h): {m: [] for m in metrics} for d in range(7) for h in range(24)}
    for rec in records:
        d, h, _ = hourkey(rec['ts'])
        for m in metrics:
            v = rec.get(m)
            if v is not None and math.isfinite(v):
                cells[(d, h)][m].append(v)
    out = {}
    for m in metrics:
        out[m] = [[round(mean(cells[(d, h)][m]) or 0, 4) for h in range(24)] for d in range(7)]
    counts = [[len(cells[(d, h)][metrics[0]]) for h in range(24)] for d in range(7)] if metrics else [[0] * 24 for _ in range(7)]
    return out, counts


def discover_pm_files():
    files = []
    # Best source: explicit week-index manifest. Include completed plus current running chunk when present.
    manifest = ART / 'rotated-orderbook-capture/week-index/latest.json'
    if manifest.exists():
        try:
            man = json.load(open(manifest))
            for c in man.get('chunks', []):
                status = c.get('status')
                if status != 'completed' and not (INCLUDE_RUNNING_PM and status == 'running'):
                    continue
                p = (c.get('tape') or {}).get('compressedEventsPath') or (c.get('tape') or {}).get('eventsPath')
                if p:
                    p = Path(p)
                    if p.suffix == '.gz' and not p.exists() and p.with_suffix('').exists():
                        p = p.with_suffix('')
                    if p.exists():
                        files.append(('rotated-week-index', str(p)))
        except Exception:
            pass
    # Older one-off live orderbook tapes. They are not continuous, but they fill historical coverage where files still exist.
    for p in sorted((ART / 'live-orderbook-tape').glob('orderbook-tape-*/events.jsonl')):
        # Only include likely recent enough files by embedded date or mtime.
        if p.stat().st_mtime * 1000 >= PM_START_MS or re.search(r'2026-05-(2[6-9]|3[0-1])|2026-06-', str(p)):
            files.append(('live-orderbook-tape', str(p)))
    # Deduplicate by real path.
    seen = set(); out = []
    for src, p in files:
        rp = str(Path(p).resolve())
        if rp not in seen:
            seen.add(rp); out.append((src, p))
    return out


def parse_pm_file(path):
    p = Path(path)
    sig = f'{p}:{p.stat().st_size}:{int(p.stat().st_mtime)}'
    cache = {}
    if CACHE.exists():
        try:
            cache = json.load(open(CACHE))
        except Exception:
            cache = {}
    if sig in cache:
        return cache[sig]
    cells = defaultdict(lambda: {'notional': 0.0, 'shares': 0.0, 'trades': 0, 'markets': set()})
    meta = {'events': 0, 'errors': 0, 'first_ms': None, 'last_ms': None, 'bytes': p.stat().st_size}
    opener = gzip.open if str(p).endswith('.gz') else open
    with opener(p, 'rt', errors='ignore') as f:
        for line in f:
            if 'last_trade_price' not in line or 'btc-updown-5m-' not in line:
                continue
            try:
                e = json.loads(line)
            except Exception:
                meta['errors'] += 1
                continue
            if e.get('eventType') != 'last_trade_price':
                continue
            if not str(e.get('slug', '')).startswith('btc-updown-5m-'):
                continue
            payload = e.get('payload') or {}
            try:
                ms = int(e.get('receivedAtMs') or int(payload.get('timestamp', 0)))
                price = float(payload.get('price') or 0)
                size = float(payload.get('size') or 0)
            except Exception:
                meta['errors'] += 1
                continue
            if ms < PM_START_MS:
                continue
            d, h, _ = hourkey(ms)
            cell = cells[f'{d},{h}']
            cell['notional'] += price * size
            cell['shares'] += size
            cell['trades'] += 1
            cell['markets'].add(e.get('slug'))
            meta['events'] += 1
            meta['first_ms'] = ms if meta['first_ms'] is None else min(meta['first_ms'], ms)
            meta['last_ms'] = ms if meta['last_ms'] is None else max(meta['last_ms'], ms)
    serial = {
        'meta': meta,
        'cells': {k: {'notional': v['notional'], 'shares': v['shares'], 'trades': v['trades'], 'markets': sorted(v['markets'])}
                  for k, v in cells.items()}
    }
    cache[sig] = serial
    # Drop stale cache entries for same path.
    prefix = f'{p}:'
    for k in list(cache.keys()):
        if k.startswith(prefix) and k != sig:
            del cache[k]
    CACHE.write_text(json.dumps(cache))
    return serial


def zgrid(mat):
    vals = [v for row in mat for v in row if v is not None]
    m = mean(vals) or 0
    sd = statistics.pstdev(vals) if len(vals) > 1 else 1
    return [[(v - m) / (sd or 1) for v in row] for row in mat]


print('Fetching Binance spot/futures/OI/taker data...', flush=True)
spot = fetch_klines('https://api.binance.com', '/api/v3/klines')
fut = fetch_klines('https://fapi.binance.com', '/fapi/v1/klines')
oi = fetch_futures_data('/futures/data/openInterestHist')
taker = fetch_futures_data('/futures/data/takerlongshortRatio')
spot_by = {int(x[0]): x for x in spot}
oi_by = {int(x['timestamp']): x for x in oi}
taker_by = {int(x['timestamp']): x for x in taker}
records = []
for row in fut:
    ts = int(row[0]); op = float(row[1]); hi = float(row[2]); lo = float(row[3]); cl = float(row[4]); quote = float(row[7])
    spotrow = spot_by.get(ts); oirow = oi_by.get(ts); tk = taker_by.get(ts)
    records.append({
        'ts': ts,
        'futures_quote_usd': quote,
        'futures_btc': float(row[5]),
        'spot_quote_usd': float(spotrow[7]) if spotrow else None,
        'trade_count': float(row[8]),
        'range_pct': (hi - lo) / op * 100 if op else None,
        'abs_return_pct': abs(cl - op) / op * 100 if op else None,
        'taker_buy_share_pct': float(row[10]) / quote * 100 if quote else None,
        'open_interest_usd': float(oirow['sumOpenInterestValue']) if oirow else None,
        'taker_buy_sell_ratio': float(tk['buySellRatio']) if tk else None,
    })
metrics = ['futures_quote_usd', 'spot_quote_usd', 'trade_count', 'range_pct', 'abs_return_pct', 'taker_buy_share_pct', 'open_interest_usd', 'taker_buy_sell_ratio']
agg, counts = aggregate(records, metrics)

print('Aggregating local Polymarket BTC5M tape last_trade_price events...', flush=True)
pm_files = discover_pm_files()
pm_cells = defaultdict(lambda: {'notional': 0.0, 'shares': 0.0, 'trades': 0, 'markets': set()})
pm_meta = {
    'source': 'local Studio1 CLOB websocket tape; eventType=last_trade_price only; USD notional ~= price*shares',
    'lookback_days': PM_LOOKBACK_DAYS,
    'candidate_files': len(pm_files),
    'processed_files': [],
    'missing_manifest_files': 'week-index references older chunks whose event files are no longer present locally; this dashboard uses every BTC5M tape file still present',
    'events': 0,
    'errors': 0,
    'first_event_utc': None,
    'last_event_utc': None,
    'bytes_scanned': 0,
}
for src, p in pm_files:
    parsed = parse_pm_file(p)
    m = parsed['meta']
    if m['events'] == 0:
        continue
    pm_meta['processed_files'].append({'source': src, 'path': p, 'events': m['events'], 'bytes': m['bytes'], 'first_ms': m['first_ms'], 'last_ms': m['last_ms']})
    pm_meta['events'] += m['events']; pm_meta['errors'] += m['errors']; pm_meta['bytes_scanned'] += m['bytes']
    if m['first_ms'] is not None:
        pm_meta['first_event_utc'] = datetime.fromtimestamp(m['first_ms'] / 1000, tz=timezone.utc).isoformat() if pm_meta['first_event_utc'] is None else min(pm_meta['first_event_utc'], datetime.fromtimestamp(m['first_ms'] / 1000, tz=timezone.utc).isoformat())
        pm_meta['last_event_utc'] = datetime.fromtimestamp(m['last_ms'] / 1000, tz=timezone.utc).isoformat() if pm_meta['last_event_utc'] is None else max(pm_meta['last_event_utc'], datetime.fromtimestamp(m['last_ms'] / 1000, tz=timezone.utc).isoformat())
    for k, v in parsed['cells'].items():
        cell = pm_cells[k]
        cell['notional'] += v['notional']; cell['shares'] += v['shares']; cell['trades'] += v['trades']; cell['markets'].update(v['markets'])

pm_notional = [[round(pm_cells[f'{d},{h}']['notional'], 2) for h in range(24)] for d in range(7)]
pm_trades = [[pm_cells[f'{d},{h}']['trades'] for h in range(24)] for d in range(7)]
pm_markets = [[len(pm_cells[f'{d},{h}']['markets']) for h in range(24)] for d in range(7)]
zvol = zgrid(agg['futures_quote_usd']); ztr = zgrid(agg['trade_count']); zrg = zgrid(agg['range_pct'])
activity = [[round(50 + 12 * (zvol[d][h] + ztr[d][h] + zrg[d][h]) / 3, 1) for h in range(24)] for d in range(7)]

suggestions = [
    {'title': 'Add realized PnL overlay by weekday/hour', 'why': 'Directly shows which hours actually made or lost money for each bot lane, not just where BTC was active.'},
    {'title': 'Add bot kill/size schedule from activity buckets', 'why': 'Convert this from descriptive dashboard into operational gates: disable, half-size, normal-size, or collect-only by hour.'},
    {'title': 'Add Deribit options volume/open-interest/liquidation proxies', 'why': 'Options/derivatives stress often precedes whale moves and stop-runs; futures-only is useful but incomplete.'},
    {'title': 'Join BTC5M Polymarket flow with Binance 5m candles, not just 1h', 'why': 'Hourly averages hide the exact 5m window microstructure where our bots win or get adverse-selected.'},
    {'title': 'Show spread/depth/queue-risk heatmaps', 'why': 'Maker strategies lose when spreads/depth look good but queue and cancel latency are toxic. Volume alone is not enough.'},
    {'title': 'Flag whale candles and liquidation-cascade hours', 'why': 'Mark windows where a large BTC move happened in thin liquidity; these are likely no-trade or special-strategy regimes.'},
    {'title': 'Separate weekday vs weekend model performance', 'why': 'Your empirical weekend losses should become hard regime tags rather than anecdotal memory.'},
    {'title': 'Add Polymarket resolution-window intensity', 'why': 'BTC5M resolves 12 times/hour; show which hours have the most late-window trading and last-minute flips.'},
    {'title': 'Add live freshness and tape-coverage panel', 'why': 'Prevents false confidence from missing local files; every chart should say exactly what tape span it covers.'},
    {'title': 'Add recommended next-market scan', 'why': 'Use the same volume/regime logic to identify softer BTC/ETH 15m/1h or slower markets where our edge is more plausible.'},
]

snap = {
    'generated_at_utc': datetime.now(timezone.utc).isoformat(),
    'days': DAYS,
    'binance_hours': len(records),
    'weekday_labels': WD,
    'hour_labels': [f'{h:02d}:00' for h in range(24)],
    'binance_metrics': agg,
    'binance_counts': counts,
    'polymarket': {'notional_usd_raw': pm_notional, 'trade_events': pm_trades, 'markets_touched': pm_markets, 'meta': pm_meta},
    'composite_activity': activity,
    'suggestions': suggestions,
    'notes': [
        'Binance spot/futures are public hourly BTCUSDT data from Binance, aggregated by UTC weekday/hour over the lookback window.',
        'Derivatives activity proxies: Binance USD-M futures quote volume, trade count, taker buy share, taker buy/sell ratio, and futures open interest. Historical BTC options volume is not included in this cut.',
        'Polymarket BTC5M volume uses every local BTC5M CLOB websocket tape file still present on Studio1 within the lookback. Notional ~= price*shares.',
        'Some week-index manifest entries point to older chunk files that are no longer present locally; the data-coverage panel makes this explicit.',
        'BTC5M markets resolve every 5 minutes; each hour has 12 windows. markets_touched indicates how many BTC5M slugs had trades in the local tape for that weekday/hour.',
    ],
}
DATA_JSON.write_text(json.dumps(snap, separators=(',', ':')))

html_template = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC / Polymarket BTC5M Weekly Activity Rhythm</title><style>:root{--bg:#07080b;--panel:#0f1117;--panel2:#151925;--text:#f5f7fb;--muted:#9aa4b2;--grid:#273043;--accent:#7dd3fc;--hot:#fb923c}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#172033 0,#07080b 38%,#050507 100%);color:var(--text);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1380px;margin:0 auto;padding:34px 22px 70px}h1{font-size:42px;line-height:1.02;margin:0 0 12px;letter-spacing:-.04em}h2{font-size:22px;margin:30px 0 12px}h3{font-size:15px;margin:0 0 8px;color:#dbeafe}.sub{color:var(--muted);max-width:980px;font-size:16px}.pill{display:inline-block;border:1px solid #334155;background:#0b1220;border-radius:999px;padding:5px 10px;margin:4px 6px 4px 0;color:#cbd5e1;font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:24px 0}.twocol{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:linear-gradient(180deg,rgba(21,25,37,.92),rgba(10,12,18,.92));border:1px solid #20283a;border-radius:18px;padding:18px;box-shadow:0 16px 50px rgba(0,0,0,.25)}.metric{font-size:28px;font-weight:750;letter-spacing:-.04em}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.heatwrap{overflow-x:auto;border:1px solid #20283a;border-radius:16px;background:#0a0d13;padding:12px;margin-bottom:22px}table.heat{border-collapse:separate;border-spacing:3px;width:100%;min-width:900px}.heat th{font-size:11px;color:#94a3b8;font-weight:500;text-align:center;padding:4px}.heat td{height:32px;border-radius:7px;text-align:center;font-size:11px;color:#e5e7eb;border:1px solid rgba(255,255,255,.04);min-width:34px;position:relative}.heat td:hover{outline:2px solid #e0f2fe;z-index:2}.legend{display:flex;gap:8px;align-items:center;color:#94a3b8;font-size:12px;margin:8px 0 14px}.bar{height:9px;width:180px;border-radius:99px;background:linear-gradient(90deg,#111827,#164e63,#0ea5e9,#f59e0b,#ef4444)}.section{display:grid;grid-template-columns:1.3fr .7fr;gap:18px;align-items:start}.note{color:#aab4c3;background:#09111d;border:1px solid #1e293b;border-radius:14px;padding:14px;margin:10px 0}.warn{border-left:3px solid var(--hot)}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}ul{padding-left:20px}.suggestions li{margin:10px 0}.small{font-size:12px;color:#94a3b8}a{color:#7dd3fc}@media(max-width:900px){.grid,.twocol,.section{grid-template-columns:1fr}h1{font-size:32px}}</style></head><body><main><div class="pill">UTC heatmaps</div><div class="pill">Binance DAYS_PLACEHOLDERd baseline</div><div class="pill">Polymarket local CLOB tape</div><div class="pill">paper/research only</div><h1>BTC / BTC5M Weekly Liquidity Rhythm</h1><p class="sub">A shareable operating dashboard for deciding when BTC5M bots are swimming with liquidity vs. being exposed to thin overnight/weekend microstructure. Cells are weekday × UTC hour. Binance metrics are averaged over the last DAYS_PLACEHOLDER days; Polymarket BTC5M cells come from Studio1 local CLOB tape <span class="mono">last_trade_price</span> events.</p><div class="grid" id="cards"></div><div class="note warn"><b>Read this correctly:</b> high BTC futures volume/open interest means the global BTC market is active; it does not prove Polymarket BTC5M edge. Thin hours are risk flags for stale quotes, whale impact, and adverse selection. Options history is not in this cut; futures/open-interest/taker flow are used as derivatives proxies.</div><div class="twocol"><div class="card"><h3>Polymarket tape coverage</h3><div id="coverage"></div></div><div class="card"><h3>Ten upgrades to make this more profitable</h3><ol class="suggestions" id="suggestions"></ol></div></div><div id="heatmaps"></div><h2>Operational takeaways</h2><div class="section"><div class="card"><h3>How to use this for bots</h3><ul><li>Prefer evaluation/trading windows where futures volume, trade count, and Polymarket BTC5M trade events are simultaneously high.</li><li>Treat low-volume UTC night/weekend cells as adverse-selection zones: widen gates, reduce size, or disable maker quotes.</li><li>Compare weekday vs weekend: if weekend derivatives activity is low and range is high, avoid strategies trained on weekday flow.</li><li>For BTC5M specifically, each UTC hour contains 12 market resolutions. <span class="mono">markets_touched</span> approximates how many 5m windows had local observed trades.</li></ul></div><div class="card"><h3>Data caveats</h3><div id="notes"></div></div></div><script id="snapshot" type="application/json">SNAPSHOT_JSON_PLACEHOLDER</script><script>const S=JSON.parse(document.getElementById('snapshot').textContent);const WD=S.weekday_labels;function flat(mat){return mat.flat().filter(x=>Number.isFinite(x));}function fmt(v,kind){if(v==null)return'—';if(kind==='usd')return'$'+(v>=1e9?(v/1e9).toFixed(2)+'B':v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toFixed(0));if(kind==='pct')return v.toFixed(2)+'%';if(kind==='num')return v>=1000?(v/1000).toFixed(1)+'k':v.toFixed(1);return String(Math.round(v));}function color(v,vals){const min=Math.min(...vals),max=Math.max(...vals);const t=max>min?(v-min)/(max-min):0;const hue=220-210*t;return `hsl(${hue} 70% ${12+42*t}%)`;}function heat(title,mat,kind,desc){const vals=flat(mat);let html=`<h2>${title}</h2><p class="sub">${desc}</p><div class="legend"><span>low</span><div class="bar"></div><span>high</span></div><div class="heatwrap"><table class="heat"><thead><tr><th>day/hour</th>${S.hour_labels.map(h=>`<th>${h.slice(0,2)}</th>`).join('')}</tr></thead><tbody>`;for(let d=0;d<7;d++){html+=`<tr><th>${WD[d]}</th>`;for(let h=0;h<24;h++){const v=mat[d][h]||0;html+=`<td style="background:${color(v,vals)}" title="${WD[d]} ${S.hour_labels[h]} UTC: ${fmt(v,kind)}">${kind==='usd'?fmt(v,kind).replace('$',''):fmt(v,kind).replace('%','')}</td>`;}html+='</tr>';}return html+'</tbody></table></div>';}const B=S.binance_metrics,P=S.polymarket;document.getElementById('cards').innerHTML=[['Binance hours',S.binance_hours.toLocaleString(),`${S.days}d hourly sample`],['Avg futures volume',fmt(flat(B.futures_quote_usd).reduce((a,b)=>a+b,0)/flat(B.futures_quote_usd).length,'usd'),'BTCUSDT perp quote volume / hour'],['Avg range',fmt(flat(B.range_pct).reduce((a,b)=>a+b,0)/flat(B.range_pct).length,'pct'),'high-low/open per hour'],['PM tape trades',P.meta.events.toLocaleString(),`${P.meta.processed_files.length} local files parsed`]].map(c=>`<div class="card"><div class="label">${c[0]}</div><div class="metric">${c[1]}</div><div class="sub">${c[2]}</div></div>`).join('');document.getElementById('coverage').innerHTML=`<p><b>${P.meta.events.toLocaleString()}</b> BTC5M trade events, <b>${fmt(P.meta.bytes_scanned,'num')}</b> bytes scanned.</p><p>First event: <span class="mono">${P.meta.first_event_utc||'n/a'}</span><br>Last event: <span class="mono">${P.meta.last_event_utc||'n/a'}</span></p><p class="small">${P.meta.missing_manifest_files}</p>`;document.getElementById('suggestions').innerHTML=S.suggestions.map(s=>`<li><b>${s.title}</b><br><span class="small">${s.why}</span></li>`).join('');let html='';html+=heat('Composite BTC activity score',S.composite_activity,'num','Z-score blend of Binance futures volume, trade count, and hourly range. Use this as the quickest “how awake is BTC?” map.');html+=heat('Binance BTCUSDT futures quote volume — mean USD/hour',B.futures_quote_usd,'usd','Primary global derivatives liquidity proxy. Thin cells are where whales can move local microstructure more easily.');html+=heat('Binance BTCUSDT spot quote volume — mean USD/hour',B.spot_quote_usd,'usd','Spot market participation by weekday/hour.');html+=heat('Hourly BTC range — mean %',B.range_pct,'pct','Price variance proxy: high range with low volume is especially dangerous for stale BTC5M makers.');html+=heat('Futures taker buy share — mean %',B.taker_buy_share_pct,'pct','Directional taker aggression proxy. Deviations from 50% show one-sided futures pressure.');html+=heat('Futures open interest — mean USD',B.open_interest_usd,'usd','Derivative positioning/open interest proxy. High OI with low volume can mean liquidation/stop-run risk.');html+=heat('Polymarket BTC5M local trade notional — raw USD/hour',P.notional_usd_raw,'usd','Local Studio1 CLOB websocket tape. Not averaged over 60d; this is observed BTC5M Polymarket trade flow in parsed chunks and historical one-off orderbook tapes still present.');html+=heat('Polymarket BTC5M trade events — raw count/hour',P.trade_events,'num','Number of local BTC5M last_trade_price events seen by hour.');html+=heat('Polymarket BTC5M windows touched — count/hour',P.markets_touched,'num','How many 5-minute BTC up/down market slugs traded in that UTC hour in the local tape; max is roughly 12 per hour per side/window set.');document.getElementById('heatmaps').innerHTML=html;document.getElementById('notes').innerHTML=S.notes.map(n=>`<p>${n}</p>`).join('')+`<p class="mono">generated_at_utc=${S.generated_at_utc}</p>`;</script></main></body></html>'''
html = html_template.replace('DAYS_PLACEHOLDER', str(DAYS)).replace('SNAPSHOT_JSON_PLACEHOLDER', json.dumps(snap))
OUT.write_text(html)
print(f'wrote {OUT} bytes={OUT.stat().st_size}')
print(f'wrote {DATA_JSON} bytes={DATA_JSON.stat().st_size}')
print(f"pm events={pm_meta['events']} files={len(pm_meta['processed_files'])} first={pm_meta['first_event_utc']} last={pm_meta['last_event_utc']}")
