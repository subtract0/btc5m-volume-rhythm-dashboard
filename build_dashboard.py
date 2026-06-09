#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, math, os, time, gzip, statistics, re
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import requests
import glob

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


def empty_grid(default=0):
    return [[default for _ in range(24)] for __ in range(7)]

def add_grid(grid, ms, val):
    d, h, _ = hourkey(ms)
    grid[d][h] += val

def count_grid(grid, ms):
    d, h, _ = hourkey(ms)
    grid[d][h] += 1

def avg_grid(sum_grid, n_grid, digits=4):
    return [[round(sum_grid[d][h] / n_grid[d][h], digits) if n_grid[d][h] else 0 for h in range(24)] for d in range(7)]

def parse_iso_ms(x):
    if not x: return None
    if isinstance(x, (int, float)): return int(x)
    try:
        return int(datetime.fromisoformat(str(x).replace('Z', '+00:00')).timestamp() * 1000)
    except Exception:
        return None

def nested_get(o, paths):
    for path in paths:
        cur = o
        ok = True
        for part in path.split('.'):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False; break
        if ok and isinstance(cur, (int, float)):
            return float(cur)
    return None


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
print('Fetching Binance 5m candles for whale/liquidation-regime overlays...', flush=True)
fut5 = fetch_klines('https://fapi.binance.com', '/fapi/v1/klines', interval='5m')
whale_count = empty_grid(0); five_range_sum = empty_grid(0.0); five_count = empty_grid(0); thin_bigmove_count = empty_grid(0)
for row in fut5:
    ts = int(row[0]); op=float(row[1]); hi=float(row[2]); lo=float(row[3]); cl=float(row[4]); quote=float(row[7])
    rng = (hi-lo)/op*100 if op else 0
    add_grid(five_range_sum, ts, rng); count_grid(five_count, ts)
# dynamic thresholds from fetched 5m set
quotes=[float(r[7]) for r in fut5]; ranges=[(float(r[2])-float(r[3]))/float(r[1])*100 for r in fut5 if float(r[1])]
q_hi=sorted(quotes)[int(0.95*(len(quotes)-1))] if quotes else 0; q_lo=sorted(quotes)[int(0.25*(len(quotes)-1))] if quotes else 0; r_hi=sorted(ranges)[int(0.95*(len(ranges)-1))] if ranges else 0
for row in fut5:
    ts=int(row[0]); op=float(row[1]); hi=float(row[2]); lo=float(row[3]); quote=float(row[7]); rng=(hi-lo)/op*100 if op else 0
    if quote >= q_hi or rng >= r_hi: count_grid(whale_count, ts)
    if quote <= q_lo and rng >= r_hi: count_grid(thin_bigmove_count, ts)
five_minute = {'avg_5m_range_pct': avg_grid(five_range_sum, five_count), 'whale_or_large_move_count': whale_count, 'thin_bigmove_count': thin_bigmove_count, 'thresholds': {'quote_usd_p95': q_hi, 'quote_usd_p25': q_lo, 'range_pct_p95': r_hi}}

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

print('Aggregating bot PnL, spread/depth, queue-risk, and resolution-window intensity...', flush=True)
pnl_sum = empty_grid(0.0); pnl_count = empty_grid(0); pnl_sources = {}
for tp in glob.glob(str(ART / '**/trades.jsonl'), recursive=True):
    source = str(Path(tp).relative_to(ART))
    try:
        with open(tp, errors='ignore') as f:
            for line in f:
                if 'CLOSE' not in line and 'RESOLVE' not in line and 'TP_FILL' not in line and 'pnl' not in line.lower():
                    continue
                try: e=json.loads(line)
                except Exception: continue
                ms = parse_iso_ms(e.get('ts') or e.get('closedAt') or nested_get(e, ['position.closedAt']))
                if ms is None or ms < PM_START_MS: continue
                val = nested_get(e, ['pnlUsd','pnl','realizedPnlUsd','realizedPnl','position.pnlUsd','position.holdPnl','position.tpPnl','position.realizedPnlUsd'])
                if val is None: continue
                add_grid(pnl_sum, ms, val); count_grid(pnl_count, ms); pnl_sources[source]=pnl_sources.get(source,0)+1
    except Exception:
        pass
pnl_avg = avg_grid(pnl_sum, pnl_count)

# Maker risk from parsed CLOB tape: average spread, top depth, and late-window trade intensity.
spread_sum = empty_grid(0.0); spread_count = empty_grid(0); depth_sum = empty_grid(0.0); depth_count = empty_grid(0); late_trade_count = empty_grid(0)
# Use cached parsed files for trades, then scan present PM files for book events. Limit to files already discovered.
for src, fp in pm_files:
    path = Path(fp)
    # Full book-event scans are expensive on multi-GB raw/gzip chunks.
    # For the share dashboard, keep maker-risk scans to small one-off tapes and rely on
    # the dedicated complete-set scanner for proof-grade thin-wall replay.
    if path.stat().st_size > 500_000_000:
        continue
    opener = gzip.open if str(path).endswith('.gz') else open
    try:
        with opener(path, 'rt', errors='ignore') as f:
            for line in f:
                if 'btc-updown-5m-' not in line: continue
                try: e=json.loads(line)
                except Exception: continue
                ms=int(e.get('receivedAtMs') or 0);
                if ms < PM_START_MS: continue
                et=e.get('eventType'); payload=e.get('payload') or {}
                if et=='book':
                    try:
                        bids=payload.get('bids') or []; asks=payload.get('asks') or []
                        if bids and asks:
                            bb=max(float(x['price']) for x in bids); aa=min(float(x['price']) for x in asks); sp=max(0,aa-bb)
                            top_depth=sum(float(x.get('size') or 0) for x in bids[:3]+asks[:3])
                            add_grid(spread_sum, ms, sp); count_grid(spread_count, ms); add_grid(depth_sum, ms, top_depth); count_grid(depth_count, ms)
                    except Exception: pass
                elif et=='last_trade_price':
                    end=int(e.get('marketEndTs') or 0)*1000
                    if end and 0 <= end-ms <= 60_000: count_grid(late_trade_count, ms)
    except Exception:
        pass
spread_avg = avg_grid(spread_sum, spread_count, 4); depth_avg = avg_grid(depth_sum, depth_count, 1)
queue_risk = [[round((spread_avg[d][h]*100) + (late_trade_count[d][h]/max(1, pm_trades[d][h]))*20 + (1/max(1, depth_avg[d][h]))*1000, 2) for h in range(24)] for d in range(7)]

print('Fetching current Deribit BTC options snapshot...', flush=True)
deribit_options = {'status':'unavailable'}
try:
    r=requests.get('https://www.deribit.com/api/v2/public/get_book_summary_by_currency', params={'currency':'BTC','kind':'option'}, timeout=30)
    arr=r.json().get('result') or []
    vol_usd=sum(float(x.get('volume_usd') or 0) for x in arr)
    oi_btc=sum(float(x.get('open_interest') or 0) for x in arr)
    ivs=[float(x.get('mark_iv')) for x in arr if x.get('mark_iv') is not None]
    deribit_options={'status':'ok','instrument_count':len(arr),'volume_usd_24h':round(vol_usd,2),'open_interest_btc':round(oi_btc,4),'avg_mark_iv':round(mean(ivs) or 0,2),'source':'Deribit public get_book_summary_by_currency current snapshot'}
except Exception as ex:
    deribit_options={'status':'error','error':str(ex)}
zvol = zgrid(agg['futures_quote_usd']); ztr = zgrid(agg['trade_count']); zrg = zgrid(agg['range_pct'])
activity = [[round(50 + 12 * (zvol[d][h] + ztr[d][h] + zrg[d][h]) / 3, 1) for h in range(24)] for d in range(7)]

# Convert descriptive metrics into a first-pass operating schedule.
bot_schedule = []
for d in range(7):
    for h in range(24):
        risk = 0
        reasons = []
        if d >= 5:
            risk += 2; reasons.append('weekend')
        if h <= 8 or h >= 22:
            risk += 1; reasons.append('UTC night')
        if five_minute['thin_bigmove_count'][d][h] > 0:
            risk += 2; reasons.append('thin big-move history')
        if queue_risk[d][h] > 12:
            risk += 2; reasons.append('high maker queue-risk')
        if pnl_count[d][h] >= 3 and pnl_sum[d][h] < 0:
            risk += 2; reasons.append('negative local bot PnL')
        if risk >= 5:
            action = 'DISABLE_OR_COLLECT_ONLY'
        elif risk >= 3:
            action = 'HALF_SIZE_OR_NO_MAKER'
        elif activity[d][h] >= 60 and queue_risk[d][h] < 10:
            action = 'NORMAL_SIZE_ALLOWED_IF_STRATEGY_EDGE_EXISTS'
        else:
            action = 'NORMAL_RESEARCH_ONLY'
        bot_schedule.append({'day': WD[d], 'hour_utc': h, 'action': action, 'risk_score': risk, 'reasons': reasons})

print('Scanning active Polymarket next-market candidates...', flush=True)
next_markets = []
try:
    arr = get_json('https://gamma-api.polymarket.com/markets', {'closed':'false','active':'true','limit':500,'offset':0,'order':'volume24hr','ascending':'false'})
    for m in arr:
        slug = str(m.get('slug','')).lower(); q = str(m.get('question','')).lower()
        vol = float(m.get('volume24hr') or m.get('volumeNum') or 0); liq = float(m.get('liquidityNum') or m.get('liquidity') or 0); spread = float(m.get('spread') or 0) if m.get('spread') not in [None,''] else None
        if 'updown-5m' in slug or '5m' in slug: continue
        tags = []
        if any(x in slug or x in q for x in ['bitcoin','btc','ethereum','eth','crypto','solana','sol ']): tags.append('crypto-slower')
        if any(x in slug or x in q for x in ['election','trump','fed','rate','cpi','inflation','sec','lawsuit']): tags.append('news/macro')
        if any(x in slug or x in q for x in ['nba','nfl','mlb','soccer','uefa','fifa','game']): tags.append('sports')
        if not tags: continue
        score = vol + 0.2*liq - (spread or 0)*10000
        next_markets.append({'question':m.get('question'), 'slug':m.get('slug'), 'volume24hr':round(vol,2), 'liquidity':round(liq,2), 'spread':spread, 'tags':tags, 'score':round(score,2)})
    next_markets = sorted(next_markets, key=lambda x:x['score'], reverse=True)[:12]
except Exception as ex:
    next_markets = [{'error': str(ex)}]


print('Loading complete-set maker thin-wall alpha artifacts...', flush=True)
REPO = Path('/Users/am/Code/autonomous-polymarket-trader-openspec')
maker_alpha_path = REPO / 'research/paper-bot-audits/complete-set-maker-alpha-20260609/maker-alpha-cross-tape-summary.json'
maker_alpha = {'status': 'missing', 'path': str(maker_alpha_path)}
try:
    ma = json.load(open(maker_alpha_path))
    thin_rows = []
    full_rows = []
    pass_rows = []
    for r in ma.get('strategyResults', []):
        sid = f"{int(round(r.get('firstAsk',0)*100))}/{int(round(r.get('loserAsk',0)*100))}"
        full_rows.append({'strategy': sid, 'n': r.get('n'), 'evCents': round(r.get('evCents',0),3), 'ciLowCents': round(r.get('ciLowCents',0),3), 'verdict': r.get('verdict'), 'fillRatePct': round(100*(r.get('fillRate') or 0),2)})
        for wb in r.get('wallBuckets', []):
            row = {'strategy': sid, 'wall': wb.get('label'), 'n': wb.get('n'), 'dumpRatePct': round(100*(wb.get('dumpRate') or 0),1), 'evCents': round(wb.get('evCents',0),3), 'ciLowCents': round(wb.get('ciLowCents',0),3)}
            if wb.get('label') in ['<10 sh','10-100','100-1k']:
                thin_rows.append(row)
            if (wb.get('n') or 0) >= 100 and (wb.get('ciLowCents') or -999) > 0:
                pass_rows.append(row)
    maker_alpha = {
        'status':'ok', 'path': str(maker_alpha_path), 'generatedAt': ma.get('generatedAt'),
        'combinedUniqueWindows': ma.get('combinedUniqueWindows'), 'resolvedSlugs': ma.get('resolvedSlugs'),
        'dateRangeUtc': (ma.get('families') or {}).get('btc5m',{}).get('dateRangeUtc'),
        'fullPopulation': full_rows, 'thinWallRows': thin_rows, 'passRows': pass_rows,
        'assumptions': ma.get('assumptions')
    }
except Exception as ex:
    maker_alpha = {'status':'error','path':str(maker_alpha_path),'error':str(ex)}

print('Scanning current DOGE15M complete-set thin books...', flush=True)
doge_summary_path = Path('/Users/am/poly-thin-complete-set-paper/doge15m/summaries/latest.json')
doge_tape_summary_path = REPO / 'artifacts/crypto-updown-orderbook-tape/doge15m-current/summary.json'
doge_events_path = REPO / 'artifacts/crypto-updown-orderbook-tape/doge15m-current/events.jsonl'
doge15m = {'status':'missing', 'paperSummaryPath':str(doge_summary_path), 'tapeSummaryPath':str(doge_tape_summary_path)}
try:
    paper = json.load(open(doge_summary_path)) if doge_summary_path.exists() else {}
    tape = json.load(open(doge_tape_summary_path)) if doge_tape_summary_path.exists() else {}
    last_books = {}
    if doge_events_path.exists():
        with open(doge_events_path, errors='ignore') as f:
            for line in f:
                if '"eventType":"book"' not in line and '"eventType": "book"' not in line: continue
                try: e=json.loads(line)
                except Exception: continue
                slug=e.get('slug'); outcome=e.get('outcome'); payload=e.get('payload') or {}
                if not slug or not outcome: continue
                asks=[]
                for a in payload.get('asks') or []:
                    try: asks.append({'price':float(a.get('price')), 'size':float(a.get('size'))})
                    except Exception: pass
                last_books[(slug,outcome)]={'slug':slug,'outcome':outcome,'marketEndTs':e.get('marketEndTs'),'receivedAt':e.get('receivedAt'),'receivedAtMs':e.get('receivedAtMs'), 'asks': asks}
    current = []
    for (slug,outcome), b in last_books.items():
        walls = {}
        for th in [0.04,0.07,0.11,0.98,0.99]:
            walls[str(th)] = round(sum(x['size'] for x in b['asks'] if x['price'] <= th),2)
        min_loser_wall = min(walls['0.04'], walls['0.07'], walls['0.11'])
        seconds_left = None
        if b.get('marketEndTs'):
            seconds_left = int(b['marketEndTs'] - time.time())
        current.append({'slug':slug,'outcome':outcome,'secondsLeft':seconds_left,'receivedAt':b.get('receivedAt'),'wall04':walls['0.04'],'wall07':walls['0.07'],'wall11':walls['0.11'],'wall98':walls['0.98'],'wall99':walls['0.99'],'thinLt10': min_loser_wall < 10,'thinLt100': min_loser_wall < 100})
    current = sorted(current, key=lambda x: (0 if x['thinLt10'] else 1 if x['thinLt100'] else 2, x['wall11'], -(x['secondsLeft'] or -999)))[:24]
    doge15m = {
        'status':'ok','paper':paper,'tape':tape,'currentThinBooks':current,
        'eventsPath':str(doge_events_path),'eventLines': sum(1 for _ in open(doge_events_path, errors='ignore')) if doge_events_path.exists() else 0
    }
except Exception as ex:
    doge15m={'status':'error','error':str(ex),'paperSummaryPath':str(doge_summary_path),'tapeSummaryPath':str(doge_tape_summary_path)}

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
    'bot_pnl': {'avg_pnl_usd': pnl_avg, 'trade_count': pnl_count, 'sum_pnl_usd': [[round(pnl_sum[d][h],4) for h in range(24)] for d in range(7)], 'sources': pnl_sources},
    'maker_risk': {'avg_spread': spread_avg, 'avg_top_depth': depth_avg, 'queue_risk_score': queue_risk, 'late_trade_count': late_trade_count},
    'five_minute': five_minute,
    'deribit_options': deribit_options,
    'composite_activity': activity,
    'suggestions': suggestions,
    'bot_schedule': bot_schedule,
    'next_markets': next_markets,
    'maker_alpha': maker_alpha,
    'doge15m_complete_set': doge15m,
    'notes': [
        'Binance spot/futures are public hourly BTCUSDT data from Binance, aggregated by UTC weekday/hour over the lookback window.',
        'Derivatives activity proxies: Binance USD-M futures quote volume, trade count, taker buy share, taker buy/sell ratio, and futures open interest. Historical BTC options volume is not included in this cut.',
        'Polymarket BTC5M volume uses every local BTC5M CLOB websocket tape file still present on Studio1 within the lookback. Notional ~= price*shares.',
        'Some week-index manifest entries point to older chunk files that are no longer present locally; the data-coverage panel makes this explicit.',
        'BTC5M markets resolve every 5 minutes; each hour has 12 windows. markets_touched indicates how many BTC5M slugs had trades in the local tape for that weekday/hour.',
    ],
}
DATA_JSON.write_text(json.dumps(snap, separators=(',', ':')))

html_template = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC / Polymarket BTC5M Weekly Activity Rhythm</title><style>:root{--bg:#07080b;--panel:#0f1117;--panel2:#151925;--text:#f5f7fb;--muted:#9aa4b2;--grid:#273043;--accent:#7dd3fc;--hot:#fb923c}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0%,#172033 0,#07080b 38%,#050507 100%);color:var(--text);font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1380px;margin:0 auto;padding:34px 22px 70px}h1{font-size:42px;line-height:1.02;margin:0 0 12px;letter-spacing:-.04em}h2{font-size:22px;margin:30px 0 12px}h3{font-size:15px;margin:0 0 8px;color:#dbeafe}.sub{color:var(--muted);max-width:980px;font-size:16px}.pill{display:inline-block;border:1px solid #334155;background:#0b1220;border-radius:999px;padding:5px 10px;margin:4px 6px 4px 0;color:#cbd5e1;font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:24px 0}.twocol{display:grid;grid-template-columns:1fr 1fr;gap:14px}.card{background:linear-gradient(180deg,rgba(21,25,37,.92),rgba(10,12,18,.92));border:1px solid #20283a;border-radius:18px;padding:18px;box-shadow:0 16px 50px rgba(0,0,0,.25)}.metric{font-size:28px;font-weight:750;letter-spacing:-.04em}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.heatwrap{overflow-x:auto;border:1px solid #20283a;border-radius:16px;background:#0a0d13;padding:12px;margin-bottom:22px}table.heat{border-collapse:separate;border-spacing:3px;width:100%;min-width:900px}.heat th{font-size:11px;color:#94a3b8;font-weight:500;text-align:center;padding:4px}.heat td{height:32px;border-radius:7px;text-align:center;font-size:11px;color:#e5e7eb;border:1px solid rgba(255,255,255,.04);min-width:34px;position:relative}.heat td:hover{outline:2px solid #e0f2fe;z-index:2}.legend{display:flex;gap:8px;align-items:center;color:#94a3b8;font-size:12px;margin:8px 0 14px}.bar{height:9px;width:180px;border-radius:99px;background:linear-gradient(90deg,#111827,#164e63,#0ea5e9,#f59e0b,#ef4444)}.section{display:grid;grid-template-columns:1.3fr .7fr;gap:18px;align-items:start}.note{color:#aab4c3;background:#09111d;border:1px solid #1e293b;border-radius:14px;padding:14px;margin:10px 0}.warn{border-left:3px solid var(--hot)}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}ul{padding-left:20px}.suggestions li{margin:10px 0}.small{font-size:12px;color:#94a3b8}a{color:#7dd3fc}@media(max-width:900px){.grid,.twocol,.section{grid-template-columns:1fr}h1{font-size:32px}}</style></head><body><main><div class="pill">UTC heatmaps</div><div class="pill">Binance DAYS_PLACEHOLDERd baseline</div><div class="pill">Polymarket local CLOB tape</div><div class="pill">paper/research only</div><h1>BTC / BTC5M Weekly Liquidity Rhythm</h1><p class="sub">A shareable operating dashboard for deciding when BTC5M bots are swimming with liquidity vs. being exposed to thin overnight/weekend microstructure. Cells are weekday × UTC hour. Binance metrics are averaged over the last DAYS_PLACEHOLDER days; Polymarket BTC5M cells come from Studio1 local CLOB tape <span class="mono">last_trade_price</span> events.</p><div class="grid" id="cards"></div><div class="note warn"><b>Read this correctly:</b> high BTC futures volume/open interest means the global BTC market is active; it does not prove Polymarket BTC5M edge. Thin hours are risk flags for stale quotes, whale impact, and adverse selection. Options history is not in this cut; futures/open-interest/taker flow are used as derivatives proxies.</div><div class="twocol"><div class="card"><h3>Polymarket tape coverage</h3><div id="coverage"></div></div><div class="card"><h3>Deribit BTC options snapshot</h3><div id="deribit"></div></div></div><div class="card"><h3>Ten upgrades to make this more profitable</h3><ol class="suggestions" id="suggestions"></ol></div><div class="twocol"><div class="card"><h3>First-pass bot schedule</h3><div id="schedule"></div></div><div class="card"><h3>Recommended next-market scan</h3><div id="nextmarkets"></div></div></div><div class="twocol"><div class="card"><h3>Complete-set maker alpha: thin loser walls</h3><div id="makeralpha"></div></div><div class="card"><h3>DOGE15M live thin-book monitor</h3><div id="dogecomplete"></div></div></div><div id="heatmaps"></div><h2>Operational takeaways</h2><div class="section"><div class="card"><h3>How to use this for bots</h3><ul><li>Prefer evaluation/trading windows where futures volume, trade count, and Polymarket BTC5M trade events are simultaneously high.</li><li>Treat low-volume UTC night/weekend cells as adverse-selection zones: widen gates, reduce size, or disable maker quotes.</li><li>Compare weekday vs weekend: if weekend derivatives activity is low and range is high, avoid strategies trained on weekday flow.</li><li>For BTC5M specifically, each UTC hour contains 12 market resolutions. <span class="mono">markets_touched</span> approximates how many 5m windows had local observed trades.</li></ul></div><div class="card"><h3>Data caveats</h3><div id="notes"></div></div></div><script id="snapshot" type="application/json">SNAPSHOT_JSON_PLACEHOLDER</script><script>const S=JSON.parse(document.getElementById('snapshot').textContent);const WD=S.weekday_labels;function flat(mat){return mat.flat().filter(x=>Number.isFinite(x));}function fmt(v,kind){if(v==null)return'—';if(kind==='usd')return'$'+(v>=1e9?(v/1e9).toFixed(2)+'B':v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toFixed(0));if(kind==='pct')return v.toFixed(2)+'%';if(kind==='num')return v>=1000?(v/1000).toFixed(1)+'k':v.toFixed(1);return String(Math.round(v));}function color(v,vals){const min=Math.min(...vals),max=Math.max(...vals);const t=max>min?(v-min)/(max-min):0;const hue=220-210*t;return `hsl(${hue} 70% ${12+42*t}%)`;}function heat(title,mat,kind,desc){const vals=flat(mat);let html=`<h2>${title}</h2><p class="sub">${desc}</p><div class="legend"><span>low</span><div class="bar"></div><span>high</span></div><div class="heatwrap"><table class="heat"><thead><tr><th>day/hour</th>${S.hour_labels.map(h=>`<th>${h.slice(0,2)}</th>`).join('')}</tr></thead><tbody>`;for(let d=0;d<7;d++){html+=`<tr><th>${WD[d]}</th>`;for(let h=0;h<24;h++){const v=mat[d][h]||0;html+=`<td style="background:${color(v,vals)}" title="${WD[d]} ${S.hour_labels[h]} UTC: ${fmt(v,kind)}">${kind==='usd'?fmt(v,kind).replace('$',''):fmt(v,kind).replace('%','')}</td>`;}html+='</tr>';}return html+'</tbody></table></div>';}const B=S.binance_metrics,P=S.polymarket,PNL=S.bot_pnl,MR=S.maker_risk,F5=S.five_minute,DOPT=S.deribit_options,MA=S.maker_alpha,CS=S.doge15m_complete_set;document.getElementById('cards').innerHTML=[['Binance hours',S.binance_hours.toLocaleString(),`${S.days}d hourly sample`],['Avg futures volume',fmt(flat(B.futures_quote_usd).reduce((a,b)=>a+b,0)/flat(B.futures_quote_usd).length,'usd'),'BTCUSDT perp quote volume / hour'],['Avg range',fmt(flat(B.range_pct).reduce((a,b)=>a+b,0)/flat(B.range_pct).length,'pct'),'high-low/open per hour'],['PM tape trades',P.meta.events.toLocaleString(),`${P.meta.processed_files.length} local files parsed`]].map(c=>`<div class="card"><div class="label">${c[0]}</div><div class="metric">${c[1]}</div><div class="sub">${c[2]}</div></div>`).join('');document.getElementById('coverage').innerHTML=`<p><b>${P.meta.events.toLocaleString()}</b> BTC5M trade events, <b>${fmt(P.meta.bytes_scanned,'num')}</b> bytes scanned.</p><p>First event: <span class="mono">${P.meta.first_event_utc||'n/a'}</span><br>Last event: <span class="mono">${P.meta.last_event_utc||'n/a'}</span></p><p class="small">${P.meta.missing_manifest_files}</p>`;document.getElementById('deribit').innerHTML=DOPT.status==='ok'?`<p><b>${fmt(DOPT.volume_usd_24h,'usd')}</b> 24h BTC options volume, <b>${DOPT.open_interest_btc.toLocaleString()}</b> BTC open interest.</p><p>Avg mark IV: <b>${DOPT.avg_mark_iv}%</b><br>Instruments: <b>${DOPT.instrument_count}</b></p><p class="small">${DOPT.source}</p>`:`<p class="small">Deribit unavailable: ${DOPT.error||DOPT.status}</p>`;document.getElementById('suggestions').innerHTML=S.suggestions.map(s=>`<li><b>${s.title}</b><br><span class="small">${s.why}</span></li>`).join('');const danger=S.bot_schedule.filter(x=>x.action==='DISABLE_OR_COLLECT_ONLY').slice(0,12);document.getElementById('schedule').innerHTML=`<p><b>${danger.length}</b> highest-risk weekday/hour buckets shown below. Use as a first-pass no-trade/collect-only map, not a live instruction.</p>`+danger.map(x=>`<p class="small"><b>${x.day} ${String(x.hour_utc).padStart(2,'0')}:00 UTC</b> — ${x.action} (${x.reasons.join(', ')||'baseline'})</p>`).join('');document.getElementById('nextmarkets').innerHTML=S.next_markets.map(m=>m.error?`<p>${m.error}</p>`:`<p class="small"><b>${m.question}</b><br>${m.tags.join(', ')} · vol24h ${fmt(m.volume24hr,'usd')} · liq ${fmt(m.liquidity,'usd')} · spread ${m.spread??'n/a'}</p>`).join('');function rowTable(rows,cols){return `<table class="heat" style="min-width:0;border-spacing:2px"><thead><tr>${cols.map(c=>`<th>${c[0]}</th>`).join('')}</tr></thead><tbody>${(rows||[]).map(r=>`<tr>${cols.map(c=>`<td style="height:28px;background:#111827">${r[c[1]]??''}</td>`).join('')}</tr>`).join('')}</tbody></table>`;}document.getElementById('makeralpha').innerHTML=MA.status==='ok'?`<p><b>${MA.combinedUniqueWindows}</b> BTC5M windows replayed. Full population is KILL, but thin loser-side ask walls are the live hypothesis.</p><p class="small">Proof rule: N is independent windows, not shares. N&lt;100 = WAIT even if EV/share looks large.</p><h3>Positive thin-wall BTC5M slices</h3>${rowTable(MA.thinWallRows.filter(r=>r.ciLowCents>0).slice(0,12),[['strat','strategy'],['wall','wall'],['N','n'],['EV c','evCents'],['CI low c','ciLowCents'],['dump%','dumpRatePct']])}<h3>Full population baseline</h3>${rowTable(MA.fullPopulation, [['strat','strategy'],['N','n'],['EV c','evCents'],['CI low c','ciLowCents'],['verdict','verdict']])}`:`<p>${MA.status}: ${MA.error||MA.path}</p>`;document.getElementById('dogecomplete').innerHTML=CS.status==='ok'?`<p><b>${CS.paper.markets}</b> DOGE15M markets, <b>${CS.paper.positions}</b> paper positions, <b>${CS.eventLines.toLocaleString()}</b> raw tape lines.</p><p class="small">paperOnly=${CS.paper.paperOnly}; signedOrders=${CS.paper.signedOrders}; postedOrders=${CS.paper.postedOrders}; first fills so far: ${(CS.paper.byStrategy||[]).reduce((a,b)=>a+(b.firstFills||0),0)}.</p><h3>Thinnest current DOGE15M walls</h3>${rowTable((CS.currentThinBooks||[]).slice(0,12), [['slug','slug'],['side','outcome'],['sec left','secondsLeft'],['wall@4c','wall04'],['wall@7c','wall07'],['wall@11c','wall11'],['<100','thinLt100']])}`:`<p>${CS.status}: ${CS.error||''}</p>`;let html='';html+=heat('Composite BTC activity score',S.composite_activity,'num','Z-score blend of Binance futures volume, trade count, and hourly range. Use this as the quickest “how awake is BTC?” map.');html+=heat('Binance BTCUSDT futures quote volume — mean USD/hour',B.futures_quote_usd,'usd','Primary global derivatives liquidity proxy. Thin cells are where whales can move local microstructure more easily.');html+=heat('Binance BTCUSDT spot quote volume — mean USD/hour',B.spot_quote_usd,'usd','Spot market participation by weekday/hour.');html+=heat('Hourly BTC range — mean %',B.range_pct,'pct','Price variance proxy: high range with low volume is especially dangerous for stale BTC5M makers.');html+=heat('Futures taker buy share — mean %',B.taker_buy_share_pct,'pct','Directional taker aggression proxy. Deviations from 50% show one-sided futures pressure.');html+=heat('Futures open interest — mean USD',B.open_interest_usd,'usd','Derivative positioning/open interest proxy. High OI with low volume can mean liquidation/stop-run risk.');html+=heat('Bot realized PnL — average USD/trade by close hour',PNL.avg_pnl_usd,'usd','Parsed local trades.jsonl CLOSE/RESOLVE/TP rows. This is the money overlay: which weekday/hour buckets actually made or lost money in paper/live-shadow artifacts.');html+=heat('Bot trade count — closed/scored positions',PNL.trade_count,'num','Sample size behind the PnL heatmap. Low-count buckets are not trustworthy yet.');html+=heat('Binance 5m whale / large-move count',F5.whale_or_large_move_count,'num','Count of 5-minute candles in each hour bucket above the 95th percentile for volume or range.');html+=heat('Thin-liquidity big-move count',F5.thin_bigmove_count,'num','Danger regime: low 5m quote volume but high range. Good candidate for no-trade / reduced-size rules.');html+=heat('Average 5m BTC range — %',F5.avg_5m_range_pct,'pct','Direct 5-minute volatility view rather than hourly smoothing.');html+=heat('Maker queue-risk score',MR.queue_risk_score,'num','Composite of spread, late-window trade intensity, and shallow top depth from BTC5M CLOB book/trade tape. Higher = more adverse-selection risk.');html+=heat('Average Polymarket BTC5M spread',MR.avg_spread,'pct','Average best ask minus best bid from local CLOB book events. Wide spreads imply taker cost; narrow spreads can still be toxic if queue risk is high.');html+=heat('Late-window BTC5M trade count',MR.late_trade_count,'num','Trades inside the last 60 seconds before market end. This approximates resolution-window intensity and last-minute flip pressure.');html+=heat('Polymarket BTC5M local trade notional — raw USD/hour',P.notional_usd_raw,'usd','Local Studio1 CLOB websocket tape. Not averaged over 60d; this is observed BTC5M Polymarket trade flow in parsed chunks and historical one-off orderbook tapes still present.');html+=heat('Polymarket BTC5M trade events — raw count/hour',P.trade_events,'num','Number of local BTC5M last_trade_price events seen by hour.');html+=heat('Polymarket BTC5M windows touched — count/hour',P.markets_touched,'num','How many 5-minute BTC up/down market slugs traded in that UTC hour in the local tape; max is roughly 12 per hour per side/window set.');document.getElementById('heatmaps').innerHTML=html;document.getElementById('notes').innerHTML=S.notes.map(n=>`<p>${n}</p>`).join('')+`<p class="mono">generated_at_utc=${S.generated_at_utc}</p>`;</script></main></body></html>'''
html = html_template.replace('DAYS_PLACEHOLDER', str(DAYS)).replace('SNAPSHOT_JSON_PLACEHOLDER', json.dumps(snap))
OUT.write_text(html)
print(f'wrote {OUT} bytes={OUT.stat().st_size}')
print(f'wrote {DATA_JSON} bytes={DATA_JSON.stat().st_size}')
print(f"pm events={pm_meta['events']} files={len(pm_meta['processed_files'])} first={pm_meta['first_event_utc']} last={pm_meta['last_event_utc']}")
