#!/usr/bin/env python3
"""
Casago PM Lead Enrichment Server — async job model
/enrich  → returns {job_id} immediately, runs in background thread
/status/<job_id> → returns {status:'running'} or {status:'done', result:{...}}
/ping    → always fast, never blocked by enrichment
"""

import os, re, time, socket, threading, uuid
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup

socket.setdefaulttimeout(12)

app  = Flask(__name__)
jobs = {}          # job_id -> {status, result, ts}
jobs_lock = threading.Lock()

EMAIL_RE  = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE  = re.compile(r'(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})')
DOMAIN_RE = re.compile(r'https?://([^/\s?#]+)')

JUNK_EMAIL = ['noreply','no-reply','example','yourname','test@','@sentry',
              '@example','placeholder','@domain','.png@','.jpg@','unsubscribe',
              'privacy@','legal@','press@','media@','careers@','jobs@']
JUNK_EXT   = {'.png','.jpg','.jpeg','.gif','.svg','.css','.js','.woff','.ico','.webp'}

GENERIC_PREFIXES = {'info','contact','hello','admin','support','help',
                    'office','general','inquiries','inquiry','booking',
                    'reservations','mail','team','sales','concierge'}

SKIP_DOMAINS = {'airbnb','vrbo','tripadvisor','yelp','google','facebook','zillow',
                'airdna','linkedin','instagram','youtube','twitter','booking',
                'expedia','pinterest','reddit','apartments','trulia','realtor',
                'homeaway','hipcamp'}

CONTACT_PATHS = ['','/contact','/contact-us','/about','/about-us',
                 '/team','/our-team','/leadership','/people']

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def is_generic(email):
    return email.split('@')[0].lower() in GENERIC_PREFIXES

def clean_emails(raw):
    out = set()
    for e in raw:
        try:
            e = e.lower().strip().rstrip('.')
            if len(e) < 6 or '@' not in e: continue
            if any(j in e for j in JUNK_EMAIL): continue
            if '.' + e.split('.')[-1] in JUNK_EXT: continue
            out.add(e)
        except BaseException: pass
    return sorted(out)

def split_emails(emails):
    return [e for e in emails if not is_generic(e)], [e for e in emails if is_generic(e)]

def clean_phones(raw):
    out = set()
    for p in raw:
        try:
            d = re.sub(r'\D','',str(p))
            if len(d) == 10: out.add(f'({d[0:3]}) {d[3:6]}-{d[6:]}')
            elif len(d) == 11 and d[0]=='1': out.add(f'({d[1:4]}) {d[4:7]}-{d[7:]}')
        except BaseException: pass
    return sorted(out)

SERPER_KEY = os.environ.get('SERPER_API_KEY', '')

def fetch(url, timeout=12):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.text[:400_000]
    except BaseException as e:
        print(f'  fetch error: {type(e).__name__}')
    return ''

def parse_page(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for t in soup(['script','style','head','noscript']): t.decompose()
        mailto, tel = [], []
        for a in soup.find_all('a', href=True):
            h = a['href']
            if h.startswith('mailto:'): mailto.append(h[7:].split('?')[0].strip())
            elif h.startswith('tel:'): tel.append(re.sub(r'\D','',h[4:]))
        text   = soup.get_text(' ', strip=True)
        emails = clean_emails(mailto + EMAIL_RE.findall(html))
        phones = clean_phones(tel + PHONE_RE.findall(text))
        return emails, phones, text
    except BaseException:
        return [], [], ''

def _parse_ddg(html, n):
    soup    = BeautifulSoup(html, 'html.parser')
    results = []
    for a in soup.select('a.result__a')[:n*2]:
        try:
            href  = a.get('href','')
            if not href.startswith('http'): continue
            title = a.get_text(strip=True)
            snip  = ''
            parent = a.find_parent('div', class_='result')
            if parent:
                s = parent.select_one('.result__snippet')
                if s: snip = s.get_text(strip=True)
            results.append({'title':title,'body':snip,'href':href})
            if len(results) >= n: break
        except BaseException: continue
    return results

def _parse_bing(html, n):
    soup    = BeautifulSoup(html, 'html.parser')
    results = []
    for li in soup.select('li.b_algo')[:n*2]:
        try:
            a    = li.select_one('h2 a')
            snip = li.select_one('p, .b_lineclamp2, .b_paractl')
            if not a: continue
            href = a.get('href','')
            if not href.startswith('http'): continue
            results.append({'title':a.get_text(strip=True),
                            'body': snip.get_text(strip=True) if snip else '',
                            'href': href})
            if len(results) >= n: break
        except BaseException: continue
    return results

def _parse_google(html, n):
    soup    = BeautifulSoup(html, 'html.parser')
    results = []
    for block in soup.select('div.g, div[data-hveid]')[:n*2]:
        try:
            title_el = block.select_one('h3')
            link_el  = block.select_one('a[href]')
            snip_el  = block.select_one('div[data-sncf],span.st,div.VwiC3b,div[style*="webkit-line-clamp"]')
            if not title_el or not link_el: continue
            href = link_el.get('href','')
            if href.startswith('/url?q='): href = href[7:].split('&')[0]
            if not href.startswith('http'): continue
            results.append({'title':title_el.get_text(strip=True),
                            'body': snip_el.get_text(strip=True) if snip_el else '',
                            'href': href})
            if len(results) >= n: break
        except BaseException: continue
    return results

def _serper_search(query, n=8):
    """Call Serper.dev Google Search API — returns same dict format as scrapers."""
    try:
        r = requests.post(
            'https://google.serper.dev/search',
            headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
            json={'q': query, 'num': n},
            timeout=10,
        )
        if r.status_code != 200:
            print(f'  Serper error: {r.status_code}')
            return []
        data    = r.json()
        results = []
        for item in data.get('organic', [])[:n]:
            results.append({
                'title': item.get('title', ''),
                'body':  item.get('snippet', ''),
                'href':  item.get('link', ''),
            })
        print(f'  Serper → {len(results)} results')
        return results
    except BaseException as e:
        print(f'  Serper error: {type(e).__name__}')
        return []

def search(query, n=8):
    """Use Serper API if key set; otherwise fall back to direct scraping."""
    print(f'  Searching: {query[:80]}')

    if SERPER_KEY:
        results = _serper_search(query, n)
        if results:
            return results

    # Fallback: try Bing direct scraping (returns HTML from Render sometimes)
    q = requests.utils.quote(query)
    try:
        html = fetch(f'https://www.bing.com/search?q={q}&count={n}', timeout=8)
        results = _parse_bing(html, n) if html else []
        if results:
            print(f'  Bing fallback → {len(results)}')
            return results
    except BaseException: pass

    return []

def google(query, n=8):
    return search(query, n)

def extract_person_names(text):
    try:
        patterns = [
            r'(?:owner|founder|CEO|president|principal|managing director|broker)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
            r'([A-Z][a-z]+ [A-Z][a-z]+)(?:\s*,\s*(?:Owner|Founder|CEO|President|Principal|Broker|Manager))',
            r'contact ([A-Z][a-z]+ [A-Z][a-z]+) (?:at|directly)',
        ]
        found = []
        for pat in patterns:
            found += re.findall(pat, text, re.IGNORECASE)
        return list(dict.fromkeys(found))
    except BaseException:
        return []

def guess_email_patterns(first, last, domain):
    try:
        f = re.sub(r'[^a-z]','',first.lower())
        l = re.sub(r'[^a-z]','',last.lower())
        if not f or not l or not domain: return []
        return list(dict.fromkeys([
            f'{f}@{domain}', f'{f}.{l}@{domain}', f'{f[0]}{l}@{domain}',
            f'{f[0]}.{l}@{domain}', f'{l}@{domain}',
        ]))
    except BaseException:
        return []

def timed_out(start, limit=90):
    return (time.time() - start) > limit

def has_personal(emails):
    return bool(split_emails(clean_emails(emails))[0])

def add_contact(contacts, name, title='', linkedin='', source=''):
    if name and not any(c['name']==name for c in contacts):
        contacts.append({'name':name,'title':title,'linkedin':linkedin,'source':source})

def scrape_and_collect(url, all_emails, all_phones, contacts, sources, label=''):
    try:
        html = fetch(url)
        if not html: return
        e, p, text = parse_page(html)
        all_emails += e; all_phones += p; sources.append(url)
        for name in extract_person_names(text):
            add_contact(contacts, name, source=label or url)
    except BaseException: pass

# ── Core enrichment (runs in background thread) ────────────────────────────────

def do_enrich(company, market, website, airbnb_url, host_id):
    print(f'\n══ Enriching: {company or host_id} / {market} ══')
    start         = time.time()
    resolved_name = ''
    all_emails, all_phones, contacts, sources = [], [], [], []
    domain = ''

    # ── 0. Resolve Airbnb host name ───────────────────────────────────────────
    if airbnb_url or host_id:
        try:
            ab_url = airbnb_url or f'https://www.airbnb.com/users/show/{host_id}'
            html   = fetch(ab_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                for sel in ['h1','h2','.t1x1odh7']:
                    el = soup.select_one(sel)
                    if el:
                        name = el.get_text(strip=True)
                        if 2 < len(name) < 80:
                            resolved_name = name; break
            company = resolved_name or company or f'Airbnb Host {host_id}'
        except BaseException as e:
            print(f'  Airbnb error: {e}')

    # ── 1. Find website ────────────────────────────────────────────────────────
    if not timed_out(start):
        try:
            if not website:
                for r in google(f'"{company}" {market} property management vacation rental'):
                    href = r.get('href','')
                    m    = DOMAIN_RE.match(href)
                    if m:
                        dom = m.group(1).lower().replace('www.','')
                        if not any(s in dom for s in SKIP_DOMAINS):
                            website = href.split('?')[0].rstrip('/'); print(f'  Website: {website}'); break
                    all_emails += EMAIL_RE.findall(r.get('body',''))
                    all_phones += PHONE_RE.findall(r.get('body',''))
            if website:
                m = DOMAIN_RE.match(website)
                if m: domain = m.group(1).replace('www.','')
        except BaseException as e:
            print(f'  Website find error: {e}')

    # ── 2. Scrape website pages ───────────────────────────────────────────────
    if website and not timed_out(start):
        base = '/'.join(website.split('/')[:3])
        for path in CONTACT_PATHS:
            if timed_out(start): break
            scrape_and_collect(base+path, all_emails, all_phones, contacts, sources, 'Website')
            time.sleep(0.25)

    # ── 3. LinkedIn ───────────────────────────────────────────────────────────
    if not timed_out(start):
        try:
            for r in google(f'site:linkedin.com/in "{company}" (owner OR founder OR CEO OR president OR broker)'):
                if timed_out(start): break
                href, title, body = r.get('href',''), r.get('title',''), r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                if 'linkedin.com/in/' in href:
                    parts = title.split(' - ')
                    name  = parts[0].strip(); role = parts[1].strip() if len(parts)>1 else ''
                    if len(name.split())>=2 and len(name)<60:
                        add_contact(contacts, name, title=role, linkedin=href, source='LinkedIn')
                        print(f'  LinkedIn: {name}')
        except BaseException as e:
            print(f'  LinkedIn error: {e}')

    # ── 4. Owner / contact searches ───────────────────────────────────────────
    if not timed_out(start):
        try:
            for r in google(f'"{company}" {market} owner email phone contact'):
                if timed_out(start): break
                body = r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body+' '+r.get('title','')):
                    add_contact(contacts, name, source='Web')
        except BaseException as e:
            print(f'  Owner search error: {e}')

    # ══ From here on, only continue if no personal email found yet ════════════

    # ── 5. Directories: BBB, Yelp, Manta, Clutch ─────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → No email yet, searching directories...')
        dir_queries = [
            f'site:bbb.org "{company}" {market}',
            f'site:yelp.com/biz "{company}" {market}',
            f'site:manta.com "{company}"',
            f'site:clutch.co "{company}"',
            f'site:thumbtack.com "{company}"',
        ]
        for q in dir_queries:
            if has_personal(all_emails) or timed_out(start): break
            try:
                for r in google(q, 5):
                    href = r.get('href','')
                    all_emails += EMAIL_RE.findall(r.get('body',''))
                    all_phones += PHONE_RE.findall(r.get('body',''))
                    if href and not any(s in href for s in ['google','bing']):
                        scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'Directory')
                        time.sleep(0.3)
            except BaseException: pass

    # ── 6. News / press / interview mentions ──────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → Searching news and press...')
        try:
            for r in google(f'"{company}" {market} (interview OR profile OR "property manager" OR founder OR owner) -site:airbnb.com'):
                if has_personal(all_emails) or timed_out(start): break
                href = r.get('href','')
                body = r.get('body','')+' '+r.get('title','')
                all_emails += EMAIL_RE.findall(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body):
                    add_contact(contacts, name, source='Press')
                if href and not any(s in href for s in list(SKIP_DOMAINS)+['google']):
                    scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'Press')
                    time.sleep(0.3)
        except BaseException as e:
            print(f'  News error: {e}')

    # ── 7. More owner name queries ─────────────────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → Trying owner name queries...')
        name_queries = [
            f'"{company}" "owned by" OR "founded by" OR "started by" {market}',
            f'"{company}" (CEO OR principal OR owner OR broker) site:linkedin.com OR site:facebook.com',
            f'"{company}" {market} "meet our team" OR "about us" owner',
            f'"{company}" {market} property manager name',
        ]
        for q in name_queries:
            if has_personal(all_emails) or timed_out(start): break
            try:
                for r in google(q, 8):
                    body = r.get('body','')+' '+r.get('title','')
                    all_emails += EMAIL_RE.findall(body)
                    all_phones += PHONE_RE.findall(body)
                    for name in extract_person_names(body):
                        add_contact(contacts, name, title='Owner', source='Deep Search')
                    href = r.get('href','')
                    if href and not any(s in href for s in list(SKIP_DOMAINS)+['google']):
                        scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'Deep')
                        time.sleep(0.3)
            except BaseException: pass

    # ── 8. Crunchbase / business profiles ─────────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → Searching business profiles...')
        try:
            for q in [
                f'site:crunchbase.com "{company}"',
                f'site:bizjournals.com "{company}" {market}',
                f'"{company}" {market} "vacation rental" email contact -site:airbnb.com -site:vrbo.com',
            ]:
                if has_personal(all_emails) or timed_out(start): break
                for r in google(q, 6):
                    body = r.get('body','')+' '+r.get('title','')
                    all_emails += EMAIL_RE.findall(body)
                    for name in extract_person_names(body):
                        add_contact(contacts, name, source='Profile')
        except BaseException as e:
            print(f'  Profile search error: {e}')

    # ── 9. Email pattern guessing (always runs if domain known) ──────────────
    guessed = []
    if domain:
        # From discovered contact names
        for c in contacts[:8]:
            parts = c['name'].split()
            if len(parts) >= 2:
                guessed += guess_email_patterns(parts[0], parts[-1], domain)
        # Always guess from company name words as a baseline
        words = re.sub(r'[^a-z\s]','', company.lower()).split()
        words = [w for w in words if len(w)>2 and w not in ('the','and','for','llc','inc','vacation','rental','rentals','property','management','properties')]
        if words:
            w0 = words[0]
            w1 = words[1] if len(words) > 1 else ''
            guessed += [
                f'owner@{domain}', f'manager@{domain}',
                f'{w0}@{domain}',
                f'{w0}.{w1}@{domain}' if w1 else f'{w0[0]}@{domain}',
                f'{w0[0]}{w1}@{domain}' if w1 else '',
            ]
            guessed = [g for g in guessed if g]

    all_clean        = clean_emails(all_emails)
    personal, generic = split_emails(all_clean)
    phones_clean     = clean_phones(all_phones)
    guessed_clean    = [g for g in dict.fromkeys(guessed) if g not in all_clean][:12]

    elapsed = round(time.time()-start, 1)
    print(f'  ✓ {elapsed}s — personal:{len(personal)} generic:{len(generic)} '
          f'phones:{len(phones_clean)} contacts:{len(contacts)} guessed:{len(guessed_clean)}')

    return {
        'emails':         personal[:12],
        'generic_emails': generic[:6],
        'phones':         phones_clean[:10],
        'contacts':       contacts[:10],
        'guessed_emails': guessed_clean,
        'website':        website,
        'sources':        list(dict.fromkeys(sources))[:15],
        'resolved_name':  resolved_name,
    }

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/ping')
def ping():
    return jsonify({'status': 'ok'})

@app.route('/debug')
def debug():
    """Diagnose search engines. Visit /debug?q=YourQuery"""
    query = request.args.get('q', 'Vacasa property management contact email')
    out   = {'query': query, 'serper_key_set': bool(SERPER_KEY), 'results': {}}

    # Test Serper
    if SERPER_KEY:
        try:
            r = requests.post(
                'https://google.serper.dev/search',
                headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
                json={'q': query, 'num': 5},
                timeout=10,
            )
            out['results']['serper'] = {
                'status': r.status_code,
                'results': r.json().get('organic', [])[:3] if r.status_code == 200 else r.text[:500],
            }
        except BaseException as e:
            out['results']['serper'] = {'error': str(e)}
    else:
        out['results']['serper'] = {'error': 'SERPER_API_KEY env variable not set'}

    # Test Bing direct
    try:
        q    = requests.utils.quote(query)
        html = fetch(f'https://www.bing.com/search?q={q}&count=5', timeout=8)
        out['results']['bing'] = {
            'html_bytes': len(html) if html else 0,
            'results': _parse_bing(html, 5) if html else [],
        }
    except BaseException as e:
        out['results']['bing'] = {'error': str(e)}

    return jsonify(out)

@app.route('/enrich')
def enrich():
    """Starts enrichment in a background thread, returns job_id immediately."""
    company    = request.args.get('company','').strip()
    market     = request.args.get('market','').strip()
    website    = request.args.get('website','').strip()
    airbnb_url = request.args.get('airbnb_url','').strip()
    host_id    = request.args.get('host_id','').strip()

    if not company and not host_id and not airbnb_url:
        return jsonify({'error':'company required'}), 400

    job_id = uuid.uuid4().hex[:10]
    with jobs_lock:
        jobs[job_id] = {'status':'running', 'ts': time.time()}
        # Clean up old jobs (>30 min)
        cutoff = time.time() - 1800
        old = [k for k,v in jobs.items() if v.get('ts',0) < cutoff]
        for k in old: del jobs[k]

    def run():
        try:
            result = do_enrich(company, market, website, airbnb_url, host_id)
            with jobs_lock:
                jobs[job_id] = {'status':'done', 'result': result, 'ts': time.time()}
        except BaseException as e:
            print(f'  !! job {job_id} crashed: {e}')
            with jobs_lock:
                jobs[job_id] = {'status':'error', 'result':{'error':str(e)}, 'ts': time.time()}

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def job_status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify({'status':'not_found'}), 404
    return jsonify(job)

@app.after_request
def cors(r):
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print('\n  Casago PM Enrichment Server')
    print(f'  Running at http://0.0.0.0:{port}\n')
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
