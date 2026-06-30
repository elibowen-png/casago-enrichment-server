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

def fetch(url, timeout=8):
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

def google(query, n=8):
    try:
        url  = f'https://www.google.com/search?q={requests.utils.quote(query)}&num={n}&hl=en'
        print(f'  Google: {query[:80]}')
        html = fetch(url)
        if not html: return []
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
        print(f'    → {len(results)} results')
        return results
    except BaseException as e:
        print(f'  google error: {type(e).__name__}')
        return []

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

def timed_out(start, limit=50):
    return (time.time() - start) > limit

# ── Core enrichment (runs in background thread) ────────────────────────────────

def do_enrich(company, market, website, airbnb_url, host_id):
    print(f'\n══ Enriching: {company or host_id} / {market} ══')
    start      = time.time()
    resolved_name = ''
    all_emails, all_phones, contacts, sources = [], [], [], []
    domain = ''

    # 0. Airbnb name resolution
    if (airbnb_url or host_id) and not timed_out(start):
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

    # 1. Find website
    if not timed_out(start):
        try:
            if not website:
                for r in google(f'"{company}" {market} property management vacation rental'):
                    href = r.get('href','')
                    m    = DOMAIN_RE.match(href)
                    if m:
                        dom = m.group(1).lower().replace('www.','')
                        if not any(s in dom for s in SKIP_DOMAINS):
                            website = href.split('?')[0].rstrip('/')
                            print(f'  Website: {website}'); break
                    all_emails += EMAIL_RE.findall(r.get('body',''))
                    all_phones += PHONE_RE.findall(r.get('body',''))
            if website:
                m = DOMAIN_RE.match(website)
                if m: domain = m.group(1).replace('www.','')
        except BaseException as e:
            print(f'  Website find error: {e}')

    # 2. Scrape website pages
    if website and not timed_out(start):
        try:
            base = '/'.join(website.split('/')[:3])
            for path in CONTACT_PATHS:
                if timed_out(start): break
                try:
                    html = fetch(base + path)
                    if not html: continue
                    e, p, text = parse_page(html)
                    all_emails += e; all_phones += p; sources.append(base + path)
                    for name in extract_person_names(text):
                        if not any(c['name']==name for c in contacts):
                            contacts.append({'name':name,'title':'','linkedin':'','source':'Website'})
                    time.sleep(0.3)
                except BaseException: pass
        except BaseException as e:
            print(f'  Scrape error: {e}')

    # 3. LinkedIn
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
                        if not any(c['name']==name for c in contacts):
                            contacts.append({'name':name,'title':role,'linkedin':href,'source':'LinkedIn'})
        except BaseException as e:
            print(f'  LinkedIn error: {e}')

    # 4. Owner search
    if not timed_out(start):
        try:
            for r in google(f'"{company}" {market} owner email phone contact'):
                if timed_out(start): break
                body = r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body+' '+r.get('title','')):
                    if not any(c['name']==name for c in contacts):
                        contacts.append({'name':name,'title':'','linkedin':'','source':'Web'})
        except BaseException as e:
            print(f'  Owner search error: {e}')

    # 5. Deep search if no personal emails yet
    current_personal, _ = split_emails(clean_emails(all_emails))
    if not current_personal and not timed_out(start):
        try:
            print('  ⚡ Deep search...')
            for r in google(f'"{company}" {market} (founded OR "owned by" OR owner) email'):
                if timed_out(start): break
                body = r.get('body','')+' '+r.get('title','')
                all_emails += EMAIL_RE.findall(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body):
                    if not any(c['name']==name for c in contacts):
                        contacts.append({'name':name,'title':'Owner','linkedin':'','source':'Deep Search'})
        except BaseException as e:
            print(f'  Deep search error: {e}')

    # 6. Email pattern guessing
    guessed = []
    try:
        for c in contacts[:5]:
            parts = c['name'].split()
            if len(parts)>=2 and domain:
                guessed += guess_email_patterns(parts[0], parts[-1], domain)
    except BaseException: pass

    all_clean   = clean_emails(all_emails)
    personal, generic = split_emails(all_clean)
    phones_clean = clean_phones(all_phones)
    guessed_clean = [g for g in dict.fromkeys(guessed) if g not in all_clean][:10]

    elapsed = round(time.time()-start, 1)
    print(f'  ✓ {elapsed}s — personal:{len(personal)} generic:{len(generic)} phones:{len(phones_clean)} contacts:{len(contacts)}')

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
