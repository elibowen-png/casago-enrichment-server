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
                 '/team','/our-team','/leadership','/people',
                 '/staff','/management','/ownership','/owners',
                 '/meet-the-team','/meet-us','/who-we-are','/founders']

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
        emails = clean_emails(mailto + EMAIL_RE.findall(html) + decode_obfuscated(text))
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
    # Try multiple selectors — Bing's structure varies by region/version
    blocks = (soup.select('li.b_algo') or
              soup.select('div.b_algo') or
              soup.select('#b_results > li'))
    if not blocks:
        # Fallback: grab any h2>a with a real href
        for a in soup.select('h2 a[href]')[:n*2]:
            try:
                href = a.get('href','')
                if not href.startswith('http'): continue
                parent = a.find_parent(['li','div'])
                snip   = parent.select_one('p') if parent else None
                results.append({'title': a.get_text(strip=True),
                                'body':  snip.get_text(strip=True) if snip else '',
                                'href':  href})
                if len(results) >= n: break
            except BaseException: continue
        return results
    for block in blocks[:n*2]:
        try:
            a    = block.select_one('h2 a') or block.select_one('a[href]')
            snip = block.select_one('p, .b_lineclamp2, .b_paractl, .b_caption p')
            if not a: continue
            href = a.get('href','')
            if not href.startswith('http'): continue
            results.append({'title': a.get_text(strip=True),
                            'body':  snip.get_text(strip=True) if snip else '',
                            'href':  href})
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
        # Organic results
        for item in data.get('organic', [])[:n]:
            results.append({
                'title': item.get('title', ''),
                'body':  item.get('snippet', ''),
                'href':  item.get('link', ''),
            })
        # Answer box — often contains contact info directly
        ab = data.get('answerBox', {})
        if ab:
            ab_text = ab.get('answer','') or ab.get('snippet','') or ab.get('snippetHighlighted','')
            if ab_text:
                results.insert(0, {'title': ab.get('title',''), 'body': str(ab_text), 'href': ab.get('link','')})
        # People Also Ask — sometimes reveals contact info
        for paa in data.get('peopleAlsoAsk', [])[:4]:
            snippet = paa.get('snippet','') or paa.get('answer','')
            if snippet:
                results.append({'title': paa.get('question',''), 'body': str(snippet), 'href': paa.get('link','')})
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

OBFUSCATED_EMAIL_RE = re.compile(
    r'([a-zA-Z0-9._%+\-]+)\s*(?:\[at\]|\(at\)|AT|at)\s*([a-zA-Z0-9.\-]+)\s*(?:\[dot\]|\(dot\)|DOT|dot)\s*([a-zA-Z]{2,})',
    re.IGNORECASE
)

def decode_obfuscated(text):
    """Detect emails written as 'john [at] company [dot] com'"""
    found = []
    try:
        for m in OBFUSCATED_EMAIL_RE.finditer(text):
            found.append(f'{m.group(1)}@{m.group(2)}.{m.group(3)}'.lower())
    except BaseException: pass
    return found

def get_sitemap_contact_urls(base):
    """Fetch sitemap.xml and return URLs that look like contact/about/team pages."""
    keywords = ['contact','about','team','staff','leadership','people','management','owner','founder']
    for path in ['/sitemap.xml', '/sitemap_index.xml']:
        try:
            html = fetch(base + path, timeout=8)
            if not html: continue
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', html)
            matched = [u for u in urls if any(kw in u.lower() for kw in keywords)]
            if matched:
                print(f'  Sitemap → {len(matched)} contact-ish URLs')
                return matched[:6]
        except BaseException: pass
    return []

def fetch_results_pages(results, all_emails, all_phones, contacts, sources, label, start, limit=150, max_pages=4):
    """Actually fetch and scrape the pages found in search results — not just snippets."""
    fetched = 0
    for r in results:
        if timed_out(start, limit) or fetched >= max_pages: break
        href = r.get('href','')
        if not href or not href.startswith('http'): continue
        if any(s in href for s in list(SKIP_DOMAINS) + ['google','bing','yahoo']): continue
        if href in sources: continue
        try:
            scrape_and_collect(href, all_emails, all_phones, contacts, sources, label)
            fetched += 1
            time.sleep(0.4)
        except BaseException: pass

def timed_out(start, limit=150):
    return (time.time() - start) > limit

def has_personal(emails):
    return bool(split_emails(clean_emails(emails))[0])

JUNK_NAMES = {'john doe','jane doe','first last','your name','full name','contact us',
              'property manager','vacation rental','real estate','the owner'}

def add_contact(contacts, name, title='', linkedin='', source=''):
    """Add a contact only if the name looks like a real person and isn't already listed."""
    if not name: return
    nl = name.lower().strip()
    if nl in JUNK_NAMES: return
    if len(name.split()) < 2 or len(name) > 60: return
    # Skip if it looks like a company name (all caps words, LLC, Inc, etc.)
    if re.search(r'\b(LLC|Inc|Corp|Co\.|Properties|Management|Rentals|Vacation)\b', name): return
    if not any(c['name'] == name for c in contacts):
        contacts.append({'name': name, 'title': title, 'linkedin': linkedin, 'source': source})

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
            site_results = google(f'"{company}" {market} property management vacation rental')
            if not website:
                for r in site_results:
                    href = r.get('href','')
                    m    = DOMAIN_RE.match(href)
                    if m:
                        dom = m.group(1).lower().replace('www.','')
                        if not any(s in dom for s in SKIP_DOMAINS):
                            website = href.split('?')[0].rstrip('/'); print(f'  Website: {website}'); break
                    all_emails += EMAIL_RE.findall(r.get('body',''))
                    all_phones += PHONE_RE.findall(r.get('body',''))
                    all_emails += decode_obfuscated(r.get('body',''))
            if website:
                m = DOMAIN_RE.match(website)
                if m: domain = m.group(1).replace('www.','')
            # Also fetch the actual pages from search results (not just snippets)
            fetch_results_pages(site_results, all_emails, all_phones, contacts, sources, 'Web Search', start, max_pages=3)
        except BaseException as e:
            print(f'  Website find error: {e}')

    # ── 2. Scrape website pages ───────────────────────────────────────────────
    if website and not timed_out(start):
        base = '/'.join(website.split('/')[:3])
        for path in CONTACT_PATHS:
            if timed_out(start): break
            scrape_and_collect(base+path, all_emails, all_phones, contacts, sources, 'Website')
            time.sleep(0.2)
        # Sitemap discovery — find team/contact pages we might have missed
        if not has_personal(all_emails) and not timed_out(start):
            for url in get_sitemap_contact_urls(base):
                if timed_out(start): break
                scrape_and_collect(url, all_emails, all_phones, contacts, sources, 'Sitemap')
                time.sleep(0.2)

    # ── 2.5 Direct domain email hunt ─────────────────────────────────────────
    if domain and not has_personal(all_emails) and not timed_out(start):
        try:
            print(f'  → Domain email hunt for @{domain}')
            # Search for any email from this domain exposed anywhere on the web
            for r in google(f'"@{domain}" (owner OR contact OR manager OR founder OR CEO)', 10):
                body = r.get('body','') + ' ' + r.get('title','')
                all_emails += EMAIL_RE.findall(body)
                for name in extract_person_names(body):
                    add_contact(contacts, name, source='Domain Search')
            # Also try: just the domain name to find email format
            for r in google(f'"{domain}" email contact property management', 6):
                body = r.get('body','') + ' ' + r.get('title','')
                all_emails += EMAIL_RE.findall(body)
        except BaseException as e:
            print(f'  Domain hunt error: {e}')

    # ── 2.7 emailformat.com — reveals the email pattern a company uses ────────
    if domain and not has_personal(all_emails) and not timed_out(start):
        try:
            ef_html = fetch(f'https://www.emailformat.com/d/{domain}', timeout=8)
            if ef_html:
                ef_emails = EMAIL_RE.findall(ef_html)
                ef_clean  = clean_emails(ef_emails)
                print(f'  emailformat.com → {len(ef_clean)} patterns')
                all_emails += ef_emails
                # Also look for stated format like "first.last@domain.com"
                soup = BeautifulSoup(ef_html, 'html.parser')
                for el in soup.select('.format, .email-format, td, th'):
                    t = el.get_text(strip=True)
                    if '@' in t and domain in t:
                        all_emails += EMAIL_RE.findall(t)
        except BaseException as e:
            print(f'  emailformat error: {e}')

    # ── 3. LinkedIn ───────────────────────────────────────────────────────────
    if not timed_out(start):
        try:
            company_lower = company.lower()
            # Words that are too generic to trust alone (would match thousands of profiles)
            generic_words = {'landing','city','home','homes','premier','elite','luxury',
                             'coastal','mountain','beach','lake','urban','capital','summit',
                             'harbor','harbour','sunrise','sunset','premier','signature'}
            company_words = [w for w in re.sub(r'[^a-z\s]','',company_lower).split() if len(w)>2]
            is_generic_name = all(w in generic_words for w in company_words[:2]) if company_words else True

            # If domain known, use that — far more specific than company name alone
            if domain:
                li_query = f'site:linkedin.com/in "{domain}" OR site:linkedin.com/in "{company}" {market} (owner OR founder OR CEO OR "property manager")'
            elif is_generic_name:
                # Generic names need the market to narrow results
                li_query = f'site:linkedin.com/in "{company}" {market} (owner OR founder OR CEO OR "property manager" OR "vacation rental")'
            else:
                li_query = f'site:linkedin.com/in "{company}" (owner OR founder OR CEO OR president OR broker OR "property manager")'

            li_results = google(li_query)
            for r in li_results:
                if timed_out(start): break
                href, title, body = r.get('href',''), r.get('title',''), r.get('body','')
                combined = (title + ' ' + body).lower()
                all_emails += EMAIL_RE.findall(body)
                all_emails += decode_obfuscated(body)
                if 'linkedin.com/in/' in href:
                    parts = title.split(' - ')
                    name  = parts[0].strip()
                    role  = parts[1].strip() if len(parts) > 1 else ''
                    # Validate: snippet must mention the company name or domain to be relevant
                    name_match = any(w in combined for w in company_words if len(w) > 3)
                    domain_match = domain and domain.split('.')[0] in combined
                    if not (name_match or domain_match):
                        print(f'  Skipping unrelated LinkedIn: {name}')
                        continue
                    if len(name.split()) >= 2 and len(name) < 60:
                        add_contact(contacts, name, title=role, linkedin=href, source='LinkedIn')
                        print(f'  LinkedIn: {name} ({role})')

            # LinkedIn company page search — also require company name in snippet
            for r in google(f'site:linkedin.com/company "{company}" {market}'):
                if timed_out(start): break
                href, body = r.get('href',''), r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                for name in extract_person_names(body):
                    if any(w in body.lower() for w in company_words if len(w) > 3):
                        add_contact(contacts, name, source='LinkedIn Company')
        except BaseException as e:
            print(f'  LinkedIn error: {e}')

    # ── 3.5 Per-contact email search (if names found) ─────────────────────────
    if contacts and not has_personal(all_emails) and not timed_out(start):
        print(f'  → Searching email for {len(contacts)} known contact(s)...')
        for c in contacts[:4]:
            if has_personal(all_emails) or timed_out(start): break
            name = c.get('name','')
            if not name or len(name.split()) < 2: continue
            try:
                # Direct email search for this person
                for r in google(f'"{name}" email OR "@" {market} property management vacation rental', 6):
                    body = r.get('body','') + ' ' + r.get('title','')
                    all_emails += EMAIL_RE.findall(body)
                # If domain known, search specifically for their email at that domain
                if domain:
                    parts = name.split()
                    first, last = parts[0].lower(), parts[-1].lower()
                    for r in google(f'"{first}" "{last}" "@{domain}"', 5):
                        body = r.get('body','') + ' ' + r.get('title','')
                        all_emails += EMAIL_RE.findall(body)
            except BaseException: pass

    # ── 4. Owner / contact searches ───────────────────────────────────────────
    if not timed_out(start):
        try:
            owner_results = google(f'"{company}" {market} owner email phone contact')
            for r in owner_results:
                if timed_out(start): break
                body = r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                all_emails += decode_obfuscated(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body+' '+r.get('title','')):
                    add_contact(contacts, name, source='Web')
            # Fetch actual pages from owner search results
            fetch_results_pages(owner_results, all_emails, all_phones, contacts, sources, 'Owner Search', start, max_pages=3)
        except BaseException as e:
            print(f'  Owner search error: {e}')

    # ── 4.5 Facebook business page ────────────────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        try:
            fb_results = google(f'site:facebook.com/pages "{company}" OR site:facebook.com "{company}" {market} property management')
            for r in fb_results[:3]:
                href, body = r.get('href',''), r.get('body','')
                all_emails += EMAIL_RE.findall(body)
                all_phones += PHONE_RE.findall(body)
                for name in extract_person_names(body):
                    add_contact(contacts, name, source='Facebook')
                if href and 'facebook.com' in href:
                    scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'Facebook')
                    time.sleep(0.4)
        except BaseException as e:
            print(f'  Facebook error: {e}')

    # ══ From here on, only continue if no personal email found yet ════════════

    # ── 5. Directories: BBB, Yelp, Manta, Clutch, HomeAdvisor ────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → No email yet, searching directories...')
        dir_queries = [
            f'site:bbb.org "{company}" {market}',
            f'site:yelp.com/biz "{company}" {market}',
            f'site:manta.com "{company}"',
            f'site:clutch.co "{company}"',
            f'site:thumbtack.com "{company}"',
            f'site:homeadvisor.com "{company}"',
            f'site:angieslist.com "{company}"',
            f'"{company}" {market} property management "contact" email -site:airbnb.com -site:vrbo.com',
        ]
        for q in dir_queries:
            if has_personal(all_emails) or timed_out(start): break
            try:
                results = google(q, 5)
                for r in results:
                    href = r.get('href','')
                    body = r.get('body','')
                    all_emails += EMAIL_RE.findall(body)
                    all_emails += decode_obfuscated(body)
                    all_phones += PHONE_RE.findall(body)
                    for name in extract_person_names(body):
                        add_contact(contacts, name, source='Directory')
                    if href and not any(s in href for s in ['google','bing']):
                        scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'Directory')
                        time.sleep(0.3)
            except BaseException: pass

    # ── 6. News / press / interview mentions ──────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → Searching news and press...')
        try:
            press_results = google(f'"{company}" {market} (interview OR profile OR "property manager" OR founder OR owner) -site:airbnb.com')
            for r in press_results:
                if has_personal(all_emails) or timed_out(start): break
                href = r.get('href','')
                body = r.get('body','')+' '+r.get('title','')
                all_emails += EMAIL_RE.findall(body)
                all_emails += decode_obfuscated(body)
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

    # ── 7.5 State license / SOS / public record search ───────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  → Checking state licensing and public records...')
        try:
            for r in google(f'"{company}" {market} real estate license broker email site:gov OR site:state OR "license lookup"', 6):
                body = r.get('body','') + ' ' + r.get('title','')
                all_emails += EMAIL_RE.findall(body)
                href = r.get('href','')
                if href and '.gov' in href and not timed_out(start):
                    scrape_and_collect(href, all_emails, all_phones, contacts, sources, 'State License')
                    time.sleep(0.3)
            # Secretary of State / business registration often lists contact email
            for r in google(f'"{company}" {market} "secretary of state" OR "business registration" OR "registered agent"', 5):
                body = r.get('body','') + ' ' + r.get('title','')
                all_emails += EMAIL_RE.findall(body)
        except BaseException as e:
            print(f'  License search error: {e}')

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
