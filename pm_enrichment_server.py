#!/usr/bin/env python3
"""
Casago PM Lead Enrichment Server — async job model
/enrich  → returns {job_id} immediately, runs in background thread
/status/<job_id> → returns {status:'running'} or {status:'done', result:{...}}
/ping    → always fast, never blocked by enrichment

Search order:
  1. Market search (geographic context)
  2. Company name search (find website, surface emails/phones in snippets)
  3. Executive/owner search (with market)
  4. Full website crawl (follow internal links, up to 30 pages)
  5. LinkedIn
  6. LLC directories / state registries
  7. Broader directories / press
  8. Per-contact email hunt
  9. Email pattern guessing
"""

import os, re, time, socket, threading, uuid
from urllib.parse import urljoin, urlparse
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup

socket.setdefaulttimeout(12)

app  = Flask(__name__)
jobs = {}
jobs_lock = threading.Lock()

EMAIL_RE  = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_RE  = re.compile(r'(?:\+?1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4})')
DOMAIN_RE = re.compile(r'https?://([^/\s?#]+)')

# ── Junk email strings — anything containing these is discarded ───────────────
JUNK_EMAIL = [
    'noreply','no-reply','example','yourname','test@','@sentry',
    '@example','placeholder','@domain','.png@','.jpg@','unsubscribe',
    'privacy@','legal@','press@','media@','careers@','jobs@',
    'donotreply','do-not-reply','bounce','mailer','daemon',
]

# ── Fake/placeholder email local-part patterns ────────────────────────────────
FAKE_EMAIL_LOCAL = re.compile(
    r'^(?:'
    r'john\.?doe|jane\.?doe|first\.?last|firstname\.?lastname|'
    r'your\.?name|your\.?email|yourname|name\.?here|'
    r'sample\.?email|email\.?here|email\.?address|'
    r'user(?:name)?|example\.?user|demo\.?user|'
    r'test\.?user|test\.?email|test(?:ing)?|'
    r'foo\.?bar|foo|bar|baz|'
    r'someone|somebody|anyone|'
    r'person|contact\.?name'
    r')$',
    re.IGNORECASE
)

JUNK_EXT = {'.png','.jpg','.jpeg','.gif','.svg','.css','.js','.woff','.ico','.webp'}

GENERIC_PREFIXES = {
    'info','contact','hello','admin','support','help',
    'office','general','inquiries','inquiry','booking',
    'reservations','mail','team','sales','concierge',
}

SKIP_DOMAINS = {
    'airbnb','vrbo','tripadvisor','yelp','google','facebook','zillow',
    'airdna','linkedin','instagram','youtube','twitter','booking',
    'expedia','pinterest','reddit','apartments','trulia','realtor',
    'homeaway','hipcamp',
}

# Standard contact/about paths to hit first during crawl
PRIORITY_PATHS = [
    '/about','/about-us','/our-story','/who-we-are','/founders',
    '/our-team','/team','/meet-the-team','/staff','/leadership',
    '/people','/management','/owners','/ownership','/about/team',
    '/contact','/contact-us','/meet-us',
]

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

SERPER_KEY = os.environ.get('SERPER_API_KEY', '')

# ── Helpers ───────────────────────────────────────────────────────────────────

def is_generic(email):
    return email.split('@')[0].lower() in GENERIC_PREFIXES

def is_fake_email(email):
    """Returns True if the email looks like a placeholder/example."""
    try:
        local = email.split('@')[0].lower()
        if FAKE_EMAIL_LOCAL.match(local):
            return True
        # local part is literally 'name', 'user', 'me', 'you', etc.
        if local in {'name','user','me','you','us','them','her','him'}:
            return True
        return False
    except BaseException:
        return False

def is_fake_phone(d):
    """Returns True if a 10-digit string looks like a fake/placeholder phone."""
    if not d or len(d) != 10:
        return False
    # All same digit: 2222222222, 5555555555
    if len(set(d)) == 1:
        return True
    # Area code == exchange == subscriber (222-222-2222)
    if d[:3] == d[3:6] == d[6:]:
        return True
    # Common sequential patterns
    if d in ('1234567890','0987654321','1231231234','9876543210'):
        return True
    # TV/movie 555 fake numbers (555-0100 through 555-0199)
    if d[3:6] == '555' and d[6] == '0':
        return True
    # All zeros in any segment
    if d[:3] == '000' or d[3:6] == '000':
        return True
    # 800-555-xxxx style placeholder
    if d[:3] in ('800','888','877','866') and d[3:6] == '555':
        return True
    return False

def clean_emails(raw):
    out = set()
    for e in raw:
        try:
            e = e.lower().strip().rstrip('.')
            if len(e) < 6 or '@' not in e:
                continue
            if any(j in e for j in JUNK_EMAIL):
                continue
            if '.' + e.split('.')[-1] in JUNK_EXT:
                continue
            if is_fake_email(e):
                continue
            out.add(e)
        except BaseException:
            pass
    return sorted(out)

def split_emails(emails):
    return [e for e in emails if not is_generic(e)], [e for e in emails if is_generic(e)]

def clean_phones(raw):
    out = set()
    for p in raw:
        try:
            d = re.sub(r'\D', '', str(p))
            if len(d) == 10:
                if not is_fake_phone(d):
                    out.add(f'({d[0:3]}) {d[3:6]}-{d[6:]}')
            elif len(d) == 11 and d[0] == '1':
                if not is_fake_phone(d[1:]):
                    out.add(f'({d[1:4]}) {d[4:7]}-{d[7:]}')
        except BaseException:
            pass
    return sorted(out)

def timed_out(start, limit=150):
    return (time.time() - start) > limit

def has_personal(emails):
    return bool(split_emails(clean_emails(emails))[0])

NOT_PERSON_WORDS = {
    'llc','inc','corp','co','ltd','the','and','for','with','our','your','their',
    'property','manager','management','vacation','rental','rentals','real','estate',
    'owner','founder','ceo','president','director','contact','info','team','staff',
    'leadership','executive','group','services','solutions','realty','associates',
    'properties','homes','house','rent','booking','hospitality','travel','host',
    'hello','email','phone','call','reach','send','click','here','more','about',
    'view','see','get','read','visit','learn','find','search','sign','log',
}

def is_real_person_name(name):
    if not name: return False
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 3: return False
    for p in parts:
        if not re.match(r"^[A-Z][a-zA-Z\-']{1,19}$", p): return False
        if p.lower() in NOT_PERSON_WORDS: return False
    return True

def add_contact(contacts, name, title='', linkedin='', source='', trusted=False):
    if not name: return
    name = name.strip()
    if any(c['name'] == name for c in contacts): return
    if trusted:
        parts = name.split()
        if len(parts) < 2 or len(name) > 60: return
        if name.isupper(): return
        if re.search(r'\b(LLC|Inc|Corp|Properties|Management|Vacation|Rental|Rentals|Services|Solutions|Group|Realty)\b', name): return
    else:
        if not is_real_person_name(name): return
    contacts.append({'name': name, 'title': title, 'linkedin': linkedin, 'source': source})
    print(f'  Contact: {name} ({title or source})')

OBFUSCATED_EMAIL_RE = re.compile(
    r'([a-zA-Z0-9._%+\-]+)\s*(?:\[at\]|\(at\)|AT|at)\s*([a-zA-Z0-9.\-]+)\s*(?:\[dot\]|\(dot\)|DOT|dot)\s*([a-zA-Z]{2,})',
    re.IGNORECASE
)

def decode_obfuscated(text):
    found = []
    try:
        for m in OBFUSCATED_EMAIL_RE.finditer(text):
            found.append(f'{m.group(1)}@{m.group(2)}.{m.group(3)}'.lower())
    except BaseException:
        pass
    return found

ABOUT_NAME_PATTERNS = [
    r"(?:Hi[,!]?\s+I'?m|My name is|I am)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
    r'([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:founded|started|created|owns|launched|established|built|runs|operates)',
    r'(?:founded|owned|operated|managed|run|led)\s+by\s+([A-Z][a-z]+ [A-Z][a-z]+)',
    r'(?:owner|founder|CEO|president|principal|broker|host|operator)[,:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
    r'([A-Z][a-z]+ [A-Z][a-z]+)\s*[,\-]\s*(?:owner|founder|CEO|president|principal|broker|operator|host)',
    r'(?:Meet|About|Introducing)\s+([A-Z][a-z]+ [A-Z][a-z]+)',
    r'([A-Z][a-z]+ [A-Z][a-z]+)\s+has\s+been\s+(?:managing|running|operating|hosting)',
    r'(?:contact|reach|email)\s+([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:at|directly|for)',
]

def extract_names_from_page(soup, text, contacts, source, trusted=False):
    """Pull names from headings, bio patterns, and structured markup."""
    # 1. Headings (h1-h3) — on about/team pages these are often the person's name
    for tag in soup.find_all(['h1','h2','h3']):
        t = tag.get_text(strip=True)
        add_contact(contacts, t, source=source, trusted=trusted)

    # 2. Bio patterns in body text
    for pat in ABOUT_NAME_PATTERNS:
        for m in re.finditer(pat, text, re.IGNORECASE):
            candidate = m.group(1).strip()
            add_contact(contacts, candidate, source=source, trusted=trusted)

    # 3. Schema.org / CSS class signals
    for el in soup.select(
        '[itemprop="name"],[class*="author"],[class*="founder"],[class*="owner"],'
        '[class*="ceo"],[class*="team-member"],[class*="person"],[class*="staff-name"],'
        '[class*="member-name"],[class*="bio-name"]'
    ):
        t = el.get_text(strip=True)
        add_contact(contacts, t, source=source, trusted=True)

def extract_person_names(text):
    """Extract names from snippet text that appear next to leadership titles."""
    patterns = [
        r'(?:owner|founder|CEO|president|principal|managing director|broker|operator|host)[:\s]+([A-Z][a-z]+(?: [A-Z][a-z]+)+)',
        r'([A-Z][a-z]+(?: [A-Z][a-z]+)+)(?:\s*[,\-]\s*(?:Owner|Founder|CEO|President|Principal|Broker|Manager|Operator|Host))',
        r'(?:contact|reach|meet)\s+([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:at|directly|for)',
        r'(?:by|with)\s+([A-Z][a-z]+ [A-Z][a-z]+)\s*,\s*(?:owner|founder|CEO)',
    ]
    found = []
    try:
        for pat in patterns:
            for m in re.finditer(pat, text, re.IGNORECASE):
                candidate = m.group(1).strip()
                if is_real_person_name(candidate):
                    found.append(candidate)
    except BaseException:
        pass
    return list(dict.fromkeys(found))

def guess_email_patterns(first, last, domain):
    try:
        f = re.sub(r'[^a-z]','', first.lower())
        l = re.sub(r'[^a-z]','', last.lower())
        if not f or not l or not domain: return []
        return list(dict.fromkeys([
            f'{f}@{domain}', f'{f}.{l}@{domain}', f'{f[0]}{l}@{domain}',
            f'{f[0]}.{l}@{domain}', f'{l}@{domain}',
        ]))
    except BaseException:
        return []

def fetch(url, timeout=12):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r.text[:400_000]
    except BaseException as e:
        print(f'  fetch error {url[:60]}: {type(e).__name__}')
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
        return emails, phones, text, soup
    except BaseException:
        return [], [], '', None

def collect_from_page(url, all_emails, all_phones, contacts, sources, label='', trusted=False):
    """Fetch a URL, extract emails/phones/names, add to shared accumulators."""
    try:
        html = fetch(url)
        if not html: return
        e, p, text, soup = parse_page(html)
        all_emails += e
        all_phones += p
        sources.append(url)
        if soup:
            extract_names_from_page(soup, text, contacts, label or url, trusted=trusted)
        else:
            for name in extract_person_names(text):
                add_contact(contacts, name, source=label or url)
    except BaseException as e:
        print(f'  collect error {url[:60]}: {type(e).__name__}')

def get_sitemap_urls(base):
    """Fetch sitemap and return any contact/team/about URLs found in it."""
    keywords = ['contact','about','team','staff','leadership','people','management','owner','founder']
    for path in ['/sitemap.xml', '/sitemap_index.xml']:
        try:
            html = fetch(base + path, timeout=8)
            if not html: continue
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', html)
            matched = [u for u in urls if any(kw in u.lower() for kw in keywords)]
            if matched:
                print(f'  Sitemap → {len(matched)} relevant URLs')
                return matched[:10]
        except BaseException:
            pass
    return []

def crawl_website(base, all_emails, all_phones, contacts, sources, start, max_pages=30, limit=150):
    """
    Full website crawl — follows internal links up to max_pages.
    Prioritizes contact/about/team pages but visits the whole site.
    This runs BEFORE LinkedIn and other search engines.
    """
    print(f'  → Full website crawl: {base} (up to {max_pages} pages)...')

    domain = urlparse(base).netloc.replace('www.','')
    visited = set()
    priority_q = []
    regular_q  = []

    CRAWL_PRIORITY = {'contact','about','team','staff','leader','owner','founder',
                      'people','management','who-we','our-story','meet','bios','bio'}

    # Seed with priority paths first
    for path in PRIORITY_PATHS:
        url = base + path
        if url not in visited:
            priority_q.append(url)

    # Then homepage
    regular_q.append(base)

    pages_crawled = 0

    def should_skip(url):
        low = url.lower()
        if any(ext in low for ext in ['.pdf','.jpg','.png','.gif','.css','.js','.xml',
                                       '.woff','.ico','.webp','.svg','.mp4','.zip']):
            return True
        if '#' in url:
            return True
        return False

    def process_url(url):
        nonlocal pages_crawled
        if url in visited or should_skip(url):
            return
        visited.add(url)

        # Only follow links that stay on this domain
        parsed = urlparse(url)
        if parsed.netloc.replace('www.','') != domain:
            return

        html = fetch(url, timeout=10)
        if not html:
            return

        e, p, text, soup = parse_page(html)
        all_emails += e
        all_phones += p
        if url not in sources:
            sources.append(url)
        pages_crawled += 1

        if soup:
            # On about/team/contact pages use trusted=True (looser name validation)
            is_about = any(kw in url.lower() for kw in CRAWL_PRIORITY)
            extract_names_from_page(soup, text, contacts, 'Website', trusted=is_about)

            # Discover more internal links
            for a in soup.find_all('a', href=True):
                href = a.get('href','').strip()
                if not href or href.startswith('mailto:') or href.startswith('tel:'):
                    continue
                # Resolve relative URLs
                full = urljoin(url, href).split('?')[0].split('#')[0]
                if full in visited or should_skip(full):
                    continue
                if urlparse(full).netloc.replace('www.','') != domain:
                    continue
                # Prioritise relevant pages
                if any(kw in full.lower() for kw in CRAWL_PRIORITY):
                    priority_q.append(full)
                else:
                    regular_q.append(full)

        time.sleep(0.25)

    # Work through queues
    while (priority_q or regular_q) and pages_crawled < max_pages and not timed_out(start, limit):
        url = (priority_q.pop(0) if priority_q else regular_q.pop(0))
        try:
            process_url(url)
        except BaseException as e:
            print(f'  Crawl error {url[:60]}: {type(e).__name__}')

    print(f'  Crawl done: {pages_crawled} pages, {len(contacts)} contacts so far')

# ── Search engine helpers ─────────────────────────────────────────────────────

def _serper_search(query, n=8):
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
                'title': item.get('title',''),
                'body':  item.get('snippet',''),
                'href':  item.get('link',''),
            })
        ab = data.get('answerBox', {})
        if ab:
            ab_text = ab.get('answer','') or ab.get('snippet','') or ab.get('snippetHighlighted','')
            if ab_text:
                results.insert(0, {'title': ab.get('title',''), 'body': str(ab_text), 'href': ab.get('link','')})
        for paa in data.get('peopleAlsoAsk', [])[:3]:
            snippet = paa.get('snippet','') or paa.get('answer','')
            if snippet:
                results.append({'title': paa.get('question',''), 'body': str(snippet), 'href': paa.get('link','')})
        print(f'  Serper → {len(results)} results')
        return results
    except BaseException as e:
        print(f'  Serper error: {type(e).__name__}')
        return []

def _parse_bing(html, n):
    soup    = BeautifulSoup(html, 'html.parser')
    results = []
    blocks  = soup.select('li.b_algo') or soup.select('div.b_algo') or soup.select('#b_results > li')
    if not blocks:
        for a in soup.select('h2 a[href]')[:n*2]:
            href = a.get('href','')
            if not href.startswith('http'): continue
            parent = a.find_parent(['li','div'])
            snip   = parent.select_one('p') if parent else None
            results.append({'title': a.get_text(strip=True),
                            'body':  snip.get_text(strip=True) if snip else '',
                            'href':  href})
            if len(results) >= n: break
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
        except BaseException:
            continue
    return results

def search(query, n=8):
    print(f'  Searching: {query[:90]}')
    if SERPER_KEY:
        results = _serper_search(query, n)
        if results:
            return results
    # Bing fallback
    try:
        q    = requests.utils.quote(query)
        html = fetch(f'https://www.bing.com/search?q={q}&count={n}', timeout=8)
        results = _parse_bing(html, n) if html else []
        if results:
            print(f'  Bing fallback → {len(results)}')
            return results
    except BaseException:
        pass
    return []

def harvest_snippets(results, all_emails, all_phones, contacts):
    """Pull emails, phones, and names out of search result snippets (no fetching)."""
    for r in results:
        body = r.get('body','') + ' ' + r.get('title','')
        all_emails += clean_emails(EMAIL_RE.findall(body) + decode_obfuscated(body))
        all_phones += PHONE_RE.findall(body)
        for name in extract_person_names(body):
            add_contact(contacts, name, source='Search Snippet')

def fetch_result_pages(results, all_emails, all_phones, contacts, sources, label, start, max_pages=3, limit=150):
    """Fetch and scrape the actual pages linked in search results."""
    fetched = 0
    for r in results:
        if timed_out(start, limit) or fetched >= max_pages: break
        href = r.get('href','')
        if not href or not href.startswith('http'): continue
        if any(s in href for s in list(SKIP_DOMAINS) + ['google','bing','yahoo']): continue
        if href in sources: continue
        try:
            collect_from_page(href, all_emails, all_phones, contacts, sources, label)
            fetched += 1
            time.sleep(0.4)
        except BaseException:
            pass

# ── Core enrichment ────────────────────────────────────────────────────────────

def do_enrich(company, market, website, airbnb_url, host_id):
    print(f'\n══ Enriching: {company or host_id} / {market} ══')
    start = time.time()
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

    company_lower = company.lower()
    company_words = [w for w in re.sub(r'[^a-z\s]','', company_lower).split() if len(w) > 2]
    generic_company_words = {
        'landing','city','home','homes','premier','elite','luxury',
        'coastal','mountain','beach','lake','urban','capital','summit',
        'harbor','harbour','sunrise','sunset','signature','village',
        'ridge','haven','crest','vista','bay','park',
    }
    is_generic_name = all(w in generic_company_words for w in company_words[:2]) if company_words else True

    def snippet_matches_company(text):
        t = text.lower()
        specific_words = [w for w in company_words if len(w) > 3 and w not in generic_company_words]
        if domain and domain.split('.')[0] in t: return True
        if specific_words and any(w in t for w in specific_words): return True
        if not specific_words and market.lower()[:5] in t: return True
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1: Market search — find the company in its geographic context
    # ─────────────────────────────────────────────────────────────────────────
    print('  Phase 1: Market search...')
    if not timed_out(start):
        try:
            market_results = search(
                f'property management {market} "{company}" contact owner email', 8
            )
            harvest_snippets(market_results, all_emails, all_phones, contacts)
            # Try to find website from market search results
            if not website:
                for r in market_results:
                    href = r.get('href','')
                    m    = DOMAIN_RE.match(href)
                    if m:
                        dom = m.group(1).lower().replace('www.','')
                        if not any(s in dom for s in SKIP_DOMAINS):
                            website = href.split('?')[0].rstrip('/')
                            print(f'  Website (market search): {website}')
                            break
        except BaseException as e:
            print(f'  Phase 1 error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2: Company name search — confirm website, surface more contact info
    # ─────────────────────────────────────────────────────────────────────────
    print('  Phase 2: Company name search...')
    if not timed_out(start):
        try:
            company_results = search(
                f'"{company}" {market} vacation rental property management site contact', 8
            )
            harvest_snippets(company_results, all_emails, all_phones, contacts)
            if not website:
                for r in company_results:
                    href = r.get('href','')
                    m    = DOMAIN_RE.match(href)
                    if m:
                        dom = m.group(1).lower().replace('www.','')
                        if not any(s in dom for s in SKIP_DOMAINS):
                            website = href.split('?')[0].rstrip('/')
                            print(f'  Website (company search): {website}')
                            break
            if website:
                m = DOMAIN_RE.match(website)
                if m: domain = m.group(1).replace('www.','')
        except BaseException as e:
            print(f'  Phase 2 error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 3: Executive / owner search — find who runs the company
    # ─────────────────────────────────────────────────────────────────────────
    print('  Phase 3: Executive/owner search...')
    if not timed_out(start):
        try:
            exec_queries = [
                f'"{company}" {market} owner OR founder OR CEO OR president OR "property manager" email contact',
                f'"{company}" {market} "owned by" OR "founded by" OR "managed by" OR "meet our team"',
                f'"{company}" {market} "about us" OR "our story" owner name',
            ]
            for q in exec_queries:
                if timed_out(start): break
                results = search(q, 8)
                harvest_snippets(results, all_emails, all_phones, contacts)
                fetch_result_pages(results, all_emails, all_phones, contacts, sources,
                                   'Executive Search', start, max_pages=2)
        except BaseException as e:
            print(f'  Phase 3 error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 4: Full website crawl — scour every page before going to LinkedIn
    # ─────────────────────────────────────────────────────────────────────────
    if website and not timed_out(start):
        base = '/'.join(website.split('/')[:3])
        if not domain:
            m = DOMAIN_RE.match(website)
            if m: domain = m.group(1).replace('www.','')

        # 4a — Full internal link crawl
        crawl_website(base, all_emails, all_phones, contacts, sources, start, max_pages=30)

        # 4b — Sitemap (may reveal pages the crawler missed)
        if not timed_out(start):
            for url in get_sitemap_urls(base):
                if timed_out(start): break
                if url not in sources:
                    collect_from_page(url, all_emails, all_phones, contacts, sources, 'Sitemap', trusted=True)
                    time.sleep(0.2)

    # If we still don't have a website, try fetching result pages from earlier searches
    elif not website and not timed_out(start):
        try:
            for res_list in [market_results if 'market_results' in dir() else [],
                             company_results if 'company_results' in dir() else []]:
                for r in res_list[:3]:
                    href = r.get('href','')
                    if href and href.startswith('http') and not any(s in href for s in SKIP_DOMAINS):
                        if href not in sources:
                            collect_from_page(href, all_emails, all_phones, contacts, sources, 'Search Result')
                            time.sleep(0.3)
        except BaseException:
            pass

    # 4c — Hunt @domain emails via search
    if domain and not timed_out(start):
        try:
            print(f'  Phase 4c: Hunting @{domain} emails...')
            for r in search(f'"@{domain}" (owner OR contact OR manager OR founder OR CEO)', 10):
                body = r.get('body','') + ' ' + r.get('title','')
                all_emails += clean_emails(EMAIL_RE.findall(body) + decode_obfuscated(body))
                for name in extract_person_names(body):
                    add_contact(contacts, name, source='Domain Search')
            # emailformat.com reveals the company's email pattern
            ef_html = fetch(f'https://www.emailformat.com/d/{domain}', timeout=8)
            if ef_html:
                all_emails += clean_emails(EMAIL_RE.findall(ef_html))
        except BaseException as e:
            print(f'  Domain hunt error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 5: LinkedIn
    # ─────────────────────────────────────────────────────────────────────────
    print('  Phase 5: LinkedIn...')
    if not timed_out(start):
        try:
            if domain:
                li_query = f'site:linkedin.com/in "{company}" {market} (owner OR founder OR CEO OR "property manager")'
            elif is_generic_name:
                li_query = f'site:linkedin.com/in "{company}" {market} (owner OR founder OR CEO OR "property manager" OR "vacation rental")'
            else:
                li_query = f'site:linkedin.com/in "{company}" (owner OR founder OR CEO OR president OR broker OR "property manager")'

            for r in search(li_query, 8):
                if timed_out(start): break
                href, title, body = r.get('href',''), r.get('title',''), r.get('body','')
                combined = title + ' ' + body
                all_emails += clean_emails(EMAIL_RE.findall(body) + decode_obfuscated(body))
                if 'linkedin.com/in/' in href:
                    parts = title.split(' - ')
                    name = parts[0].strip()
                    role = parts[1].strip() if len(parts) > 1 else ''
                    if not snippet_matches_company(combined):
                        print(f'  Skipping unrelated LI result: {name}')
                        continue
                    add_contact(contacts, name, title=role, linkedin=href, source='LinkedIn')
        except BaseException as e:
            print(f'  Phase 5 error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 6: LLC directories / state registries
    # ─────────────────────────────────────────────────────────────────────────
    print('  Phase 6: LLC directories...')
    if not has_personal(all_emails) and not timed_out(start):
        try:
            llc_queries = [
                f'"{company}" {market} site:bbb.org',
                f'"{company}" {market} site:manta.com',
                f'"{company}" {market} "secretary of state" OR "registered agent" OR "business registration"',
                f'"{company}" {market} site:opencorporates.com',
                f'"{company}" {market} real estate license site:gov OR "license lookup"',
                f'"{company}" {market} site:corporationwiki.com',
            ]
            for q in llc_queries:
                if has_personal(all_emails) or timed_out(start): break
                results = search(q, 5)
                harvest_snippets(results, all_emails, all_phones, contacts)
                fetch_result_pages(results, all_emails, all_phones, contacts, sources,
                                   'LLC Directory', start, max_pages=2)
        except BaseException as e:
            print(f'  Phase 6 error: {e}')

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 7: Broader directories + press
    # ─────────────────────────────────────────────────────────────────────────
    if not has_personal(all_emails) and not timed_out(start):
        print('  Phase 7: Directories and press...')
        dir_queries = [
            f'site:yelp.com/biz "{company}" {market}',
            f'site:thumbtack.com "{company}" {market}',
            f'site:homeadvisor.com "{company}" {market}',
            f'site:bizjournals.com "{company}" {market}',
            f'"{company}" {market} (interview OR profile OR founder OR owner) -site:airbnb.com',
            f'"{company}" {market} contact email -site:airbnb.com -site:vrbo.com',
        ]
        for q in dir_queries:
            if has_personal(all_emails) or timed_out(start): break
            try:
                results = search(q, 5)
                harvest_snippets(results, all_emails, all_phones, contacts)
                fetch_result_pages(results, all_emails, all_phones, contacts, sources,
                                   'Directory', start, max_pages=2)
            except BaseException:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 8: Per-contact email hunt (once names are known)
    # ─────────────────────────────────────────────────────────────────────────
    if contacts and not has_personal(all_emails) and not timed_out(start):
        print(f'  Phase 8: Email hunt for {len(contacts)} contact(s)...')
        for c in contacts[:4]:
            if has_personal(all_emails) or timed_out(start): break
            name = c.get('name','')
            if not is_real_person_name(name): continue
            try:
                for r in search(f'"{name}" {market} property management email', 6):
                    body = r.get('body','') + ' ' + r.get('title','')
                    all_emails += clean_emails(EMAIL_RE.findall(body))
                if domain:
                    parts = name.split()
                    first, last = parts[0].lower(), parts[-1].lower()
                    for r in search(f'"{first}" "{last}" "@{domain}"', 5):
                        body = r.get('body','') + ' ' + r.get('title','')
                        all_emails += clean_emails(EMAIL_RE.findall(body))
            except BaseException:
                pass

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 9: Email pattern guessing
    # ─────────────────────────────────────────────────────────────────────────
    guessed = []
    if domain:
        for c in contacts[:8]:
            parts = c['name'].split()
            if len(parts) >= 2:
                guessed += guess_email_patterns(parts[0], parts[-1], domain)
        words = re.sub(r'[^a-z\s]','', company.lower()).split()
        words = [w for w in words if len(w) > 2 and w not in (
            'the','and','for','llc','inc','vacation','rental','rentals',
            'property','management','properties',
        )]
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

    elapsed = round(time.time() - start, 1)
    print(f'  ✓ {elapsed}s — personal:{len(personal)} generic:{len(generic)} '
          f'phones:{len(phones_clean)} contacts:{len(contacts)} guessed:{len(guessed_clean)}')

    return {
        'emails':         personal[:12],
        'generic_emails': generic[:6],
        'phones':         phones_clean[:10],
        'contacts':       contacts[:10],
        'guessed_emails': guessed_clean,
        'website':        website,
        'sources':        list(dict.fromkeys(sources))[:20],
        'resolved_name':  resolved_name,
    }

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/ping')
def ping():
    return jsonify({'status': 'ok'})

@app.route('/debug')
def debug():
    query = request.args.get('q', 'Vacasa property management contact email')
    out   = {'query': query, 'serper_key_set': bool(SERPER_KEY), 'results': {}}
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
        out['results']['serper'] = {'error': 'SERPER_API_KEY not set'}
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
