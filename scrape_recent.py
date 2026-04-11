#!/usr/bin/env python3
"""
scrape_recent.py
Fetches recent Deadpod blog posts and inserts/updates them in deadpod.db.
Scrapes the monthly archive pages for the last N months and picks up any
posts not already in the DB.

Usage:
    python3 ~/python/scrape_recent.py
"""

import sqlite3
import re
import os
from datetime import datetime, timezone, date
from urllib.request import urlopen, Request
from urllib.error import URLError

DB_PATH   = os.path.expanduser('~/python/deadpod.db')
BLOG_BASE = 'https://deadshow.blogspot.com'
MONTHS_BACK = 2   # how many recent months to check for new posts

# --- Helpers -----------------------------------------------------------------

def fetch(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0 (Deadpod-Scraper/1.0)'})
    with urlopen(req, timeout=15) as r:
        return r.read().decode('utf-8', errors='replace')


def get_recent_post_urls(months_back=2):
    """
    Scrape the monthly archive pages for the last N months and return
    all post URLs found, sorted newest-first.
    """
    today = date.today()
    urls  = []
    seen  = set()

    for i in range(months_back):
        year  = today.year - ((today.month - 1 - i) < 0)
        month = ((today.month - 1 - i) % 12) + 1
        archive_url = f'{BLOG_BASE}/{year}/{month:02d}/'
        print(f"  Checking archive: {archive_url}")
        try:
            html = fetch(archive_url)
        except URLError as e:
            print(f"    Could not fetch {archive_url}: {e}")
            continue

        found = re.findall(
            rf'blogspot\.com/({year}/{month:02d}/[a-z0-9][^\'"<> ]+\.html)',
            html
        )
        for slug in found:
            full_url = f'https://{BLOG_BASE.split("//")[1]}/{slug}'
            if full_url not in seen:
                seen.add(full_url)
                urls.append(full_url)

    return urls


def parse_post_date(title):
    """Extract podcast_date from title like 'Dead Show/podcast for 3/27/26'."""
    m = re.search(r'for\s+(\d{1,2})/(\d{1,2})/(\d{2,4})', title, re.IGNORECASE)
    if m:
        mo, dy, yr = m.group(1), m.group(2), m.group(3)
        yr = ('20' + yr) if len(yr) == 2 else yr
        try:
            return datetime(int(yr), int(mo), int(dy)).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None


def parse_show_date(content):
    """Extract concert date from lines like: Uniondale, NY 3/12/1992 - Thursday"""
    m = re.search(r'\b(\d{1,2})/(\d{1,2})/(\d{4})\b', content)
    if m:
        mo, dy, yr = m.group(1), m.group(2), m.group(3)
        try:
            return datetime(int(yr), int(mo), int(dy)).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None


# --- Venue / city extraction --------------------------------------------------
# US state abbreviations
_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
    'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
    'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
    'TX','UT','VT','VA','WA','WV','WI','WY','DC'
}

# Grateful Dead tour cities — used to split venue from city on single-line blocks
_GD_CITIES = {
    'New York','New York City','Brooklyn','Albany','Syracuse','Buffalo','Rochester',
    'Uniondale','East Rutherford','Philadelphia','Pittsburgh','Cleveland','Columbus',
    'Cincinnati','Detroit','Ann Arbor','Chicago','Milwaukee','Minneapolis','St Paul',
    'St. Paul','Kansas City','Omaha','Denver','Boulder','Colorado Springs','Tempe',
    'Phoenix','Tucson','Albuquerque','Salt Lake City','Las Vegas','Los Angeles',
    'San Francisco','San Jose','Oakland','Sacramento','Santa Barbara','Santa Cruz',
    'Eugene','Portland','Seattle','Anchorage','Atlanta','Nashville','Memphis',
    'New Orleans','Houston','Dallas','Austin','San Antonio','Miami','Tampa',
    'Orlando','Jacksonville','Charlotte','Raleigh','Washington','Baltimore',
    'Boston','Providence','Hartford','Springfield','Hampton','Norfolk','Richmond',
    'Landover','Foxborough','East Troy','Columbia','Chapel Hill','Greensboro',
    'St. Louis','Indianapolis','Louisville','Lexington','Birmingham','Tuscaloosa',
    'Baton Rouge','Little Rock','Tulsa','Oklahoma City','Wichita','Des Moines',
    'Madison','Green Bay','Duluth','Fargo','Missoula','Bozeman','Spokane',
    'Long Beach','Inglewood','Irvine','San Diego','Fresno','Bakersfield',
    'Reno','Concord','Laguna Hills','East Lansing','Kalamazoo','Dayton',
    'Akron','Erie','Hershey','Saratoga','Monterey','Ventura','Devore',
    'Mountain View','Shoreline','Berkeley','Morrison','Englewood','Foxboro',
    'Williamsburg','Bristow','Noblesville','Tinley Park','Bonner Springs',
    'Maryland Heights','Antioch','Pelham','Holmdel','Mansfield','Deer Creek',
    'Pine Knob','Canandaigua','Burgettstown','Meadows','Camden','Gorge',
}

_ARTIST_LINE_PAT = re.compile(
    r'(?:Grateful Dead|Bob Weir(?:\s*&\s*Ratdog)?|Ratdog|'
    r'Jerry Garcia(?:\s+Band)?|Phil Lesh(?:\s+and\s+Friends)?)',
    re.IGNORECASE
)


def _split_venue_city(line):
    """
    Split a string like:
      "Community War Memorial Auditorium Rochester, NY 4/9/1982 - Friday"
    into (venue, city).
    1. Strip trailing date / weekday.
    2. Find rightmost ", STATE" anchor.
    3. Walk left using _GD_CITIES lookup; fall back to last 1-2 words.
    """
    line = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', line).strip()
    line = re.sub(r'\s*-\s*\w+day\s*$', '', line).strip()

    # Find rightmost valid state abbreviation
    state_m = None
    for m in re.finditer(r',\s+([A-Z]{2})\b', line):
        if m.group(1) in _US_STATES:
            state_m = m
    if not state_m:
        return None, None

    state        = state_m.group(1)
    before_comma = line[:state_m.start()].strip()

    # Try known GD city lookup (longest match first)
    for city in sorted(_GD_CITIES, key=len, reverse=True):
        if before_comma.endswith(city):
            venue = before_comma[:-len(city)].strip().rstrip(' ,-')
            return (venue or None), f'{city}, {state}'

    # Fallback: last 2, then 1 word(s) as city name
    words = before_comma.split()
    for n in (2, 1):
        city_try  = ' '.join(words[-n:])
        venue_try = ' '.join(words[:-n]).strip().rstrip(' ,-')
        if venue_try and len(venue_try) > 3:
            return venue_try, f'{city_try}, {state}'

    return None, f'{before_comma}, {state}'


def parse_venue_city(content):
    """
    Handles both multi-line and single-line venue blocks.

    Multi-line (older posts, full structure preserved):
        Grateful Dead
        Nassau Veterans Memorial Coliseum
        Uniondale, NY 3/12/1992 - Thursday

    Single-line (newer posts / after Blogger's whitespace collapse):
        Grateful Dead Community War Memorial Auditorium Rochester, NY 4/9/1982 - Friday

    Returns (venue, city) or (None, None).
    """
    # Case 1: multi-line block
    m = re.search(
        r'(?:Grateful Dead|Bob Weir[^\n]*?|Jerry Garcia[^\n]*?|Phil Lesh[^\n]*?|Ratdog)'
        r'\s*\n([^\n]{3,})\n([^\n]+)',
        content, re.IGNORECASE
    )
    if m:
        venue_cand = m.group(1).strip()
        city_line  = m.group(2).strip()
        sm = re.search(r',\s+([A-Z]{2})\b', city_line)
        if sm and sm.group(1) in _US_STATES:
            city_clean = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', city_line).strip()
            city_clean = re.sub(r'\s*-\s*\w+day\s*$', '', city_clean).strip()
            city_clean = re.sub(r'\s*\|.*$', '', city_clean).strip()
            return venue_cand, city_clean

    # Case 2: single-line — artist + venue + city all on one line
    for am in _ARTIST_LINE_PAT.finditer(content):
        line_end  = content.find('\n', am.end())
        if line_end == -1:
            line_end = len(content)
        remainder = content[am.end():line_end].strip()
        if len(remainder) < 5:
            continue
        v, c = _split_venue_city(remainder)
        if v and c and not re.match(r'^(this week|i |we |the show)', v, re.I):
            return v, c

    return None, None


# --- Setlist / audio ---------------------------------------------------------

def parse_setlist(content):
    """
    Extract songs from the structured block between the date line and the Libsyn URL.
    Returns (setlist_text, audio_url).
    """
    block_match = re.search(
        r'\d{1,2}/\d{1,2}/\d{4}[^\n]*\n(.*?)(https?://traffic\.libsyn\.com/\S+)',
        content, re.DOTALL
    )
    if not block_match:
        audio_match = re.search(r'(https?://traffic\.libsyn\.com/\S+)', content)
        audio_url   = audio_match.group(1).strip() if audio_match else None
        return '', audio_url

    raw_block = block_match.group(1)
    audio_url = block_match.group(2).strip()

    set_labels = re.compile(
        r'^\s*(Set\s*\d+|One|Two|Three|Four|Encore\s*\d*|E\d?):?\s*$',
        re.IGNORECASE
    )

    songs = []
    for line in raw_block.splitlines():
        line = line.strip()
        if not line:
            continue
        if set_labels.match(line):
            continue
        if re.search(r'\d{1,2}/\d{1,2}/\d{4}', line):
            continue
        line = re.sub(r'\[\d+[:\d#]*\]', '', line)
        line = re.sub(r'\(\d+:\d+\)', '', line)
        line = re.sub(r'\s*\(\d+\)\s*$', '', line)
        line = re.sub(r'\s*\+\s*$', '', line)
        line = line.strip()
        if len(line) < 2:
            continue
        if re.match(r'^[*+#\-]+\s', line):
            continue
        songs.append(line)

    return ' ; '.join(songs), audio_url


def parse_notes(content):
    """Return the narrative intro (text before the venue block), up to 1000 chars."""
    m = re.search(r'^(.*?)(?:Grateful Dead|Bob Weir|Jerry Garcia|Phil Lesh)',
                  content, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()[:1000]
    return ''


def scrape_post(url):
    """Fetch and parse one blog post. Returns a dict or None on failure."""
    try:
        html = fetch(url)
    except URLError as e:
        print(f"    ERROR: {e}")
        return None

    # Strip HTML tags and normalise whitespace
    text = re.sub(r'<[^>]+>', ' ', html)
    for entity, char in [('&gt;', '>'), ('&lt;', '<'), ('&#39;', "'"),
                          ('&amp;', '&'), ('&nbsp;', ' ')]:
        text = text.replace(entity, char)
    text = re.sub(r'\s{2,}', '\n', text)

    # Title
    title_m = re.search(r'<title>([^<]+)</title>', html)
    title   = title_m.group(1).strip() if title_m else ''
    title   = re.sub(r'\s*[-|].*$', '', title).strip()
    title   = re.sub(r'^The Deadpod:\s*', '', title).strip()

    podcast_date            = parse_post_date(title)
    show_date               = parse_show_date(text)
    venue, city             = parse_venue_city(text)
    setlist_text, audio_url = parse_setlist(text)
    notes                   = parse_notes(text)

    artist = 'Grateful Dead'
    if re.search(r'Bob Weir.*Ratdog|Ratdog', text, re.IGNORECASE):
        artist = 'Bob Weir & Ratdog'
    elif re.search(r'Jerry Garcia Band|JGB', text, re.IGNORECASE):
        artist = 'Jerry Garcia Band'
    elif re.search(r'Phil Lesh and Friends', text, re.IGNORECASE):
        artist = 'Phil Lesh and Friends'

    return {
        'post_url':     url,
        'podcast_date': podcast_date,
        'show_date':    show_date,
        'artist':       artist,
        'venue':        venue,
        'city':         city,
        'audio_url':    audio_url,
        'setlist_text': setlist_text,
        'notes':        notes,
        'title':        title,
        'scraped_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


# --- Main --------------------------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    cur.execute('SELECT post_url FROM shows')
    existing = {row[0] for row in cur.fetchall()}

    post_urls = get_recent_post_urls(MONTHS_BACK)
    if not post_urls:
        print("No posts found.")
        conn.close()
        return

    print(f"\nFound {len(post_urls)} post(s) in the last {MONTHS_BACK} months.\n")

    inserted = 0
    for url in post_urls:
        if url in existing:
            print(f"  Already in DB: {url}")
            continue

        print(f"  Scraping: {url}")
        data = scrape_post(url)
        if not data:
            continue

        cur.execute("""
            INSERT OR IGNORE INTO shows
                (post_url, podcast_date, show_date, artist, venue, city,
                 audio_url, setlist_text, notes, title, scraped_at)
            VALUES
                (:post_url, :podcast_date, :show_date, :artist, :venue, :city,
                 :audio_url, :setlist_text, :notes, :title, :scraped_at)
        """, data)

        if cur.rowcount:
            songs_preview = data['setlist_text'][:60] if data['setlist_text'] else '(none)'
            print(f"    + Inserted: {data['title']}")
            print(f"      show_date={data['show_date']} | venue={data['venue']} | city={data['city']}")
            print(f"      songs={songs_preview}...")
            inserted += 1
        else:
            print(f"    Skipped (duplicate): {url}")

    conn.commit()
    conn.close()
    print(f"\nDone. {inserted} new show(s) added.")


if __name__ == '__main__':
    main()
