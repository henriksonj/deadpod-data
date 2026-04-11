#!/usr/bin/env python3
"""
fix_db.py
Re-scrapes every post URL already in deadpod.db and UPDATEs all fields
using the corrected parsing logic from scrape_recent.py.

Fixes:
  - podcast_date = None  (all 1016 rows)
  - venue bleeding into city  (126 rows)
  - narrative notes text in venue field  (18 rows)
  - empty venue  (350 rows)
  - empty city   (264 rows)

Usage:
    python3 ~/python/fix_db.py [--dry-run] [--limit N] [--url URL]

Options:
    --dry-run    Show what would change but don't write to DB
    --limit N    Only process first N rows (useful for spot-checking)
    --url URL    Fix a single specific post URL

The script is safe to re-run: it always does UPDATE (never INSERT),
and skips rows where the live fetch fails (404, timeout, etc.).
"""

import sqlite3
import re
import os
import sys
import time
import argparse
from datetime import datetime, timezone, date
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

DB_PATH   = os.path.expanduser('~/python/deadpod.db')
DELAY_SEC = 1.0          # polite crawl delay between requests

# ---------------------------------------------------------------------------
# Parsing helpers — exact copies from scrape_recent.py (patched version)
# ---------------------------------------------------------------------------

_US_STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
    'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
    'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
    'TX','UT','VT','VA','WA','WV','WI','WY','DC'
}

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


def fetch(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0 (Deadpod-Scraper/1.0)'})
    with urlopen(req, timeout=20) as r:
        return r.read().decode('utf-8', errors='replace')


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


def _split_venue_city(line):
    """
    Split a string like:
      "Community War Memorial Auditorium Rochester, NY 4/9/1982 - Friday"
    into (venue, city).
    """
    line = re.sub(r'\s+\d{1,2}/\d{1,2}/\d{2,4}.*$', '', line).strip()
    line = re.sub(r'\s*-\s*\w+day\s*$', '', line).strip()

    state_m = None
    for m in re.finditer(r',\s+([A-Z]{2})\b', line):
        if m.group(1) in _US_STATES:
            state_m = m
    if not state_m:
        return None, None

    state        = state_m.group(1)
    before_comma = line[:state_m.start()].strip()

    for city in sorted(_GD_CITIES, key=len, reverse=True):
        if before_comma.endswith(city):
            venue = before_comma[:-len(city)].strip().rstrip(' ,-')
            return (venue or None), f'{city}, {state}'

    words = before_comma.split()
    for n in (2, 1):
        city_try  = ' '.join(words[-n:])
        venue_try = ' '.join(words[:-n]).strip().rstrip(' ,-')
        if venue_try and len(venue_try) > 3:
            return venue_try, f'{city_try}, {state}'

    return None, f'{before_comma}, {state}'


def parse_venue_city(content):
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

    # Case 2: single-line
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


def parse_setlist(content):
    block_match = re.search(
        r'\d{1,2}/\d{1,2}/\d{4}[^\n]*\n(.*?)(https?://traffic\.libsyn\.com/\S+)',
        content, re.DOTALL
    )
    if not block_match:
        # Also try media.libsyn.com and www.libsyn.com
        audio_match = re.search(
            r'(https?://(?:traffic|media|www)\.libsyn\.com/\S+)', content
        )
        audio_url = audio_match.group(1).strip() if audio_match else None
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
    m = re.search(r'^(.*?)(?:Grateful Dead|Bob Weir|Jerry Garcia|Phil Lesh)',
                  content, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()[:1000]
    return ''


def scrape_post(url):
    """Fetch and parse one blog post. Returns a dict or None on failure."""
    try:
        html = fetch(url)
    except HTTPError as e:
        if e.code == 404:
            print(f'    SKIP 404: {url}')
        else:
            print(f'    HTTP {e.code}: {url}')
        return None
    except URLError as e:
        print(f'    ERROR fetching {url}: {e}')
        return None

    text = re.sub(r'<[^>]+>', ' ', html)
    for entity, char in [('&gt;', '>'), ('&lt;', '<'), ('&#39;', "'"),
                          ('&amp;', '&'), ('&nbsp;', ' ')]:
        text = text.replace(entity, char)
    text = re.sub(r'\s{2,}', '\n', text)

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
        'venue':        venue or '',
        'city':         city  or '',
        'audio_url':    audio_url or '',
        'setlist_text': setlist_text or '',
        'notes':        notes or '',
        'title':        title,
        'scraped_at':   datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Re-scrape all DB rows and fix fields.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change but do not write to DB')
    parser.add_argument('--limit', type=int, default=0,
                        help='Only process first N rows (0 = all)')
    parser.add_argument('--url', type=str, default='',
                        help='Fix a single specific post URL')
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()

    if args.url:
        cur.execute('SELECT * FROM shows WHERE post_url = ?', (args.url,))
    else:
        cur.execute('SELECT * FROM shows ORDER BY podcast_date, post_url')

    rows = cur.fetchall()
    if args.limit and not args.url:
        rows = rows[:args.limit]

    total    = len(rows)
    updated  = 0
    skipped  = 0
    errors   = 0

    print(f'fix_db.py  |  {"DRY RUN — " if args.dry_run else ""}processing {total} row(s)')
    print('-' * 72)

    for i, row in enumerate(rows, 1):
        url = row['post_url']
        print(f'[{i:4d}/{total}] {url}')

        data = scrape_post(url)
        if data is None:
            errors += 1
            continue

        # Build a human-readable diff for logging
        changed_fields = []
        for field in ('podcast_date','show_date','artist','venue','city',
                      'audio_url','setlist_text','notes','title'):
            old_val = row[field] or ''
            new_val = data[field] or ''
            if old_val != new_val:
                changed_fields.append(f'  {field}: {repr(old_val)!s:40s} → {repr(new_val)}')

        if not changed_fields:
            print('         no changes')
            skipped += 1
            continue

        for line in changed_fields:
            print(line)

        if not args.dry_run:
            cur.execute("""
                UPDATE shows SET
                    podcast_date = :podcast_date,
                    show_date    = :show_date,
                    artist       = :artist,
                    venue        = :venue,
                    city         = :city,
                    audio_url    = :audio_url,
                    setlist_text = :setlist_text,
                    notes        = :notes,
                    title        = :title,
                    scraped_at   = :scraped_at
                WHERE post_url   = :post_url
            """, data)
            conn.commit()

        updated += 1
        time.sleep(DELAY_SEC)

    conn.close()

    print()
    print('=' * 72)
    print(f'Done.  {"(DRY RUN) " if args.dry_run else ""}updated={updated}  unchanged={skipped}  errors={errors}  total={total}')
    if args.dry_run:
        print('Run without --dry-run to apply these changes.')


if __name__ == '__main__':
    main()
