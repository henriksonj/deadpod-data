#!/bin/bash
# update-jukebox.sh
# Scrape recent Deadpod posts, export shows.json, copy it to deadpod-data,
# then pull/rebase, commit, and push safely.
# Exits non-zero on failure and logs every step.

set -euo pipefail

LOG="$HOME/python/update-jukebox.log"
REPO="$HOME/python/deadpod-data"
SRC_JSON="$HOME/python/shows.json"
DEST_JSON="$REPO/shows.json"

log() {
    local msg="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" | tee -a "$LOG"
}

fail() {
    local msg="$1"
    log "FAIL: $msg"
    exit 1
}

log "=== update-jukebox.sh started ==="

log "Scraping recent posts..."
python3 "$HOME/python/scrape_recent.py" >> "$LOG" 2>&1 || fail "scrape_recent.py failed"

log "Exporting shows from deadpod.db..."
python3 "$HOME/python/export_shows.py" >> "$LOG" 2>&1 || fail "export_shows.py failed"

if [[ ! -f "$SRC_JSON" ]]; then
    fail "shows.json was not created at $SRC_JSON"
fi

if [[ ! -d "$REPO/.git" ]]; then
    fail "git repo not found at $REPO"
fi

cd "$REPO"

log "Syncing local repo with origin/main (git pull --rebase)..."
if ! git pull --rebase origin main >> "$LOG" 2>&1; then
    log "git pull --rebase failed; aborting any in-progress rebase"
    git rebase --abort >> "$LOG" 2>&1 || true
    fail "git pull --rebase origin main failed; repo needs attention"
fi

log "Copying shows.json to deadpod-data..."
cp "$SRC_JSON" "$DEST_JSON" || fail "copy to $DEST_JSON failed"

log "Checking for changes..."
git add shows.json

if git diff --cached --quiet; then
    log "No changes detected in shows.json after export; nothing to commit"
    log "=== update-jukebox.sh completed successfully ==="
    exit 0
fi

COMMIT_MSG="update shows $(date +%Y-%m-%d)"
log "Committing changes: $COMMIT_MSG"
git commit -m "$COMMIT_MSG" >> "$LOG" 2>&1 || fail "git commit failed"

log "Pushing to GitHub..."
git push origin main >> "$LOG" 2>&1 || fail "git push origin main failed"

log "Done! Jukebox updated."
log "=== update-jukebox.sh completed successfully ==="
