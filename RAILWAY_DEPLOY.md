# Railway deploy notes

1. Put the extracted contents of this folder at the root of your GitHub repository.
2. Deploy the repository to Railway.
3. Add two volumes:
   - `/app/Playlist`
   - `/app/data`
4. Add these basic variables:
   - `PORT=10000`
   - `BOT_INTERVAL_MINUTES=30`
   - `BOT_START_DELAY_MS=15000`
   - `BOT_REQUEST_TIMEOUT_MS=12000`
5. Generate a public domain for the service.
6. If Railway asks for a target port, use `10000`.

## Optional GitHub auto sync

To automatically mirror the live server playlist files back into your GitHub repo, add these Railway variables too:

- `GITHUB_SYNC_ENABLED=true`
- `GITHUB_TOKEN=your_github_personal_access_token`
- `GITHUB_OWNER=your_github_username_or_org`
- `GITHUB_REPO=your_repo_name`
- `GITHUB_BRANCH=main`
- `GITHUB_PATH_PREFIX=` leave empty unless the app is inside a subfolder in the repo

The app will auto sync these live files back to GitHub:

- `Playlist/AI.m3u`
- `Playlist/Human.m3u`
- `Playlist/Review.m3u`
- `Playlist/Failed.m3u`
- `Playlist/playlists.json`

Useful endpoints:

- `/api/github-sync-status`
- `POST /api/run-github-sync`

This package seeds an empty mounted `/app/Playlist` volume from `/app/defaults/Playlist` on first boot.
