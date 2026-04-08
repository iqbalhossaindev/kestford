# Channel Management Notes

This build adds the scripted verification flow.

## Server playlist files

The server now uses these persistent playlist files:

- `Playlist/AI.m3u`
- `Playlist/Human.m3u`
- `Playlist/Review.m3u`
- `Playlist/Failed.m3u`

## What the backend does

- The AI bot checks main channels automatically.
- Working AI checked channels are saved into `AI.m3u`.
- Human playback success saves channels into `Human.m3u`.
- Human playback failure moves channels into `Failed.m3u`.
- If a failed human channel was already AI verified, it is moved from `AI.m3u` to `Review.m3u`.
- The second AI pass rechecks `Failed.m3u`.
- After 10 AI failures, a channel is permanently removed from all playlist files and server state.

## Main files changed

- `server.js`
- `app.js`
- `index.html`
- `style.css`
- `assets/icons/playlist-review.svg`

## Runtime state files

- `data/channel_state.json`
- `data/bot_status.json`
- `data/removals.json`
