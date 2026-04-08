'use strict';

const PORT = Number(process.env.PORT || 10000);
const SERVER_URL = process.env.SERVER_URL || `http://127.0.0.1:${PORT}`;

async function run() {
  const res = await fetch(`${SERVER_URL}/api/run-bot`, { method: 'POST' });
  const data = await res.json();
  if (!res.ok) {
    console.error('Bot run failed:', data);
    process.exit(1);
  }
  console.log(JSON.stringify(data, null, 2));
}

run().catch(error => {
  console.error(error);
  process.exit(1);
});
