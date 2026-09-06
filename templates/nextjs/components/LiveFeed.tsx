'use client';

// Live updates via the HTTP bridge SSE stream (browsers can't do TCP).
// Polls would also work (pollChanges); SSE is lower-latency and shows
// the /v1/stream contract: ?table=&since=, data frames, abort to stop.
import { useEffect, useState } from 'react';
import { browserDb } from '../lib/http';

interface FeedItem {
  table: string;
  op: string;
  rowId: number | bigint;
}

export default function LiveFeed() {
  const [items, setItems] = useState<FeedItem[]>([]);
  const [live, setLive] = useState(false);

  useEffect(() => {
    const ac = new AbortController();
    let stopped = false;
    (async () => {
      try {
        await browserDb().stream(
          'posts',
          (rec) => {
            if (stopped) return;
            setLive(true);
            setItems((prev) => [{ table: rec.table, op: rec.op, rowId: rec.rowId }, ...prev].slice(0, 20));
          },
          { signal: ac.signal },
        );
      } catch (e) {
        if (e instanceof Error && e.name !== 'AbortError') setLive(false);
      }
    })();
    return () => {
      stopped = true;
      ac.abort();
    };
  }, []);

  return (
    <div>
      <p>{live ? '● live' : '○ connecting…'}</p>
      <ul>
        {items.map((it, i) => (
          <li key={`${String(it.rowId)}-${i}`}>
            {it.op} on {it.table} #{String(it.rowId)}
          </li>
        ))}
      </ul>
    </div>
  );
}
