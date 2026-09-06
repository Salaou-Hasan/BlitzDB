// Home: server-rendered post list (RSC reads via TCP SDK) + forms.
// BlitzDB primitives exercised: Scan (cursor pages), Call (procedure).
import { cookies } from 'next/headers';
import { withClient, currentOwner } from '../lib/blitz';
import NewPostForm from '../components/NewPostForm';
import LiveFeed from '../components/LiveFeed';
import LoginForm from '../components/LoginForm';

async function recentPosts() {
  return withClient(async (db) => db.scan('posts', 20, null, true));
}

export default async function Home() {
  const owner = await currentOwner(await cookies());
  const posts = await recentPosts();
  return (
    <>
      {!owner ? (
        <LoginForm />
      ) : (
        <>
          <p>
            Posting as <strong>{owner}</strong> <LoginForm compact />
          </p>
          <NewPostForm />
        </>
      )}
      <h2>Latest</h2>
      <ul>
        {posts.map((p) => (
          <li key={String(p.id)}>
            <strong>{String(p.values['owner'] ?? '?')}</strong>: {String(p.values['body'] ?? '')}
          </li>
        ))}
      </ul>
      <h2>Live</h2>
      <LiveFeed />
    </>
  );
}
