'use server';

// Server Actions: mutations go through the createPost PROCEDURE (one
// atomic transaction server-side), never raw inserts from the client.
import { cookies } from 'next/headers';
import { redirect } from 'next/navigation';
import { withClient } from '../lib/blitz';

export async function login(formData: FormData) {
  const owner = String(formData.get('owner') ?? '').trim().slice(0, 64);
  if (!owner) return;
  // Dev login: the owner NAME is the identity (open dev server — see
  // README "Production checklist" for bearer sessions + row_owner).
  (await cookies()).set('blitz_owner', owner, { httpOnly: true, path: '/' });
  redirect('/');
}

export async function logout() {
  (await cookies()).delete('blitz_owner');
  redirect('/');
}

export async function createPost(formData: FormData) {
  const jar = await cookies();
  const owner = jar.get('blitz_owner')?.value;
  if (!owner) throw new Error('not logged in');
  const body = String(formData.get('body') ?? '').trim().slice(0, 280);
  if (!body) return;
  await withClient(async (db) => {
    await db.call('createPost', { owner, body });
  });
}
