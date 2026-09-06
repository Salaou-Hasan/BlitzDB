'use client';

import { login, logout } from '../app/actions';

export default function LoginForm({ compact = false }: { compact?: boolean }) {
  if (compact) {
    return (
      <form action={logout} style={{ display: 'inline' }}>
        <button type="submit">log out</button>
      </form>
    );
  }
  return (
    <form action={login}>
      <input name="owner" placeholder="pick a username" maxLength={64} required />
      <button type="submit">log in</button>
    </form>
  );
}
