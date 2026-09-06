'use client';

import { useActionState } from 'react';
import { createPost } from '../app/actions';

export default function NewPostForm() {
  const [, action, pending] = useActionState(createPost, null);
  return (
    <form action={action}>
      <input name="body" placeholder="say something (280 chars)" maxLength={280} required />
      <button type="submit" disabled={pending}>
        {pending ? 'posting…' : 'post'}
      </button>
    </form>
  );
}
