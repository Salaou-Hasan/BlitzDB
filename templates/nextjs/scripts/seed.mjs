// Seed a dev server over the HTTP bridge (no SDK install needed):
// creates the posts table, deploys the createPost procedure, writes hello.
//
//   node scripts/seed.mjs [http-base]
// e.g. node scripts/seed.mjs http://127.0.0.1:7421
const base = (process.argv[2] ?? 'http://127.0.0.1:7421').replace(/\/$/, '');

async function op(body) {
  const res = await fetch(`${base}/v1/op`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok || json.ok !== true) {
    throw new Error(`op failed (status ${res.status}): ${JSON.stringify(json)}`);
  }
  return json;
}

await op({
  id: 1, op: 'table_create',
  values: {
    schema: {
      table: 'posts',
      columns: [
        { name: 'id', type: 'int64', nullable: true },
        { name: 'owner', type: 'string', nullable: true },
        { name: 'body', type: 'string', nullable: true },
      ],
    },
  },
});
console.log('table posts ready');

await op({
  id: 2, op: 'proc_deploy', table: 'fn:createPost',
  values: {
    v: 1,
    procedure: {
      name: 'createPost',
      description: 'Validated micro-post insert (empty bodies rejected).',
      steps: [
        {
          If: {
            condition: { IsNotNull: 'body' },
            then_steps: [
              {
                Insert: {
                  table: 'posts',
                  values: { owner: '$owner', body: '$body' },
                  into: 'post_id',
                },
              },
              { Return: { value: '$post_id' } },
            ],
            else_steps: [{ Fail: { message: 'empty post' } }],
          },
        },
      ],
    },
  },
});
console.log('procedure createPost deployed');

const hello = await op({
  id: 3, op: 'call', table: 'fn:createPost',
  values: { owner: 'ada', body: 'hello blitz' },
});
console.log('hello post applied:', JSON.stringify(hello.rows[0].values._applied));
