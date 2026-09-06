export const metadata = { title: 'Pulse — BlitzDB Next.js template' };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: 'system-ui', maxWidth: 640, margin: '2rem auto' }}>
        <header>
          <h1>Pulse</h1>
          <p>Microblog on BlitzDB (tables + functions, nothing else).</p>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
