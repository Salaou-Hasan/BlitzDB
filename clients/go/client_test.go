package blitzdb

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var testAddr string

// TestMain spawns a real BlitzDB server binary (skips everything if the
// binary is missing: `cargo build -p blitz-cli`).
func TestMain(m *testing.M) {
	bin := os.Getenv("BLITZ_BIN")
	if bin == "" {
		// Repo layout: clients/go -> repo root -> target/release/Blitz.
		wd, err := os.Getwd()
		if err == nil {
			cand := filepath.Join(wd, "..", "..", "target", "release", "Blitz")
			if st, err := os.Stat(cand); err == nil && !st.IsDir() {
				bin = cand
			}
		}
	}
	if bin == "" {
		fmt.Println("SKIP: Blitz binary missing (cargo build -p blitz-cli)")
		os.Exit(0)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println("SKIP: no loopback:", err)
		os.Exit(0)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	proc := exec.Command(bin, "serve", "--port", fmt.Sprint(port))
	proc.Stdout = nil
	proc.Stderr = nil
	if err := proc.Start(); err != nil {
		fmt.Println("SKIP: cannot start server:", err)
		os.Exit(0)
	}
	defer proc.Process.Kill()
	testAddr = fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", testAddr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			fmt.Println("SKIP: server did not start")
			os.Exit(0)
		}
		time.Sleep(50 * time.Millisecond)
	}
	os.Exit(m.Run())
}

func dial(t *testing.T) *Client {
	t.Helper()
	host, portStr, err := net.SplitHostPort(testAddr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	c, err := Connect(host, port)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCreateTable(t *testing.T) {
	c := dial(t)
	name, err := c.CreateTable(map[string]any{
		"table": "go_t",
		"columns": []any{
			map[string]any{"name": "id", "type": "int64"},
			map[string]any{"name": "v", "type": "string", "nullable": true},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if name != "go_t" {
		t.Fatalf("name: %s", name)
	}
	if _, err := c.Insert("go_t", map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func TestCRUDRoundtrip(t *testing.T) {
	c := dial(t)
	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	row, err := c.Insert("users", map[string]any{"id": int64(700001), "name": "Ada", "email": "goada@x.com"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := c.Get("users", row.ID)
	if err != nil || got == nil {
		t.Fatalf("get: %v %+v", err, got)
	}
	if got.Values["name"] != "Ada" {
		t.Fatalf("name: %v", got.Values)
	}
	upd, err := c.Update("users", row.ID, map[string]any{"name": "Ada L."})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if upd.Values["name"] != "Ada L." {
		t.Fatalf("updated: %v", upd.Values)
	}
	found, err := c.Find("users", "email", "goada@x.com")
	if err != nil || found == nil {
		t.Fatalf("find: %v", err)
	}
	miss, err := c.Find("users", "email", "go-nope@x.com")
	if err != nil || miss != nil {
		t.Fatalf("find miss: %v %+v", err, miss)
	}
	rows, err := c.Scan("users", 1000, nil, false)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	seen := false
	for _, r := range rows {
		if r.ID == row.ID {
			seen = true
		}
	}
	if !seen {
		t.Fatal("inserted row missing from scan")
	}
	if err := c.Delete("users", row.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	gone, err := c.Get("users", row.ID)
	if err != nil || gone != nil {
		t.Fatalf("get after delete: %v %+v", err, gone)
	}
}

func TestConcurrentSharingBatches(t *testing.T) {
	c := dial(t)
	var mu sync.Mutex
	var ids []uint64
	var wg sync.WaitGroup
	var firstErr atomic_Error
	for th := 0; th < 30; th++ {
		wg.Add(1)
		go func(th int) {
			defer wg.Done()
			var local []uint64
			for i := 0; i < 10; i++ {
				row, err := c.Insert("users", map[string]any{
					"id":    int64(710000 + th*100 + i),
					"name":  fmt.Sprintf("gou%d-%d", th, i),
					"email": fmt.Sprintf("gou%d-%d@x.com", th, i),
				})
				if err != nil {
					firstErr.set(err)
					return
				}
				local = append(local, row.ID)
			}
			mu.Lock()
			ids = append(ids, local...)
			mu.Unlock()
		}(th)
	}
	wg.Wait()
	if err := firstErr.get(); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(ids) != 300 {
		t.Fatalf("ids: %d", len(ids))
	}
	seen := map[uint64]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("dup id %d", id)
		}
		seen[id] = true
	}
}

type atomic_Error struct {
	mu  sync.Mutex
	err error
}

func (a *atomic_Error) set(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		a.err = err
	}
}

func (a *atomic_Error) get() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

func TestErrorMapping(t *testing.T) {
	c := dial(t)
	got, err := c.Get("users", 424242)
	if err != nil || got != nil {
		t.Fatalf("get miss: %v %+v", err, got)
	}
	_, err = c.Update("users", 424242, map[string]any{"name": "x"})
	se, ok := err.(*SdkError)
	if !ok || se.Kind != ErrNotFound {
		t.Fatalf("update miss: %v", err)
	}
	_, err = c.Scan("nope", 10, nil, false)
	se, ok = err.(*SdkError)
	if !ok || se.Kind != ErrNotFound {
		t.Fatalf("scan unknown: %v", err)
	}
}

func TestCallUnknownProcedure(t *testing.T) {
	c := dial(t)
	_, err := c.Call("nope", map[string]any{})
	se, ok := err.(*SdkError)
	if !ok || (se.Kind != ErrNotFound && se.Kind != ErrServer) {
		t.Fatalf("call unknown: %v", err)
	}
}
