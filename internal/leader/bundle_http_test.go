package leader_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"filippo.io/age"
)

func call(t *testing.T, url, method string, body any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(url+"/call/"+method, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != 200 {
		return map[string]any{"status": float64(resp.StatusCode)}
	}
	return out
}

func TestTheLocalSocketServesTheKeyAndTheBundleStore(t *testing.T) {
	f := two(t)
	srv := httptest.NewServer(f.l.Handler())
	defer srv.Close()
	key, _ := call(t, srv.URL, "leader.key", map[string]string{})["publicKey"].(string)
	if !strings.HasPrefix(key, "age1") || key != f.store.PublicKey() {
		t.Fatalf("key %q", key)
	}
	rec, _ := age.ParseX25519Recipient(key)
	var sealed bytes.Buffer
	w, _ := age.Encrypt(&sealed, rec)
	w.Write([]byte(`{"v":1,"kind":"app","workload":"prj_7","deploymentId":"dep_7","stack":{"slug":"x"}}`))
	w.Close()
	put := map[string]any{"cluster": "clu_1", "kind": "app", "id": "prj_7", "deploymentId": "dep_7",
		"sealed": base64.StdEncoding.EncodeToString(sealed.Bytes()), "meta": map[string]any{"name": "x"}}
	if r := call(t, srv.URL, "leader.bundle.put", put); r["ok"] != true {
		t.Fatalf("put %v", r)
	}
	list := call(t, srv.URL, "leader.bundle.list", map[string]string{"cluster": "clu_1"})["bundles"].([]any)
	found := false
	for _, b := range list {
		if b.(map[string]any)["id"] == "prj_7" && b.(map[string]any)["deploymentId"] == "dep_7" {
			found = true
		}
	}
	if !found {
		t.Fatalf("list %v", list)
	}
	put["sealed"] = base64.StdEncoding.EncodeToString([]byte("not age"))
	if r := call(t, srv.URL, "leader.bundle.put", put); r["status"] != float64(400) {
		t.Fatalf("a bundle that is not sealed to this key was taken: %v", r)
	}
	call(t, srv.URL, "leader.bundle.drop", map[string]string{"cluster": "clu_1", "kind": "app", "id": "prj_7"})
	for _, b := range call(t, srv.URL, "leader.bundle.list", map[string]string{"cluster": "clu_1"})["bundles"].([]any) {
		if b.(map[string]any)["id"] == "prj_7" {
			t.Fatal("dropped bundle still listed")
		}
	}
}
