// Package bundles keeps the leader's age key and the sealed failover bundles the control plane sends (ADR-0045).
package bundles

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"filippo.io/age"
)

// Meta is what the leader needs to place a workload without the control plane; it holds no secret.
type Meta struct {
	Name        string   `json:"name"`
	Environment string   `json:"environment"`
	Image       string   `json:"image"`
	Archs       []string `json:"archs"`
	LimitMB     int      `json:"limitMb"`
	PeakMB      int      `json:"peakMb"`
	HasDomain   bool     `json:"hasDomain"`
	CreatedAt   string   `json:"createdAt"`
}

// RegistryAuth is a registry login a pulled image needs again.
type RegistryAuth struct {
	Host     string `json:"host"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// Stack is what a member's deploy received, a contract with the control plane's FailoverStack.
type Stack struct {
	Slug          string            `json:"slug"`
	ProjectID     string            `json:"projectId"`
	ImageRef      string            `json:"imageRef"`
	ComposeYAML   string            `json:"composeYaml"`
	Network       string            `json:"network"`
	Env           map[string]string `json:"env"`
	RegistryAuth  []RegistryAuth    `json:"registryAuth"`
	ComposeUpArgs []string          `json:"composeUpArgs"`
	TunnelToken   string            `json:"tunnelToken,omitempty"`
	// Ready and RestartRule are the deploy's own messages as JSON; the leader passes them on unread.
	Ready            json.RawMessage `json:"ready,omitempty"`
	RestartRule      json.RawMessage `json:"restartRule,omitempty"`
	StartAfterReady  []string        `json:"startAfterReady,omitempty"`
	Mounts           []MountFile     `json:"mounts,omitempty"`
	FilesBindSources []string        `json:"filesBindSources,omitempty"`
}

// MountFile is a file the deploy writes beside the stack.
type MountFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// Bundle is the opened plaintext. It lives in memory only as long as one restart needs it.
type Bundle struct {
	V            int    `json:"v"`
	Kind         string `json:"kind"`
	Workload     string `json:"workload"`
	DeploymentID string `json:"deploymentId"`
	Stack        Stack  `json:"stack"`
}

// Held is one stored bundle as the list shows it.
type Held struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	DeploymentID string `json:"deploymentId"`
}

// ErrNone is a workload the leader holds no bundle for.
var ErrNone = errors.New("the leader holds no bundle for this workload")

var name = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)

type index struct {
	DeploymentID string `json:"deploymentId"`
	Meta         Meta   `json:"meta"`
}

// Store keeps the identity and the sealed bundles under one directory, owner-only.
type Store struct {
	dir string
	id  *age.X25519Identity
	mu  sync.Mutex
}

// Open reads the age identity from dir, making it on the first run. Only the public half is ever shown.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	file := filepath.Join(dir, "leader.key")
	raw, err := os.ReadFile(file)
	var id *age.X25519Identity
	switch {
	case err == nil:
		id, err = age.ParseX25519Identity(string(bytes.TrimSpace(raw)))
		if err != nil {
			return nil, fmt.Errorf("the leader's key is unreadable: %w", err)
		}
	case errors.Is(err, os.ErrNotExist):
		id, err = age.GenerateX25519Identity()
		if err != nil {
			return nil, err
		}
		if err := write(file, []byte(id.String()+"\n")); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return &Store{dir: dir, id: id}, nil
}

// PublicKey is the age recipient the control plane seals to.
func (s *Store) PublicKey() string { return s.id.Recipient().String() }

func (s *Store) path(cluster, kind, id, ext string) (string, error) {
	for _, v := range []string{cluster, kind, id} {
		if !name.MatchString(v) {
			return "", fmt.Errorf("%q is not a usable name", v)
		}
	}
	return filepath.Join(s.dir, "bundles", cluster, kind+"."+id+ext), nil
}

func (s *Store) open(sealed []byte) (Bundle, error) {
	r, err := age.Decrypt(bytes.NewReader(sealed), s.id)
	if err != nil {
		return Bundle{}, fmt.Errorf("the bundle was not sealed to this leader's key: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(r, 32<<20))
	if err != nil {
		return Bundle{}, err
	}
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return Bundle{}, fmt.Errorf("the bundle is unreadable: %w", err)
	}
	return b, nil
}

func write(file string, raw []byte) error {
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return err
	}
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, file)
}

// Put stores a sealed bundle in place of the workload's earlier one. A blob this leader cannot open is refused,
// so the control plane learns its key is out of date instead of trusting a bundle that could never be used.
func (s *Store) Put(cluster, kind, id, deploymentID string, sealed []byte, meta Meta) error {
	file, err := s.path(cluster, kind, id, ".age")
	if err != nil {
		return err
	}
	b, err := s.open(sealed)
	if err != nil {
		return err
	}
	if b.Kind != kind || b.Workload != id || b.DeploymentID != deploymentID {
		return errors.New("the bundle names another workload or deployment than the call")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, _ := s.path(cluster, kind, id, ".json")
	raw, _ := json.Marshal(index{DeploymentID: deploymentID, Meta: meta})
	if err := write(file, sealed); err != nil {
		return err
	}
	return write(idx, raw)
}

// PutBase64 is Put for the call's base64 payload.
func (s *Store) PutBase64(cluster, kind, id, deploymentID, sealed string, meta Meta) error {
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return errors.New("the sealed bundle is not base64")
	}
	return s.Put(cluster, kind, id, deploymentID, raw, meta)
}

// Drop forgets the workload's bundle; nothing held is not an error.
func (s *Store) Drop(cluster, kind, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ext := range []string{".age", ".json"} {
		file, err := s.path(cluster, kind, id, ext)
		if err != nil {
			return err
		}
		if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// List is every bundle held for the cluster, in a stable order.
func (s *Store) List(cluster string) ([]Held, error) {
	if !name.MatchString(cluster) {
		return nil, fmt.Errorf("%q is not a usable name", cluster)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	files, _ := filepath.Glob(filepath.Join(s.dir, "bundles", cluster, "*.json"))
	out := []Held{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var ix index
		if json.Unmarshal(raw, &ix) != nil {
			continue
		}
		base := strings.TrimSuffix(filepath.Base(f), ".json")
		if kind, id, ok := strings.Cut(base, "."); ok {
			out = append(out, Held{Kind: kind, ID: id, DeploymentID: ix.DeploymentID})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind+"/"+out[i].ID < out[j].Kind+"/"+out[j].ID })
	return out, nil
}

// Meta is what was sent with the bundle.
func (s *Store) Meta(cluster, kind, id string) (Meta, error) {
	file, err := s.path(cluster, kind, id, ".json")
	if err != nil {
		return Meta{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(file)
	if err != nil {
		return Meta{}, ErrNone
	}
	var ix index
	if err := json.Unmarshal(raw, &ix); err != nil {
		return Meta{}, ErrNone
	}
	return ix.Meta, nil
}

// Unseal opens the workload's bundle in memory, to send one restart.
func (s *Store) Unseal(cluster, kind, id string) (Bundle, error) {
	file, err := s.path(cluster, kind, id, ".age")
	if err != nil {
		return Bundle{}, err
	}
	s.mu.Lock()
	raw, err := os.ReadFile(file)
	s.mu.Unlock()
	if err != nil {
		return Bundle{}, ErrNone
	}
	return s.open(raw)
}
