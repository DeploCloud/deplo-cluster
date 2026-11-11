# AGENTS.md

The **deplo cluster binary**: one static Go binary that runs as a **cluster leader** or, with
`relay`, as a **cluster relay**. The leader places workloads, holds leases and epochs, stores sealed
failover bundles and orders failovers; the relay carries member traffic over TCP. Module
`github.com/DeploCloud/deplo-cluster`, AGPL-3.0-only.

**The architecture is documented in the control-plane repo, not here.** Read `../deplo/AGENTS.md`
for the rules, `../deplo/CONTEXT.md` for the vocabulary (use those words) and
`../deplo/docs/adr/0043-a-cluster-is-led-by-its-own-leader-not-kubernetes.md`,
`0044-cluster-members-meet-through-a-relay-over-tcp.md` and
`0045-a-cluster-leader-holds-sealed-failover-bundles.md` for why this binary exists.

- **It never builds compose.** It re-sends the last stack the control plane rendered for an App
  (ADR-0043). Never port routing, compose or gateway logic here.
- **The control plane never dials it.** The server agent that supervises it forwards calls; the
  panel pins this binary as `deplo.cluster` / `deplo.clusterMin` in its `package.json`.
- **Releases mirror the agent's.** A `v*` tag runs `.github/workflows/release.yml`, which gates on
  `go test ./...`, builds static `linux/amd64` + `linux/arm64`, and publishes
  `deplo-cluster-linux-<arch>`, `checksums.txt` and a provenance attestation. The asset names are a
  contract with the control plane. Never tag a release on your own initiative.

## Layout

- `main.go` - mode (`relay` or none), `--version`, signal handling.
- `internal/cluster/` - role, version (stamped by the build) and epoch.
- `internal/wire/` - the relay's names, stream headers and mutual TLS rules (a contract with `deplo-agent`).
- `internal/relay/` - the relay: the door, the roster pushed on its local socket, per-address rate limit.
- `internal/lease/` - the 5 s / 15 s / 60 s timings and the order each renewal carries (a contract with `deplo-agent`).
- `internal/events/` - the sequenced buffer a control plane that was away replays from.
- `internal/leader/` - the leader: renews leases, tracks member state, serves `/call/*` and `/watch` on its local socket.
- `internal/party/` - a party's side (leader or member): dials out to every relay, never listens.

`internal/party/party.go`, `internal/wire/` and `internal/lease/` each have a **hand-kept copy** in `deplo-agent`
(`internal/clusterlink/`). Change both sides in one go, new fields with `omitempty`; the agent's
`internal/clusterlink/copies_test.go` fails when they drift.

## Commands

`make build` · `make test` · `make vet` · `make fmt`.

## Comments

Few and short. **Hard cap about 3 lines per block.** No file-header essays, no design narratives.
Go doc comments on exported identifiers keep their `// Identifier ...` opening. Pragmas and
`ponytail:` markers are code, never delete them. **Never name a competitor.**
