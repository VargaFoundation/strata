# Strata developer Makefile. The build targets run the disk guard FIRST so a runaway target/
# (heavy native deps × many builds accumulate to 100s of GB) can never silently saturate the disk.
# Override the cap with `make CAP_GB=25 build`.

CAP_GB ?= 40
export CAP_GB
GUARD  := scripts/target-guard.sh

.PHONY: build test check fmt clippy run guard disk clean clean-all release bench bench-smoke cluster-up cluster-down cluster-failover

## Guarded common tasks (auto-clean target/ if it's over the cap, then run cargo).
build: guard ; cargo build --workspace
test:  guard ; cargo test --workspace
check: guard ; cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
release: guard ; cargo build --release --bin strata-server
run: guard ; cargo run --bin strata-server

fmt:    ; cargo fmt --all
clippy: guard ; cargo clippy --workspace --all-targets -- -D warnings

## LoCoMo benchmark (needs a logged-in `claude` CLI + Ollama :11434 — see ops/bench/README.md).
## `make bench-smoke` validates the whole pipeline in minutes (1 conversation, 5 QA); `make bench`
## runs the full overnight eval. Results are teed under /tmp/strata-bench/.
bench-smoke: guard ; CONVS=1 QA_LIMIT=5 EXTRACTION=none bash ops/bench/run-locomo-claude.sh
bench:       guard ; bash ops/bench/run-locomo-claude.sh

## Local N-node Raft cluster (real processes; needs `make release` first). See ops/cluster-local/.
cluster-up:       ; bash ops/cluster-local/run-cluster.sh
cluster-failover: ; bash ops/cluster-local/failover-test.sh
cluster-down:     ; bash ops/cluster-local/stop-cluster.sh

## Disk hygiene.
guard:     ; @bash $(GUARD)            # clean only if over the cap
disk:      ; @bash $(GUARD) --check    # report sizes; non-zero exit if over cap
clean:     ; cargo clean; [ -d ops/operator ] && (cd ops/operator && cargo clean) || true
clean-all: clean ; rm -rf $$(find . -type d -name data -not -path './node_modules/*' 2>/dev/null)
