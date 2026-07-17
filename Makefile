# Strata developer Makefile. The build targets run the disk guard FIRST so a runaway target/
# (heavy native deps × many builds accumulate to 100s of GB) can never silently saturate the disk.
# Override the cap with `make CAP_GB=25 build`.

CAP_GB ?= 40
export CAP_GB
GUARD  := scripts/target-guard.sh

.PHONY: build test check fmt clippy run guard disk clean clean-all release

## Guarded common tasks (auto-clean target/ if it's over the cap, then run cargo).
build: guard ; cargo build --workspace
test:  guard ; cargo test --workspace
check: guard ; cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
release: guard ; cargo build --release --bin strata-server
run: guard ; cargo run --bin strata-server

fmt:    ; cargo fmt --all
clippy: guard ; cargo clippy --workspace --all-targets -- -D warnings

## Disk hygiene.
guard:     ; @bash $(GUARD)            # clean only if over the cap
disk:      ; @bash $(GUARD) --check    # report sizes; non-zero exit if over cap
clean:     ; cargo clean; [ -d ops/operator ] && (cd ops/operator && cargo clean) || true
clean-all: clean ; rm -rf $$(find . -type d -name data -not -path './node_modules/*' 2>/dev/null)
