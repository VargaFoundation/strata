#!/usr/bin/env bash
# gen-pg-tls.sh — generate a self-signed TLS cert/key for the Ecphoria PostgreSQL-wire listener.
#
# The PG-wire password IS the API key/JWT, so it must not cross the network in cleartext. Enabling
# TLS on :5432 encrypts the handshake. This helper makes a **development / internal** self-signed
# cert; for production use a CA-issued cert (cert-manager, your PKI) and rotate it (see below).
#
# Usage:
#   scripts/gen-pg-tls.sh [OUT_DIR] [CN] [DAYS]
#     OUT_DIR  where to write pg.crt / pg.key   (default ./data/tls)
#     CN       certificate Common Name / SAN    (default localhost)
#     DAYS     validity                          (default 825)
#
# Then point Ecphoria at them:
#   [gateway.pg_tls]
#   cert_path = "<OUT_DIR>/pg.crt"
#   key_path  = "<OUT_DIR>/pg.key"
# and connect with a TLS-verifying client (psql "sslmode=verify-full sslrootcert=<OUT_DIR>/pg.crt …").
set -euo pipefail

OUT_DIR="${1:-./data/tls}"
CN="${2:-localhost}"
DAYS="${3:-825}"

command -v openssl >/dev/null || { echo "openssl not found" >&2; exit 1; }
mkdir -p "$OUT_DIR"

# RSA-2048, SAN = CN (so verify-full works), no passphrase (server reads it unattended).
openssl req -x509 -newkey rsa:2048 -sha256 -days "$DAYS" -nodes \
  -keyout "$OUT_DIR/pg.key" -out "$OUT_DIR/pg.crt" \
  -subj "/CN=${CN}" -addext "subjectAltName=DNS:${CN}" 2>/dev/null

chmod 600 "$OUT_DIR/pg.key"
echo "Wrote $OUT_DIR/pg.crt and $OUT_DIR/pg.key (CN=${CN}, ${DAYS}d)."
echo "Configure [gateway.pg_tls] cert_path/key_path to point at them (see this script's header)."
