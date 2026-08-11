#!/usr/bin/env bash
# Audit a fleet of client certificates for what mqttd requires, BEFORE migration day
# (#172). mqttd/rustls rejects a client certificate that lacks the clientAuth Extended Key
# Usage — a rejection OpenSSL-based brokers never made, so a fleet minted against Mosquitto
# connects everywhere except here. Discovering that per device, by outage, is the failure
# this exists to prevent.
#
#   scripts/migrate/cert-audit.sh /path/to/certs           # audit every *.crt / *.pem there
#   scripts/migrate/cert-audit.sh cert1.pem cert2.pem      # or specific files
#
# For each certificate it reports, and flags as a MIGRATION BLOCKER where it would stop the
# device connecting to mqttd:
#   - clientAuth EKU        MISSING => rustls rejects at the handshake (the trap)
#   - key type / size       RSA < 2048 or EC < 256 => refused
#   - signature algorithm   MD5 / SHA-1 => refused by webpki
#   - validity              expired / not-yet-valid => refused
#
# Exit 0 if every cert would connect; exit 1 if any is a blocker (so CI/pre-migration
# gates can use it). It reads certs only — nothing is modified, nothing leaves the host.
set -uo pipefail

command -v openssl >/dev/null 2>&1 || { echo "FATAL: need openssl on PATH" >&2; exit 2; }

# Collect the certificate files: expand any directory argument to its *.crt / *.pem.
FILES=()
for arg in "$@"; do
  if [[ -d "$arg" ]]; then
    while IFS= read -r f; do FILES+=("$f"); done \
      < <(find "$arg" -maxdepth 1 -type f \( -name '*.crt' -o -name '*.pem' \) | sort)
  elif [[ -f "$arg" ]]; then
    FILES+=("$arg")
  else
    echo "warning: $arg is neither a file nor a directory; skipping" >&2
  fi
done

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "usage: cert-audit.sh <dir-of-certs | cert.pem ...>" >&2
  exit 2
fi

blockers=0
total=0

for cert in "${FILES[@]}"; do
  total=$((total + 1))
  text="$(openssl x509 -in "$cert" -noout -text 2>/dev/null)" || {
    echo "BLOCKER  $cert — not a readable X.509 certificate"
    blockers=$((blockers + 1))
    continue
  }

  # A CA certificate is not a client credential — skip it rather than flag it for the
  # clientAuth EKU it correctly does not carry (so pointing this at a dir holding the CA
  # alongside device certs is not noisy).
  if openssl x509 -in "$cert" -noout -ext basicConstraints 2>/dev/null | grep -q 'CA:TRUE'; then
    echo "skip     $cert — a CA certificate, not a client credential"
    continue
  fi

  issues=()

  # clientAuth EKU — the trap. openssl prints it as "TLS Web Client Authentication".
  eku="$(openssl x509 -in "$cert" -noout -ext extendedKeyUsage 2>/dev/null)"
  if ! grep -q 'TLS Web Client Authentication' <<<"$eku"; then
    issues+=("no clientAuth EKU (rustls rejects the handshake — add extendedKeyUsage=clientAuth and re-issue)")
  fi

  # Key type + size.
  if grep -q 'Public Key Algorithm: rsaEncryption' <<<"$text"; then
    bits="$(grep -oE 'Public-Key: \(([0-9]+) bit\)' <<<"$text" | grep -oE '[0-9]+' | head -1)"
    if [[ -n "$bits" && "$bits" -lt 2048 ]]; then
      issues+=("RSA key is ${bits}-bit (< 2048, refused)")
    fi
  elif grep -qE 'Public Key Algorithm: id-ecPublicKey' <<<"$text"; then
    bits="$(grep -oE 'Public-Key: \(([0-9]+) bit\)' <<<"$text" | grep -oE '[0-9]+' | head -1)"
    if [[ -n "$bits" && "$bits" -lt 256 ]]; then
      issues+=("EC key is ${bits}-bit (< 256, refused)")
    fi
  fi

  # Signature algorithm — MD5 / SHA-1 are refused by webpki.
  sigalg="$(grep -m1 'Signature Algorithm:' <<<"$text" | sed 's/.*Signature Algorithm: //')"
  case "$sigalg" in
    *md5*|*MD5*|*sha1*|*SHA1*|*ecdsa-with-SHA1*)
      issues+=("weak signature algorithm ($sigalg — refused)") ;;
  esac

  # Validity window.
  if ! openssl x509 -in "$cert" -noout -checkend 0 >/dev/null 2>&1; then
    issues+=("EXPIRED (or not yet valid)")
  fi

  subject="$(openssl x509 -in "$cert" -noout -subject 2>/dev/null | sed 's/^subject=//')"
  if [[ ${#issues[@]} -eq 0 ]]; then
    echo "ok       $cert — $subject"
  else
    echo "BLOCKER  $cert — $subject"
    for i in "${issues[@]}"; do echo "           • $i"; done
    blockers=$((blockers + 1))
  fi
done

echo
if [[ $blockers -eq 0 ]]; then
  echo "PASS: all $total certificate(s) would connect to mqttd."
  exit 0
else
  echo "FAIL: $blockers of $total certificate(s) would be REJECTED by mqttd — fix before migrating."
  echo "The most common cause is a missing clientAuth EKU; re-issue those certs with it."
  exit 1
fi
