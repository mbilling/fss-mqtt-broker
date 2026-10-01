# The audit trail: every admin request this scenario makes — actions, reads and refusals —
# is one admin.request record naming the certificate subject, role and outcome, and the
# reload is also a security.reload record with trigger=admin.

audit_log() { docker logs "$(container 1)" 2>&1 | sed 's/\x1b\[[0-9;]*m//g'; }

run() {
  eventually "the cluster forms" 90 formed 1
  api root 1 POST /admin/v1/reload >/dev/null
  api root 1 POST /admin/v1/cordon >/dev/null
  api root 1 POST /admin/v1/uncordon >/dev/null
  api root 1 POST '/admin/v1/kick?client=nobody' >/dev/null
  api oncall 1 GET /admin/v1/clients >/dev/null
  api stranger 1 GET /admin/v1/whoami >/dev/null
  sleep 1  # SETTLE: let the last records reach the container log
  local log
  log=$(audit_log)
  expect "reload recorded with the operator" 'role=operator POST /admin/v1/reload -> 200' "$log"
  expect "cordon recorded" 'role=operator POST /admin/v1/cordon -> 200' "$log"
  expect "uncordon recorded" 'role=operator POST /admin/v1/uncordon -> 200' "$log"
  expect "a refused-for-absence kick recorded (404)" 'role=operator POST /admin/v1/kick?client=nobody' "$log"
  expect "a viewer's read recorded" 'role=viewer GET /admin/v1/clients -> 200' "$log"
  expect "an unlisted certificate's refusal recorded" 'role=none GET /admin/v1/whoami -> 403' "$log"
  expect "the operator's subject is on the records" 'subject="CN=root, O=example"' "$log"
  expect "the reload is also a security.reload with trigger=admin" 'ok (trigger=admin)' "$log"
}
