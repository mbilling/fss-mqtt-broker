#!/usr/bin/env python3
"""Run ``mqttd --check-config`` on a generated config.

Issue #671: the gate opens ``password_file``, ``acl_file``, and ``[tls]``
material under the checking uid. A converted config names the *operator's*
paths (``/etc/certs/server.crt``, ``C:\\certs\\server.crt``), which are not on
the machine that runs the migration sweep. Those paths are not a converter
defect. This wrapper overrides only the file keys the config already sets,
with readable stand-ins, then execs the broker. Schema errors and unbindable
addresses still fail the gate.

Usage::

    python3 scripts/migrate/check_config_fixture.py <mqttd> <config.toml>

Stdout, stderr, and the exit code are the broker's.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tomllib
from pathlib import Path

# One stand-in set per uid. Parallel sweep processes share it; generation is
# locked so two of them do not write half a PEM.
_STANDIN = Path(os.environ.get("TMPDIR", "/tmp")) / f"mqttd-checkcfg-standin-{os.getuid()}"


def _run(argv: list[str]) -> None:
    subprocess.run(argv, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def standin_paths() -> dict[str, Path]:
    """Cert, key, CA, CRL, password file, and ACL the gate can open and parse."""
    dest = _STANDIN
    dest.mkdir(parents=True, exist_ok=True)
    lock_path = dest.with_suffix(".lock")
    with lock_path.open("a", encoding="utf-8") as lock:
        try:
            import fcntl

            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            pass
        ca_key = dest / "ca.key"
        ca_crt = dest / "ca.crt"
        srv_key = dest / "server.key"
        srv_crt = dest / "server.crt"
        crl = dest / "crl.pem"
        # rustls rejects an X.509 v1 certificate (UnsupportedCertVersion). The
        # stamp forces a rebuild when this recipe changes; a partial earlier
        # attempt must not be reused.
        stamp = dest / "stamp-v3"
        if not (
            stamp.is_file()
            and ca_crt.is_file()
            and srv_crt.is_file()
            and srv_key.is_file()
            and crl.is_file()
        ):
            _run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    str(ca_key),
                    "-out",
                    str(ca_crt),
                    "-days",
                    "2",
                    "-nodes",
                    "-subj",
                    "/CN=mqttd-check-standin-ca",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-addext",
                    "keyUsage=critical,keyCertSign,cRLSign",
                ]
            )
            csr = dest / "server.csr"
            _run(
                [
                    "openssl",
                    "req",
                    "-newkey",
                    "rsa:2048",
                    "-keyout",
                    str(srv_key),
                    "-out",
                    str(csr),
                    "-nodes",
                    "-subj",
                    "/CN=localhost",
                ]
            )
            ext = dest / "server.ext"
            ext.write_text(
                "basicConstraints=CA:FALSE\n"
                "keyUsage=digitalSignature,keyEncipherment\n"
                "extendedKeyUsage=serverAuth\n"
                "subjectAltName=DNS:localhost\n",
                encoding="utf-8",
            )
            _run(
                [
                    "openssl",
                    "x509",
                    "-req",
                    "-in",
                    str(csr),
                    "-CA",
                    str(ca_crt),
                    "-CAkey",
                    str(ca_key),
                    "-CAcreateserial",
                    "-out",
                    str(srv_crt),
                    "-days",
                    "2",
                    "-extfile",
                    str(ext),
                ]
            )
            index = dest / "index.txt"
            index.write_text("", encoding="utf-8")
            (dest / "crlnumber").write_text("01\n", encoding="utf-8")
            cnf = dest / "ca.cnf"
            cnf.write_text(
                "[ca]\n"
                "default_ca = CA_default\n"
                "[CA_default]\n"
                f"database = {index}\n"
                f"crlnumber = {dest / 'crlnumber'}\n"
                "default_crl_days = 1\n"
                "default_md = sha256\n",
                encoding="utf-8",
            )
            _run(
                [
                    "openssl",
                    "ca",
                    "-config",
                    str(cnf),
                    "-gencrl",
                    "-keyfile",
                    str(ca_key),
                    "-cert",
                    str(ca_crt),
                    "-out",
                    str(crl),
                ]
            )
            stamp.write_text("v3\n", encoding="utf-8")
        pw = dest / "passwd"
        if not pw.is_file():
            # The loader checks `username:hash` shape, not that the hash verifies.
            pw.write_text("standin:hash\n", encoding="utf-8")
        acl = dest / "acl.toml"
        if not acl.is_file():
            acl.write_text('default = "deny"\n', encoding="utf-8")
    return {
        "password_file": pw,
        "acl_file": acl,
        "cert": srv_crt,
        "key": srv_key,
        "client_ca": ca_crt,
        "crl": crl,
    }


_ENV = {
    "password_file": "MQTTD_PASSWORD_FILE",
    "acl_file": "MQTTD_ACL_FILE",
    "cert": "MQTTD_TLS_CERT",
    "key": "MQTTD_TLS_KEY",
    "client_ca": "MQTTD_TLS_CLIENT_CA",
    "crl": "MQTTD_TLS_CRL",
}


def standin_env(config_path: Path) -> dict[str, str]:
    """Overrides for file keys this config already sets. Empty if it will not parse."""
    try:
        cfg = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError):
        return {}
    if not isinstance(cfg, dict):
        return {}
    security = cfg.get("security") if isinstance(cfg.get("security"), dict) else {}
    tls = cfg.get("tls") if isinstance(cfg.get("tls"), dict) else {}
    present = {
        "password_file": security.get("password_file"),
        "acl_file": security.get("acl_file"),
        "cert": tls.get("cert"),
        "key": tls.get("key"),
        "client_ca": tls.get("client_ca"),
        "crl": tls.get("crl"),
    }
    if not any(isinstance(v, str) and v for v in present.values()):
        return {}
    paths = standin_paths()
    env: dict[str, str] = {}
    for key, value in present.items():
        if isinstance(value, str) and value:
            env[_ENV[key]] = str(paths[key])
    return env


def main(argv: list[str]) -> None:
    if len(argv) != 3:
        sys.stderr.write(
            "usage: check_config_fixture.py <mqttd> <config.toml>\n"
        )
        sys.exit(2)
    mqttd, config = argv[1], argv[2]
    env = os.environ.copy()
    env.update(standin_env(Path(config)))
    os.execvpe(mqttd, [mqttd, "--check-config", "--config", config], env)


if __name__ == "__main__":
    main(sys.argv)
