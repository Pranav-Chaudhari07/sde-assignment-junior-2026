"""PII protection at rest.

Two different treatments, for two different needs:

* Phone: must still be joinable (events arrive with a phone and have to
  find their lead), so it is stored only as a deterministic SHA-256 hash
  of the canonical '+91XXXXXXXXXX' form, plus the last 4 digits (which
  the report is required to show, masked as '+91******1234'). No full
  phone number ever reaches the database, a log line, or a report.
  An optional pepper from ``AU_PII_PEPPER`` is mixed into the hash;
  without it the hash is still one-way, but a peppered hash is not
  reversible by a rainbow table of all 10-digit numbers. In production
  the pepper belongs in a secret manager, not in code.

* Customer name: never needed for a join and never shown in a report,
  so it is stored reversibly encrypted with Fernet (AES-128-CBC +
  HMAC, via the ``cryptography`` package). The key comes from
  ``AU_PII_KEY`` when set, otherwise a key is generated once and kept
  in ``<db>.key`` next to the database file (0600, never committed).
  Losing the key file makes names unrecoverable -- that is the point:
  the database alone must not reveal PII.

Nothing in this module ever logs a plaintext phone or name.
"""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken


_PEPPER = os.environ.get("AU_PII_PEPPER", "")


def phone_hash(canonical_phone: str, pepper: str | None = None) -> str:
    """Deterministic SHA-256 hex of the canonical phone (+ optional pepper)."""
    if pepper is None:
        pepper = _PEPPER
    material = (pepper + canonical_phone).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def phone_last4(canonical_phone: str) -> str:
    return canonical_phone[-4:]


def _key_from_env() -> bytes | None:
    raw = os.environ.get("AU_PII_KEY")
    if not raw:
        return None
    try:
        # A real Fernet key is urlsafe-base64 32 bytes; use it directly.
        Fernet(raw.encode())
        return raw.encode()
    except Exception:
        # Otherwise treat it as a passphrase and derive a key from it.
        digest = hashlib.sha256(raw.encode()).digest()
        return base64.urlsafe_b64encode(digest)


def _key_from_file(db_path: Path) -> bytes:
    key_path = Path(str(db_path) + ".key")
    if key_path.exists():
        return key_path.read_bytes().strip()
    key = Fernet.generate_key()
    key_path.write_bytes(key)
    try:
        os.chmod(key_path, 0o600)
    except OSError:
        pass
    return key


class PiiProtector:
    """Encrypt/decrypt PII names for one database file."""

    def __init__(self, db_path: str | Path):
        key = _key_from_env() or _key_from_file(Path(db_path))
        self._fernet = Fernet(key)
        # Names repeat a lot (a few hundred distinct names across
        # 140k leads). Fernet encryption is the slowest per-row cost
        # in lead ingest, so cache ciphertext per plaintext name.
        # Reusing a ciphertext leaks only that two leads share a
        # name -- the same equality a hash would leak.
        self._cache: dict[str, str] = {}

    def encrypt_name(self, name: str | None) -> str | None:
        if name is None:
            return None
        cached = self._cache.get(name)
        if cached is None:
            cached = self._fernet.encrypt(name.encode("utf-8")).decode("ascii")
            self._cache[name] = cached
        return cached

    def decrypt_name(self, token: str | None) -> str | None:
        if token is None:
            return None
        try:
            return self._fernet.decrypt(token.encode("ascii")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("cannot decrypt stored name: wrong PII key") from exc
