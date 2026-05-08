from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, cast

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from ._nats_server_binary import kinopio_cache_root


@dataclass(frozen=True)
class LeafTLSMaterials:
    cert_file: Path
    key_file: Path
    ca_file: Path
    auto_generated: bool


def resolve_leaf_tls_materials(
    *,
    runtime_dir: Path,
    advertised_hosts: Iterable[str],
    cert_file: str | os.PathLike[str] | None = None,
    key_file: str | os.PathLike[str] | None = None,
    ca_file: str | os.PathLike[str] | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
) -> LeafTLSMaterials:
    if cert_file is not None or key_file is not None:
        if cert_file is None or key_file is None:
            raise ValueError("cert_file and key_file must be provided together")
        resolved_ca = (
            Path(ca_file).expanduser()
            if ca_file is not None
            else Path(cert_file).expanduser()
        )
        return LeafTLSMaterials(
            cert_file=Path(cert_file).expanduser(),
            key_file=Path(key_file).expanduser(),
            ca_file=resolved_ca,
            auto_generated=False,
        )

    tls_cache_dir = (
        Path(cache_dir).expanduser()
        if cache_dir is not None
        else kinopio_cache_root() / "tls"
    )
    tls_cache_dir.mkdir(parents=True, exist_ok=True)

    ca_cert_file = tls_cache_dir / "leaf-ca-cert.pem"
    ca_key_file = tls_cache_dir / "leaf-ca-key.pem"
    if not ca_cert_file.exists() or not ca_key_file.exists():
        _generate_ca(ca_cert_file=ca_cert_file, ca_key_file=ca_key_file)

    cert_path = runtime_dir / "leaf-websocket-cert.pem"
    key_path = runtime_dir / "leaf-websocket-key.pem"
    _generate_server_certificate(
        cert_path=cert_path,
        key_path=key_path,
        ca_cert_file=ca_cert_file,
        ca_key_file=ca_key_file,
        advertised_hosts=tuple(advertised_hosts),
    )
    return LeafTLSMaterials(
        cert_file=cert_path,
        key_file=key_path,
        ca_file=ca_cert_file,
        auto_generated=True,
    )


def _generate_ca(*, ca_cert_file: Path, ca_key_file: Path) -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "KinopioHub Local Leaf CA")]
    )
    now = _utc_now()
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(private_key=private_key, algorithm=hashes.SHA256())
    )

    ca_cert_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    ca_key_file.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _generate_server_certificate(
    *,
    cert_path: Path,
    key_path: Path,
    ca_cert_file: Path,
    ca_key_file: Path,
    advertised_hosts: tuple[str, ...],
) -> None:
    server_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_certificate = x509.load_pem_x509_certificate(ca_cert_file.read_bytes())
    ca_private_key = cast(
        rsa.RSAPrivateKey,
        serialization.load_pem_private_key(
        ca_key_file.read_bytes(),
        password=None,
        ),
    )

    common_name = next((host for host in advertised_hosts if host), "localhost")
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    san_entries = [
        _host_to_san(host)
        for host in _dedupe_hosts(("localhost", "127.0.0.1", "::1", *advertised_hosts))
    ]

    now = _utc_now()
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_certificate.subject)
        .public_key(server_private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(private_key=ca_private_key, algorithm=hashes.SHA256())
    )

    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        server_private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _dedupe_hosts(hosts: Iterable[str]) -> tuple[str, ...]:
    deduped: list[str] = []
    for host in hosts:
        normalized = host.strip()
        if not normalized or normalized in deduped:
            continue
        deduped.append(normalized)
    return tuple(deduped)


def _host_to_san(host: str) -> x509.GeneralName:
    try:
        return x509.IPAddress(ipaddress.ip_address(host))
    except ValueError:
        return x509.DNSName(host)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
