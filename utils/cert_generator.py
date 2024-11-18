import os
import asyncio
from datetime import datetime, timedelta
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

async def generate_test_certificates():
    """Generate test certificates for all services"""
    try:
        cert_dir = "certs"
        if not os.path.exists(cert_dir):
            os.makedirs(cert_dir)

        certificates = {}
        services = [
            'collector',
            'processor',
            'storage',
            'notification',
            'ui'
        ]

        for service in services:
            # Generate key
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )

            # Generate certificate
            subject = issuer = x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, f'{service}.healthcare.local')
            ])

            cert = x509.CertificateBuilder().subject_name(
                subject
            ).issuer_name(
                issuer
            ).public_key(
                private_key.public_key()
            ).serial_number(
                x509.random_serial_number()
            ).not_valid_before(
                datetime.utcnow()
            ).not_valid_after(
                datetime.utcnow() + timedelta(days=365)
            ).add_extension(
                x509.SubjectAlternativeName([
                    x509.DNSName('localhost'),
                    x509.DNSName('127.0.0.1')
                ]),
                critical=False,
            ).sign(private_key, hashes.SHA256(), default_backend())

            # Save certificate
            cert_path = os.path.join(cert_dir, f"{service}_cert.pem")
            key_path = os.path.join(cert_dir, f"{service}_key.pem")

            with open(cert_path, "wb") as f:
                f.write(cert.public_bytes(serialization.Encoding.PEM))

            with open(key_path, "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))

            certificates[service] = (cert_path, key_path)

        return certificates

    except Exception as e:
        print(f"Error generating certificates: {e}")
        raise

if __name__ == "__main__":
    # Test certificate generation
    asyncio.run(generate_test_certificates())
