from OpenSSL import crypto
import os
from pathlib import Path
import logging

class CertificateGenerator:
    def __init__(self, cert_dir="config/certs"):
        self.cert_dir = Path(cert_dir)
        self.cert_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("CertGenerator")

    def generate_node_certificates(self, node_id, country="CA", state="Ontario", 
                                 locality="Ottawa", organization="Healthcare System",
                                 organizational_unit="Monitoring", common_name=None):
        """Generate certificate and private key for a node"""
        try:
            # Generate key
            key = crypto.PKey()
            key.generate_key(crypto.TYPE_RSA, 2048)

            # Generate certificate
            cert = crypto.X509()
            cert.get_subject().C = country
            cert.get_subject().ST = state
            cert.get_subject().L = locality
            cert.get_subject().O = organization
            cert.get_subject().OU = organizational_unit
            cert.get_subject().CN = common_name or f"node_{node_id}"

            cert.set_serial_number(int.from_bytes(os.urandom(16), byteorder="big"))
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(365*24*60*60)  # Valid for one year
            cert.set_issuer(cert.get_subject())
            cert.set_pubkey(key)
            cert.sign(key, 'sha256')

            # Save certificate and private key
            cert_path = self.cert_dir / f"node_{node_id}_cert.pem"
            key_path = self.cert_dir / f"node_{node_id}_key.pem"

            with open(cert_path, "wb") as f:
                f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))

            with open(key_path, "wb") as f:
                f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))

            self.logger.info(f"Generated certificates for node {node_id}")
            return cert_path, key_path

        except Exception as e:
            self.logger.error(f"Error generating certificates for node {node_id}: {e}")
            raise

    def generate_ca_certificate(self):
        """Generate a Certificate Authority certificate"""
        try:
            # Generate key
            key = crypto.PKey()
            key.generate_key(crypto.TYPE_RSA, 4096)

            # Generate certificate
            cert = crypto.X509()
            cert.get_subject().C = "CA"
            cert.get_subject().ST = "Ontario"
            cert.get_subject().L = "Ottawa"
            cert.get_subject().O = "Healthcare Monitoring CA"
            cert.get_subject().OU = "Security"
            cert.get_subject().CN = "Healthcare Monitoring Root CA"

            cert.set_serial_number(int.from_bytes(os.urandom(16), byteorder="big"))
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(365*24*60*60*10)  # Valid for 10 years
            cert.set_issuer(cert.get_subject())
            cert.set_pubkey(key)
            cert.add_extensions([
                crypto.X509Extension(b"basicConstraints", True, b"CA:TRUE, pathlen:0"),
                crypto.X509Extension(b"keyUsage", True, b"keyCertSign, cRLSign"),
                crypto.X509Extension(b"subjectKeyIdentifier", False, b"hash", subject=cert),
            ])
            cert.sign(key, 'sha256')

            # Save CA certificate and private key
            ca_cert_path = self.cert_dir / "ca_cert.pem"
            ca_key_path = self.cert_dir / "ca_key.pem"

            with open(ca_cert_path, "wb") as f:
                f.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))

            with open(ca_key_path, "wb") as f:
                f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))

            self.logger.info("Generated CA certificates")
            return ca_cert_path, ca_key_path

        except Exception as e:
            self.logger.error(f"Error generating CA certificates: {e}")
            raise
