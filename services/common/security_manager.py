from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.fernet import Fernet
import base64
import os
import logging

class SecurityManager:
    def __init__(self):
        self.logger = logging.getLogger('Security')
        self.symmetric_key = os.urandom(32)  # AES-256
        self.iv = os.urandom(16)  # AES block size
        
        # Generate RSA key pair
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()
        
        # For simpler symmetric encryption
        self.fernet = Fernet(Fernet.generate_key())

    def encrypt_data(self, data: bytes) -> bytes:
        """Encrypt data using AES"""
        cipher = Cipher(algorithms.AES(self.symmetric_key), modes.CBC(self.iv))
        encryptor = cipher.encryptor()
        padded_data = self._pad_data(data)
        return encryptor.update(padded_data) + encryptor.finalize()

    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using AES"""
        cipher = Cipher(algorithms.AES(self.symmetric_key), modes.CBC(self.iv))
        decryptor = cipher.decryptor()
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
        return self._unpad_data(decrypted_data)

    def verify_integrity(self, data: bytes, signature: bytes) -> bool:
        """Verify data integrity using SHA-256"""
        try:
            self.public_key.verify(
                signature,
                data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

    def _pad_data(self, data: bytes) -> bytes:
        """PKCS7 padding"""
        pad_length = 16 - (len(data) % 16)
        padding = bytes([pad_length] * pad_length)
        return data + padding

    def _unpad_data(self, padded_data: bytes) -> bytes:
        """Remove PKCS7 padding"""
        pad_length = padded_data[-1]
        return padded_data[:-pad_length] 