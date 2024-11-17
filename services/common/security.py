from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.fernet import Fernet
import base64
import os

class SecurityConfig:
    # Generate a single key to be shared across all services
    SHARED_KEY = Fernet.generate_key()
    
    @classmethod
    def get_cipher_suite(cls):
        return Fernet(cls.SHARED_KEY)

class SecurityManager:
    def __init__(self):
        # Generate RSA key pair for asymmetric encryption
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()
        
        # Generate AES key for symmetric encryption
        self.aes_key = os.urandom(32)
        self.aes_iv = os.urandom(16)
        
        # Fernet for simpler symmetric encryption (can be used for less sensitive data)
        self.fernet = Fernet(Fernet.generate_key())

    def get_public_key_bytes(self):
        """Export public key in PEM format"""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def asymmetric_encrypt(self, data: bytes, public_key=None) -> bytes:
        """Encrypt data using RSA"""
        key = public_key if public_key else self.public_key
        return key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def asymmetric_decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using RSA private key"""
        return self.private_key.decrypt(
            encrypted_data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def symmetric_encrypt(self, data: bytes) -> bytes:
        """Encrypt data using AES"""
        cipher = Cipher(algorithms.AES(self.aes_key), modes.CBC(self.aes_iv))
        encryptor = cipher.encryptor()
        
        # Pad the data
        padded_data = self._pad_data(data)
        
        return encryptor.update(padded_data) + encryptor.finalize()

    def symmetric_decrypt(self, encrypted_data: bytes) -> bytes:
        """Decrypt data using AES"""
        cipher = Cipher(algorithms.AES(self.aes_key), modes.CBC(self.aes_iv))
        decryptor = cipher.decryptor()
        
        decrypted_data = decryptor.update(encrypted_data) + decryptor.finalize()
        return self._unpad_data(decrypted_data)

    def calculate_hash(self, data: bytes) -> bytes:
        """Calculate SHA-256 hash of data"""
        digest = hashes.Hash(hashes.SHA256())
        digest.update(data)
        return digest.finalize()

    def _pad_data(self, data: bytes) -> bytes:
        """PKCS7 padding for AES"""
        padding_length = 16 - (len(data) % 16)
        padding = bytes([padding_length] * padding_length)
        return data + padding

    def _unpad_data(self, padded_data: bytes) -> bytes:
        """Remove PKCS7 padding"""
        padding_length = padded_data[-1]
        return padded_data[:-padding_length]
