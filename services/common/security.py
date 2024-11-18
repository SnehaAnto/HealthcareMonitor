import ssl
import os
import json
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from typing import Dict, Any
import logging

class SecurityManager:
    # Class-level shared key for all instances
    _shared_key = None
    
    def __init__(self, certfile: str, keyfile: str):
        self.certfile = certfile
        self.keyfile = keyfile
        
        # Initialize or use shared key
        if SecurityManager._shared_key is None:
            SecurityManager._shared_key = Fernet.generate_key()
        
        self.fernet = Fernet(SecurityManager._shared_key)
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_ssl_context(self, server_side: bool = False) -> ssl.SSLContext:
        """Create SSL context for secure communication"""
        try:
            context = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH if server_side else ssl.Purpose.SERVER_AUTH
            )
            
            context.load_cert_chain(self.certfile, self.keyfile)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE  # For testing only
            
            return context
            
        except Exception as e:
            self.logger.error(f"Error creating SSL context: {e}")
            raise

    def create_secure_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Create encrypted and signed message"""
        try:
            # Convert message to JSON string
            message_json = json.dumps(message)
            
            # Encrypt message
            encrypted_data = self.fernet.encrypt(message_json.encode())
            
            # Create hash for integrity check
            hasher = hashes.Hash(hashes.SHA256())
            hasher.update(encrypted_data)
            message_hash = base64.b64encode(hasher.finalize()).decode('utf-8')
            
            return {
                'data': base64.b64encode(encrypted_data).decode('utf-8'),
                'hash': message_hash
            }
            
        except Exception as e:
            self.logger.error(f"Error creating secure message: {e}")
            raise

    def verify_and_decrypt_message(self, secure_message: Dict[str, Any]) -> Dict[str, Any]:
        """Verify and decrypt a secure message"""
        try:
            encrypted_data = base64.b64decode(secure_message['data'].encode('utf-8'))
            
            # Verify hash
            hasher = hashes.Hash(hashes.SHA256())
            hasher.update(encrypted_data)
            computed_hash = base64.b64encode(hasher.finalize()).decode('utf-8')
            
            if computed_hash != secure_message['hash']:
                raise ValueError("Message integrity check failed")
            
            # Decrypt message
            decrypted_data = self.fernet.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode())
            
        except Exception as e:
            self.logger.error(f"Error verifying/decrypting message: {e}")
            raise

    @classmethod
    def get_shared_key(cls) -> bytes:
        """Get the shared encryption key"""
        if cls._shared_key is None:
            cls._shared_key = Fernet.generate_key()
        return cls._shared_key
