from cryptography.fernet import Fernet
import socket
import ssl
import threading
import logging
from .security import SecurityConfig, SecurityManager
import json
from typing import Dict, Any
import base64

class BaseNode:
    def __init__(self, service_name, node_id, host, port, certfile, keyfile):
        self.service_name = service_name
        self.node_id = node_id
        self.host = host
        self.port = port
        self.certfile = certfile
        self.keyfile = keyfile
        self.is_server = True  # Default as server
        self.server_ssl_context = self._create_ssl_context()
        self.client_ssl_context = self._create_ssl_context_client()
        self.peers = {}
        self.cipher_suite = SecurityConfig.get_cipher_suite()
        self.security = SecurityManager()
        self.peer_public_keys: Dict[str, bytes] = {}
        
        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format=f'[{self.service_name}] %(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.service_name)

    def _create_ssl_context(self):
        """Create SSL context for both client and server"""
        # For server
        if self.is_server:
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        # For client
        else:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.load_verify_locations(cafile=self.certfile)
            context.check_hostname = False  # For development only
            context.verify_mode = ssl.CERT_NONE  # For development only
        
        return context

    def _create_ssl_context_client(self):
        """Create SSL context for client connections"""
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = False  # For development only
        context.verify_mode = ssl.CERT_NONE  # For development only
        return context

    def start(self):
        """Start the service node"""
        self.logger.info(f"Starting {self.service_name} service on {self.host}:{self.port}")
        threading.Thread(target=self._start_server).start()

    def _start_server(self):
        """Internal method to start the server socket"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen(5)
            self.logger.info(f"Listening for connections on {self.host}:{self.port}")
            
            while True:
                try:
                    client_sock, addr = sock.accept()
                    ssl_sock = self.server_ssl_context.wrap_socket(
                        client_sock, 
                        server_side=True
                    )
                    self.logger.info(f"Accepted secure connection from {addr}")
                    threading.Thread(
                        target=self._handle_client,
                        args=(ssl_sock, addr)
                    ).start()
                except Exception as e:
                    self.logger.error(f"Error accepting connection: {e}")

    def _handle_client(self, conn, addr):
        """Handle client connections"""
        try:
            self.logger.info(f"Handling connection from {addr}")
            while True:
                try:
                    # Receive encrypted data
                    data = conn.recv(4096)
                    if not data:
                        break
                    
                    # Process the received data
                    try:
                        # Decrypt the data using shared key
                        decrypted_data = self.cipher_suite.decrypt(data)
                        self.logger.debug(f"Decrypted data: {decrypted_data}")
                        
                        message = json.loads(decrypted_data.decode())
                        self.logger.debug(f"Parsed message: {message}")
                        
                        # Handle the decrypted message
                        self.handle_message(message, conn)
                    except Exception as e:
                        self.logger.error(f"Error processing message: {str(e)}", exc_info=True)
                        
                except ConnectionError as e:
                    self.logger.error(f"Connection error: {e}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error handling client {addr}: {str(e)}", exc_info=True)
        finally:
            try:
                conn.close()
                self.logger.info(f"Closed connection from {addr}")
            except:
                pass

    def secure_send(self, conn, data: dict):
        """Securely send data to peer"""
        try:
            # Convert data to JSON and encode
            json_data = json.dumps(data).encode()
            self.logger.debug(f"Sending data: {data}")
            
            # Encrypt the data using shared key
            encrypted_data = self.cipher_suite.encrypt(json_data)
            self.logger.debug(f"Encrypted data length: {len(encrypted_data)}")
            
            # Send the encrypted data
            conn.sendall(encrypted_data)
            self.logger.debug("Data sent successfully")
            
        except Exception as e:
            self.logger.error(f"Error in secure_send: {str(e)}", exc_info=True)
            raise

    def handle_message(self, message: dict, conn):
        """To be implemented by specific services"""
        raise NotImplementedError

    def establish_secure_connection(self, peer_addr: str, peer_port: int):
        """Establish secure connection with peer"""
        try:
            # Create socket and wrap with SSL (as client)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ssl_sock = self.client_ssl_context.wrap_socket(
                sock,
                server_hostname=peer_addr
            )
            ssl_sock.connect((peer_addr, peer_port))
            
            # Store connection
            self.peers[(peer_addr, peer_port)] = ssl_sock
            self.logger.info(f"Established secure connection to {peer_addr}:{peer_port}")
            
            return ssl_sock
            
        except Exception as e:
            self.logger.error(f"Error establishing secure connection: {e}")
            raise

    def _exchange_keys(self, conn):
        """Exchange public keys with peer"""
        try:
            # Send our public key
            public_key_bytes = self.security.get_public_key_bytes()
            conn.send(public_key_bytes)
            
            # Receive peer's public key
            peer_public_key_bytes = conn.recv(4096)
            self.peer_public_keys[conn.getpeername()] = peer_public_key_bytes
            
        except Exception as e:
            self.logger.error(f"Error in key exchange: {e}")
            raise
