import asyncio
import logging
import ssl
import json
from typing import Dict, Any, Optional, Tuple
from .security import SecurityManager
from .fault_tolerance import FaultToleranceManager

__all__ = ['BaseNode']

class BaseNode:
    def __init__(self, node_id: str, host: str, port: int, certfile: str, keyfile: str):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize security with shared key
        self.security = SecurityManager(certfile, keyfile)
        self.current_writer = None
        
        # Initialize fault tolerance
        self.fault_tolerance = FaultToleranceManager(
            self.__class__.__name__,
            node_id
        )
        
        # Connection management
        self.server = None
        self.connections = {}
        self.is_running = False
        self.loop = asyncio.get_event_loop()

    async def start(self) -> None:
        """Start the node server"""
        try:
            # Create SSL context
            ssl_context = self.security.create_ssl_context(server_side=True)
            
            # Start server
            self.server = await asyncio.start_server(
                self.handle_client,
                self.host,
                self.port,
                ssl=ssl_context
            )
            
            # Start fault tolerance monitoring
            await self.fault_tolerance.start()
            
            self.is_running = True
            self.logger.info(f"Started {self.__class__.__name__} node on {self.host}:{self.port}")
            
        except Exception as e:
            self.logger.error(f"Failed to start node: {e}")
            raise

    async def stop(self) -> None:
        """Stop the node server"""
        try:
            self.is_running = False
            
            # Stop fault tolerance
            await self.fault_tolerance.stop()
            
            # Close all connections
            for conn in self.connections.values():
                conn.close()
                await conn.wait_closed()
            self.connections.clear()
            
            # Close server
            if self.server:
                self.server.close()
                await self.server.wait_closed()
                
            self.logger.info(f"Stopped {self.__class__.__name__} node")
            
        except Exception as e:
            self.logger.error(f"Error stopping node: {e}")
            raise

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle client connection"""
        try:
            peer = writer.get_extra_info('peername')
            self.logger.info(f"New connection from {peer}")
            self.current_writer = writer

            while True:
                # Read message length
                length_bytes = await reader.read(4)
                if not length_bytes:
                    break
                message_length = int.from_bytes(length_bytes, 'big')
                
                # Read message data
                message_bytes = await reader.read(message_length)
                if not message_bytes:
                    break
                    
                self.logger.debug(f"Received message of length {message_length}")
                
                # Process message
                try:
                    secure_message = json.loads(message_bytes.decode())
                    message = self.security.verify_and_decrypt_message(secure_message)
                    
                    self.logger.debug(f"Decrypted message: {message}")
                    
                    response = await self.process_message(message)
                    
                    # Send response
                    secure_response = self.security.create_secure_message(response)
                    response_bytes = json.dumps(secure_response).encode()
                    
                    writer.write(len(response_bytes).to_bytes(4, 'big'))
                    writer.write(response_bytes)
                    await writer.drain()
                    
                except Exception as e:
                    self.logger.error(f"Error handling message: {e}", exc_info=True)
                    error_response = {
                        'status': 'error',
                        'message': str(e)
                    }
                    secure_error = self.security.create_secure_message(error_response)
                    error_bytes = json.dumps(secure_error).encode()
                    writer.write(len(error_bytes).to_bytes(4, 'big'))
                    writer.write(error_bytes)
                    await writer.drain()

        except Exception as e:
            self.logger.error(f"Error handling client {peer}: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Connection closed from {peer}")

    async def connect_to_node(self, host: str, port: int) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Establish secure connection to another node"""
        try:
            ssl_context = self.security.create_ssl_context(server_side=False)
            reader, writer = await asyncio.open_connection(
                host,
                port,
                ssl=ssl_context
            )
            return reader, writer
            
        except Exception as e:
            self.logger.error(f"Failed to connect to {host}:{port}: {e}")
            raise

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process received message - to be implemented by subclasses"""
        raise NotImplementedError

    async def _stop_node(self):
        """Stop the node without stopping fault tolerance"""
        if hasattr(self, 'server'):
            self.server.close()
            await self.server.wait_closed()
        
        if hasattr(self, 'writer'):
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        
        self._is_running = False

    async def close(self):
        """Close the service and cleanup resources"""
        try:
            if hasattr(self, 'server'):
                self.server.close()
                await self.server.wait_closed()
            
            if hasattr(self, 'writer'):
                self.writer.close()
                try:
                    await self.writer.wait_closed()
                except Exception:
                    pass
            
            if hasattr(self, '_connections'):
                for writer in self._connections.values():
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
            
            self._is_running = False
            
        except Exception as e:
            logging.error(f"Error closing {self.__class__.__name__}: {e}")
            raise
