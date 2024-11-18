import asyncio
import json
import logging
from typing import Dict, Any, Optional, List, Tuple
from ..common.node import BaseNode

class DataCollectionService(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, certfile: str, keyfile: str):
        super().__init__(node_id, host, port, certfile, keyfile)
        self.processors = {}  # processor_id -> (reader, writer)
        self.current_processor = None
        self.backup_processors = []  # List of (host, port, node_id)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.reconnect_lock = asyncio.Lock()
        self.processor_discovery_interval = 5  # seconds

    async def discover_processors(self, known_processors: List[Tuple[str, int]]) -> None:
        """Discover available processors"""
        for host, port in known_processors:
            if (host, port) not in [(h, p) for h, p, _ in self.backup_processors]:
                try:
                    reader, writer = await asyncio.wait_for(
                        self.connect_to_node(host, port),
                        timeout=2.0
                    )
                    
                    # Send discovery handshake
                    handshake = {
                        'type': 'handshake',
                        'node_id': self.node_id,
                        'node_type': 'collector',
                        'discovery': True
                    }
                    
                    secure_handshake = self.security.create_secure_message(handshake)
                    handshake_bytes = json.dumps(secure_handshake).encode()
                    
                    writer.write(len(handshake_bytes).to_bytes(4, 'big'))
                    writer.write(handshake_bytes)
                    await writer.drain()
                    
                    # Read response
                    length_bytes = await asyncio.wait_for(reader.read(4), timeout=2.0)
                    if length_bytes:
                        response_length = int.from_bytes(length_bytes, 'big')
                        response_bytes = await asyncio.wait_for(reader.read(response_length), timeout=2.0)
                        
                        if response_bytes:
                            secure_response = json.loads(response_bytes.decode())
                            response = self.security.verify_and_decrypt_message(secure_response)
                            
                            if response.get('status') == 'ok':
                                processor_id = response.get('node_id')
                                self.backup_processors.append((host, port, processor_id))
                                self.logger.info(f"Discovered processor {processor_id} at {host}:{port}")
                    
                    writer.close()
                    await writer.wait_closed()
                    
                except Exception as e:
                    self.logger.warning(f"Failed to discover processor at {host}:{port}: {e}")

    async def connect_to_processor(self, host: str, port: int) -> None:
        """Connect to a processor node"""
        try:
            reader, writer = await self.connect_to_node(host, port)
            
            # Send handshake
            handshake = {
                'type': 'handshake',
                'node_id': self.node_id,
                'node_type': 'collector'
            }
            
            secure_handshake = self.security.create_secure_message(handshake)
            handshake_bytes = json.dumps(secure_handshake).encode()
            
            writer.write(len(handshake_bytes).to_bytes(4, 'big'))
            writer.write(handshake_bytes)
            await writer.drain()
            
            # Read response
            length_bytes = await reader.read(4)
            if not length_bytes:
                raise ConnectionError("Connection closed during handshake")
            
            response_length = int.from_bytes(length_bytes, 'big')
            response_bytes = await reader.read(response_length)
            
            if not response_bytes:
                raise ConnectionError("Connection closed during handshake")
            
            secure_response = json.loads(response_bytes.decode())
            response = self.security.verify_and_decrypt_message(secure_response)
            
            if response.get('status') != 'ok':
                raise ValueError(f"Handshake failed: {response.get('message', 'Unknown error')}")
            
            processor_id = response.get('node_id')
            if not processor_id:
                raise ValueError("No processor ID in handshake response")
            
            self.processors[processor_id] = (reader, writer)
            self.current_processor = processor_id
            self.logger.info(f"Connected to processor {processor_id} at {host}:{port}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to processor at {host}:{port}: {e}")
            raise

    async def try_failover(self) -> bool:
        """Attempt to failover to a backup processor with discovery"""
        async with self.reconnect_lock:
            # Try to discover new processors first
            known_processors = [(h, p) for h, p, _ in self.backup_processors]
            await self.discover_processors(known_processors)
            
            # Remove current processor from backup list if it failed
            if self.current_processor:
                self.backup_processors = [
                    (h, p, pid) for h, p, pid in self.backup_processors 
                    if pid != self.current_processor
                ]
            
            # Try each backup processor
            for host, port, processor_id in self.backup_processors:
                try:
                    self.logger.info(f"Attempting failover to processor {processor_id} at {host}:{port}")
                    await self.connect_to_processor(host, port)
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to connect to backup processor {processor_id}: {e}")
                    continue
            
            self.logger.error("No backup processors available")
            return False

    async def send_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Send data to current processor with improved failover"""
        if not self.current_processor:
            if not await self.try_failover():
                raise RuntimeError("No processor available")
            
        max_retries = 3
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                reader, writer = self.processors[self.current_processor]
                
                # Create secure message
                message = {
                    'type': 'data',
                    'data': data
                }
                secure_message = self.security.create_secure_message(message)
                message_bytes = json.dumps(secure_message).encode()
                
                # Send message with timeout
                writer.write(len(message_bytes).to_bytes(4, 'big'))
                writer.write(message_bytes)
                await asyncio.wait_for(writer.drain(), timeout=2.0)
                
                # Read response with timeout
                length_bytes = await asyncio.wait_for(reader.read(4), timeout=2.0)
                if not length_bytes:
                    raise ConnectionError("Connection lost while reading response")
                response_length = int.from_bytes(length_bytes, 'big')
                
                response_bytes = await asyncio.wait_for(reader.read(response_length), timeout=2.0)
                if not response_bytes:
                    raise ConnectionError("Connection lost while reading response")

                secure_response = json.loads(response_bytes.decode())
                response = self.security.verify_and_decrypt_message(secure_response)
                
                return response
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Error sending data to processor {self.current_processor}: {e}")
                retry_count += 1
                
                # Remove failed processor
                if self.current_processor in self.processors:
                    reader, writer = self.processors.pop(self.current_processor)
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except:
                        pass
                
                self.current_processor = None
                
                # Try failover if not last retry
                if retry_count < max_retries:
                    if await self.try_failover():
                        continue
                    else:
                        break
                        
        raise ConnectionError(f"Failed to send data after {max_retries} retries. Last error: {last_error}")

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process received message"""
        try:
            message_type = message.get('type')
            
            if message_type == 'heartbeat':
                self.fault_tolerance.handle_heartbeat(message)
                return {'status': 'ok'}
            else:
                self.logger.warning(f"Unknown message type: {message_type}")
                return {'status': 'error', 'message': 'Unknown message type'}
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return {'status': 'error', 'message': str(e)}

    async def stop(self) -> None:
        """Stop the service"""
        try:
            self.logger.info("Stopping DataCollection node...")
            
            # Close processor connections
            for processor_id, (reader, writer) in self.processors.items():
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    self.logger.error(f"Error closing connection to processor {processor_id}: {e}")
                    
            self.processors.clear()
            self.current_processor = None
            
            await super().stop()
            self.logger.info("Stopped DataCollection node")
            
        except Exception as e:
            self.logger.error(f"Error stopping DataCollection node: {e}")
            raise
