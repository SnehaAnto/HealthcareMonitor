from ..common import BaseNode, FailoverManager
import asyncio
import json
import logging
from typing import Dict, Any, Optional
import time

class StorageService(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, certfile: str, keyfile: str):
        super().__init__(node_id, host, port, certfile, keyfile)
        self.data_store = {}  # Simple in-memory storage
        self.backup_nodes = set()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.failover_manager = FailoverManager(node_id)
        self.processors = {}  # Track connected processors

    async def retrieve_data(self, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Retrieve data from storage based on query parameters"""
        try:
            # For now, we'll do a simple patient_id lookup
            patient_id = query.get('patient_id')
            if not patient_id:
                raise ValueError("Query must include patient_id")

            # Search through stored data
            for data_id, data in self.data_store.items():
                if data.get('patient_id') == patient_id:
                    self.logger.info(f"Retrieved data for patient {patient_id}")
                    return data

            self.logger.info(f"No data found for patient {patient_id}")
            return None

        except Exception as e:
            self.logger.error(f"Error retrieving data: {e}")
            raise

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process received message"""
        try:
            message_type = message.get('type')
            
            if message_type == 'store_data':
                data = message.get('data')
                if not data:
                    raise ValueError("No data provided")
                    
                # Generate unique ID for data
                data_id = f"{message.get('processor_id')}_{time.time()}"
                self.data_store[data_id] = data
                
                self.logger.info(f"Stored and replicated data with ID {data_id}")
                return {'status': 'ok', 'data_id': data_id}
                
            elif message_type == 'retrieve_data':
                query = message.get('query')
                if not query:
                    raise ValueError("No query provided")
                    
                result = await self.retrieve_data(query)
                return {
                    'status': 'ok',
                    'data': result
                }
                
            elif message_type == 'handshake':
                node_id = message.get('node_id')
                node_type = message.get('node_type')
                
                if node_type == 'processor':
                    self.processors[node_id] = True
                    self.logger.info(f"Registered processor {node_id}")
                    
                return {
                    'status': 'ok',
                    'node_id': self.node_id
                }
                
            else:
                raise ValueError(f"Unknown message type: {message_type}")
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def _store_data(self, data: Dict[str, Any]) -> None:
        """Store data and handle replication"""
        try:
            # Generate unique ID for data
            data_id = f"{data.get('processor_id')}_{data.get('processed_at')}"
            self.data_store[data_id] = data
            
            # Replicate to backup nodes
            replication_message = {
                'type': 'replicate',
                'data': {data_id: data}
            }
            
            secure_message = self.security.create_secure_message(replication_message)
            message_bytes = json.dumps(secure_message).encode()
            
            for backup in self.backup_nodes:
                try:
                    writer = self.connections.get(backup)
                    if writer:
                        writer.write(len(message_bytes).to_bytes(4, 'big'))
                        writer.write(message_bytes)
                        await writer.drain()
                except Exception as e:
                    self.logger.error(f"Failed to replicate to backup {backup}: {e}")
            
            self.logger.info(f"Stored and replicated data with ID {data_id}")
            
        except Exception as e:
            self.logger.error(f"Error storing data: {e}")
            raise

    async def _retrieve_data(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve data based on query parameters"""
        try:
            # Simple query implementation - can be enhanced based on requirements
            if not query:
                return self.data_store
                
            result = {}
            for data_id, data in self.data_store.items():
                match = True
                for key, value in query.items():
                    if data.get(key) != value:
                        match = False
                        break
                if match:
                    result[data_id] = data
                    
            return result
            
        except Exception as e:
            self.logger.error(f"Error retrieving data: {e}")
            raise

    async def _replicate_data(self, data: Dict[str, Any]) -> None:
        """Handle incoming data replication"""
        try:
            self.data_store.update(data)
            self.logger.info(f"Replicated {len(data)} data items")
        except Exception as e:
            self.logger.error(f"Error replicating data: {e}")
            raise

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle client connection with better error handling"""
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

                # Process message
                try:
                    secure_message = json.loads(message_bytes.decode())
                    message = self.security.verify_and_decrypt_message(secure_message)
                    
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
