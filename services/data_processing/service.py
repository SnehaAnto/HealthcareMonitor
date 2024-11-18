from ..common.node import BaseNode
from ..common.failover_manager import FailoverManager
from ..common.security import SecurityManager
import json
import time
import numpy as np
import asyncio
import logging
import ssl
import socket
from typing import Dict, Any, Optional, Tuple

class DataProcessingService(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, certfile: str, keyfile: str):
        # Call parent class constructor first
        super().__init__(node_id, host, port, certfile, keyfile)
        
        # Initialize service-specific attributes
        self.storage_conn = None
        self.notification_conn = None
        self.collectors = {}  # collector_id -> (reader, writer)
        self.data_buffer = []  # Buffer for data replication
        self.is_primary = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.closing = False

    async def connect_to_storage(self, host: str, port: int) -> None:
        """Connect to storage service"""
        try:
            self.storage_reader, self.storage_conn = await self.connect_to_node(host, port)
            
            # Send handshake
            handshake = {
                'type': 'handshake',
                'node_id': self.node_id,
                'node_type': 'processor'
            }
            
            secure_handshake = self.security.create_secure_message(handshake)
            handshake_bytes = json.dumps(secure_handshake).encode()
            
            self.storage_conn.write(len(handshake_bytes).to_bytes(4, 'big'))
            self.storage_conn.write(handshake_bytes)
            await self.storage_conn.drain()
            
            # Read response
            length_bytes = await self.storage_reader.read(4)
            if not length_bytes:
                raise ConnectionError("Connection closed during handshake")
            response_length = int.from_bytes(length_bytes, 'big')
            
            response_bytes = await self.storage_reader.read(response_length)
            if not response_bytes:
                raise ConnectionError("Connection closed during handshake")
                
            secure_response = json.loads(response_bytes.decode())
            response = self.security.verify_and_decrypt_message(secure_response)
            
            if response.get('status') != 'ok':
                raise RuntimeError(f"Storage handshake failed: {response.get('message')}")
                
            self.logger.info(f"Connected to storage at {host}:{port}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to storage: {e}")
            raise

    async def connect_to_notification(self, host: str, port: int) -> None:
        """Connect to notification service"""
        try:
            self.logger.info(f"Connecting to notification at {host}:{port}...")
            reader, writer = await self.connect_to_node(host, port)
            
            # Send handshake
            handshake = {
                'type': 'handshake',
                'node_id': self.node_id,
                'node_type': 'processor'
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
                raise ConnectionError(f"Notification handshake failed: {response.get('message')}")
                
            self.notification_conn = writer
            self.logger.info(f"Connected to notification at {host}:{port}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to notification: {e}")
            if hasattr(self, 'notification_conn') and self.notification_conn:
                self.notification_conn.close()
                await self.notification_conn.wait_closed()
                self.notification_conn = None
            raise

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process received message"""
        try:
            message_type = message.get('type')
            
            if message_type == 'data':
                if not self.storage_conn:
                    raise RuntimeError("No storage connection available")
                    
                data = message.get('data')
                if not data:
                    raise ValueError("No data provided")
                
                # Forward data to storage
                storage_message = {
                    'type': 'store_data',
                    'processor_id': self.node_id,
                    'data': data
                }
                
                secure_message = self.security.create_secure_message(storage_message)
                message_bytes = json.dumps(secure_message).encode()
                
                # Send to storage
                self.storage_conn.write(len(message_bytes).to_bytes(4, 'big'))
                self.storage_conn.write(message_bytes)
                await self.storage_conn.drain()
                
                # Read storage response
                length_bytes = await self.storage_reader.read(4)
                if not length_bytes:
                    raise ConnectionError("Storage connection lost")
                response_length = int.from_bytes(length_bytes, 'big')
                
                response_bytes = await self.storage_reader.read(response_length)
                if not response_bytes:
                    raise ConnectionError("Storage connection lost")
                    
                secure_response = json.loads(response_bytes.decode())
                storage_response = self.security.verify_and_decrypt_message(secure_response)
                
                if storage_response.get('status') != 'ok':
                    raise RuntimeError(f"Storage error: {storage_response.get('message')}")
                
                # Send notification if configured
                if self.notification_conn:
                    notification_message = {
                        'type': 'notify',
                        'data': {
                            'event': 'data_processed',
                            'patient_id': data.get('patient_id'),
                            'processor_id': self.node_id
                        }
                    }
                    
                    secure_notification = self.security.create_secure_message(notification_message)
                    notification_bytes = json.dumps(secure_notification).encode()
                    
                    self.notification_conn.write(len(notification_bytes).to_bytes(4, 'big'))
                    self.notification_conn.write(notification_bytes)
                    await self.notification_conn.drain()
                
                return {'status': 'ok', 'message': 'Data processed successfully'}
                
            elif message_type == 'handshake':
                node_id = message.get('node_id')
                node_type = message.get('node_type')
                
                if node_type == 'collector':
                    # Store the connection objects from the current context
                    self.collectors[node_id] = (self.current_reader, self.current_writer)
                    self.logger.info(f"Registered collector {node_id}")
                    
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

    async def _process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process health data and generate alerts if needed"""
        try:
            processed_data = data.copy()
            
            # Add processing timestamp
            processed_data['processed_at'] = time.time()
            processed_data['processor_id'] = self.node_id
            
            # Check for anomalies and generate alerts
            if self._check_anomalies(processed_data):
                await self._send_alert(processed_data)
                
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            raise

    def _check_anomalies(self, data: Dict[str, Any]) -> bool:
        """Check for health data anomalies"""
        try:
            # Example anomaly checks (customize based on requirements)
            heart_rate = data.get('heart_rate', 0)
            blood_pressure = data.get('blood_pressure', {})
            temperature = data.get('temperature', 0)
            
            # Define thresholds
            if (heart_rate > 100 or heart_rate < 50 or
                blood_pressure.get('systolic', 0) > 140 or
                blood_pressure.get('diastolic', 0) > 90 or
                temperature > 38.5):
                return True
                
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking anomalies: {e}")
            return False

    async def _send_alert(self, data: Dict[str, Any]) -> None:
        """Send alert to notification service"""
        try:
            if not self.notification_conn:
                self.logger.error("No notification connection available")
                return
                
            alert = {
                'type': 'alert',
                'data': data,
                'timestamp': time.time()
            }
            
            secure_alert = self.security.create_secure_message(alert)
            alert_bytes = json.dumps(secure_alert).encode()
            
            self.notification_conn.write(len(alert_bytes).to_bytes(4, 'big'))
            self.notification_conn.write(alert_bytes)
            await self.notification_conn.drain()
            
        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
            raise

    async def stop(self) -> None:
        """Stop the service with graceful connection closure"""
        if self.closing:
            return
            
        self.closing = True
        try:
            self.logger.info("Stopping DataProcessing node...")
            
            # Close collector connections first
            collector_ids = list(self.collectors.keys())  # Create a copy of keys
            for collector_id in collector_ids:
                reader, writer = self.collectors.get(collector_id, (None, None))
                if writer:
                    try:
                        if not writer.is_closing():
                            try:
                                writer.write_eof()
                                await writer.drain()
                            except Exception:
                                pass  # Ignore EOF errors
                            writer.close()
                            try:
                                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
                            except asyncio.TimeoutError:
                                self.logger.warning(f"Timeout waiting for collector {collector_id} connection to close")
                    except Exception as e:
                        self.logger.error(f"Error closing connection to collector {collector_id}: {str(e)}")
                    finally:
                        self.collectors.pop(collector_id, None)
            
            # Close storage connection
            if self.storage_conn:
                try:
                    if not self.storage_conn.is_closing():
                        try:
                            self.storage_conn.write_eof()
                            await self.storage_conn.drain()
                        except Exception:
                            pass  # Ignore EOF errors
                        self.storage_conn.close()
                        try:
                            await asyncio.wait_for(self.storage_conn.wait_closed(), timeout=1.0)
                        except asyncio.TimeoutError:
                            self.logger.warning("Timeout waiting for storage connection to close")
                except Exception as e:
                    self.logger.error(f"Error closing storage connection: {str(e)}")
                finally:
                    self.storage_conn = None
                
            # Close notification connection
            if self.notification_conn:
                try:
                    if not self.notification_conn.is_closing():
                        try:
                            self.notification_conn.write_eof()
                            await self.notification_conn.drain()
                        except Exception:
                            pass  # Ignore EOF errors
                        self.notification_conn.close()
                        try:
                            await asyncio.wait_for(self.notification_conn.wait_closed(), timeout=1.0)
                        except asyncio.TimeoutError:
                            self.logger.warning("Timeout waiting for notification connection to close")
                except Exception as e:
                    self.logger.error(f"Error closing notification connection: {str(e)}")
                finally:
                    self.notification_conn = None
            
            # Wait a bit for connections to fully close
            await asyncio.sleep(0.5)
            
            # Stop server
            await super().stop()
            self.logger.info("Stopped DataProcessing node")
            
        except Exception as e:
            self.logger.error(f"Error stopping DataProcessing node: {str(e)}")
            raise
        finally:
            self.closing = False

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle client connection"""
        try:
            peer = writer.get_extra_info('peername')
            self.logger.info(f"New connection from {peer}")
            
            # Store current connection context
            self.current_reader = reader
            self.current_writer = writer
            
            while True:
                # Read message length
                length_bytes = await reader.read(4)
                if not length_bytes:
                    break
                    
                message_length = int.from_bytes(length_bytes, 'big')
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
                    self.logger.error(f"Error processing message: {e}")
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
            self.logger.error(f"Error handling client connection: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            self.logger.info(f"Connection closed from {peer}")
