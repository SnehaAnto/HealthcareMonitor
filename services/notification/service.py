from ..common import BaseNode, FailoverManager
import time
import asyncio
import threading
import logging
import socket
import ssl
import json
from typing import Dict, Any

class NotificationService(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, certfile: str, keyfile: str):
        super().__init__(node_id, host, port, certfile, keyfile)
        self.subscribers = {}  # subscriber_id -> connection
        self.alert_history = []  # Keep track of recent alerts
        self.logger = logging.getLogger(self.__class__.__name__)
        self.notifications = []
        self.failover_manager = FailoverManager(node_id)

    async def start(self):
        """Start the notification service"""
        try:
            self.logger.info(f"Starting Notification service {self.node_id}...")
            result = await super().start()
            if result:
                self.logger.info(f"Notification service {self.node_id} started successfully")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Failed to start Notification service: {e}")
            return False

    async def stop(self):
        """Stop the notification service"""
        try:
            self.logger.info(f"Stopping Notification service {self.node_id}")
            await super().stop()
            self.logger.info(f"Stopped Notification service {self.node_id}")
        except Exception as e:
            self.logger.error(f"Error stopping Notification service: {e}")
            raise

    async def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming messages"""
        try:
            self.logger.debug(f"Processing message: {message}")
            message_type = message.get('type')
            
            if message_type == 'handshake':
                node_id = message.get('node_id')
                node_type = message.get('node_type')
                
                if node_type == 'subscriber':
                    self.subscribers[node_id] = self.current_writer
                    
                self.logger.info(f"Handshake from {node_type} node {node_id}")
                return {
                    'type': 'handshake_response',
                    'node_id': self.node_id,
                    'status': 'ok'
                }
                
            elif message_type == 'alert':
                # Process and distribute alert
                await self._handle_alert(message.get('data', {}))
                return {'status': 'ok'}
                
            elif message_type == 'subscribe':
                # Handle subscription request
                subscriber_id = message.get('subscriber_id')
                self.subscribers[subscriber_id] = self.current_writer
                return {'status': 'ok', 'message': 'Subscribed successfully'}
                
            elif message_type == 'send_notification':
                data = message.get('data', {})
                self.logger.info(f"Sending notification for device {data.get('device_id')}")
                
                try:
                    # Send notification (implement actual notification logic here)
                    # For now, just log it
                    self.logger.info(f"Notification sent: {data}")
                    return {'status': 'success'}
                except Exception as e:
                    self.logger.error(f"Error sending notification: {e}")
                    return {'status': 'error', 'message': str(e)}
            
            elif message_type == 'notify':
                # Handle notification event
                event_data = message.get('data', {})
                await self.send_notification(event_data)
                return {'status': 'ok', 'message': 'Notification processed'}
            
            self.logger.warning(f"Unknown message type: {message_type}")
            return {'status': 'error', 'message': f'Unknown message type: {message_type}'}
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return {'status': 'error', 'message': str(e)}

    async def _handle_alert(self, alert_data: dict) -> None:
        """Process and distribute alert to subscribers"""
        try:
            # Add alert to history
            self.alert_history.append(alert_data)
            if len(self.alert_history) > 100:  # Limit history size
                self.alert_history = self.alert_history[-100:]
                
            # Prepare alert message
            alert_message = {
                'type': 'alert_notification',
                'data': alert_data
            }
            
            secure_message = self.security.create_secure_message(alert_message)
            message_bytes = json.dumps(secure_message).encode()
            
            # Distribute to all subscribers
            for subscriber_id, writer in self.subscribers.items():
                try:
                    writer.write(len(message_bytes).to_bytes(4, 'big'))
                    writer.write(message_bytes)
                    await writer.drain()
                except Exception as e:
                    self.logger.error(f"Failed to send alert to subscriber {subscriber_id}: {e}")
                    # Remove failed subscriber
                    self.subscribers.pop(subscriber_id)
                    
        except Exception as e:
            self.logger.error(f"Error handling alert: {e}")
            raise

    async def send_notification(self, data: dict):
        """Send notification to all subscribers"""
        try:
            notification = {
                'timestamp': time.time(),
                'data': data
            }
            
            self.notifications.append(notification)
            
            # Prepare notification message for subscribers
            notification_message = {
                'type': 'notification',
                'data': notification
            }
            
            secure_message = self.security.create_secure_message(notification_message)
            message_bytes = json.dumps(secure_message).encode()
            
            # Send to all subscribers
            for subscriber_id, writer in self.subscribers.items():
                try:
                    writer.write(len(message_bytes).to_bytes(4, 'big'))
                    writer.write(message_bytes)
                    await writer.drain()
                    self.logger.debug(f"Sent notification to subscriber {subscriber_id}")
                except Exception as e:
                    self.logger.error(f"Failed to send notification to subscriber {subscriber_id}: {e}")
                    # Remove failed subscriber
                    self.subscribers.pop(subscriber_id)
            
            self.logger.info(f"Sent notification: {data.get('event', 'unknown_event')}")
            
        except Exception as e:
            self.logger.error(f"Error sending notification: {e}")
            raise
