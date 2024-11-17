from ..common.node import BaseNode
import time
import asyncio
import threading

class NotificationService(BaseNode):
    def __init__(self, node_id, host, port, certfile, keyfile):
        super().__init__("Notification", node_id, host, port, certfile, keyfile)
        self.alert_queue = asyncio.Queue()
        self.notification_handlers = {
            'alert': self._send_alert_notification,
            'console': self._send_console_notification
        }
        # Create event loop in new thread
        self.loop = None
        self.loop_thread = None
        
    def start(self):
        """Start the service with alert processing"""
        super().start()
        # Start alert processing in separate thread
        self.loop_thread = threading.Thread(target=self._run_alert_loop)
        self.loop_thread.daemon = True
        self.loop_thread.start()

    def _run_alert_loop(self):
        """Run async alert processing in separate thread"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._process_alert_queue())

    def handle_message(self, message: dict, conn):
        """Handle received health data and alerts"""
        try:
            message_type = message.get('type')
            self.logger.debug(f"Received message: {message}")

            if message_type == 'processed_health_data':
                # Forward to UI service
                for peer_conn in self.peers.values():
                    self.secure_send(peer_conn, {
                        'type': 'processed_health_data',
                        'data': message['data'],
                        'timestamp': time.time()
                    })
                    self.logger.debug("Forwarded processed data to UI service")
                
                # Send acknowledgment
                ack = {
                    "status": "forwarded",
                    "timestamp": time.time(),
                    "message_id": message.get('id', 'unknown')
                }
                self.secure_send(conn, ack)
            else:
                self.logger.warning(f"Unknown message type: {message_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling message: {e}", exc_info=True)

    async def _process_alert_queue(self):
        """Process queued alerts"""
        while True:
            try:
                alert_data = await self.alert_queue.get()
                
                # Process alert through all configured channels
                for handler in self.notification_handlers.values():
                    try:
                        await handler(alert_data)
                    except Exception as e:
                        self.logger.error(f"Error in notification handler: {e}")
                
                self.alert_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error processing alert queue: {e}")
                await asyncio.sleep(1)

    async def _send_alert_notification(self, alert_data):
        """Send alert notification"""
        self.logger.info(f"Would send alert to UI: {alert_data['alerts']}")

    async def _send_console_notification(self, alert_data):
        """Send console notification"""
        self.logger.warning(f"HEALTH ALERT for device {alert_data['device_id']}:")
        for alert in alert_data['alerts']:
            self.logger.warning(f"- {alert}")
