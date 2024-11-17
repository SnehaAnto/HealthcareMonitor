from ..common.node import BaseNode
import json
import time

class DataCollectionService(BaseNode):
    def __init__(self, node_id, host, port, certfile, keyfile):
        super().__init__("DataCollection", node_id, host, port, certfile, keyfile)
        self.health_data = {}

    def handle_message(self, message: dict, conn):
        """Handle received health data"""
        try:
            # Store health data
            self._store_health_data(message)
            
            # Send acknowledgment
            ack = {
                "status": "received",
                "timestamp": time.time(),
                "message_id": message.get('id', 'unknown')
            }
            self.secure_send(conn, ack)
            
            # Forward to processing service
            self._forward_to_processing(message)
            
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            raise

    def _store_health_data(self, metrics):
        """Temporarily store health data"""
        device_id = metrics.get('device_id')
        self.health_data[device_id] = {
            'timestamp': time.time(),
            'metrics': metrics
        }
        self.logger.info(f"Stored health data for device {device_id}")

    def _forward_to_processing(self, metrics):
        """Forward data to processing service"""
        try:
            for peer_conn in self.peers.values():
                self.secure_send(peer_conn, {
                    'type': 'health_metrics',
                    'data': metrics,
                    'source': self.node_id,
                    'timestamp': time.time()
                })
        except Exception as e:
            self.logger.error(f"Error forwarding to processing: {e}")

    def simulate_health_data(self):
        """Method to simulate incoming health data for testing"""
        while True:
            test_data = {
                'device_id': 'dev001',
                'heart_rate': 75,
                'temperature': 98.6,
                'blood_pressure': '120/80',
                'timestamp': time.time()
            }
            self._store_health_data(test_data)
            time.sleep(5)  # Simulate data every 5 seconds
