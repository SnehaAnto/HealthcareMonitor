from datetime import datetime, timedelta
import threading
import time
import logging

class HeartbeatMonitor:
    def __init__(self, node_id, timeout_seconds=30):
        self.node_id = node_id
        self.timeout_seconds = timeout_seconds
        self.last_heartbeats = {}
        self.node_status = {}
        self.logger = logging.getLogger('Heartbeat')
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
        self.is_running = False
        self.monitoring_thread = None

    def start(self):
        """Start the heartbeat monitoring"""
        if not self.is_running:
            self.is_running = True
            self._stop_event.clear()
            self.monitoring_thread = threading.Thread(target=self._monitor_heartbeats)
            self.monitoring_thread.daemon = True
            self.monitoring_thread.start()
            self.logger.info(f"Heartbeat monitor started for node {self.node_id}")

    def stop(self):
        """Stop the heartbeat monitoring"""
        if self.is_running:
            self.is_running = False
            self._stop_event.set()
            if self.monitoring_thread:
                self.monitoring_thread.join(timeout=2)
            self.logger.info(f"Heartbeat monitor stopped for node {self.node_id}")

    def update_node_status(self, node_id: str, status_info: dict):
        """Update status information for a node"""
        with self.lock:
            if node_id not in self.node_status:
                self.node_status[node_id] = {}
            self.node_status[node_id].update(status_info)
            self.node_status[node_id]['last_updated'] = time.time()
            self.logger.debug(f"Updated status for node {node_id}: {status_info}")

    def get_node_status(self, node_id: str) -> dict:
        """Get status information for a node"""
        with self.lock:
            return self.node_status.get(node_id, {})

    def record_heartbeat(self, node_id):
        """Record a heartbeat from a node"""
        with self.lock:
            self.last_heartbeats[node_id] = datetime.now()
            self.node_status[node_id] = True
            self.logger.debug(f"Recorded heartbeat from node {node_id}")
            
    def is_node_alive(self, node_id):
        """Check if a node is considered alive"""
        with self.lock:
            return self.node_status.get(node_id, False)
            
    def _monitor_heartbeats(self):
        """Monitor heartbeats and update node status"""
        while not self._stop_event.is_set():
            try:
                with self.lock:
                    current_time = time.time()
                    for node_id, last_heartbeat in self.last_heartbeats.items():
                        if current_time - last_heartbeat > self.timeout_seconds:
                            self.update_node_status(node_id, {
                                'active': False,
                                'last_seen': last_heartbeat
                            })
                            self.logger.warning(f"Node {node_id} considered failed - no heartbeat")
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error in heartbeat monitoring: {e}")
                time.sleep(1)