from ..common.node import BaseNode
import json
import time
import sqlite3
from pathlib import Path

class StorageService(BaseNode):
    def __init__(self, node_id, host, port, certfile, keyfile, db_path='health_data.db'):
        super().__init__("Storage", node_id, host, port, certfile, keyfile)
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create tables
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS health_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    heart_rate REAL,
                    temperature REAL,
                    blood_pressure TEXT,
                    timestamp REAL
                )
                ''')
                
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    alert_message TEXT,
                    timestamp REAL
                )
                ''')
                
                conn.commit()
        except Exception as e:
            self.logger.error(f"Database initialization error: {e}")
            raise

    def _process_message(self, conn, addr):
        try:
            data = self.secure_receive(conn)
            if not data:
                return

            if data['type'] == 'processed_health_data':
                self._store_health_data(data['data'])
            
            # Send acknowledgment
            ack = {"status": "stored", "timestamp": time.time()}
            self.secure_send(conn, ack)
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def _store_health_data(self, processed_data):
        """Store health data and alerts in database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Store original metrics
                metrics = processed_data['original_metrics']
                cursor.execute('''
                INSERT INTO health_metrics 
                (device_id, heart_rate, temperature, blood_pressure, timestamp)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    metrics['device_id'],
                    metrics.get('heart_rate'),
                    metrics.get('temperature'),
                    metrics.get('blood_pressure'),
                    metrics.get('timestamp')
                ))
                
                # Store alerts if any
                if processed_data.get('alerts'):
                    for alert in processed_data['alerts']:
                        cursor.execute('''
                        INSERT INTO alerts (device_id, alert_message, timestamp)
                        VALUES (?, ?, ?)
                        ''', (
                            metrics['device_id'],
                            alert,
                            time.time()
                        ))
                
                conn.commit()
                self.logger.info(f"Stored health data for device {metrics['device_id']}")
                
        except Exception as e:
            self.logger.error(f"Database storage error: {e}")
            raise

    def handle_message(self, message: dict, conn):
        """Handle received processed health data"""
        try:
            if message.get('type') == 'processed_health_data':
                # Store the processed data
                self._store_health_data(message['data'])
                
                # Send acknowledgment
                ack = {
                    "status": "stored",
                    "timestamp": time.time(),
                    "message_id": message.get('id', 'unknown')
                }
                self.secure_send(conn, ack)
            else:
                self.logger.warning(f"Unknown message type: {message.get('type')}")
                
        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            raise
