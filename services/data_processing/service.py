from ..common.node import BaseNode
import json
import time
import numpy as np

class DataProcessingService(BaseNode):
    def __init__(self, node_id, host, port, certfile, keyfile):
        super().__init__("DataProcessing", node_id, host, port, certfile, keyfile)
        self.alert_thresholds = {
            'heart_rate': {'min': 60, 'max': 100},
            'temperature': {'min': 97.0, 'max': 99.5},
            'blood_pressure_systolic': {'min': 90, 'max': 140},
            'blood_pressure_diastolic': {'min': 60, 'max': 90}
        }

    def handle_message(self, message: dict, conn):
        """Handle received health metrics"""
        try:
            self.logger.info(f"Received message type: {message.get('type')}")
            
            if message.get('type') == 'health_metrics':
                self.logger.debug(f"Processing health metrics: {message.get('data')}")
                
                processed_data = self._analyze_health_metrics(message['data'])
                self.logger.debug(f"Processed data: {processed_data}")
                
                # Send acknowledgment
                ack = {
                    "status": "processed",
                    "timestamp": time.time(),
                    "message_id": message.get('id', 'unknown')
                }
                self.secure_send(conn, ack)
                
                # Forward processed data if there are alerts
                if processed_data.get('alerts'):
                    self.logger.info(f"Alerts detected: {processed_data['alerts']}")
                    self._forward_to_notification(processed_data)
                
                # Always forward to storage
                self._forward_to_storage(processed_data)
            else:
                self.logger.warning(f"Unknown message type: {message.get('type')}")
                
        except KeyError as e:
            self.logger.error(f"Missing key in message: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error handling message: {str(e)}", exc_info=True)

    def _analyze_health_metrics(self, metrics):
        """Analyze health metrics and detect anomalies"""
        try:
            self.logger.debug(f"Analyzing metrics: {metrics}")
            alerts = []
            
            # Blood pressure analysis
            if 'blood_pressure' in metrics:
                try:
                    systolic, diastolic = map(int, metrics['blood_pressure'].split('/'))
                    if systolic > self.alert_thresholds['blood_pressure_systolic']['max'] or \
                       systolic < self.alert_thresholds['blood_pressure_systolic']['min']:
                        alerts.append(f"Abnormal systolic pressure: {systolic}")
                    if diastolic > self.alert_thresholds['blood_pressure_diastolic']['max'] or \
                       diastolic < self.alert_thresholds['blood_pressure_diastolic']['min']:
                        alerts.append(f"Abnormal diastolic pressure: {diastolic}")
                except ValueError as e:
                    self.logger.error(f"Error parsing blood pressure: {e}")

            # Heart rate analysis
            if 'heart_rate' in metrics:
                heart_rate = metrics['heart_rate']
                if heart_rate > self.alert_thresholds['heart_rate']['max'] or \
                   heart_rate < self.alert_thresholds['heart_rate']['min']:
                    alerts.append(f"Abnormal heart rate: {heart_rate}")

            # Temperature analysis
            if 'temperature' in metrics:
                temp = metrics['temperature']
                if temp > self.alert_thresholds['temperature']['max'] or \
                   temp < self.alert_thresholds['temperature']['min']:
                    alerts.append(f"Abnormal temperature: {temp}")

            result = {
                'original_metrics': metrics,
                'alerts': alerts,
                'analysis_timestamp': time.time(),
                'device_id': metrics.get('device_id')
            }
            
            self.logger.debug(f"Analysis result: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in health metrics analysis: {str(e)}", exc_info=True)
            raise

    def _forward_to_storage(self, processed_data):
        """Forward processed data to storage service"""
        try:
            for peer_conn in self.peers.values():
                self.secure_send(peer_conn, {
                    'type': 'processed_health_data',
                    'data': processed_data,
                    'timestamp': time.time()
                })
        except Exception as e:
            self.logger.error(f"Error forwarding to storage: {e}")

    def _forward_to_notification(self, processed_data):
        """Forward alerts to notification service"""
        try:
            for peer_conn in self.peers.values():
                self.secure_send(peer_conn, {
                    'type': 'processed_health_data',
                    'data': processed_data,
                    'timestamp': time.time()
                })
                self.logger.debug(f"Forwarded processed data to notification service: {processed_data}")
        except Exception as e:
            self.logger.error(f"Error forwarding to notification: {e}")
