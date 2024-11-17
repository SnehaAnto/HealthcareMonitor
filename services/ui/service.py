from ..common.node import BaseNode
import json
import time
import threading
from flask import Flask, render_template_string
from flask_socketio import SocketIO
import logging

class UIService(BaseNode):
    def __init__(self, node_id, host, port, certfile, keyfile, ui_port=8000):
        super().__init__("UI", node_id, host, port, certfile, keyfile)
        self.ui_port = ui_port
        self.app = Flask(__name__)
        self.socketio = SocketIO(self.app)
        self.health_data = {}
        self.setup_routes()
        
    def setup_routes(self):
        @self.app.route('/')
        def home():
            return render_template_string(self.get_html())

    def get_html(self):
        """Return the HTML for the dashboard"""
        return """
        <!DOCTYPE html>
        <html>
            <head>
                <title>Healthcare Monitoring Dashboard</title>
                <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
                <style>
                    body { 
                        font-family: Arial, sans-serif;
                        margin: 20px;
                        background-color: #f0f2f5;
                    }
                    .dashboard {
                        max-width: 800px;
                        margin: 0 auto;
                    }
                    .alert {
                        background-color: #ffe0e0;
                        border-left: 4px solid #ff0000;
                        padding: 10px;
                        margin: 10px 0;
                        border-radius: 4px;
                    }
                    .metrics {
                        background-color: white;
                        padding: 15px;
                        margin: 10px 0;
                        border-radius: 4px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    .timestamp {
                        color: #666;
                        font-size: 0.8em;
                    }
                </style>
            </head>
            <body>
                <div class="dashboard">
                    <h1>Healthcare Monitoring Dashboard</h1>
                    <div id="alerts"></div>
                    <div id="metrics"></div>
                </div>
                <script>
                    const socket = io();
                    
                    socket.on('health_update', function(data) {
                        updateDashboard(data);
                    });

                    function updateDashboard(data) {
                        if (data.alerts && data.alerts.length > 0) {
                            const alertsDiv = document.getElementById('alerts');
                            const alertElement = document.createElement('div');
                            alertElement.className = 'alert';
                            alertElement.innerHTML = `
                                <strong>Alerts for Device ${data.device_id}:</strong><br>
                                ${data.alerts.join('<br>')}
                                <div class="timestamp">
                                    ${new Date(data.timestamp * 1000).toLocaleString()}
                                </div>
                            `;
                            alertsDiv.insertBefore(alertElement, alertsDiv.firstChild);
                        }

                        const metricsDiv = document.getElementById('metrics');
                        const metricsElement = document.createElement('div');
                        metricsElement.className = 'metrics';
                        metricsElement.innerHTML = `
                            <h3>Latest Metrics - Device ${data.original_metrics.device_id}</h3>
                            <p>Heart Rate: ${data.original_metrics.heart_rate} bpm</p>
                            <p>Temperature: ${data.original_metrics.temperature}°F</p>
                            <p>Blood Pressure: ${data.original_metrics.blood_pressure}</p>
                            <div class="timestamp">
                                ${new Date(data.original_metrics.timestamp * 1000).toLocaleString()}
                            </div>
                        `;
                        metricsDiv.insertBefore(metricsElement, metricsDiv.firstChild);
                    }
                </script>
            </body>
        </html>
        """

    def start(self):
        """Start both the node service and the web interface"""
        super().start()
        # Start Flask in a separate thread
        self.web_thread = threading.Thread(target=self._run_web_server)
        self.web_thread.daemon = True
        self.web_thread.start()
        self.logger.info(f"UI service started on port {self.ui_port}")

    def _run_web_server(self):
        """Run the Flask server"""
        self.socketio.run(self.app, host="0.0.0.0", port=self.ui_port, debug=False)

    def handle_message(self, message: dict, conn):
        """Handle received health data and alerts"""
        try:
            if message.get('type') in ['health_alert', 'processed_health_data']:
                data = message.get('data', {})
                
                # Store the latest data
                device_id = data.get('device_id')
                if device_id:
                    self.health_data[device_id] = data

                # Emit to web clients
                self.socketio.emit('health_update', data)
                
                # Send acknowledgment
                ack = {
                    "status": "displayed",
                    "timestamp": time.time(),
                    "message_id": message.get('id', 'unknown')
                }
                self.secure_send(conn, ack)
            
        except Exception as e:
            self.logger.error(f"Error handling message: {e}", exc_info=True)
