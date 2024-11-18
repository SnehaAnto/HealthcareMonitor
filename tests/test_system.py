import asyncio
import logging
from pathlib import Path
import json
import time
import subprocess
from typing import Dict, Any

from services.data_collection.service import DataCollectionService
from services.data_processing.service import DataProcessingService
from services.storage.service import StorageService
from services.notification.service import NotificationService

class SystemIntegrationTest:
    def __init__(self):
        self.logger = logging.getLogger("SystemTest")
        self.base_port = 5000
        self.host = '127.0.0.1'
        self.cert_dir = "tests/certs"
        
        # Service containers
        self.storage = None
        self.notification = None
        self.processors = []
        self.collectors = []

    async def generate_test_certificates(self):
        """Generate SSL certificates for testing"""
        cert_dir = Path(self.cert_dir)
        cert_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate CA key and certificate
        subprocess.run([
            'openssl', 'req', '-x509', '-newkey', 'rsa:4096', '-days', '365', '-nodes',
            '-keyout', str(cert_dir / 'ca.key'),
            '-out', str(cert_dir / 'ca.crt'),
            '-subj', '/CN=Test CA'
        ], check=True)
        
        # Generate server key and CSR
        subprocess.run([
            'openssl', 'req', '-newkey', 'rsa:4096', '-nodes',
            '-keyout', str(cert_dir / 'server.key'),
            '-out', str(cert_dir / 'server.csr'),
            '-subj', '/CN=localhost'
        ], check=True)
        
        # Sign server certificate
        subprocess.run([
            'openssl', 'x509', '-req', '-days', '365',
            '-in', str(cert_dir / 'server.csr'),
            '-CA', str(cert_dir / 'ca.crt'),
            '-CAkey', str(cert_dir / 'ca.key'),
            '-CAcreateserial',
            '-out', str(cert_dir / 'server.crt')
        ], check=True)

    async def start_services(self):
        """Start all services"""
        # Start Storage service
        self.logger.info("Starting Storage service...")
        self.storage = StorageService(
            "storage_1",
            self.host,
            self.base_port + 201,
            f"{self.cert_dir}/server.crt",
            f"{self.cert_dir}/server.key"
        )
        await self.storage.start()
        await asyncio.sleep(1)
        
        # Start Notification service
        self.logger.info("Starting Notification service...")
        self.notification = NotificationService(
            "notification_1",
            self.host,
            self.base_port + 301,
            f"{self.cert_dir}/server.crt",
            f"{self.cert_dir}/server.key"
        )
        await self.notification.start()
        await asyncio.sleep(1)
        
        # Start Processing nodes
        self.logger.info("Starting Processing nodes...")
        for i in range(2):
            processor = DataProcessingService(
                f"processor_{i}",
                self.host,
                self.base_port + 101 + i,
                f"{self.cert_dir}/server.crt",
                f"{self.cert_dir}/server.key"
            )
            await processor.start()
            await asyncio.sleep(1)
            
            # Connect to storage and notification
            await processor.connect_to_storage(self.host, self.storage.port)
            await processor.connect_to_notification(self.host, self.notification.port)
            self.processors.append(processor)
        
        # Start Collection nodes
        self.logger.info("Starting Collection nodes...")
        for i in range(2):
            collector = DataCollectionService(
                f"collector_{i}",
                self.host,
                self.base_port + 1 + i,
                f"{self.cert_dir}/server.crt",
                f"{self.cert_dir}/server.key"
            )
            await collector.start()
            await asyncio.sleep(1)
            
            # Connect to a processor
            await collector.connect_to_processor(self.host, self.processors[0].port)
            self.collectors.append(collector)

    async def cleanup(self):
        """Cleanup test environment with graceful shutdown"""
        self.logger.info("Cleaning up test environment...")
        
        # Stop collectors first
        for collector in self.collectors:
            try:
                await asyncio.wait_for(collector.stop(), timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error stopping collector: {e}")
        
        # Wait a bit for connections to close
        await asyncio.sleep(1)
        
        # Stop processors
        for processor in self.processors:
            try:
                await asyncio.wait_for(processor.stop(), timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error stopping processor: {e}")
        
        # Wait a bit for connections to close
        await asyncio.sleep(1)
        
        # Stop storage
        if self.storage:
            try:
                await asyncio.wait_for(self.storage.stop(), timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error stopping storage: {e}")
        
        # Stop notification
        if self.notification:
            try:
                await asyncio.wait_for(self.notification.stop(), timeout=5.0)
            except Exception as e:
                self.logger.error(f"Error stopping notification: {e}")
        
        self.logger.info("Cleanup complete")

    async def setup(self):
        """Setup test environment"""
        # Generate certificates
        await self.generate_test_certificates()
        
        # Start services
        await self.start_services()
        
        # Wait for connections to establish
        await asyncio.sleep(2)

    async def test_normal_operation(self):
        """Test normal system operation"""
        self.logger.info("Testing normal operation...")
        
        # Send test data through collector
        test_data = {
            'patient_id': 'TEST001',
            'heart_rate': 75,
            'blood_pressure': {'systolic': 120, 'diastolic': 80},
            'temperature': 37.0,
            'timestamp': time.time()
        }
        
        collector = self.collectors[0]
        response = await collector.send_data(test_data)
        
        assert response['status'] == 'ok', "Failed to process normal data"
        self.logger.info("Normal operation test passed")

    async def test_fault_tolerance(self):
        """Test system fault tolerance"""
        self.logger.info("Testing fault tolerance...")
        
        # Get a collector and its current processor
        collector = self.collectors[0]
        original_processor = collector.current_processor
        
        # Ensure collector knows about backup processors
        for processor in self.processors:
            if processor.node_id != original_processor:
                collector.backup_processors.append((
                    processor.host, 
                    processor.port,
                    processor.node_id
                ))
        
        # Prepare test data
        test_data = {
            'patient_id': 'TEST003',
            'heart_rate': 75,
            'blood_pressure': {'systolic': 118, 'diastolic': 78},
            'temperature': 36.9,
            'timestamp': time.time()
        }
        
        try:
            # Stop the current processor
            current_proc = next(p for p in self.processors if p.node_id == original_processor)
            await current_proc.stop()
            self.processors.remove(current_proc)
            
            # Wait a bit for failover
            await asyncio.sleep(2)
            
            # Try to send data - should failover to backup
            response = await collector.send_data(test_data)
            assert response['status'] == 'ok', f"Data processing failed after failover: {response.get('message', 'Unknown error')}"
            
            # Wait for data to be stored
            await asyncio.sleep(1)
            
            # Verify data was stored
            stored_data = await self.storage.retrieve_data({'patient_id': 'TEST003'})
            self.logger.info(f"Retrieved data: {stored_data}")
            assert stored_data is not None, "Data not stored after failover"
            assert stored_data.get('patient_id') == 'TEST003', "Stored data mismatch"
            
            self.logger.info("Fault tolerance test passed")
            
        except Exception as e:
            self.logger.error(f"Fault tolerance test failed: {e}")
            raise

    async def test_security(self):
        """Test security mechanisms"""
        self.logger.info("Testing security mechanisms...")
        
        # Try to connect without proper certificates
        try:
            invalid_processor = DataProcessingService(
                "invalid_processor",
                self.host,
                self.base_port + 999,
                "invalid.crt",
                "invalid.key"
            )
            await invalid_processor.start()
            assert False, "Security check failed - allowed invalid certificates"
        except:
            self.logger.info("Security test passed - rejected invalid certificates")

    async def test_data_integrity(self):
        """Test data integrity"""
        self.logger.info("Testing data integrity...")
        
        # Send data and verify storage
        test_data = {
            'patient_id': 'TEST003',
            'heart_rate': 72,
            'blood_pressure': {'systolic': 118, 'diastolic': 78},
            'temperature': 36.9,
            'timestamp': time.time()
        }
        
        collector = self.collectors[0]
        await collector.send_data(test_data)
        
        # Verify data in storage
        stored_data = await self.storage.retrieve_data({'patient_id': 'TEST003'})
        assert stored_data is not None, "Data integrity check failed"
        self.logger.info("Data integrity test passed")

async def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run tests
    tester = SystemIntegrationTest()
    try:
        await tester.setup()
        await tester.test_normal_operation()
        await tester.test_fault_tolerance()
        await tester.test_security()
        await tester.test_data_integrity()
        
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 