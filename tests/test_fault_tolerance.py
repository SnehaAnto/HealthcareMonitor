import asyncio
import logging
import os
import random
import time
from typing import List
import ssl
import subprocess
from pathlib import Path

from services.data_collection.service import DataCollectionService
from services.data_processing.service import DataProcessingService
from services.notification.service import NotificationService
from services.storage.service import StorageService 

def setup_logging():
    """Configure logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('SystemTest')

class SystemTest:
    def __init__(self):
        """Initialize test environment"""
        self.logger = setup_logging()
        self.base_port = 5000
        self.host = '127.0.0.1'
        self.cert_dir = 'tests/certs'
        self.collectors = []
        self.processors = []
        self.storage = None
        self.notification = None
        self.is_running = True

    async def setup(self):
        """Set up test environment"""
        try:
            self.logger.info("Setting up services...")
            
            # Generate test certificates
            self.logger.info("Generating test certificates...")
            await self.generate_test_certificates()
            self.logger.info("Generated test certificates successfully")
            
            # Start services
            await self.start_services()
            
            self.logger.info("All services started successfully")
            
        except Exception as e:
            self.logger.error(f"Setup failed: {e}")
            await self.cleanup()
            raise

    async def run_tests(self):
        """Run all tests"""
        try:
            await self.test_load_balancing()
            await self.test_failover()
            self.logger.info("All tests passed!")
        except Exception as e:
            self.logger.error(f"Test failed: {e}")
            raise
        finally:
            await self.cleanup()

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
            await self.connect_processor(processor)
            self.processors.append(processor)
            
        # Start Collection nodes
        self.logger.info("Starting Collection nodes...")
        for i in range(3):
            collector = DataCollectionService(
                f"collector_{i}",
                self.host,
                self.base_port + 1 + i,
                f"{self.cert_dir}/server.crt",
                f"{self.cert_dir}/server.key"
            )
            await collector.start()
            await asyncio.sleep(1)
            
            # Connect to processors
            await self.connect_collector(collector)
            self.collectors.append(collector)

    async def connect_processor(self, processor: DataProcessingService):
        """Connect processor to storage and notification services"""
        # Connect to storage
        self.logger.info(f"Connecting {processor.node_id} to storage (attempt 1)")
        await processor.connect_to_storage(self.host, self.storage.port)
        
        # Connect to notification
        self.logger.info(f"Connecting {processor.node_id} to notification (attempt 1)")
        await processor.connect_to_notification(self.host, self.notification.port)

    async def connect_collector(self, collector: DataCollectionService):
        """Connect collector to all processors"""
        for processor in self.processors:
            self.logger.info(f"Connecting {collector.node_id} to {processor.node_id} (attempt 1)")
            await collector.connect_to_processor(self.host, processor.port)

    async def cleanup(self):
        """Clean up test environment"""
        try:
            # Stop all services in reverse order
            for collector in self.collectors:
                await collector.stop()
                
            for processor in self.processors:
                await processor.stop()
                
            if self.notification:
                await self.notification.stop()
                
            if self.storage:
                await self.storage.stop()
                
            # Clean up certificate directory
            cert_dir = Path(self.cert_dir)
            if cert_dir.exists():
                for file in cert_dir.glob('*'):
                    file.unlink()
                cert_dir.rmdir()
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

async def main():
    """Main test function"""
    tester = SystemTest()
    try:
        await tester.setup()
        await tester.run_tests()
    except Exception as e:
        logging.error(f"Test suite failed: {e}")
        raise
    finally:
        await tester.cleanup()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Tests interrupted by user")
    except Exception as e:
        logging.error(f"Tests failed: {e}")
        raise 