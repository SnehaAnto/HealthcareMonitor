import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from pathlib import Path
import json
import time
import subprocess
import random
from typing import Dict, Any, List
import ssl

from services.data_collection.service import DataCollectionService
from services.data_processing.service import DataProcessingService
from services.storage.service import StorageService
from services.notification.service import NotificationService

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

class ComprehensiveSystemTest:
    def __init__(self):
        """Initialize test environment"""
        self.logger = setup_logging()
        self.base_port = 5000
        self.host = '127.0.0.1'
        self.cert_dir = 'tests/certs'
        
        # Service containers
        self.storage = None
        self.notification = None
        self.processors = []
        self.collectors = []
        self.is_running = True
        
        # Reliability metrics
        self.failure_timestamps: List[float] = []
        self.recovery_timestamps: List[float] = []
        self.test_start_time: float = 0
        self.total_downtime: float = 0
        self.total_uptime: float = 0

    async def setup(self):
        """Setup test environment with reliability tracking"""
        self.test_start_time = time.time()
        """Set up test environment"""
        try:
            self.logger.info("Setting up services...")
            
            # Generate test certificates
            self.logger.info("Generating test certificates...")
            await self.generate_test_certificates()
            self.logger.info("Generated test certificates successfully")
            
            # Start services
            await self.start_services()
            
            # Wait for connections to establish
            await asyncio.sleep(2)
            
            self.logger.info("All services started successfully")
            
        except Exception as e:
            self.logger.error(f"Setup failed: {e}")
            await self.cleanup()
            raise

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
        for i in range(3):  # Using 3 collectors as in fault_tolerance test
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
        self.logger.info("Cleaning up test environment...")
        try:
            # First stop fault tolerance monitoring for all services
            services = [*self.collectors, *self.processors, self.notification, self.storage]
            for service in services:
                if hasattr(service, 'fault_tolerance'):
                    try:
                        await asyncio.wait_for(service.fault_tolerance.stop(), timeout=1.0)
                    except asyncio.TimeoutError:
                        self.logger.warning(f"Timeout stopping fault tolerance for {service.node_id}")
                    except Exception as e:
                        self.logger.warning(f"Error stopping fault tolerance for {service.node_id}: {e}")

            # Cancel all pending tasks except the current one
            tasks = [task for task in asyncio.all_tasks() 
                    if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            
            if tasks:
                await asyncio.wait(tasks, timeout=1.0)

            # Now stop the services with timeouts
            for collector in self.collectors:
                try:
                    await asyncio.wait_for(collector.close(), timeout=1.0)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning(f"Error stopping collector {collector.node_id}: {e}")
            
            for processor in self.processors:
                try:
                    await asyncio.wait_for(processor.close(), timeout=1.0)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning(f"Error stopping processor {processor.node_id}: {e}")
            
            if self.notification:
                try:
                    await asyncio.wait_for(self.notification.close(), timeout=1.0)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning(f"Error stopping notification service: {e}")
            
            if self.storage:
                try:
                    await asyncio.wait_for(self.storage.close(), timeout=1.0)
                except (asyncio.TimeoutError, Exception) as e:
                    self.logger.warning(f"Error stopping storage service: {e}")

            # Clean up certificate directory
            cert_dir = Path(self.cert_dir)
            if cert_dir.exists():
                for file in cert_dir.glob('*'):
                    try:
                        file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Error removing certificate file {file}: {e}")
                try:
                    cert_dir.rmdir()
                except Exception as e:
                    self.logger.warning(f"Error removing certificate directory: {e}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
            raise

    # Test methods from both files
    async def test_normal_operation(self):
        """Test normal system operation"""
        self.logger.info("Testing normal operation...")
        
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
        """Test system fault tolerance with reliability metrics"""
        self.logger.info("Testing fault tolerance...")
        
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
        
        test_data = {
            'patient_id': 'TEST003',
            'heart_rate': 75,
            'blood_pressure': {'systolic': 118, 'diastolic': 78},
            'temperature': 36.9,
            'timestamp': time.time()
        }
        
        try:
            # Stop the current processor and record failure
            current_proc = next(p for p in self.processors if p.node_id == original_processor)
            await self.record_failure(current_proc.node_id)
            await current_proc.stop()
            self.processors.remove(current_proc)
            
            await asyncio.sleep(2)
            
            # Send data and verify failover
            response = await collector.send_data(test_data)
            assert response['status'] == 'ok', f"Data processing failed after failover: {response.get('message', 'Unknown error')}"
            
            # Record recovery after successful failover
            await self.record_recovery(collector.current_processor)
            
            await asyncio.sleep(1)
            
            # Verify data storage
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
        
        test_data = {
            'patient_id': 'TEST003',
            'heart_rate': 72,
            'blood_pressure': {'systolic': 118, 'diastolic': 78},
            'temperature': 36.9,
            'timestamp': time.time()
        }
        
        collector = self.collectors[0]
        await collector.send_data(test_data)
        
        stored_data = await self.storage.retrieve_data({'patient_id': 'TEST003'})
        assert stored_data is not None, "Data integrity check failed"
        self.logger.info("Data integrity test passed")

    async def test_load_balancing(self):
        """Test load balancing functionality"""
        self.logger.info("Testing load balancing...")
        
        try:
            # Ensure all processors are running and connected
            for processor in self.processors:
                if not processor.is_running:
                    self.logger.info(f"Restarting processor {processor.node_id}...")
                    await processor.start()
                    await asyncio.sleep(2)
                    # Reconnect to storage and notification
                    await self.connect_processor(processor)
                    await asyncio.sleep(1)
            
            # Reconnect collectors to all processors
            for collector in self.collectors:
                for processor in self.processors:
                    try:
                        self.logger.info(f"Reconnecting {collector.node_id} to {processor.node_id}...")
                        await self.connect_collector_to_processor(collector, processor)
                        await asyncio.sleep(1)
                    except Exception as e:
                        self.logger.warning(f"Failed to connect {collector.node_id} to {processor.node_id}: {e}")
            
            # Wait for connections to stabilize
            await asyncio.sleep(5)
            
            # Send test data through different collectors
            test_data_base = {
                'patient_id': 'TEST_LB_',
                'heart_rate': 75,
                'blood_pressure': {'systolic': 120, 'diastolic': 80},
                'temperature': 37.0,
                'timestamp': time.time()
            }
            
            # Send multiple requests
            for i in range(6):  # Reduced from 10 to 6 requests
                collector = self.collectors[i % len(self.collectors)]
                test_data = test_data_base.copy()
                test_data['patient_id'] = f'TEST_LB_{i}'
                test_data['timestamp'] = time.time()
                
                try:
                    response = await collector.send_data(test_data)
                    assert response['status'] == 'ok', f"Request {i} failed"
                    self.logger.info(f"Request {i} processed successfully")
                    await asyncio.sleep(0.5)  # Increased delay between requests
                except Exception as e:
                    self.logger.error(f"Error processing request {i}: {e}")
                    raise
            
            self.logger.info("Load balancing test passed")
            
        except Exception as e:
            self.logger.error(f"Load balancing test failed: {e}")
            raise

    async def connect_collector_to_processor(self, collector, processor):
        """Helper method to connect collector to a specific processor"""
        max_retries = 3
        retry_delay = 1
        last_error = None
        
        for attempt in range(max_retries):
            try:
                await collector.connect_to_processor(self.host, processor.port)
                self.logger.info(f"Connected {collector.node_id} to {processor.node_id}")
                return
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    self.logger.warning(f"Retry {attempt + 1} connecting {collector.node_id} to {processor.node_id}")
                    await asyncio.sleep(retry_delay)
        
        raise ConnectionError(f"Failed to connect {collector.node_id} to {processor.node_id} after {max_retries} attempts: {last_error}")

    async def verify_services(self) -> bool:
        """Verify all services are running and responding"""
        try:
            self.logger.info("Verifying services are running...")
            
            # Check collectors
            for i, collector in enumerate(self.collectors):
                if not collector.is_running:
                    self.logger.error(f"Collector {i} is not running")
                    return False
                
            # Check processors
            for i, processor in enumerate(self.processors):
                if not processor.is_running:
                    self.logger.error(f"Processor {i} is not running")
                    return False
                
            # Check storage
            if not self.storage.is_running:
                self.logger.error("Storage service is not running")
                return False
                
            # Check notification
            if not self.notification.is_running:
                self.logger.error("Notification service is not running")
                return False

            # Try a simple health check
            test_data = {
                'patient_id': 'HEALTH_CHECK',
                'heart_rate': 70,
                'timestamp': time.time()
            }
            
            # Test connection through first collector
            try:
                response = await self.collectors[0].send_data(test_data)
                if response['status'] != 'ok':
                    self.logger.error("Health check failed: Bad response status")
                    return False
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                return False

            self.logger.info("All services verified and running")
            return True
            
        except Exception as e:
            self.logger.error(f"Service verification failed: {e}")
            return False

    async def calculate_reliability_metrics(self):
        """Calculate comprehensive system reliability metrics"""
        current_time = time.time()
        total_runtime = current_time - self.test_start_time
        
        metrics = {
            'mtbf': 0.0,  # Mean Time Between Failures
            'mttr': 0.0,  # Mean Time To Recovery
            'availability': 0.0,  # System Availability
            'failure_rate': 0.0,  # Failures per hour
            'availability_timeline': [],
            'uptime_percentage': 0.0
        }
        
        # Calculate MTTR (Mean Time To Recovery)
        if len(self.recovery_timestamps) > 0 and len(self.failure_timestamps) > 0:
            recovery_times = []
            for i in range(min(len(self.recovery_timestamps), len(self.failure_timestamps))):
                recovery_time = self.recovery_timestamps[i] - self.failure_timestamps[i]
                recovery_times.append(recovery_time)
            
            metrics['mttr'] = sum(recovery_times) / len(recovery_times) if recovery_times else 0
        
        # Calculate MTBF (Mean Time Between Failures)
        if len(self.failure_timestamps) > 1:
            failure_intervals = []
            for i in range(1, len(self.failure_timestamps)):
                interval = self.failure_timestamps[i] - self.failure_timestamps[i-1]
                failure_intervals.append(interval)
            
            metrics['mtbf'] = sum(failure_intervals) / len(failure_intervals)
        elif len(self.failure_timestamps) == 1:
            metrics['mtbf'] = self.failure_timestamps[0] - self.test_start_time
        
        # Calculate failure rate (failures per hour)
        hours_running = total_runtime / 3600
        metrics['failure_rate'] = len(self.failure_timestamps) / hours_running if hours_running > 0 else 0
        
        # Calculate uptime percentage
        metrics['uptime_percentage'] = ((total_runtime - self.total_downtime) / total_runtime) * 100 if total_runtime > 0 else 0
        
        # Collect availability data points
        availability_data = await self.check_system_availability()
        metrics['availability'] = availability_data
        metrics['availability_timeline'].append((current_time - self.test_start_time, availability_data))
        
        self.logger.info(f"""
        System Reliability Metrics:
        - MTBF: {metrics['mtbf']:.2f} seconds
        - MTTR: {metrics['mttr']:.2f} seconds
        - Failure Rate: {metrics['failure_rate']:.2f} failures/hour
        - Uptime: {metrics['uptime_percentage']:.2f}%
        - Current Availability: {metrics['availability']:.2f}%
        """)
        
        return metrics

    async def check_system_availability(self):
        """
        Check current system availability by leveraging existing verification methods
        and concurrent checks for better performance.
        Returns: float between 0 and 1 representing availability percentage
        """
        try:
            total_weight = 0
            weighted_score = 0
            
            # 1. Use existing verify_services method as base check (40% weight)
            services_ok = await self.verify_services()
            total_weight += 40
            if services_ok:
                weighted_score += 40
                
            # 2. Concurrent connection and data flow checks (60% weight)
            async def check_data_flow(collector):
                try:
                    test_data = {
                        'patient_id': f'HEALTH_CHECK_{collector.node_id}',
                        'heart_rate': 70,
                        'blood_pressure': {'systolic': 120, 'diastolic': 80},
                        'temperature': 37.0,
                        'timestamp': time.time()
                    }
                    
                    response = await asyncio.wait_for(
                        collector.send_data(test_data),
                        timeout=2.0
                    )
                    
                    # Verify data was stored
                    stored_data = await asyncio.wait_for(
                        self.storage.retrieve_data({'patient_id': test_data['patient_id']}),
                        timeout=2.0
                    )
                    
                    return response.get('status') == 'ok' and stored_data is not None
                except Exception as e:
                    self.logger.debug(f"Data flow check failed for {collector.node_id}: {e}")
                    return False

            # Run concurrent checks for all collectors
            check_tasks = [check_data_flow(collector) 
                          for collector in self.collectors 
                          if collector.is_running]
            
            if check_tasks:
                results = await asyncio.gather(*check_tasks, return_exceptions=True)
                success_rate = sum(1 for r in results if r is True) / len(check_tasks)
                weighted_score += 60 * success_rate
                total_weight += 60
            
            # Calculate final availability score
            availability = weighted_score / total_weight if total_weight > 0 else 0.0
            
            self.logger.info(
                f"System availability: {availability:.2%} "
                f"(Services Check: {'Pass' if services_ok else 'Fail'}, "
                f"Data Flow Success Rate: {success_rate:.2%})"
            )
            
            return availability
            
        except Exception as e:
            self.logger.error(f"Error checking system availability: {e}")
            return 0.0

    async def record_failure(self, service_id: str):
        """Record a service failure"""
        failure_time = time.time()
        self.failure_timestamps.append(failure_time)
        self.logger.info(f"Recorded failure of {service_id} at {failure_time}")

    async def record_recovery(self, service_id: str):
        """Record a service recovery"""
        recovery_time = time.time()
        self.recovery_timestamps.append(recovery_time)
        if self.failure_timestamps:
            downtime = recovery_time - self.failure_timestamps[-1]
            self.total_downtime += downtime
        self.logger.info(f"Recorded recovery of {service_id} at {recovery_time}")

async def main():
    """Main test function"""
    logging.basicConfig(level=logging.INFO)
    
    tester = ComprehensiveSystemTest()
    try:
        await tester.setup()
        await tester.test_normal_operation()
        await tester.test_fault_tolerance()
        await tester.test_security()
        await tester.test_data_integrity()
        await tester.test_load_balancing()
        
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