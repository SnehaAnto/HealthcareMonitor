import time
import asyncio
from services.data_collection.service import DataCollectionService
from services.data_processing.service import DataProcessingService
from services.storage.service import StorageService
from services.notification.service import NotificationService
from services.ui.service import UIService
import logging
from pathlib import Path
from utils.cert_generator import CertificateGenerator
import uuid
from services.common.security import SecurityConfig
from utils.logger import LoggerSetup

async def setup_certificates():
    """Generate certificates for all services"""
    cert_generator = CertificateGenerator()
    
    # Generate CA certificate
    ca_cert, ca_key = cert_generator.generate_ca_certificate()
    
    # Generate certificates for each service
    certificates = {}
    services = ['collector', 'processor', 'storage', 'notification', 'ui']
    
    for service in services:
        cert_path, key_path = cert_generator.generate_node_certificates(
            node_id=service,
            common_name=f"{service}.healthsystem.local"
        )
        certificates[service] = (cert_path, key_path)
    
    return certificates

async def simulate_health_data(collection_service):
    """Simulate health data"""
    logger = logging.getLogger("Simulation")
    
    while True:
        try:
            # Normal data
            test_data = {
                'type': 'health_metrics',
                'data': {
                    'device_id': 'dev001',
                    'heart_rate': 75,
                    'temperature': 98.6,
                    'blood_pressure': '120/80',
                    'timestamp': time.time()
                },
                'id': str(uuid.uuid4()),
                'source': 'simulator'
            }
            
            logger.debug(f"Sending normal data: {test_data}")
            
            # Get processing service connection
            proc_conn = collection_service.peers.get(('127.0.0.1', 5002))
            if proc_conn:
                collection_service.secure_send(proc_conn, test_data)
                await asyncio.sleep(5)
            else:
                logger.warning("No connection to processing service")
            
            # Abnormal data
            test_data_abnormal = {
                'type': 'health_metrics',
                'data': {
                    'device_id': 'dev001',
                    'heart_rate': 150,
                    'temperature': 102.0,
                    'blood_pressure': '160/95',
                    'timestamp': time.time()
                },
                'id': str(uuid.uuid4()),
                'source': 'simulator'
            }
            
            logger.debug(f"Sending abnormal data: {test_data_abnormal}")
            
            if proc_conn:
                collection_service.secure_send(proc_conn, test_data_abnormal)
            
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Error simulating health data: {str(e)}", exc_info=True)
            await asyncio.sleep(1)

async def main():
    try:
        # Setup logging first
        loggers = LoggerSetup.setup_logging()
        system_logger = loggers['System']
        
        system_logger.info("Starting Healthcare Monitoring System")
        
        # Generate certificates
        system_logger.info("Generating certificates for services...")
        certificates = await setup_certificates()

        # Initialize services
        collection_service = DataCollectionService(
            node_id="collector_1",
            host='127.0.0.1',
            port=5001,
            certfile=certificates['collector'][0],
            keyfile=certificates['collector'][1]
        )

        processing_service = DataProcessingService(
            node_id="processor_1",
            host='127.0.0.1',
            port=5002,
            certfile=certificates['processor'][0],
            keyfile=certificates['processor'][1]
        )

        storage_service = StorageService(
            node_id="storage_1",
            host='127.0.0.1',
            port=5003,
            certfile=certificates['storage'][0],
            keyfile=certificates['storage'][1]
        )

        notification_service = NotificationService(
            node_id="notification_1",
            host='127.0.0.1',
            port=5004,
            certfile=certificates['notification'][0],
            keyfile=certificates['notification'][1]
        )

        # Initialize UI service
        ui_service = UIService(
            node_id="ui_1",
            host='127.0.0.1',
            port=5005,
            certfile=certificates['ui'][0],
            keyfile=certificates['ui'][1],
            ui_port=8000
        )

        # Add UI service to the list of services
        services = [
            collection_service,
            processing_service,
            storage_service,
            notification_service,
            ui_service
        ]
        
        # Start all services
        for service in services:
            service.start()
            system_logger.info(f"Started {service.service_name} service")

        # Connect notification service to UI service
        notification_service.establish_secure_connection('127.0.0.1', 5005)
        system_logger.info("Connected notification service to UI service")

        # Wait for services to start
        await asyncio.sleep(2)

        # Establish connections
        try:
            collection_service.establish_secure_connection('127.0.0.1', 5002)
            system_logger.info("Connected collection service to processing service")
            
            processing_service.establish_secure_connection('127.0.0.1', 5003)
            system_logger.info("Connected processing service to storage service")
            
            processing_service.establish_secure_connection('127.0.0.1', 5004)
            system_logger.info("Connected processing service to notification service")
            
        except Exception as e:
            system_logger.error(f"Failed to establish connections: {e}", exc_info=True)
            return

        # Start data simulation
        simulation_task = asyncio.create_task(simulate_health_data(collection_service))
        
        # Keep the main task running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            simulation_task.cancel()
            system_logger.info("Shutting down services...")
            # Cleanup services
            for service in services:
                if hasattr(service, 'stop'):
                    service.stop()

    except Exception as e:
        system_logger.error(f"System error: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())
