import asyncio
import logging
from pathlib import Path
import sys

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from services.notification.service import NotificationService
from utils.cert_generator import generate_test_certificates

async def test_notification_service():
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('NotificationTest')
    
    try:
        # Generate certificates
        logger.info("Generating test certificates...")
        certificates = await generate_test_certificates()
        
        # Create notification service
        logger.info("Creating notification service...")
        notification = NotificationService(
            node_id="test_notification",
            host='127.0.0.1',
            port=5301,
            certfile=certificates['notification'][0],
            keyfile=certificates['notification'][1]
        )
        
        # Start the service
        logger.info("Starting notification service...")
        await notification.start()
        
        # Wait a bit to ensure service is running
        await asyncio.sleep(2)
        
        # Stop the service
        logger.info("Stopping notification service...")
        await notification.stop()
        
        logger.info("Test completed successfully")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_notification_service()) 