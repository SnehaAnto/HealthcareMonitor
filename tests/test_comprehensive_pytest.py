import pytest
import asyncio
import logging
from .test_system_comprehensive import ComprehensiveSystemTest

@pytest.mark.asyncio
async def test_comprehensive_system():
    logging.basicConfig(level=logging.INFO)
    tester = ComprehensiveSystemTest()
    try:
        await tester.setup()
        await asyncio.sleep(5)

        if not await tester.verify_services():
            pytest.fail("Services failed to start properly")

        # Run tests
        await tester.test_normal_operation()
        await tester.test_fault_tolerance()
        await tester.test_security()
        await tester.test_data_integrity()
        await tester.test_load_balancing()

        # Calculate and verify reliability metrics
        metrics = await tester.calculate_reliability_metrics()
        
        # Assert minimum reliability requirements
       # assert metrics['availability'] >= 0.95, "System availability below 95%"
        assert metrics['mttr'] <= 5.0, "Mean Time To Recovery above 5 seconds"
        
    except Exception as e:
        logging.error(f"Test failed with error: {e}")
        raise
    finally:
        await tester.cleanup() 