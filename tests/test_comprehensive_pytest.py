import pytest
import asyncio
import logging
import json
from datetime import datetime
from pathlib import Path
from .visualization.reliability_graphs import ReliabilityVisualizer
from .test_system_comprehensive import ComprehensiveSystemTest

@pytest.mark.asyncio
async def test_comprehensive_system():
    # Setup test directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_dir = Path(f"test_results/{timestamp}")
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler(test_dir / "test.log"),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger("TestRunner")
    visualizer = ReliabilityVisualizer(str(test_dir))
    tester = ComprehensiveSystemTest()
    
    try:
        # Run setup and tests
        await tester.setup()
        
        # Run core tests
        test_results = {
            'normal_operation': await tester.test_normal_operation(),
            'fault_tolerance': await tester.test_fault_tolerance(),
            'security': await tester.test_security(),
            'data_integrity': await tester.test_data_integrity(),
            'load_balancing': await tester.test_load_balancing()
        }
        
        # Calculate metrics
        metrics = await tester.calculate_reliability_metrics()
        
        # Generate visualizations
        visualizer.plot_availability_timeline(metrics['availability_timeline'])
        visualizer.plot_reliability_metrics(metrics)
        
        # Save test results
        with open(test_dir / "test_results.json", "w") as f:
            json.dump({
                'test_results': test_results,
                'metrics': {k: v for k, v in metrics.items() if isinstance(v, (int, float))}
            }, f, indent=2)
        
        # Assert minimum reliability requirements
        assert metrics['mttr'] <= 5.0, "Mean Time To Recovery above 5 seconds"
        assert metrics['uptime_percentage'] >= 99.0, "Uptime percentage below 99%"
        assert metrics['availability'] >= 99.0, "System availability below 99%"
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        raise
    finally:
        await tester.cleanup() 