import pytest
import asyncio
import logging
import matplotlib.pyplot as plt
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
        
        # Plot availability graph
        plot_availability_graph(metrics['availability_timeline'])
        
        # Assert minimum reliability requirements
        assert metrics['mttr'] <= 5.0, "Mean Time To Recovery above 5 seconds"
        
    except Exception as e:
        logging.error(f"Test failed with error: {e}")
        raise
    finally:
        await tester.cleanup()

def plot_availability_graph(availability_timeline):
    """
    Plot system availability over time
    
    Args:
        availability_timeline: List of tuples containing (timestamp, availability_value)
    """
    timestamps, availability_values = zip(*availability_timeline)
    
    plt.figure(figsize=(10, 6))
    plt.plot(timestamps, availability_values, 'b-', linewidth=2)
    plt.grid(True)
    
    plt.title('System Availability Over Time')
    plt.xlabel('Time (seconds)')
    plt.ylabel('Availability (%)')
    plt.ylim(0, 1.1)  # Set y-axis from 0 to 110%
    
    # Add horizontal line at 95% availability threshold
    plt.axhline(y=0.95, color='r', linestyle='--', label='95% Threshold')
    plt.legend()
    
    # Save the plot
    plt.savefig('system_availability.png')
    plt.close() 