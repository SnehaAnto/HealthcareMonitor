import matplotlib.pyplot as plt
from datetime import datetime
import os

class ReliabilityVisualizer:
    def __init__(self, output_dir="test_results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set default figure size
        plt.rcParams['figure.figsize'] = [12, 6]
    
    def plot_availability_timeline(self, timeline_data, save=True):
        """Plot system availability over time"""
        times, availabilities = zip(*timeline_data)
        
        plt.figure()
        plt.plot(times, availabilities, '-o', linewidth=2, color='blue')
        plt.title('System Availability Over Time')
        plt.xlabel('Time (seconds)')
        plt.ylabel('Availability (%)')
        plt.grid(True)
        
        if save:
            plt.savefig(f"{self.output_dir}/availability_timeline.png")
            plt.close()
    
    def plot_reliability_metrics(self, metrics, save=True):
        """Create a comprehensive reliability dashboard"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # MTBF vs MTTR
        ax1.bar(['MTBF', 'MTTR'], [metrics['mtbf'], metrics['mttr']], color=['green', 'red'])
        ax1.set_title('MTBF vs MTTR')
        ax1.set_ylabel('Seconds')
        ax1.grid(True)
        
        # Failure Rate
        ax2.bar(['Failure Rate'], [metrics['failure_rate']], color='orange')
        ax2.set_title('Failure Rate (per hour)')
        ax2.grid(True)
        
        # Uptime Percentage
        ax3.pie([metrics['uptime_percentage'], 100 - metrics['uptime_percentage']], 
                labels=['Uptime', 'Downtime'],
                colors=['lightgreen', 'lightcoral'],
                autopct='%1.1f%%')
        ax3.set_title('System Uptime/Downtime')
        
        # Current Availability
        ax4.bar(['Availability'], [metrics['availability']], color='blue')
        ax4.set_title('Current System Availability')
        ax4.set_ylabel('Percentage')
        ax4.grid(True)
        
        plt.tight_layout()
        
        if save:
            plt.savefig(f"{self.output_dir}/reliability_dashboard.png")
            plt.close() 