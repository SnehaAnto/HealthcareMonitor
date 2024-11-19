import matplotlib.pyplot as plt
from datetime import datetime
import os
import time

class ReliabilityVisualizer:
    def __init__(self, output_dir="test_results"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set default figure size and style
        plt.rcParams['figure.figsize'] = [12, 6]
        plt.rcParams['axes.grid'] = True
        plt.rcParams['grid.linestyle'] = '--'
        plt.rcParams['grid.alpha'] = 0.7
        
        # Store historical metrics
        self.metrics_history = {
            'mtbf': [],
            'mttr': [],
            'availability': [],
            'failure_rate': [],
            'uptime_percentage': []
        }
        self.timestamps = []
    
    def add_metrics_snapshot(self, metrics, timestamp=None):
        """Add a snapshot of metrics to the history"""
        if timestamp is None:
            timestamp = time.time()
            
        self.timestamps.append(timestamp)
        for key in self.metrics_history:
            if key in metrics:
                self.metrics_history[key].append(metrics[key])
    
    def plot_metrics_over_time(self, save=True):
        """Create line charts showing metrics trends over time"""
        if not self.timestamps:
            return
            
        # Convert timestamps to relative time in minutes
        relative_times = [(t - self.timestamps[0])/60 for t in self.timestamps]
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # Plot MTBF and MTTR together
        ax1.plot(relative_times, self.metrics_history['mtbf'], 
                label='MTBF', color='#2ecc71', marker='o', linewidth=2)
        ax1.plot(relative_times, self.metrics_history['mttr'], 
                label='MTTR', color='#e74c3c', marker='o', linewidth=2)
        ax1.set_title('MTBF and MTTR Over Time', pad=10, fontsize=12)
        ax1.set_xlabel('Time (minutes)')
        ax1.set_ylabel('Seconds')
        ax1.legend()
        
        # Plot Failure Rate
        ax2.plot(relative_times, self.metrics_history['failure_rate'], 
                color='#f39c12', marker='o', linewidth=2)
        ax2.set_title('Failure Rate Over Time', pad=10, fontsize=12)
        ax2.set_xlabel('Time (minutes)')
        ax2.set_ylabel('Failures per Hour')
        
        # Plot Uptime Percentage
        ax3.plot(relative_times, self.metrics_history['uptime_percentage'], 
                color='#27ae60', marker='o', linewidth=2)
        ax3.set_title('System Uptime Percentage Over Time', pad=10, fontsize=12)
        ax3.set_xlabel('Time (minutes)')
        ax3.set_ylabel('Percentage')
        ax3.set_ylim([0, 100])
        
        # Plot Availability
        ax4.plot(relative_times, self.metrics_history['availability'], 
                color='#3498db', marker='o', linewidth=2)
        ax4.set_title('System Availability Over Time', pad=10, fontsize=12)
        ax4.set_xlabel('Time (minutes)')
        ax4.set_ylabel('Percentage')
        ax4.set_ylim([0, 100])
        
        # Add some padding between subplots
        plt.tight_layout(pad=3.0)
        
        if save:
            plt.savefig(f"{self.output_dir}/metrics_timeline.png", 
                       dpi=300, bbox_inches='tight')
            plt.close() 

    def plot_availability_timeline(self, timeline_data, save=True):
        """Plot system availability over time"""
        if not timeline_data:
            return
            
        # Unpack the timeline data
        times, availabilities = zip(*timeline_data)
        
        # Convert timestamps to relative time in minutes
        relative_times = [(t - times[0])/60 for t in times]
        
        plt.figure(figsize=(12, 6))
        plt.plot(relative_times, availabilities, 
                '-o', linewidth=2, color='#3498db')
        plt.title('System Availability Over Time', pad=10, fontsize=12)
        plt.xlabel('Time (minutes)')
        plt.ylabel('Availability (%)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.ylim([0, 100])
        
        if save:
            plt.savefig(f"{self.output_dir}/availability_timeline.png", 
                       dpi=300, bbox_inches='tight')
            plt.close()

    def plot_reliability_metrics(self, metrics, save=True):
        """Create a comprehensive reliability dashboard"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # MTBF vs MTTR
        ax1.bar(['MTBF', 'MTTR'], 
                [metrics['mtbf'], metrics['mttr']], 
                color=['#2ecc71', '#e74c3c'])
        ax1.set_title('MTBF vs MTTR', pad=10, fontsize=12)
        ax1.set_ylabel('Seconds')
        
        # Failure Rate
        ax2.bar(['Failure Rate'], 
                [metrics['failure_rate']], 
                color='#f39c12')
        ax2.set_title('Failure Rate (per hour)', pad=10, fontsize=12)
        
        # Uptime Percentage
        ax3.pie([metrics['uptime_percentage'], 100 - metrics['uptime_percentage']], 
                labels=['Uptime', 'Downtime'],
                colors=['#27ae60', '#e74c3c'],
                autopct='%1.1f%%')
        ax3.set_title('System Uptime/Downtime', pad=10, fontsize=12)
        
        # Current Availability
        ax4.bar(['Availability'], 
                [metrics['availability']], 
                color='#3498db')
        ax4.set_title('Current System Availability', pad=10, fontsize=12)
        ax4.set_ylabel('Percentage')
        ax4.set_ylim([0, 100])
        
        # Add grid to bar charts
        for ax in [ax1, ax2, ax4]:
            ax.grid(True, linestyle='--', alpha=0.7)
        
        plt.tight_layout(pad=3.0)
        
        if save:
            plt.savefig(f"{self.output_dir}/reliability_dashboard.png", 
                       dpi=300, bbox_inches='tight')
            plt.close()