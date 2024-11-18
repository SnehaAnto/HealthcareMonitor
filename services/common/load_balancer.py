from typing import List, Optional, Dict
import random
import logging

class LoadBalancer:
    def __init__(self, service_type: str):
        """Initialize LoadBalancer
        Args:
            service_type: Optional identifier for the service using this load balancer
        """
        self.service_type = service_type
        self.nodes = {}  # node_id -> request_count
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_node(self, node_id: str) -> None:
        """Register a new node"""
        self.nodes[node_id] = 0
        self.logger.debug(f"Registered node {node_id}")

    def unregister_node(self, node_id: str) -> None:
        """Unregister a node"""
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.logger.debug(f"Unregistered node {node_id}")

    def get_next_node(self) -> str:
        """Get the next node based on least connections"""
        if not self.nodes:
            return None
            
        # Find node with minimum load
        min_load = min(self.nodes.values())
        candidates = [nid for nid, load in self.nodes.items() if load == min_load]
        
        # Select randomly from candidates with minimum load
        selected = random.choice(candidates)
        self.logger.debug(f"Selected node {selected} (load: {min_load})")
        return selected

    def register_request(self, node_id: str) -> None:
        """Register a successful request for a node"""
        if node_id in self.nodes:
            self.nodes[node_id] += 1
            self.logger.debug(f"Registered request for node {node_id} (total: {self.nodes[node_id]})")

    def get_load_distribution(self) -> dict:
        """Get current load distribution"""
        total = sum(self.nodes.values()) or 1  # Avoid division by zero
        distribution = {
            node_id: (count / total) * 100 
            for node_id, count in self.nodes.items()
        }
        self.logger.debug(f"Load distribution: {distribution}")
        return distribution

    def clear(self) -> None:
        """Clear all registered nodes"""
        self.nodes.clear()
        self.logger.debug("Cleared all registered nodes")
        