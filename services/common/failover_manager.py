from typing import Dict, Set, Optional, Callable, Any
import asyncio
import logging
from datetime import datetime
from threading import Lock
import threading
import time
from enum import Enum

class FailoverState(Enum):
    UNKNOWN = "unknown"
    PRIMARY = "primary"
    BACKUP = "backup"

class FailoverManager:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.nodes = {}  # node_id -> timestamp
        self.primary_node = None
        self.last_heartbeat = None
        self.logger = logging.getLogger("FaultTolerance")
        self.callbacks = []

    def register_node(self, node_id: str, is_primary: bool = False) -> None:
        """Register a node with the failover manager"""
        self.nodes[node_id] = time.time()
        if is_primary:
            self.primary_node = node_id
        self.logger.info(f"Registered {'primary' if is_primary else 'backup'} node: {node_id}")

    def unregister_node(self, node_id: str) -> None:
        """Unregister a node from the failover manager"""
        if node_id in self.nodes:
            self.nodes.pop(node_id)
            if self.primary_node == node_id:
                self.primary_node = None
            self.logger.info(f"Unregistered node: {node_id}")

    def handle_heartbeat(self, heartbeat: Dict[str, Any]) -> None:
        """Handle heartbeat message from a node"""
        node_id = heartbeat.get('node_id')
        if node_id in self.nodes:
            self.nodes[node_id] = time.time()
            if node_id == self.primary_node:
                self.last_heartbeat = time.time()

    async def become_primary(self) -> None:
        """Transition to primary state"""
        self.state = FailoverState.PRIMARY
        self.logger.info(f"Node {self.node_id} became PRIMARY")

    async def become_backup(self) -> None:
        """Transition to backup state"""
        self.state = FailoverState.BACKUP
        self.logger.info(f"Node {self.node_id} became BACKUP")

    def is_primary(self) -> bool:
        """Check if node is primary"""
        return self.state == FailoverState.PRIMARY

    async def monitor_primary(self, check_interval: float = 5.0):
        """Monitor primary node health"""
        while self.is_active:
            try:
                if self.state == FailoverState.BACKUP:
                    # Implement primary health check logic here
                    pass
                await asyncio.sleep(check_interval)
            except Exception as e:
                self.logger.error(f"Error monitoring primary: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        """Stop the failover manager"""
        self.is_active = False
        