from typing import Dict, List, Set, Optional, Callable
import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from threading import Lock

class NodeState(Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"

@dataclass
class NodeHealth:
    state: NodeState = NodeState.ACTIVE
    last_heartbeat: float = 0.0
    failure_count: int = 0
    recovery_attempts: int = 0
    last_recovery: float = 0.0

class FaultToleranceManager:
    def __init__(self, service_type: str, node_id: str):
        self.service_type = service_type
        self.node_id = node_id
        self.logger = logging.getLogger('FaultTolerance')
        self.lock = Lock()
        
        # Node health tracking
        self.nodes: Dict[str, NodeHealth] = {}
        self.primary_node: Optional[str] = None
        self.backup_nodes: Set[str] = set()
        
        # Recovery settings
        self.max_recovery_attempts = 3
        self.recovery_cooldown = 300  # 5 minutes
        self.heartbeat_timeout = 30  # seconds
        
        # Callbacks
        self.recovery_callbacks: Dict[str, Callable] = {}
        self.failover_callbacks: Dict[str, Callable] = {}
        
        # Start monitoring task
        self.monitoring_task = None
        self._stop_event = asyncio.Event()

    async def start(self):
        """Start fault tolerance monitoring"""
        self.monitoring_task = asyncio.create_task(self._monitor_nodes())
        self.logger.info(f"Started fault tolerance monitoring for {self.service_type}")

    async def stop(self):
        """Stop fault tolerance monitoring"""
        self._stop_event.set()
        if self.monitoring_task:
            await self.monitoring_task
        self.logger.info("Stopped fault tolerance monitoring")

    def register_node(self, node_id: str, is_primary: bool = False):
        """Register a node for fault tolerance monitoring"""
        with self.lock:
            self.nodes[node_id] = NodeHealth(
                state=NodeState.ACTIVE,
                last_heartbeat=time.time()
            )
            
            if is_primary:
                self.primary_node = node_id
            else:
                self.backup_nodes.add(node_id)
                
            self.logger.info(
                f"Registered {'primary' if is_primary else 'backup'} node: {node_id}"
            )

    async def handle_node_failure(self, failed_node_id: str):
        """Handle node failure"""
        async with asyncio.Lock():
            try:
                if failed_node_id not in self.nodes:
                    return

                node = self.nodes[failed_node_id]
                node.failure_count += 1
                node.state = NodeState.FAILED
                
                self.logger.warning(f"Node failure detected: {failed_node_id}")

                if failed_node_id == self.primary_node:
                    await self._handle_primary_failure()
                else:
                    await self._handle_backup_failure(failed_node_id)

                # Attempt recovery if conditions are met
                await self._attempt_recovery(failed_node_id)

            except Exception as e:
                self.logger.error(
                    f"Error handling node failure: {str(e)}", 
                    exc_info=True
                )

    async def _handle_primary_failure(self):
        """Handle primary node failure"""
        try:
            # Select new primary from backup nodes
            new_primary = None
            for backup_node in sorted(self.backup_nodes):
                if self.nodes[backup_node].state == NodeState.ACTIVE:
                    new_primary = backup_node
                    break

            if new_primary:
                old_primary = self.primary_node
                self.primary_node = new_primary
                self.backup_nodes.remove(new_primary)
                
                # Execute failover callbacks
                if old_primary in self.failover_callbacks:
                    await self.failover_callbacks[old_primary](new_primary)
                
                self.logger.info(f"Promoted node {new_primary} to primary")
            else:
                self.logger.error("No available backup nodes for promotion")

        except Exception as e:
            self.logger.error(
                f"Error handling primary failure: {str(e)}", 
                exc_info=True
            )

    async def _handle_backup_failure(self, failed_node_id: str):
        """Handle backup node failure"""
        try:
            self.backup_nodes.remove(failed_node_id)
            self.logger.info(f"Removed failed backup node: {failed_node_id}")
            
            # Trigger data replication if needed
            if failed_node_id in self.recovery_callbacks:
                await self.recovery_callbacks[failed_node_id]()

        except Exception as e:
            self.logger.error(
                f"Error handling backup failure: {str(e)}", 
                exc_info=True
            )

    async def _attempt_recovery(self, node_id: str):
        """Attempt to recover a failed node"""
        try:
            node = self.nodes[node_id]
            current_time = time.time()
            
            # Check if we should attempt recovery
            if (node.recovery_attempts < self.max_recovery_attempts and
                current_time - node.last_recovery > self.recovery_cooldown):
                
                node.state = NodeState.RECOVERING
                node.recovery_attempts += 1
                node.last_recovery = current_time
                
                # Execute recovery callback if registered
                if node_id in self.recovery_callbacks:
                    success = await self.recovery_callbacks[node_id]()
                    if success:
                        node.state = NodeState.ACTIVE
                        node.failure_count = 0
                        self.logger.info(f"Successfully recovered node: {node_id}")
                    else:
                        node.state = NodeState.FAILED
                        self.logger.warning(
                            f"Failed to recover node: {node_id} "
                            f"(Attempt {node.recovery_attempts}/{self.max_recovery_attempts})"
                        )

        except Exception as e:
            self.logger.error(
                f"Error attempting node recovery: {str(e)}", 
                exc_info=True
            )

    async def _monitor_nodes(self):
        """Monitor node health"""
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                
                with self.lock:
                    for node_id, health in self.nodes.items():
                        if (health.state == NodeState.ACTIVE and
                            current_time - health.last_heartbeat > self.heartbeat_timeout):
                            await self.handle_node_failure(node_id)
                
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error in node monitoring: {str(e)}")
                await asyncio.sleep(1)

    def update_heartbeat(self, node_id: str):
        """Update node heartbeat"""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_heartbeat = time.time() 