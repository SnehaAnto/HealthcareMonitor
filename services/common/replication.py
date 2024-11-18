from typing import Dict, List, Any, Optional
import asyncio
import json
import logging
from datetime import datetime
from uuid import uuid4

class ReplicationManager:
    def __init__(self, node_id: str, replication_factor: int = 3):
        self.node_id = node_id
        self.replication_factor = replication_factor
        self.logger = logging.getLogger('Replication')
        self.data_store: Dict[str, Dict] = {}  # Local data store
        self.version_history: Dict[str, List[Dict]] = {}  # Track versions of data
        self.replica_nodes: Dict[str, 'NodeConnection'] = {}  # Connected replica nodes
        
    async def replicate_data(self, data: Dict, data_id: Optional[str] = None) -> str:
        """
        Replicate data to other nodes in the system
        Returns the data_id used for the replication
        """
        try:
            # Generate unique ID if not provided
            data_id = data_id or str(uuid4())
            
            # Create version metadata
            version = {
                'timestamp': datetime.utcnow().isoformat(),
                'node_id': self.node_id,
                'version_id': str(uuid4())
            }
            
            # Store locally first
            self.data_store[data_id] = data
            self.version_history.setdefault(data_id, []).append(version)
            
            # Create replication package
            replication_package = {
                'data_id': data_id,
                'data': data,
                'version': version,
                'source_node': self.node_id
            }
            
            # Replicate to other nodes
            replication_tasks = []
            for node_id, node in self.replica_nodes.items():
                if len(replication_tasks) >= self.replication_factor - 1:
                    break
                    
                task = asyncio.create_task(
                    node.send_replication(replication_package)
                )
                replication_tasks.append(task)
            
            if replication_tasks:
                # Wait for replication to complete with timeout
                await asyncio.wait(replication_tasks, timeout=10)
                
                # Check results
                successful_replications = sum(
                    1 for task in replication_tasks 
                    if not task.exception()
                )
                
                if successful_replications < self.replication_factor - 1:
                    self.logger.warning(
                        f"Data {data_id} only replicated to {successful_replications} "
                        f"nodes out of {self.replication_factor - 1} required"
                    )
                else:
                    self.logger.info(
                        f"Data {data_id} successfully replicated to "
                        f"{successful_replications} nodes"
                    )
            
            return data_id
            
        except Exception as e:
            self.logger.error(f"Error during data replication: {str(e)}", exc_info=True)
            raise

    async def handle_replication_request(self, replication_package: Dict):
        """Handle incoming replication request from another node"""
        try:
            data_id = replication_package['data_id']
            data = replication_package['data']
            version = replication_package['version']
            
            # Store the data locally
            self.data_store[data_id] = data
            self.version_history.setdefault(data_id, []).append(version)
            
            self.logger.debug(
                f"Stored replicated data {data_id} from node {version['node_id']}"
            )
            
            return {'status': 'success', 'data_id': data_id}
            
        except Exception as e:
            self.logger.error(
                f"Error handling replication request: {str(e)}", 
                exc_info=True
            )
            return {'status': 'error', 'error': str(e)}

    async def sync_with_replicas(self):
        """Synchronize data with replica nodes"""
        try:
            # Get list of all data IDs and their latest versions
            local_versions = {
                data_id: sorted(
                    versions, 
                    key=lambda x: x['timestamp']
                )[-1]
                for data_id, versions in self.version_history.items()
            }
            
            # Request version info from all replicas
            version_requests = []
            for node in self.replica_nodes.values():
                task = asyncio.create_task(node.get_version_info())
                version_requests.append(task)
            
            replica_versions = {}
            if version_requests:
                responses = await asyncio.gather(*version_requests, return_exceptions=True)
                for node_id, response in zip(self.replica_nodes.keys(), responses):
                    if not isinstance(response, Exception):
                        replica_versions[node_id] = response
            
            # Identify data that needs synchronization
            sync_tasks = []
            for data_id, local_version in local_versions.items():
                for node_id, node_versions in replica_versions.items():
                    node_version = node_versions.get(data_id)
                    if not node_version or node_version['timestamp'] < local_version['timestamp']:
                        # This node needs an update
                        sync_tasks.append(
                            self.replicate_data(
                                self.data_store[data_id],
                                data_id
                            )
                        )
            
            if sync_tasks:
                await asyncio.gather(*sync_tasks)
                self.logger.info(f"Completed synchronization with {len(sync_tasks)} updates")
                
        except Exception as e:
            self.logger.error(f"Error during replica synchronization: {str(e)}", exc_info=True) 