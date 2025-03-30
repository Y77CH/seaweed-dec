
import grpc
import logging
import volume_server_pb2
import volume_server_pb2_grpc
from typing import Optional

# Setup logging
logging.basicConfig(level=logging.INFO)

class VolumeServerClient:
    """Client for interacting with the VolumeServer gRPC service."""
    
    def __init__(
        self, 
        server_address: str,
        use_secure_channel: bool = False,
        root_certificates: Optional[bytes] = None,
        timeout: int = 10
    ):
        self.server_address = server_address
        self.timeout = timeout
        
        if use_secure_channel and root_certificates:
            credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates)
            self.channel = grpc.secure_channel(server_address, credentials)
        else:
            self.channel = grpc.insecure_channel(server_address)
        
        self.stub = volume_server_pb2_grpc.VolumeServerStub(self.channel)
    
    def close(self):
        """Close the gRPC channel."""
        self.channel.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def vacuum_volume_check(self, volume_id: int) -> float:
        """Check a volume and return its garbage ratio.
        
        Args:
            volume_id: The ID of the volume to check
            
        Returns:
            float: The garbage ratio of the volume
        """
        try:
            request = volume_server_pb2.VacuumVolumeCheckRequest(volume_id=volume_id)
            response = self.stub.VacuumVolumeCheck(
                request,
                timeout=self.timeout
            )
            return response.garbage_ratio
        except grpc.RpcError as e:
            logging.error(f"RPC error in vacuum_volume_check: {e}")
            raise
    
    def vacuum_volume_compact(self, volume_id: int, preallocate: int = 0):
        """Compact a volume and yield progress updates.
        
        Args:
            volume_id: The ID of the volume to compact
            preallocate: Size to preallocate for compaction
            
        Yields:
            tuple: (processed_bytes, load_avg_1m) for each update
        """
        try:
            request = volume_server_pb2.VacuumVolumeCompactRequest(
                volume_id=volume_id,
                preallocate=preallocate
            )
            responses = self.stub.VacuumVolumeCompact(
                request,
                timeout=self.timeout
            )
            
            for response in responses:
                yield response.processed_bytes, response.load_avg_1m
        except grpc.RpcError as e:
            logging.error(f"RPC error in vacuum_volume_compact: {e}")
            raise
    
    def vacuum_volume_commit(self, volume_id: int):
        """Commit a volume and return its status.
        
        Args:
            volume_id: The ID of the volume to commit
            
        Returns:
            tuple: (is_read_only, volume_size)
        """
        try:
            request = volume_server_pb2.VacuumVolumeCommitRequest(volume_id=volume_id)
            response = self.stub.VacuumVolumeCommit(
                request,
                timeout=self.timeout
            )
            return response.is_read_only, response.volume_size
        except grpc.RpcError as e:
            logging.error(f"RPC error in vacuum_volume_commit: {e}")
            raise
    
    def vacuum_volume_cleanup(self, volume_id: int):
        """Clean up a volume.
        
        Args:
            volume_id: The ID of the volume to clean up
            
        Returns:
            bool: True if cleanup was successful
        """
        try:
            request = volume_server_pb2.VacuumVolumeCleanupRequest(volume_id=volume_id)
            self.stub.VacuumVolumeCleanup(
                request,
                timeout=self.timeout
            )
            # Response is empty, so we just return True for success
            return True
        except grpc.RpcError as e:
            logging.error(f"RPC error in vacuum_volume_cleanup: {e}")
            raise


def main():
    server_address = '172.31.4.65:18080'
    
    volume_id = 1
    
    try:
        with VolumeServerClient(server_address) as client:
            garbage_ratio = client.vacuum_volume_check(volume_id)
            logging.info(f"Volume {volume_id} garbage ratio: {garbage_ratio}")
    
    except Exception as e:
        logging.error(f"Error during volume vacuum: {e}")


if __name__ == '__main__':
    main()