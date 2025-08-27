"""
Database connection pooling for improved performance.

This module provides connection pooling functionality to optimize database
access patterns and reduce connection overhead.
"""

import sqlite3
import threading
import time
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Dict, Any
from queue import Queue, Empty, Full
from dataclasses import dataclass

from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


@dataclass
class ConnectionInfo:
    """Information about a database connection."""
    connection: sqlite3.Connection
    created_at: float
    last_used: float
    in_use: bool = False


class DatabaseConnectionPool:
    """Thread-safe database connection pool for SQLite."""
    
    def __init__(self, db_path: str, pool_size: int = 10, max_idle_time: int = 300):
        """
        Initialize connection pool.
        
        Args:
            db_path: Path to SQLite database
            pool_size: Maximum number of connections in pool
            max_idle_time: Maximum idle time before connection is closed (seconds)
        """
        self.db_path = Path(db_path)
        self.pool_size = pool_size
        self.max_idle_time = max_idle_time
        
        self._pool: Queue[ConnectionInfo] = Queue(maxsize=pool_size)
        self._active_connections: Dict[int, ConnectionInfo] = {}
        self._lock = threading.RLock()
        self._closed = False
        
        # Initialize pool with minimum connections
        self._initialize_pool()
        
        # Start cleanup thread
        self._cleanup_thread = threading.Thread(target=self._cleanup_idle_connections, daemon=True)
        self._cleanup_thread.start()
        
        logger.info(f"Database connection pool initialized with {pool_size} max connections")
    
    def _initialize_pool(self):
        """Initialize pool with a few connections."""
        initial_size = min(3, self.pool_size)  # Start with 3 connections or pool_size if smaller
        
        for _ in range(initial_size):
            try:
                conn_info = self._create_connection()
                self._pool.put_nowait(conn_info)
            except Full:
                break  # Pool is full
            except Exception as e:
                logger.error(f"Failed to create initial connection: {e}")
    
    def _create_connection(self) -> ConnectionInfo:
        """Create a new database connection."""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,  # Allow connection sharing between threads
            timeout=30.0  # 30 second timeout
        )
        
        # Configure connection
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance safety and performance
        conn.execute("PRAGMA cache_size=10000")  # Increase cache size
        conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory map
        
        current_time = time.time()
        return ConnectionInfo(
            connection=conn,
            created_at=current_time,
            last_used=current_time
        )    

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        if self._closed:
            raise RuntimeError("Connection pool is closed")
        
        conn_info = None
        try:
            # Try to get connection from pool
            try:
                conn_info = self._pool.get_nowait()
            except Empty:
                # Pool is empty, create new connection if under limit
                with self._lock:
                    if len(self._active_connections) < self.pool_size:
                        conn_info = self._create_connection()
                    else:
                        # Wait for a connection to become available
                        conn_info = self._pool.get(timeout=10.0)
            
            # Mark connection as in use
            conn_info.in_use = True
            conn_info.last_used = time.time()
            
            with self._lock:
                self._active_connections[id(conn_info.connection)] = conn_info
            
            yield conn_info.connection
            
        except Exception as e:
            logger.error(f"Error getting database connection: {e}")
            raise
        finally:
            # Return connection to pool
            if conn_info:
                conn_info.in_use = False
                conn_info.last_used = time.time()
                
                with self._lock:
                    self._active_connections.pop(id(conn_info.connection), None)
                
                # Check if connection is still valid
                try:
                    conn_info.connection.execute("SELECT 1")
                    self._pool.put_nowait(conn_info)
                except (sqlite3.Error, Full):
                    # Connection is invalid or pool is full, close it
                    try:
                        conn_info.connection.close()
                    except Exception:
                        pass
    
    def _cleanup_idle_connections(self):
        """Background thread to cleanup idle connections."""
        while not self._closed:
            try:
                time.sleep(60)  # Check every minute
                
                current_time = time.time()
                connections_to_close = []
                
                # Check pool for idle connections
                temp_connections = []
                while True:
                    try:
                        conn_info = self._pool.get_nowait()
                        if current_time - conn_info.last_used > self.max_idle_time:
                            connections_to_close.append(conn_info)
                        else:
                            temp_connections.append(conn_info)
                    except Empty:
                        break
                
                # Put back non-idle connections
                for conn_info in temp_connections:
                    try:
                        self._pool.put_nowait(conn_info)
                    except Full:
                        connections_to_close.append(conn_info)
                
                # Close idle connections
                for conn_info in connections_to_close:
                    try:
                        conn_info.connection.close()
                        logger.debug("Closed idle database connection")
                    except Exception as e:
                        logger.warning(f"Error closing idle connection: {e}")
                
            except Exception as e:
                logger.error(f"Error in connection cleanup thread: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self._lock:
            return {
                "pool_size": self.pool_size,
                "available_connections": self._pool.qsize(),
                "active_connections": len(self._active_connections),
                "total_connections": self._pool.qsize() + len(self._active_connections),
                "max_idle_time": self.max_idle_time,
                "closed": self._closed
            }
    
    def close(self):
        """Close all connections and shutdown pool."""
        self._closed = True
        
        # Close all connections in pool
        while True:
            try:
                conn_info = self._pool.get_nowait()
                conn_info.connection.close()
            except Empty:
                break
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {e}")
        
        # Close active connections
        with self._lock:
            for conn_info in self._active_connections.values():
                try:
                    conn_info.connection.close()
                except Exception as e:
                    logger.warning(f"Error closing active connection: {e}")
            self._active_connections.clear()
        
        logger.info("Database connection pool closed")


# Global connection pool instance
_connection_pool: Optional[DatabaseConnectionPool] = None
_pool_lock = threading.Lock()


def get_connection_pool(db_path: str = "data/normative_data.db", 
                       pool_size: int = 10) -> DatabaseConnectionPool:
    """Get or create global connection pool."""
    global _connection_pool
    
    with _pool_lock:
        if _connection_pool is None or _connection_pool._closed:
            _connection_pool = DatabaseConnectionPool(db_path, pool_size)
    
    return _connection_pool


def close_connection_pool():
    """Close global connection pool."""
    global _connection_pool
    
    with _pool_lock:
        if _connection_pool:
            _connection_pool.close()
            _connection_pool = None