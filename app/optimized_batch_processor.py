"""
Optimized batch processing for large datasets.

This module provides efficient batch processing algorithms with memory optimization,
parallel processing, and progress tracking for handling large MRIQC datasets.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Callable, Any, Iterator, Tuple
import multiprocessing as mp
from functools import partial
import gc

import pandas as pd
import numpy as np

from .cache_service import cache_service
from .connection_pool import get_connection_pool
from .mriqc_processor import MRIQCProcessor
from .age_normalizer import AgeNormalizer
from .quality_assessor import QualityAssessor
from .models import ProcessedSubject, ProcessingError, QualityStatus
from .common_utils.logging_config import setup_logging

logger = setup_logging(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 100  # Number of files to process in each chunk
    max_workers: int = None  # Number of worker processes/threads
    use_multiprocessing: bool = True  # Use multiprocessing vs threading
    memory_limit_mb: int = 1024  # Memory limit per worker in MB
    cache_results: bool = True  # Whether to cache intermediate results
    progress_callback: Optional[Callable] = None  # Progress callback function


@dataclass
class BatchResult:
    """Result of batch processing operation."""
    total_files: int
    successful: int
    failed: int
    processing_time: float
    results: List[ProcessedSubject]
    errors: List[ProcessingError]
    memory_usage_mb: float


class OptimizedBatchProcessor:
    """Optimized batch processor for large MRIQC datasets."""
    
    def __init__(self, config: BatchConfig = None):
        """Initialize optimized batch processor."""
        self.config = config or BatchConfig()
        
        # Set default max_workers based on CPU count
        if self.config.max_workers is None:
            self.config.max_workers = min(mp.cpu_count(), 8)  # Cap at 8 to avoid resource exhaustion
        
        self.processor = MRIQCProcessor()
        self.normalizer = AgeNormalizer()
        self.assessor = QualityAssessor()
        
        logger.info(f"Optimized batch processor initialized with {self.config.max_workers} workers")
    
    def process_files_batch(self, file_paths: List[str], 
                           apply_quality_assessment: bool = True,
                           custom_thresholds: Optional[Dict] = None) -> BatchResult:
        """
        Process multiple MRIQC files in optimized batches.
        
        Args:
            file_paths: List of file paths to process
            apply_quality_assessment: Whether to apply quality assessment
            custom_thresholds: Custom quality thresholds
            
        Returns:
            BatchResult with processing statistics and results
        """
        start_time = time.time()
        total_files = len(file_paths)
        
        logger.info(f"Starting optimized batch processing of {total_files} files")
        
        # Split files into chunks for memory efficiency
        file_chunks = self._create_file_chunks(file_paths)
        
        results = []
        errors = []
        processed_count = 0
        
        # Process chunks
        for chunk_idx, chunk in enumerate(file_chunks):
            logger.info(f"Processing chunk {chunk_idx + 1}/{len(file_chunks)} ({len(chunk)} files)")
            
            chunk_results, chunk_errors = self._process_chunk(
                chunk, apply_quality_assessment, custom_thresholds
            )
            
            results.extend(chunk_results)
            errors.extend(chunk_errors)
            processed_count += len(chunk)
            
            # Update progress
            if self.config.progress_callback:
                progress = {
                    'processed': processed_count,
                    'total': total_files,
                    'progress_percent': (processed_count / total_files) * 100,
                    'chunk': chunk_idx + 1,
                    'total_chunks': len(file_chunks)
                }
                self.config.progress_callback(progress)
            
            # Force garbage collection between chunks
            gc.collect()
        
        processing_time = time.time() - start_time
        memory_usage = self._get_memory_usage()
        
        batch_result = BatchResult(
            total_files=total_files,
            successful=len(results),
            failed=len(errors),
            processing_time=processing_time,
            results=results,
            errors=errors,
            memory_usage_mb=memory_usage
        )
        
        logger.info(f"Batch processing completed: {batch_result.successful}/{total_files} successful "
                   f"in {processing_time:.2f}s")
        
        return batch_result 
   
    def _create_file_chunks(self, file_paths: List[str]) -> List[List[str]]:
        """Split file paths into processing chunks."""
        chunks = []
        for i in range(0, len(file_paths), self.config.chunk_size):
            chunk = file_paths[i:i + self.config.chunk_size]
            chunks.append(chunk)
        return chunks
    
    def _process_chunk(self, file_paths: List[str], 
                      apply_quality_assessment: bool,
                      custom_thresholds: Optional[Dict]) -> Tuple[List[ProcessedSubject], List[ProcessingError]]:
        """Process a chunk of files using parallel processing."""
        if self.config.use_multiprocessing:
            return self._process_chunk_multiprocessing(
                file_paths, apply_quality_assessment, custom_thresholds
            )
        else:
            return self._process_chunk_threading(
                file_paths, apply_quality_assessment, custom_thresholds
            )
    
    def _process_chunk_multiprocessing(self, file_paths: List[str],
                                     apply_quality_assessment: bool,
                                     custom_thresholds: Optional[Dict]) -> Tuple[List[ProcessedSubject], List[ProcessingError]]:
        """Process chunk using multiprocessing."""
        results = []
        errors = []
        
        # Create partial function with fixed arguments
        process_func = partial(
            _process_single_file_worker,
            apply_quality_assessment=apply_quality_assessment,
            custom_thresholds=custom_thresholds
        )
        
        with ProcessPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(process_func, file_path): file_path 
                for file_path in file_paths
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_path):
                file_path = future_to_path[future]
                try:
                    result = future.result()
                    if isinstance(result, ProcessedSubject):
                        results.append(result)
                    else:
                        errors.append(ProcessingError(
                            file_path=file_path,
                            error_type="processing_error",
                            message=str(result)
                        ))
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    errors.append(ProcessingError(
                        file_path=file_path,
                        error_type="exception",
                        message=str(e)
                    ))
        
        return results, errors
    
    def _process_chunk_threading(self, file_paths: List[str],
                               apply_quality_assessment: bool,
                               custom_thresholds: Optional[Dict]) -> Tuple[List[ProcessedSubject], List[ProcessingError]]:
        """Process chunk using threading."""
        results = []
        errors = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(
                    self._process_single_file,
                    file_path,
                    apply_quality_assessment,
                    custom_thresholds
                ): file_path 
                for file_path in file_paths
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_path):
                file_path = future_to_path[future]
                try:
                    result = future.result()
                    if isinstance(result, ProcessedSubject):
                        results.append(result)
                    else:
                        errors.append(ProcessingError(
                            file_path=file_path,
                            error_type="processing_error",
                            message=str(result)
                        ))
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")
                    errors.append(ProcessingError(
                        file_path=file_path,
                        error_type="exception",
                        message=str(e)
                    ))
        
        return results, errors    

    def _process_single_file(self, file_path: str, 
                           apply_quality_assessment: bool,
                           custom_thresholds: Optional[Dict]) -> ProcessedSubject:
        """Process a single MRIQC file with caching."""
        try:
            # Check cache first if enabled
            if self.config.cache_results:
                file_hash = cache_service.generate_hash(f"{file_path}:{Path(file_path).stat().st_mtime}")
                cached_result = cache_service.get_processed_subject(file_hash)
                if cached_result:
                    logger.debug(f"Using cached result for {file_path}")
                    return ProcessedSubject(**cached_result)
            
            # Process file
            processed_subject = self.processor.process_file(
                file_path, 
                apply_quality_assessment=apply_quality_assessment,
                custom_thresholds=custom_thresholds
            )
            
            # Cache result if enabled
            if self.config.cache_results and processed_subject:
                cache_service.set_processed_subject(
                    file_hash, 
                    processed_subject.model_dump(),
                    ttl=3600  # 1 hour cache
                )
            
            return processed_subject
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            raise
    
    def process_dataframe_batch(self, df: pd.DataFrame,
                               apply_quality_assessment: bool = True,
                               custom_thresholds: Optional[Dict] = None) -> BatchResult:
        """
        Process MRIQC data from a pandas DataFrame in optimized batches.
        
        Args:
            df: DataFrame containing MRIQC data
            apply_quality_assessment: Whether to apply quality assessment
            custom_thresholds: Custom quality thresholds
            
        Returns:
            BatchResult with processing statistics and results
        """
        start_time = time.time()
        total_rows = len(df)
        
        logger.info(f"Starting optimized DataFrame batch processing of {total_rows} rows")
        
        # Split DataFrame into chunks
        df_chunks = self._create_dataframe_chunks(df)
        
        results = []
        errors = []
        processed_count = 0
        
        # Process chunks
        for chunk_idx, chunk_df in enumerate(df_chunks):
            logger.info(f"Processing DataFrame chunk {chunk_idx + 1}/{len(df_chunks)} ({len(chunk_df)} rows)")
            
            chunk_results, chunk_errors = self._process_dataframe_chunk(
                chunk_df, apply_quality_assessment, custom_thresholds
            )
            
            results.extend(chunk_results)
            errors.extend(chunk_errors)
            processed_count += len(chunk_df)
            
            # Update progress
            if self.config.progress_callback:
                progress = {
                    'processed': processed_count,
                    'total': total_rows,
                    'progress_percent': (processed_count / total_rows) * 100,
                    'chunk': chunk_idx + 1,
                    'total_chunks': len(df_chunks)
                }
                self.config.progress_callback(progress)
            
            # Force garbage collection between chunks
            gc.collect()
        
        processing_time = time.time() - start_time
        memory_usage = self._get_memory_usage()
        
        batch_result = BatchResult(
            total_files=total_rows,
            successful=len(results),
            failed=len(errors),
            processing_time=processing_time,
            results=results,
            errors=errors,
            memory_usage_mb=memory_usage
        )
        
        logger.info(f"DataFrame batch processing completed: {batch_result.successful}/{total_rows} successful "
                   f"in {processing_time:.2f}s")
        
        return batch_result 
   
    def _create_dataframe_chunks(self, df: pd.DataFrame) -> List[pd.DataFrame]:
        """Split DataFrame into processing chunks."""
        chunks = []
        for i in range(0, len(df), self.config.chunk_size):
            chunk = df.iloc[i:i + self.config.chunk_size].copy()
            chunks.append(chunk)
        return chunks
    
    def _process_dataframe_chunk(self, df: pd.DataFrame,
                               apply_quality_assessment: bool,
                               custom_thresholds: Optional[Dict]) -> Tuple[List[ProcessedSubject], List[ProcessingError]]:
        """Process a DataFrame chunk."""
        results = []
        errors = []
        
        # Process each row in the chunk
        for idx, row in df.iterrows():
            try:
                # Convert row to ProcessedSubject
                processed_subject = self.processor.process_dataframe_row(
                    row,
                    apply_quality_assessment=apply_quality_assessment,
                    custom_thresholds=custom_thresholds
                )
                
                if processed_subject:
                    results.append(processed_subject)
                else:
                    errors.append(ProcessingError(
                        file_path=f"row_{idx}",
                        error_type="processing_error",
                        message="Failed to process DataFrame row"
                    ))
                    
            except Exception as e:
                logger.error(f"Error processing DataFrame row {idx}: {e}")
                errors.append(ProcessingError(
                    file_path=f"row_{idx}",
                    error_type="exception",
                    message=str(e)
                ))
        
        return results, errors
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # Convert to MB
        except ImportError:
            return 0.0
    
    async def process_files_async(self, file_paths: List[str],
                                 apply_quality_assessment: bool = True,
                                 custom_thresholds: Optional[Dict] = None) -> BatchResult:
        """
        Asynchronously process multiple MRIQC files.
        
        Args:
            file_paths: List of file paths to process
            apply_quality_assessment: Whether to apply quality assessment
            custom_thresholds: Custom quality thresholds
            
        Returns:
            BatchResult with processing statistics and results
        """
        start_time = time.time()
        total_files = len(file_paths)
        
        logger.info(f"Starting async batch processing of {total_files} files")
        
        # Create semaphore to limit concurrent operations
        semaphore = asyncio.Semaphore(self.config.max_workers)
        
        async def process_file_with_semaphore(file_path: str):
            async with semaphore:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    self._process_single_file,
                    file_path,
                    apply_quality_assessment,
                    custom_thresholds
                )
        
        # Process all files concurrently
        tasks = [process_file_with_semaphore(fp) for fp in file_paths]
        
        results = []
        errors = []
        
        # Collect results as they complete
        for i, task in enumerate(asyncio.as_completed(tasks)):
            try:
                result = await task
                if isinstance(result, ProcessedSubject):
                    results.append(result)
                else:
                    errors.append(ProcessingError(
                        file_path=file_paths[i],
                        error_type="processing_error",
                        message=str(result)
                    ))
            except Exception as e:
                logger.error(f"Error in async processing: {e}")
                errors.append(ProcessingError(
                    file_path=file_paths[i],
                    error_type="exception",
                    message=str(e)
                ))
            
            # Update progress
            if self.config.progress_callback:
                progress = {
                    'processed': len(results) + len(errors),
                    'total': total_files,
                    'progress_percent': ((len(results) + len(errors)) / total_files) * 100
                }
                self.config.progress_callback(progress)
        
        processing_time = time.time() - start_time
        memory_usage = self._get_memory_usage()
        
        return BatchResult(
            total_files=total_files,
            successful=len(results),
            failed=len(errors),
            processing_time=processing_time,
            results=results,
            errors=errors,
            memory_usage_mb=memory_usage
        )


def _process_single_file_worker(file_path: str, 
                               apply_quality_assessment: bool,
                               custom_thresholds: Optional[Dict]) -> ProcessedSubject:
    """Worker function for multiprocessing."""
    try:
        # Create new instances for each worker process
        processor = MRIQCProcessor()
        
        return processor.process_file(
            file_path,
            apply_quality_assessment=apply_quality_assessment,
            custom_thresholds=custom_thresholds
        )
    except Exception as e:
        logger.error(f"Worker error processing {file_path}: {e}")
        raise