#!/usr/bin/env python3
"""
Example usage of the MRIQC processor.

This script demonstrates how to use the MRIQCProcessor class to parse
and process MRIQC output files.
"""

import asyncio
import sys
from pathlib import Path

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from app.mriqc_processor import MRIQCProcessor


async def main():
    """Main example function."""
    print("MRIQC Processor Example")
    print("=" * 50)
    
    # Initialize processor
    processor = MRIQCProcessor(max_workers=2)
    
    # Get sample data files
    data_dir = Path(__file__).parent.parent / "data"
    anatomical_file = data_dir / "sample_mriqc_anatomical.csv"
    functional_file = data_dir / "sample_mriqc_functional.csv"
    
    print(f"\n1. Processing single anatomical file: {anatomical_file.name}")
    print("-" * 50)
    
    try:
        # Process single file
        subjects = processor.process_single_file(anatomical_file)
        print(f"Successfully processed {len(subjects)} subjects")
        
        # Display first subject details
        if subjects:
            subject = subjects[0]
            print(f"\nFirst subject details:")
            print(f"  Subject ID: {subject.subject_info.subject_id}")
            print(f"  Age: {subject.subject_info.age}")
            print(f"  Sex: {subject.subject_info.sex}")
            print(f"  Scan Type: {subject.subject_info.scan_type}")
            print(f"  Site: {subject.subject_info.site}")
            
            print(f"\nQuality metrics:")
            metrics = subject.raw_metrics
            print(f"  SNR: {metrics.snr}")
            print(f"  CNR: {metrics.cnr}")
            print(f"  FBER: {metrics.fber}")
            print(f"  EFC: {metrics.efc}")
            print(f"  FWHM avg: {metrics.fwhm_avg}")
            print(f"  QI1: {metrics.qi1}")
            
    except Exception as e:
        print(f"Error processing anatomical file: {e}")
    
    print(f"\n2. Processing single functional file: {functional_file.name}")
    print("-" * 50)
    
    try:
        # Process functional file
        subjects = processor.process_single_file(functional_file)
        print(f"Successfully processed {len(subjects)} subjects")
        
        # Display first subject details
        if subjects:
            subject = subjects[0]
            print(f"\nFirst subject details:")
            print(f"  Subject ID: {subject.subject_info.subject_id}")
            print(f"  Scan Type: {subject.subject_info.scan_type}")
            
            print(f"\nFunctional metrics:")
            metrics = subject.raw_metrics
            print(f"  DVARS: {metrics.dvars}")
            print(f"  FD mean: {metrics.fd_mean}")
            print(f"  FD num: {metrics.fd_num}")
            print(f"  FD perc: {metrics.fd_perc}")
            print(f"  GCOR: {metrics.gcor}")
            print(f"  Outlier fraction: {metrics.outlier_fraction}")
            
    except Exception as e:
        print(f"Error processing functional file: {e}")
    
    print(f"\n3. Batch processing multiple files")
    print("-" * 50)
    
    # Progress callback
    def progress_callback(status):
        percent = status['progress_percent']
        completed = status['completed']
        total = status['total']
        print(f"Progress: {percent:.1f}% ({completed}/{total} files)")
    
    try:
        # Batch process files
        file_paths = [anatomical_file, functional_file]
        subjects, errors = await processor.batch_process_files(
            file_paths, 
            progress_callback=progress_callback
        )
        
        print(f"\nBatch processing complete:")
        print(f"  Total subjects processed: {len(subjects)}")
        print(f"  Processing errors: {len(errors)}")
        
        # Group by scan type
        scan_types = {}
        for subject in subjects:
            scan_type = subject.subject_info.scan_type
            if scan_type not in scan_types:
                scan_types[scan_type] = 0
            scan_types[scan_type] += 1
        
        print(f"\nSubjects by scan type:")
        for scan_type, count in scan_types.items():
            print(f"  {scan_type}: {count}")
        
        if errors:
            print(f"\nErrors encountered:")
            for error in errors:
                print(f"  {error.error_type}: {error.message}")
    
    except Exception as e:
        print(f"Error in batch processing: {e}")
    
    print(f"\n4. Supported metrics")
    print("-" * 50)
    
    # Show supported metrics
    supported_metrics = processor.get_supported_metrics()
    print(f"Anatomical metrics: {', '.join(supported_metrics['anatomical'])}")
    print(f"Functional metrics: {', '.join(supported_metrics['functional'])}")
    
    print(f"\nExample complete!")


if __name__ == "__main__":
    asyncio.run(main())