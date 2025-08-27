"""
Example usage of the Age-Normed MRIQC Dashboard API endpoints.

This script demonstrates how to use the API for uploading MRIQC files,
processing them, and retrieving results.
"""

import requests
import time
import json
from pathlib import Path
import tempfile
import csv


def create_sample_mriqc_file():
    """Create a sample MRIQC CSV file for testing."""
    sample_data = [
        {
            'bids_name': 'sub-001_T1w.nii.gz',
            'snr': 12.5,
            'cnr': 3.2,
            'fber': 1500.0,
            'efc': 0.45,
            'fwhm_avg': 2.8,
            'qi_1': 0.85,
            'cjv': 0.42,
            'age': 25.5,
            'sex': 'F'
        },
        {
            'bids_name': 'sub-002_T1w.nii.gz',
            'snr': 10.8,
            'cnr': 2.9,
            'fber': 1200.0,
            'efc': 0.52,
            'fwhm_avg': 3.1,
            'qi_1': 0.78,
            'cjv': 0.48,
            'age': 30.2,
            'sex': 'M'
        },
        {
            'bids_name': 'sub-003_T1w.nii.gz',
            'snr': 8.2,
            'cnr': 2.1,
            'fber': 900.0,
            'efc': 0.68,
            'fwhm_avg': 3.8,
            'qi_1': 0.65,
            'cjv': 0.62,
            'age': 45.8,
            'sex': 'F'
        }
    ]
    
    # Create temporary CSV file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    
    fieldnames = ['bids_name', 'snr', 'cnr', 'fber', 'efc', 'fwhm_avg', 'qi_1', 'cjv', 'age', 'sex']
    writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(sample_data)
    
    temp_file.close()
    return Path(temp_file.name)


def main():
    """Demonstrate API usage workflow."""
    base_url = "http://localhost:8000/api"
    
    print("Age-Normed MRIQC Dashboard API Example")
    print("=" * 50)
    
    # Step 1: Health check
    print("\n1. Checking API health...")
    response = requests.get(f"{base_url}/health")
    if response.status_code == 200:
        print("✓ API is healthy")
        print(f"  Response: {response.json()}")
    else:
        print("✗ API health check failed")
        return
    
    # Step 2: Create and upload sample file
    print("\n2. Creating and uploading sample MRIQC file...")
    sample_file = create_sample_mriqc_file()
    
    try:
        with open(sample_file, 'rb') as f:
            files = {'file': ('sample_mriqc.csv', f, 'text/csv')}
            response = requests.post(f"{base_url}/upload", files=files)
        
        if response.status_code == 200:
            upload_data = response.json()
            file_id = upload_data['file_id']
            print("✓ File uploaded successfully")
            print(f"  File ID: {file_id}")
            print(f"  Subjects count: {upload_data['subjects_count']}")
        else:
            print(f"✗ File upload failed: {response.json()}")
            return
    
    finally:
        # Clean up temp file
        sample_file.unlink(missing_ok=True)
    
    # Step 3: Process the file
    print("\n3. Processing uploaded file...")
    process_request = {
        "file_id": file_id,
        "apply_quality_assessment": True
    }
    
    response = requests.post(f"{base_url}/process", json=process_request)
    if response.status_code == 200:
        process_data = response.json()
        batch_id = process_data['batch_id']
        print("✓ Processing started")
        print(f"  Batch ID: {batch_id}")
    else:
        print(f"✗ Processing failed: {response.json()}")
        return
    
    # Step 4: Monitor batch status
    print("\n4. Monitoring batch processing status...")
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        response = requests.get(f"{base_url}/batch/{batch_id}/status")
        if response.status_code == 200:
            status_data = response.json()
            status = status_data['status']
            progress = status_data['progress']
            
            print(f"  Status: {status}")
            if 'progress_percent' in progress:
                print(f"  Progress: {progress['progress_percent']:.1f}%")
            
            if status == 'completed':
                print("✓ Processing completed successfully")
                break
            elif status == 'failed':
                print("✗ Processing failed")
                if 'error_message' in status_data:
                    print(f"  Error: {status_data['error_message']}")
                return
            
            time.sleep(1)  # Wait 1 second before checking again
            attempt += 1
        else:
            print(f"✗ Failed to get batch status: {response.json()}")
            return
    
    if attempt >= max_attempts:
        print("⚠ Processing is taking longer than expected")
    
    # Step 5: Get processed subjects
    print("\n5. Retrieving processed subjects...")
    response = requests.get(f"{base_url}/subjects", params={'batch_id': batch_id})
    if response.status_code == 200:
        subjects_data = response.json()
        subjects = subjects_data['subjects']
        print(f"✓ Retrieved {len(subjects)} subjects")
        
        for i, subject in enumerate(subjects, 1):
            subject_info = subject['subject_info']
            quality = subject['quality_assessment']
            print(f"  Subject {i}: {subject_info['subject_id']}")
            print(f"    Age: {subject_info.get('age', 'N/A')}")
            print(f"    Quality Status: {quality['overall_status']}")
            print(f"    Composite Score: {quality['composite_score']:.1f}")
    else:
        print(f"✗ Failed to retrieve subjects: {response.json()}")
        return
    
    # Step 6: Get individual subject details
    if subjects:
        print("\n6. Getting detailed information for first subject...")
        first_subject_id = subjects[0]['subject_info']['subject_id']
        response = requests.get(f"{base_url}/subjects/{first_subject_id}")
        if response.status_code == 200:
            detail_data = response.json()
            subject = detail_data['subject']
            recommendations = detail_data['recommendations']
            
            print(f"✓ Retrieved details for {first_subject_id}")
            print(f"  Raw metrics available: {len([k for k, v in subject['raw_metrics'].items() if v is not None])}")
            print(f"  Recommendations: {len(recommendations)}")
            for rec in recommendations[:3]:  # Show first 3 recommendations
                print(f"    - {rec}")
        else:
            print(f"✗ Failed to get subject details: {response.json()}")
    
    # Step 7: Get dashboard summary
    print("\n7. Getting dashboard summary...")
    response = requests.get(f"{base_url}/dashboard/summary", params={'batch_id': batch_id})
    if response.status_code == 200:
        summary_data = response.json()
        print("✓ Retrieved dashboard summary")
        print(f"  Total subjects: {summary_data['total_subjects']}")
        print(f"  Quality distribution: {summary_data['quality_distribution']}")
        print(f"  Exclusion rate: {summary_data['exclusion_rate']:.1%}")
    else:
        print(f"✗ Failed to get dashboard summary: {response.json()}")
    
    # Step 8: Clean up (optional)
    print("\n8. Cleaning up batch data...")
    response = requests.delete(f"{base_url}/batch/{batch_id}")
    if response.status_code == 200:
        print("✓ Batch data cleaned up")
    else:
        print(f"⚠ Failed to clean up batch data: {response.json()}")
    
    print("\n" + "=" * 50)
    print("API demonstration completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to API server.")
        print("  Make sure the server is running on http://localhost:8000")
        print("  Start the server with: uvicorn app.main:app --reload")
    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
    except Exception as e:
        print(f"✗ Unexpected error: {str(e)}")