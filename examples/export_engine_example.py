"""
Example usage of the ExportEngine for generating CSV and PDF reports.

This example demonstrates how to use the export functionality to generate
comprehensive reports from processed MRIQC data.
"""

import sys
import asyncio
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.export_engine import ExportEngine
from app.models import (
    ProcessedSubject, SubjectInfo, MRIQCMetrics, QualityAssessment,
    NormalizedMetrics, QualityStatus, AgeGroup, ScanType, Sex
)


def create_sample_subjects():
    """Create sample processed subjects for demonstration."""
    subjects = []
    
    # Subject 1: High quality young adult
    subject1 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-001",
            age=25.5,
            sex=Sex.FEMALE,
            session="ses-01",
            scan_type=ScanType.T1W,
            acquisition_date=datetime(2024, 1, 15, 10, 30),
            site="site-A",
            scanner="Siemens Prisma 3T"
        ),
        raw_metrics=MRIQCMetrics(
            snr=15.2,
            cnr=4.1,
            fber=2100.0,
            efc=0.42,
            fwhm_avg=2.6,
            qi1=0.88,
            cjv=0.38
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(snr=15.2, cnr=4.1),
            percentiles={
                "snr": 85.0,
                "cnr": 78.0,
                "fber": 82.0,
                "efc": 65.0,
                "fwhm_avg": 70.0
            },
            z_scores={
                "snr": 1.2,
                "cnr": 0.9,
                "fber": 1.1,
                "efc": 0.4,
                "fwhm_avg": 0.6
            },
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="HCP-YA"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={
                "snr": QualityStatus.PASS,
                "cnr": QualityStatus.PASS,
                "fber": QualityStatus.PASS,
                "efc": QualityStatus.PASS,
                "fwhm_avg": QualityStatus.PASS
            },
            composite_score=85.3,
            recommendations=["Excellent quality scan, suitable for all analyses"],
            flags=[],
            confidence=0.95
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 30),
        processing_version="1.0.0"
    )
    subjects.append(subject1)
    
    # Subject 2: Poor quality elderly subject
    subject2 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-002",
            age=72.0,
            sex=Sex.MALE,
            session="ses-01",
            scan_type=ScanType.T1W,
            acquisition_date=datetime(2024, 1, 15, 11, 15),
            site="site-B",
            scanner="GE Discovery MR750 3T"
        ),
        raw_metrics=MRIQCMetrics(
            snr=7.8,
            cnr=1.9,
            fber=650.0,
            efc=0.72,
            fwhm_avg=4.8,
            qi1=0.45,
            cjv=0.85
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(snr=7.8, cnr=1.9),
            percentiles={
                "snr": 12.0,
                "cnr": 8.0,
                "fber": 15.0,
                "efc": 95.0,
                "fwhm_avg": 92.0
            },
            z_scores={
                "snr": -2.1,
                "cnr": -2.5,
                "fber": -2.0,
                "efc": 2.8,
                "fwhm_avg": 2.6
            },
            age_group=AgeGroup.ELDERLY,
            normative_dataset="elderly_norms_v1"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.FAIL,
            metric_assessments={
                "snr": QualityStatus.FAIL,
                "cnr": QualityStatus.FAIL,
                "fber": QualityStatus.FAIL,
                "efc": QualityStatus.FAIL,
                "fwhm_avg": QualityStatus.FAIL
            },
            composite_score=22.1,
            recommendations=[
                "Exclude from analysis due to poor image quality",
                "Consider rescanning if possible",
                "Check scanner calibration and subject motion"
            ],
            flags=[
                "low_snr",
                "low_cnr",
                "high_noise",
                "excessive_smoothing",
                "motion_artifacts"
            ],
            confidence=0.98,
            threshold_violations={
                "snr": {"value": 7.8, "threshold": 10.0, "severity": "fail"},
                "cnr": {"value": 1.9, "threshold": 2.5, "severity": "fail"},
                "fwhm_avg": {"value": 4.8, "threshold": 3.5, "severity": "fail"}
            }
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 35),
        processing_version="1.0.0"
    )
    subjects.append(subject2)
    
    # Subject 3: Borderline quality middle-aged subject
    subject3 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-003",
            age=45.2,
            sex=Sex.FEMALE,
            session="ses-01",
            scan_type=ScanType.T1W,
            acquisition_date=datetime(2024, 1, 15, 12, 45),
            site="site-A",
            scanner="Siemens Prisma 3T"
        ),
        raw_metrics=MRIQCMetrics(
            snr=11.5,
            cnr=2.8,
            fber=1200.0,
            efc=0.55,
            fwhm_avg=3.2,
            qi1=0.72,
            cjv=0.52
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(snr=11.5, cnr=2.8),
            percentiles={
                "snr": 45.0,
                "cnr": 42.0,
                "fber": 48.0,
                "efc": 78.0,
                "fwhm_avg": 65.0
            },
            z_scores={
                "snr": -0.2,
                "cnr": -0.3,
                "fber": -0.1,
                "efc": 0.8,
                "fwhm_avg": 0.4
            },
            age_group=AgeGroup.MIDDLE_AGE,
            normative_dataset="middle_age_norms"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.WARNING,
            metric_assessments={
                "snr": QualityStatus.WARNING,
                "cnr": QualityStatus.WARNING,
                "fber": QualityStatus.PASS,
                "efc": QualityStatus.PASS,
                "fwhm_avg": QualityStatus.PASS
            },
            composite_score=68.7,
            recommendations=[
                "Manual review recommended",
                "Consider inclusion with caution",
                "Monitor for analysis sensitivity"
            ],
            flags=[
                "borderline_snr",
                "borderline_cnr"
            ],
            confidence=0.75,
            threshold_violations={
                "snr": {"value": 11.5, "threshold": 12.0, "severity": "warning"},
                "cnr": {"value": 2.8, "threshold": 3.0, "severity": "warning"}
            }
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 40),
        processing_version="1.0.0"
    )
    subjects.append(subject3)
    
    # Subject 4: Functional scan (different metrics)
    subject4 = ProcessedSubject(
        subject_info=SubjectInfo(
            subject_id="sub-004",
            age=28.0,
            sex=Sex.MALE,
            session="ses-01",
            scan_type=ScanType.BOLD,
            acquisition_date=datetime(2024, 1, 15, 13, 30),
            site="site-C",
            scanner="Philips Achieva 3T"
        ),
        raw_metrics=MRIQCMetrics(
            dvars=1.15,
            fd_mean=0.12,
            fd_num=8,
            fd_perc=3.2,
            gcor=0.03,
            outlier_fraction=0.02
        ),
        normalized_metrics=NormalizedMetrics(
            raw_metrics=MRIQCMetrics(dvars=1.15, fd_mean=0.12),
            percentiles={
                "dvars": 65.0,
                "fd_mean": 70.0,
                "gcor": 55.0,
                "outlier_fraction": 60.0
            },
            z_scores={
                "dvars": 0.4,
                "fd_mean": 0.5,
                "gcor": 0.1,
                "outlier_fraction": 0.2
            },
            age_group=AgeGroup.YOUNG_ADULT,
            normative_dataset="HCP-YA-BOLD"
        ),
        quality_assessment=QualityAssessment(
            overall_status=QualityStatus.PASS,
            metric_assessments={
                "dvars": QualityStatus.PASS,
                "fd_mean": QualityStatus.PASS,
                "gcor": QualityStatus.PASS,
                "outlier_fraction": QualityStatus.PASS
            },
            composite_score=78.9,
            recommendations=["Good quality functional scan"],
            flags=[],
            confidence=0.88
        ),
        processing_timestamp=datetime(2024, 1, 15, 14, 45),
        processing_version="1.0.0"
    )
    subjects.append(subject4)
    
    return subjects


def main():
    """Main example function."""
    print("Age-Normed MRIQC Dashboard Export Engine Example")
    print("=" * 50)
    
    # Create export engine
    export_engine = ExportEngine()
    print("✓ Export engine initialized")
    
    # Create sample data
    subjects = create_sample_subjects()
    print(f"✓ Created {len(subjects)} sample subjects")
    
    # Generate study summary
    print("\n1. Generating study summary...")
    study_summary = export_engine.generate_study_summary(
        subjects, 
        study_name="Multi-Site Aging Study"
    )
    
    print(f"   - Total subjects: {study_summary.total_subjects}")
    print(f"   - Quality distribution:")
    for status, count in study_summary.quality_distribution.items():
        percentage = (count / study_summary.total_subjects * 100)
        print(f"     • {status.value}: {count} ({percentage:.1f}%)")
    
    print(f"   - Exclusion rate: {study_summary.exclusion_rate:.1%}")
    
    # Export CSV
    print("\n2. Exporting to CSV...")
    csv_content = export_engine.export_subjects_csv(
        subjects,
        include_raw_metrics=True,
        include_normalized_metrics=True,
        include_quality_assessment=True
    )
    
    # Save CSV to file
    csv_path = Path("example_export.csv")
    with open(csv_path, 'w') as f:
        f.write(csv_content)
    
    print(f"   ✓ CSV exported to {csv_path} ({len(csv_content)} characters)")
    
    # Export study summary CSV
    print("\n3. Exporting study summary to CSV...")
    summary_csv = export_engine.export_study_summary_csv(study_summary)
    
    summary_csv_path = Path("example_study_summary.csv")
    with open(summary_csv_path, 'w') as f:
        f.write(summary_csv)
    
    print(f"   ✓ Study summary CSV exported to {summary_csv_path}")
    
    # Generate PDF report
    print("\n4. Generating PDF report...")
    pdf_content = export_engine.generate_pdf_report(
        subjects,
        study_name="Multi-Site Aging Study",
        include_individual_subjects=True,
        include_summary_charts=True
    )
    
    # Save PDF to file
    pdf_path = Path("example_report.pdf")
    with open(pdf_path, 'wb') as f:
        f.write(pdf_content)
    
    print(f"   ✓ PDF report generated: {pdf_path} ({len(pdf_content)} bytes)")
    
    # Test different export options
    print("\n5. Testing export options...")
    
    # Export only raw metrics
    raw_only_csv = export_engine.export_subjects_csv(
        subjects,
        include_raw_metrics=True,
        include_normalized_metrics=False,
        include_quality_assessment=False
    )
    print(f"   ✓ Raw metrics only CSV: {len(raw_only_csv)} characters")
    
    # Export only quality assessments
    qa_only_csv = export_engine.export_subjects_csv(
        subjects,
        include_raw_metrics=False,
        include_normalized_metrics=False,
        include_quality_assessment=True
    )
    print(f"   ✓ Quality assessment only CSV: {len(qa_only_csv)} characters")
    
    # Filter by quality status
    passed_subjects = [s for s in subjects if s.quality_assessment.overall_status == QualityStatus.PASS]
    passed_csv = export_engine.export_subjects_csv(passed_subjects)
    print(f"   ✓ Passed subjects only CSV: {len(passed_csv)} characters ({len(passed_subjects)} subjects)")
    
    print("\n6. Export validation...")
    
    # Validate CSV structure
    import csv
    import io
    
    reader = csv.DictReader(io.StringIO(csv_content))
    rows = list(reader)
    fieldnames = reader.fieldnames
    
    print(f"   ✓ CSV has {len(rows)} rows and {len(fieldnames)} columns")
    print(f"   ✓ Sample columns: {', '.join(list(fieldnames)[:5])}...")
    
    # Validate data integrity
    for i, (row, subject) in enumerate(zip(rows, subjects)):
        assert row['subject_id'] == subject.subject_info.subject_id
        assert row['overall_quality_status'] == subject.quality_assessment.overall_status.value
        if subject.subject_info.age:
            assert float(row['age']) == subject.subject_info.age
    
    print(f"   ✓ Data integrity validated for all {len(rows)} subjects")
    
    print("\n" + "=" * 50)
    print("Export engine example completed successfully!")
    print("\nGenerated files:")
    print(f"  • {csv_path}")
    print(f"  • {summary_csv_path}")
    print(f"  • {pdf_path}")
    print("\nThe export engine provides comprehensive reporting capabilities")
    print("for age-normalized MRIQC quality control data.")


if __name__ == "__main__":
    main()