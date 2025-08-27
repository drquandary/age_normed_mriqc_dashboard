#!/usr/bin/env python3
"""
Example usage of the QualityAssessor class.

This script demonstrates how to use the quality assessment engine
to evaluate MRIQC metrics with age-appropriate thresholds.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.quality_assessor import QualityAssessor
from app.models import MRIQCMetrics, SubjectInfo, ScanType, Sex, AgeGroup


def main():
    """Demonstrate quality assessment functionality."""
    print("=== Age-Normed MRIQC Quality Assessment Example ===\n")
    
    # Initialize the quality assessor
    assessor = QualityAssessor()
    
    # Example 1: High quality scan
    print("1. High Quality Scan Assessment:")
    print("-" * 40)
    
    good_metrics = MRIQCMetrics(
        snr=20.0,      # High SNR
        cnr=4.5,       # Good CNR
        fber=1800.0,   # Good FBER
        efc=0.42,      # Low EFC (good)
        fwhm_avg=2.6,  # Low FWHM (good)
        qi1=0.88,      # High QI1
        cjv=0.38       # Low CJV (good)
    )
    
    young_adult = SubjectInfo(
        subject_id="sub-good-001",
        age=28.0,
        sex=Sex.FEMALE,
        scan_type=ScanType.T1W,
        session="ses-01"
    )
    
    assessment = assessor.assess_quality(good_metrics, young_adult)
    print_assessment_results(assessment, good_metrics, young_adult)
    
    # Example 2: Poor quality scan
    print("\n2. Poor Quality Scan Assessment:")
    print("-" * 40)
    
    poor_metrics = MRIQCMetrics(
        snr=8.0,       # Low SNR
        cnr=2.0,       # Low CNR
        fber=800.0,    # Low FBER
        efc=0.65,      # High EFC (bad)
        fwhm_avg=3.8,  # High FWHM (bad)
        qi1=0.65,      # Low QI1
        cjv=0.75       # High CJV (bad)
    )
    
    assessment = assessor.assess_quality(poor_metrics, young_adult)
    print_assessment_results(assessment, poor_metrics, young_adult)
    
    # Example 3: Age comparison
    print("\n3. Age Group Comparison:")
    print("-" * 40)
    
    moderate_metrics = MRIQCMetrics(
        snr=14.0,
        cnr=3.2,
        efc=0.50,
        fwhm_avg=3.0
    )
    
    # Same metrics, different ages
    ages_and_names = [
        (8.0, "pediatric"),
        (15.0, "adolescent"), 
        (25.0, "young_adult"),
        (50.0, "middle_age"),
        (75.0, "elderly")
    ]
    
    for age, age_group_name in ages_and_names:
        subject = SubjectInfo(
            subject_id=f"sub-{age_group_name}",
            age=age,
            scan_type=ScanType.T1W
        )
        
        assessment = assessor.assess_quality(moderate_metrics, subject)
        print(f"{age_group_name.replace('_', ' ').title()} (age {age}): "
              f"{assessment.overall_status.value} "
              f"(score: {assessment.composite_score:.1f})")
    
    # Example 4: Threshold summary
    print("\n4. Quality Thresholds by Age Group:")
    print("-" * 40)
    
    for age_group in AgeGroup:
        print(f"\n{age_group.value.replace('_', ' ').title()}:")
        thresholds = assessor.get_threshold_summary(age_group)
        
        for metric, thresh_info in thresholds.items():
            direction = "↑" if thresh_info['direction'] == 'higher_better' else "↓"
            print(f"  {metric}: warn {thresh_info['warning_threshold']}, "
                  f"fail {thresh_info['fail_threshold']} {direction}")
    
    # Example 5: Assessment without age
    print("\n5. Assessment Without Age Information:")
    print("-" * 40)
    
    no_age_subject = SubjectInfo(
        subject_id="sub-no-age",
        age=None,
        scan_type=ScanType.T1W
    )
    
    assessment = assessor.assess_quality(moderate_metrics, no_age_subject)
    print_assessment_results(assessment, moderate_metrics, no_age_subject)


def print_assessment_results(assessment, metrics, subject):
    """Print formatted assessment results."""
    print(f"Subject: {subject.subject_id}")
    if subject.age:
        print(f"Age: {subject.age} years")
    print(f"Overall Status: {assessment.overall_status.value.upper()}")
    print(f"Composite Score: {assessment.composite_score:.1f}/100")
    print(f"Confidence: {assessment.confidence:.2f}")
    
    print("\nMetric Assessments:")
    for metric, status in assessment.metric_assessments.items():
        metric_value = getattr(metrics, metric, None)
        if metric_value is not None:
            print(f"  {metric}: {metric_value} -> {status.value}")
    
    if assessment.threshold_violations:
        print("\nThreshold Violations:")
        for metric, violation in assessment.threshold_violations.items():
            print(f"  {metric}: {violation['value']:.2f} "
                  f"({violation['severity']} threshold: {violation['threshold']:.2f})")
    
    if assessment.recommendations:
        print("\nRecommendations:")
        for i, rec in enumerate(assessment.recommendations, 1):
            print(f"  {i}. {rec}")
    
    if assessment.flags:
        print(f"\nFlags: {', '.join(assessment.flags)}")


if __name__ == "__main__":
    main()