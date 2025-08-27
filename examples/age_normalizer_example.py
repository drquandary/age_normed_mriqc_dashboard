"""
Example usage of the age normalization functionality.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.age_normalizer import AgeNormalizer
from app.models import MRIQCMetrics, AgeGroup


def main():
    """Demonstrate age normalization functionality."""
    
    # Initialize the age normalizer
    print("Initializing Age Normalizer...")
    normalizer = AgeNormalizer("data/normative_data.db")
    
    # Example 1: Age group assignment
    print("\n=== Age Group Assignment ===")
    test_ages = [8.0, 15.0, 25.0, 45.0, 70.0, 150.0]
    
    for age in test_ages:
        age_group = normalizer.get_age_group(age)
        print(f"Age {age}: {age_group.value if age_group else 'No group found'}")
    
    # Example 2: Metric normalization
    print("\n=== Metric Normalization ===")
    
    # Create sample MRIQC metrics
    metrics = MRIQCMetrics(
        snr=15.5,
        cnr=3.8,
        fber=1600.0,
        efc=0.48,
        fwhm_avg=2.9
    )
    
    # Normalize for different age groups
    test_ages = [8.0, 25.0, 70.0]
    
    for age in test_ages:
        print(f"\nAge {age} years:")
        normalized = normalizer.normalize_metrics(metrics, age)
        
        if normalized:
            print(f"  Age Group: {normalized.age_group.value}")
            print("  Percentiles:")
            for metric, percentile in normalized.percentiles.items():
                print(f"    {metric}: {percentile:.1f}th percentile")
            
            print("  Z-scores:")
            for metric, z_score in normalized.z_scores.items():
                print(f"    {metric}: {z_score:.2f}")
        else:
            print("  Could not normalize metrics")
    
    # Example 3: Quality assessment
    print("\n=== Quality Assessment ===")
    
    # Get age group statistics
    young_adult_stats = normalizer.get_age_group_statistics(AgeGroup.YOUNG_ADULT)
    print(f"Young Adult SNR statistics:")
    if 'snr' in young_adult_stats:
        stats = young_adult_stats['snr']
        print(f"  Mean: {stats['mean']:.1f}")
        print(f"  Std: {stats['std']:.1f}")
        print(f"  5th percentile: {stats['percentiles']['5']:.1f}")
        print(f"  95th percentile: {stats['percentiles']['95']:.1f}")
    
    # Example 4: Age coverage validation
    print("\n=== Age Coverage Validation ===")
    
    study_ages = [7.0, 12.0, 18.0, 25.0, 35.0, 50.0, 75.0, 120.0, None]
    coverage = normalizer.validate_age_coverage(study_ages)
    
    print(f"Total ages: {len(study_ages)}")
    print(f"Covered ages: {len(coverage['covered'])}")
    print(f"Uncovered ages: {len(coverage['uncovered'])}")
    print(f"Coverage rate: {coverage['coverage_rate']:.1%}")
    
    # Example 5: Recommendations
    print("\n=== Metric Recommendations ===")
    
    # Test with concerning metrics
    concerning_metrics = MRIQCMetrics(
        snr=8.0,    # Low SNR
        cnr=2.0,    # Low CNR
        efc=0.65    # High EFC (worse)
    )
    
    normalized_concerning = normalizer.normalize_metrics(concerning_metrics, 25.0)
    if normalized_concerning:
        recommendations = normalizer.get_metric_recommendations(normalized_concerning)
        print("Recommendations for concerning metrics:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")


if __name__ == "__main__":
    main()