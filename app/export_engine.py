"""
Export and reporting engine for the Age-Normed MRIQC Dashboard.

This module provides functionality for generating CSV exports and PDF reports
from processed MRIQC data with age-normalized quality assessments.
"""

import csv
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import tempfile
import statistics

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.platypus.flowables import Image
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

from .models import (
    ProcessedSubject, StudySummary, QualityStatus, AgeGroup,
    MRIQCMetrics, QualityAssessment, SubjectInfo
)

logger = logging.getLogger(__name__)


class ExportError(Exception):
    """Custom exception for export-related errors."""
    pass


class ExportEngine:
    """
    Engine for exporting MRIQC data and generating reports.
    
    Supports CSV export and PDF report generation with study-level summaries.
    """
    
    def __init__(self):
        """Initialize the export engine."""
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Set up custom styles for PDF reports."""
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=18,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        ))
        
        # Subtitle style
        self.styles.add(ParagraphStyle(
            name='CustomSubtitle',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceAfter=20,
            alignment=TA_CENTER,
            textColor=colors.darkblue
        ))
        
        # Section header style
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=12,
            spaceBefore=20,
            spaceAfter=10,
            textColor=colors.darkblue,
            borderWidth=1,
            borderColor=colors.darkblue,
            borderPadding=5
        ))
        
        # Warning style
        self.styles.add(ParagraphStyle(
            name='Warning',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.red,
            leftIndent=20
        ))
        
        # Success style
        self.styles.add(ParagraphStyle(
            name='Success',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.green,
            leftIndent=20
        ))
    
    def export_subjects_csv(
        self,
        subjects: List[ProcessedSubject],
        include_raw_metrics: bool = True,
        include_normalized_metrics: bool = True,
        include_quality_assessment: bool = True,
        custom_fields: Optional[List[str]] = None
    ) -> str:
        """
        Export subjects data to CSV format.
        
        Args:
            subjects: List of processed subjects
            include_raw_metrics: Include raw MRIQC metrics
            include_normalized_metrics: Include age-normalized metrics
            include_quality_assessment: Include quality assessment results
            custom_fields: Additional custom fields to include
            
        Returns:
            CSV content as string
            
        Raises:
            ExportError: If export fails
        """
        try:
            if not subjects:
                raise ExportError("No subjects provided for export")
            
            # Prepare CSV data
            csv_data = []
            
            for subject in subjects:
                row = {}
                
                # Basic subject information
                row.update({
                    'subject_id': subject.subject_info.subject_id,
                    'age': subject.subject_info.age,
                    'sex': subject.subject_info.sex.value if subject.subject_info.sex else None,
                    'session': subject.subject_info.session,
                    'scan_type': subject.subject_info.scan_type.value,
                    'acquisition_date': subject.subject_info.acquisition_date.isoformat() if subject.subject_info.acquisition_date else None,
                    'site': subject.subject_info.site,
                    'scanner': subject.subject_info.scanner,
                    'processing_timestamp': subject.processing_timestamp.isoformat(),
                    'processing_version': subject.processing_version
                })
                
                # Raw MRIQC metrics
                if include_raw_metrics:
                    metrics_dict = subject.raw_metrics.model_dump(exclude_none=True)
                    for metric_name, value in metrics_dict.items():
                        row[f'raw_{metric_name}'] = value
                
                # Normalized metrics
                if include_normalized_metrics and subject.normalized_metrics:
                    row['age_group'] = subject.normalized_metrics.age_group.value
                    row['normative_dataset'] = subject.normalized_metrics.normative_dataset
                    
                    # Add percentiles
                    for metric_name, percentile in subject.normalized_metrics.percentiles.items():
                        row[f'percentile_{metric_name}'] = percentile
                    
                    # Add z-scores
                    for metric_name, z_score in subject.normalized_metrics.z_scores.items():
                        row[f'zscore_{metric_name}'] = z_score
                
                # Quality assessment
                if include_quality_assessment:
                    qa = subject.quality_assessment
                    row.update({
                        'overall_quality_status': qa.overall_status.value,
                        'composite_score': qa.composite_score,
                        'confidence': qa.confidence,
                        'recommendations': '; '.join(qa.recommendations),
                        'flags': '; '.join(qa.flags)
                    })
                    
                    # Individual metric assessments
                    for metric_name, status in qa.metric_assessments.items():
                        row[f'quality_{metric_name}'] = status.value
                    
                    # Threshold violations
                    for metric_name, violation in qa.threshold_violations.items():
                        row[f'violation_{metric_name}_value'] = violation.get('value')
                        row[f'violation_{metric_name}_threshold'] = violation.get('threshold')
                        row[f'violation_{metric_name}_severity'] = violation.get('severity')
                
                # Custom fields
                if custom_fields:
                    for field in custom_fields:
                        if hasattr(subject, field):
                            row[field] = getattr(subject, field)
                
                csv_data.append(row)
            
            # Convert to CSV string
            if not csv_data:
                raise ExportError("No data to export")
            
            # Collect all possible fieldnames from all rows
            all_fieldnames = set()
            for row in csv_data:
                all_fieldnames.update(row.keys())
            
            # Sort fieldnames for consistent output
            fieldnames = sorted(all_fieldnames)
            
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(csv_data)
            
            csv_content = output.getvalue()
            output.close()
            
            logger.info(f"Exported {len(subjects)} subjects to CSV ({len(csv_content)} characters)")
            return csv_content
            
        except Exception as e:
            logger.error(f"CSV export failed: {str(e)}")
            raise ExportError(f"Failed to export CSV: {str(e)}")
    
    def generate_study_summary(self, subjects: List[ProcessedSubject], study_name: Optional[str] = None) -> StudySummary:
        """
        Generate study-level summary statistics.
        
        Args:
            subjects: List of processed subjects
            study_name: Optional study name
            
        Returns:
            StudySummary object with aggregated statistics
            
        Raises:
            ExportError: If summary generation fails
        """
        try:
            if not subjects:
                raise ExportError("No subjects provided for summary")
            
            # Quality distribution
            quality_counts = {status: 0 for status in QualityStatus}
            for subject in subjects:
                quality_counts[subject.quality_assessment.overall_status] += 1
            
            # Age group distribution
            age_group_counts = {group: 0 for group in AgeGroup}
            for subject in subjects:
                if subject.normalized_metrics:
                    age_group_counts[subject.normalized_metrics.age_group] += 1
            
            # Metric statistics
            metric_stats = {}
            
            # Get all available metrics from first subject
            first_subject = subjects[0]
            available_metrics = [
                field for field, value in first_subject.raw_metrics.model_dump().items()
                if value is not None
            ]
            
            for metric_name in available_metrics:
                values = []
                for subject in subjects:
                    value = getattr(subject.raw_metrics, metric_name, None)
                    if value is not None:
                        values.append(value)
                
                if values:
                    metric_stats[metric_name] = {
                        'count': len(values),
                        'mean': statistics.mean(values),
                        'median': statistics.median(values),
                        'std': statistics.stdev(values) if len(values) > 1 else 0.0,
                        'min': min(values),
                        'max': max(values),
                        'q25': statistics.quantiles(values, n=4)[0] if len(values) >= 4 else values[0],
                        'q75': statistics.quantiles(values, n=4)[2] if len(values) >= 4 else values[-1]
                    }
            
            # Calculate exclusion rate
            failed_count = quality_counts[QualityStatus.FAIL]
            exclusion_rate = failed_count / len(subjects) if subjects else 0.0
            
            return StudySummary(
                total_subjects=len(subjects),
                quality_distribution=quality_counts,
                age_group_distribution=age_group_counts,
                metric_statistics=metric_stats,
                exclusion_rate=exclusion_rate,
                processing_date=datetime.now(),
                study_name=study_name
            )
            
        except Exception as e:
            logger.error(f"Study summary generation failed: {str(e)}")
            raise ExportError(f"Failed to generate study summary: {str(e)}")
    
    def generate_pdf_report(
        self,
        subjects: List[ProcessedSubject],
        study_name: Optional[str] = None,
        include_individual_subjects: bool = True,
        include_summary_charts: bool = True,
        max_subjects_detailed: int = 50
    ) -> bytes:
        """
        Generate comprehensive PDF report.
        
        Args:
            subjects: List of processed subjects
            study_name: Optional study name
            include_individual_subjects: Include individual subject details
            include_summary_charts: Include summary charts
            max_subjects_detailed: Maximum subjects to include detailed info
            
        Returns:
            PDF content as bytes
            
        Raises:
            ExportError: If PDF generation fails
        """
        try:
            if not subjects:
                raise ExportError("No subjects provided for PDF report")
            
            # Create temporary file for PDF
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                pdf_path = tmp_file.name
            
            # Create PDF document
            doc = SimpleDocTemplate(
                pdf_path,
                pagesize=A4,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )
            
            # Build story (content)
            story = []
            
            # Title page
            story.extend(self._create_title_page(subjects, study_name))
            
            # Executive summary
            story.extend(self._create_executive_summary(subjects))
            
            # Study summary
            study_summary = self.generate_study_summary(subjects, study_name)
            story.extend(self._create_study_summary_section(study_summary))
            
            # Quality distribution charts
            if include_summary_charts:
                story.extend(self._create_charts_section(study_summary))
            
            # Individual subjects (limited)
            if include_individual_subjects:
                story.extend(self._create_individual_subjects_section(
                    subjects[:max_subjects_detailed]
                ))
            
            # Appendices
            story.extend(self._create_appendices(subjects, study_summary))
            
            # Build PDF
            doc.build(story)
            
            # Read PDF content
            with open(pdf_path, 'rb') as f:
                pdf_content = f.read()
            
            # Clean up temporary file
            Path(pdf_path).unlink(missing_ok=True)
            
            logger.info(f"Generated PDF report for {len(subjects)} subjects ({len(pdf_content)} bytes)")
            return pdf_content
            
        except Exception as e:
            logger.error(f"PDF report generation failed: {str(e)}")
            raise ExportError(f"Failed to generate PDF report: {str(e)}")
    
    def _create_title_page(self, subjects: List[ProcessedSubject], study_name: Optional[str]) -> List:
        """Create title page content."""
        story = []
        
        # Title
        title = study_name or "MRIQC Quality Control Report"
        story.append(Paragraph(title, self.styles['CustomTitle']))
        story.append(Spacer(1, 20))
        
        # Subtitle
        subtitle = f"Age-Normed Quality Assessment Report"
        story.append(Paragraph(subtitle, self.styles['CustomSubtitle']))
        story.append(Spacer(1, 30))
        
        # Report info
        report_info = [
            f"<b>Total Subjects:</b> {len(subjects)}",
            f"<b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"<b>Processing Version:</b> {subjects[0].processing_version if subjects else 'Unknown'}",
        ]
        
        for info in report_info:
            story.append(Paragraph(info, self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        story.append(PageBreak())
        return story
    
    def _create_executive_summary(self, subjects: List[ProcessedSubject]) -> List:
        """Create executive summary section."""
        story = []
        
        story.append(Paragraph("Executive Summary", self.styles['SectionHeader']))
        story.append(Spacer(1, 12))
        
        # Calculate key statistics
        total_subjects = len(subjects)
        passed = sum(1 for s in subjects if s.quality_assessment.overall_status == QualityStatus.PASS)
        failed = sum(1 for s in subjects if s.quality_assessment.overall_status == QualityStatus.FAIL)
        warning = sum(1 for s in subjects if s.quality_assessment.overall_status == QualityStatus.WARNING)
        
        pass_rate = (passed / total_subjects * 100) if total_subjects > 0 else 0
        fail_rate = (failed / total_subjects * 100) if total_subjects > 0 else 0
        
        # Summary text
        summary_text = f"""
        This report presents the quality control assessment results for {total_subjects} subjects 
        using age-normalized MRIQC metrics. The assessment applies age-appropriate thresholds to 
        reduce false rejections in pediatric and aging populations.
        
        <b>Key Findings:</b>
        • {passed} subjects ({pass_rate:.1f}%) passed quality control
        • {failed} subjects ({fail_rate:.1f}%) failed quality control
        • {warning} subjects require manual review
        
        The age-normalization approach helps ensure that quality assessments are appropriate 
        for the subject's age group, reducing bias against younger and older participants.
        """
        
        story.append(Paragraph(summary_text, self.styles['Normal']))
        story.append(Spacer(1, 20))
        
        return story
    
    def _create_study_summary_section(self, summary: StudySummary) -> List:
        """Create study summary section."""
        story = []
        
        story.append(Paragraph("Study Summary", self.styles['SectionHeader']))
        story.append(Spacer(1, 12))
        
        # Quality distribution table
        quality_data = [['Quality Status', 'Count', 'Percentage']]
        for status, count in summary.quality_distribution.items():
            percentage = (count / summary.total_subjects * 100) if summary.total_subjects > 0 else 0
            quality_data.append([status.value.title(), str(count), f"{percentage:.1f}%"])
        
        quality_table = Table(quality_data, colWidths=[2*inch, 1*inch, 1*inch])
        quality_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(Paragraph("Quality Distribution", self.styles['Heading3']))
        story.append(quality_table)
        story.append(Spacer(1, 20))
        
        # Age group distribution
        if any(count > 0 for count in summary.age_group_distribution.values()):
            age_data = [['Age Group', 'Count', 'Percentage']]
            for group, count in summary.age_group_distribution.items():
                if count > 0:
                    percentage = (count / summary.total_subjects * 100) if summary.total_subjects > 0 else 0
                    age_data.append([group.value.replace('_', ' ').title(), str(count), f"{percentage:.1f}%"])
            
            age_table = Table(age_data, colWidths=[2*inch, 1*inch, 1*inch])
            age_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(Paragraph("Age Group Distribution", self.styles['Heading3']))
            story.append(age_table)
            story.append(Spacer(1, 20))
        
        return story
    
    def _create_charts_section(self, summary: StudySummary) -> List:
        """Create charts section (placeholder for now)."""
        story = []
        
        story.append(Paragraph("Quality Metrics Overview", self.styles['SectionHeader']))
        story.append(Spacer(1, 12))
        
        # Metric statistics table
        if summary.metric_statistics:
            metrics_data = [['Metric', 'Mean', 'Std Dev', 'Min', 'Max', 'Count']]
            
            for metric_name, stats in summary.metric_statistics.items():
                metrics_data.append([
                    metric_name.upper(),
                    f"{stats['mean']:.2f}",
                    f"{stats['std']:.2f}",
                    f"{stats['min']:.2f}",
                    f"{stats['max']:.2f}",
                    str(stats['count'])
                ])
            
            metrics_table = Table(metrics_data, colWidths=[1.2*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.8*inch, 0.6*inch])
            metrics_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(metrics_table)
            story.append(Spacer(1, 20))
        
        return story
    
    def _create_individual_subjects_section(self, subjects: List[ProcessedSubject]) -> List:
        """Create individual subjects section."""
        story = []
        
        story.append(PageBreak())
        story.append(Paragraph("Individual Subject Details", self.styles['SectionHeader']))
        story.append(Spacer(1, 12))
        
        if len(subjects) > 50:
            story.append(Paragraph(
                f"<i>Note: Showing first 50 subjects out of {len(subjects)} total subjects.</i>",
                self.styles['Normal']
            ))
            story.append(Spacer(1, 12))
        
        for i, subject in enumerate(subjects[:50]):  # Limit to first 50
            story.extend(self._create_subject_detail(subject, i + 1))
            if i < len(subjects) - 1:  # Don't add spacer after last subject
                story.append(Spacer(1, 15))
        
        return story
    
    def _create_subject_detail(self, subject: ProcessedSubject, subject_num: int) -> List:
        """Create individual subject detail section."""
        story = []
        
        # Subject header
        header_text = f"Subject {subject_num}: {subject.subject_info.subject_id}"
        story.append(Paragraph(header_text, self.styles['Heading3']))
        
        # Basic info
        info_data = [
            ['Age', str(subject.subject_info.age) if subject.subject_info.age else 'Unknown'],
            ['Sex', subject.subject_info.sex.value if subject.subject_info.sex else 'Unknown'],
            ['Scan Type', subject.subject_info.scan_type.value],
            ['Quality Status', subject.quality_assessment.overall_status.value.title()],
            ['Composite Score', f"{subject.quality_assessment.composite_score:.1f}"],
            ['Confidence', f"{subject.quality_assessment.confidence:.2f}"]
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 2*inch])
        info_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)
        ]))
        
        story.append(info_table)
        
        # Recommendations and flags
        if subject.quality_assessment.recommendations:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Recommendations:</b>", self.styles['Normal']))
            for rec in subject.quality_assessment.recommendations:
                story.append(Paragraph(f"• {rec}", self.styles['Normal']))
        
        if subject.quality_assessment.flags:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Quality Flags:</b>", self.styles['Warning']))
            for flag in subject.quality_assessment.flags:
                story.append(Paragraph(f"• {flag}", self.styles['Warning']))
        
        return story
    
    def _create_appendices(self, subjects: List[ProcessedSubject], summary: StudySummary) -> List:
        """Create appendices section."""
        story = []
        
        story.append(PageBreak())
        story.append(Paragraph("Appendices", self.styles['SectionHeader']))
        story.append(Spacer(1, 12))
        
        # Appendix A: Methodology
        story.append(Paragraph("Appendix A: Methodology", self.styles['Heading3']))
        methodology_text = """
        This report uses age-normalized MRIQC quality metrics to assess MRI data quality. 
        The normalization process compares each subject's metrics against age-appropriate 
        reference populations to reduce false rejections in pediatric and aging cohorts.
        
        Quality thresholds are applied based on percentile rankings within age groups:
        • Pass: Metrics within acceptable ranges for age group
        • Warning: Some metrics borderline, manual review recommended  
        • Fail: Critical metrics exceed age-appropriate thresholds
        • Uncertain: Insufficient data or conflicting indicators
        """
        story.append(Paragraph(methodology_text, self.styles['Normal']))
        story.append(Spacer(1, 15))
        
        # Appendix B: Metric Definitions
        story.append(Paragraph("Appendix B: MRIQC Metric Definitions", self.styles['Heading3']))
        
        metric_definitions = [
            ('SNR', 'Signal-to-Noise Ratio - measure of signal strength relative to noise'),
            ('CNR', 'Contrast-to-Noise Ratio - measure of tissue contrast relative to noise'),
            ('FBER', 'Foreground-Background Energy Ratio - measure of ghosting artifacts'),
            ('EFC', 'Entropy Focus Criterion - measure of ghosting and blurring'),
            ('FWHM', 'Full-Width Half-Maximum - measure of image smoothness'),
            ('QI1', 'Quality Index 1 - composite anatomical quality measure'),
            ('CJV', 'Coefficient of Joint Variation - measure of noise in tissue contrast')
        ]
        
        for metric, definition in metric_definitions:
            story.append(Paragraph(f"<b>{metric}:</b> {definition}", self.styles['Normal']))
            story.append(Spacer(1, 5))
        
        return story
    
    def export_study_summary_csv(self, summary: StudySummary) -> str:
        """
        Export study summary to CSV format.
        
        Args:
            summary: StudySummary object
            
        Returns:
            CSV content as string
        """
        try:
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header information
            writer.writerow(['Study Summary Report'])
            writer.writerow(['Generated:', summary.processing_date.isoformat()])
            writer.writerow(['Study Name:', summary.study_name or 'Unnamed Study'])
            writer.writerow(['Total Subjects:', summary.total_subjects])
            writer.writerow(['Exclusion Rate:', f"{summary.exclusion_rate:.3f}"])
            writer.writerow([])  # Empty row
            
            # Quality distribution
            writer.writerow(['Quality Distribution'])
            writer.writerow(['Status', 'Count', 'Percentage'])
            for status, count in summary.quality_distribution.items():
                percentage = (count / summary.total_subjects * 100) if summary.total_subjects > 0 else 0
                writer.writerow([status.value, count, f"{percentage:.1f}%"])
            writer.writerow([])  # Empty row
            
            # Age group distribution
            if any(count > 0 for count in summary.age_group_distribution.values()):
                writer.writerow(['Age Group Distribution'])
                writer.writerow(['Age Group', 'Count', 'Percentage'])
                for group, count in summary.age_group_distribution.items():
                    if count > 0:
                        percentage = (count / summary.total_subjects * 100) if summary.total_subjects > 0 else 0
                        writer.writerow([group.value, count, f"{percentage:.1f}%"])
                writer.writerow([])  # Empty row
            
            # Metric statistics
            if summary.metric_statistics:
                writer.writerow(['Metric Statistics'])
                writer.writerow(['Metric', 'Count', 'Mean', 'Std Dev', 'Min', 'Max', 'Q25', 'Median', 'Q75'])
                for metric_name, stats in summary.metric_statistics.items():
                    writer.writerow([
                        metric_name,
                        stats['count'],
                        f"{stats['mean']:.3f}",
                        f"{stats['std']:.3f}",
                        f"{stats['min']:.3f}",
                        f"{stats['max']:.3f}",
                        f"{stats['q25']:.3f}",
                        f"{stats['median']:.3f}",
                        f"{stats['q75']:.3f}"
                    ])
            
            csv_content = output.getvalue()
            output.close()
            
            return csv_content
            
        except Exception as e:
            logger.error(f"Study summary CSV export failed: {str(e)}")
            raise ExportError(f"Failed to export study summary CSV: {str(e)}")