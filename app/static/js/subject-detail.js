/**
 * Subject detail page functionality for Age-Normed MRIQC Dashboard
 */

// Subject detail state
let currentSubject = null;
let subjectCharts = {};

/**
 * Initialize subject detail page
 */
document.addEventListener('DOMContentLoaded', function() {
    initializeSubjectDetail();
    setupEventListeners();
});

/**
 * Initialize subject detail page
 */
async function initializeSubjectDetail() {
    try {
        showLoading(true, 'Loading subject details...');
        
        // Get subject ID from URL
        const subjectId = getSubjectIdFromURL();
        if (!subjectId) {
            utils.showAlert('Subject ID not found in URL', 'danger');
            return;
        }
        
        // Load subject data
        await loadSubjectData(subjectId);
        
        // Render subject details
        renderSubjectDetails();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to load subject details');
    } finally {
        showLoading(false);
    }
}

/**
 * Get subject ID from URL
 */
function getSubjectIdFromURL() {
    const pathParts = window.location.pathname.split('/');
    return pathParts[pathParts.length - 1];
}

/**
 * Load subject data from API
 */
async function loadSubjectData(subjectId) {
    try {
        currentSubject = await api.getSubjectDetails(subjectId);
        console.log('Subject data loaded:', currentSubject);
    } catch (error) {
        console.error('Error loading subject data:', error);
        throw error;
    }
}

/**
 * Render subject details
 */
function renderSubjectDetails() {
    if (!currentSubject) return;
    
    renderSubjectInfo();
    renderQualityAssessment();
    renderRecommendations();
    renderMetrics();
    renderCharts();
    renderQualityFlags();
}

/**
 * Render subject information
 */
function renderSubjectInfo() {
    const subjectInfoDiv = document.getElementById('subject-info');
    if (!subjectInfoDiv) return;
    
    const subject = currentSubject.subject;
    const info = subject.subject_info;
    
    const infoHTML = `
        <div class="row">
            <div class="col-sm-6">
                <p class="mb-2">
                    <strong>Subject ID:</strong><br>
                    <span class="text-primary fw-medium">${info.subject_id}</span>
                </p>
                <p class="mb-2">
                    <strong>Age:</strong><br>
                    ${info.age ? `${info.age} years` : 'Not specified'}
                </p>
                <p class="mb-2">
                    <strong>Sex:</strong><br>
                    ${info.sex || 'Not specified'}
                </p>
            </div>
            <div class="col-sm-6">
                <p class="mb-2">
                    <strong>Session:</strong><br>
                    ${info.session || 'Not specified'}
                </p>
                <p class="mb-2">
                    <strong>Scan Type:</strong><br>
                    <span class="badge bg-info">${info.scan_type}</span>
                </p>
                <p class="mb-2">
                    <strong>Age Group:</strong><br>
                    ${subject.normalized_metrics ? 
                        utils.getAgeGroupBadge(subject.normalized_metrics.age_group) : 
                        'Not determined'
                    }
                </p>
            </div>
        </div>
        
        ${info.acquisition_date ? `
            <hr>
            <p class="mb-0">
                <strong>Acquisition Date:</strong><br>
                <small class="text-muted">${utils.formatDateTime(info.acquisition_date)}</small>
            </p>
        ` : ''}
        
        <hr>
        <p class="mb-0">
            <strong>Processed:</strong><br>
            <small class="text-muted">${utils.formatDateTime(subject.processing_timestamp)}</small>
        </p>
    `;
    
    subjectInfoDiv.innerHTML = infoHTML;
}

/**
 * Render quality assessment
 */
function renderQualityAssessment() {
    const qualityDiv = document.getElementById('quality-assessment');
    if (!qualityDiv) return;
    
    const assessment = currentSubject.subject.quality_assessment;
    
    const assessmentHTML = `
        <div class="text-center mb-3">
            <div class="mb-2">
                ${utils.getQualityStatusBadge(assessment.overall_status)}
            </div>
            <h4 class="mb-1">Composite Score</h4>
            <h2 class="text-primary mb-0">${utils.formatNumber(assessment.composite_score)}</h2>
            <small class="text-muted">Confidence: ${utils.formatPercentage(assessment.confidence)}</small>
        </div>
        
        <hr>
        
        <h6 class="mb-3">Metric Assessments</h6>
        <div class="row">
            ${Object.entries(assessment.metric_assessments).map(([metric, status]) => `
                <div class="col-6 mb-2">
                    <div class="d-flex justify-content-between align-items-center">
                        <small class="text-muted">${metric.toUpperCase()}</small>
                        ${utils.getQualityStatusBadge(status)}
                    </div>
                </div>
            `).join('')}
        </div>
        
        ${assessment.flags && assessment.flags.length > 0 ? `
            <hr>
            <h6 class="mb-2">Quality Flags</h6>
            <div class="d-flex flex-wrap gap-1">
                ${assessment.flags.map(flag => `
                    <span class="badge bg-warning text-dark">${flag}</span>
                `).join('')}
            </div>
        ` : ''}
    `;
    
    qualityDiv.innerHTML = assessmentHTML;
}

/**
 * Render recommendations
 */
function renderRecommendations() {
    const recommendationsDiv = document.getElementById('recommendations');
    if (!recommendationsDiv) return;
    
    const recommendations = currentSubject.recommendations || [];
    
    if (recommendations.length === 0) {
        recommendationsDiv.innerHTML = `
            <div class="text-center text-muted py-3">
                <i class="bi bi-check-circle"></i>
                <p class="mb-0">No specific recommendations</p>
            </div>
        `;
        return;
    }
    
    const recommendationsHTML = `
        <div class="list-group list-group-flush">
            ${recommendations.map(rec => `
                <div class="list-group-item border-0 px-0">
                    <i class="bi bi-lightbulb text-warning me-2"></i>
                    ${rec}
                </div>
            `).join('')}
        </div>
    `;
    
    recommendationsDiv.innerHTML = recommendationsHTML;
}

/**
 * Render metrics
 */
function renderMetrics() {
    const metricsDiv = document.getElementById('metrics-content');
    if (!metricsDiv) return;
    
    const subject = currentSubject.subject;
    const rawMetrics = subject.raw_metrics;
    const normalizedMetrics = subject.normalized_metrics;
    
    // Default to raw view
    renderRawMetrics(rawMetrics, normalizedMetrics);
}

/**
 * Render raw metrics
 */
function renderRawMetrics(rawMetrics, normalizedMetrics) {
    const metricsDiv = document.getElementById('metrics-content');
    if (!metricsDiv) return;
    
    const metricGroups = {
        'Anatomical Metrics': ['snr', 'cnr', 'fber', 'efc', 'fwhm_avg', 'qi1', 'cjv'],
        'Functional Metrics': ['dvars', 'fd_mean', 'gcor']
    };
    
    let metricsHTML = '';
    
    Object.entries(metricGroups).forEach(([groupName, metrics]) => {
        const groupMetrics = metrics.filter(metric => 
            rawMetrics[metric] !== null && rawMetrics[metric] !== undefined
        );
        
        if (groupMetrics.length === 0) return;
        
        metricsHTML += `
            <h6 class="mb-3">${groupName}</h6>
            <div class="row mb-4">
                ${groupMetrics.map(metric => {
                    const value = rawMetrics[metric];
                    const percentile = normalizedMetrics?.percentiles?.[metric];
                    const zScore = normalizedMetrics?.z_scores?.[metric];
                    
                    return `
                        <div class="col-md-4 mb-3">
                            <div class="card h-100">
                                <div class="card-body text-center">
                                    <h6 class="card-title text-muted">${metric.toUpperCase()}</h6>
                                    <h4 class="text-primary mb-1">${utils.formatNumber(value, 3)}</h4>
                                    ${percentile ? `
                                        <small class="text-muted d-block">
                                            ${utils.formatNumber(percentile, 1)}th percentile
                                        </small>
                                    ` : ''}
                                    ${zScore ? `
                                        <small class="text-muted d-block">
                                            Z-score: ${utils.formatNumber(zScore, 2)}
                                        </small>
                                    ` : ''}
                                </div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    });
    
    if (metricsHTML === '') {
        metricsHTML = `
            <div class="text-center text-muted py-4">
                <i class="bi bi-info-circle"></i>
                <p class="mb-0">No metrics available</p>
            </div>
        `;
    }
    
    metricsDiv.innerHTML = metricsHTML;
}

/**
 * Render normalized metrics
 */
function renderNormalizedMetrics() {
    const metricsDiv = document.getElementById('metrics-content');
    if (!metricsDiv || !currentSubject.subject.normalized_metrics) return;
    
    const normalizedMetrics = currentSubject.subject.normalized_metrics;
    const zScores = normalizedMetrics.z_scores;
    
    let metricsHTML = `
        <h6 class="mb-3">Z-Scores (Age-Normalized)</h6>
        <div class="row">
            ${Object.entries(zScores).map(([metric, zScore]) => `
                <div class="col-md-4 mb-3">
                    <div class="card h-100">
                        <div class="card-body text-center">
                            <h6 class="card-title text-muted">${metric.toUpperCase()}</h6>
                            <h4 class="mb-1 ${getZScoreColor(zScore)}">${utils.formatNumber(zScore, 2)}</h4>
                            <small class="text-muted">
                                ${getZScoreInterpretation(zScore)}
                            </small>
                        </div>
                    </div>
                </div>
            `).join('')}
        </div>
        
        <div class="alert alert-info mt-3">
            <small>
                <i class="bi bi-info-circle"></i>
                Z-scores show how many standard deviations the subject's metrics are from the age group mean.
                Values between -2 and +2 are typically considered normal.
            </small>
        </div>
    `;
    
    metricsDiv.innerHTML = metricsHTML;
}

/**
 * Render percentile metrics
 */
function renderPercentileMetrics() {
    const metricsDiv = document.getElementById('metrics-content');
    if (!metricsDiv || !currentSubject.subject.normalized_metrics) return;
    
    const normalizedMetrics = currentSubject.subject.normalized_metrics;
    const percentiles = normalizedMetrics.percentiles;
    
    let metricsHTML = `
        <h6 class="mb-3">Percentile Rankings (Age-Normalized)</h6>
        <div class="row">
            ${Object.entries(percentiles).map(([metric, percentile]) => `
                <div class="col-md-4 mb-3">
                    <div class="card h-100">
                        <div class="card-body text-center">
                            <h6 class="card-title text-muted">${metric.toUpperCase()}</h6>
                            <h4 class="mb-1 ${getPercentileColor(percentile)}">${utils.formatNumber(percentile, 1)}th</h4>
                            <div class="progress mt-2" style="height: 8px;">
                                <div class="progress-bar ${getPercentileProgressColor(percentile)}" 
                                     style="width: ${percentile}%"></div>
                            </div>
                            <small class="text-muted mt-1 d-block">
                                ${getPercentileInterpretation(percentile)}
                            </small>
                        </div>
                    </div>
                </div>
            `).join('')}
        </div>
        
        <div class="alert alert-info mt-3">
            <small>
                <i class="bi bi-info-circle"></i>
                Percentile rankings show where the subject falls compared to others in the same age group.
                50th percentile is average for the age group.
            </small>
        </div>
    `;
    
    metricsDiv.innerHTML = metricsHTML;
}

/**
 * Render charts
 */
function renderCharts() {
    // Destroy existing charts
    Object.values(subjectCharts).forEach(chart => {
        if (chart) charts.destroyChart(chart);
    });
    subjectCharts = {};
    
    // Create metric comparison chart
    createMetricComparisonChart();
    
    // Create percentile radar chart
    createPercentileRadarChart();
}

/**
 * Create metric comparison chart
 */
function createMetricComparisonChart() {
    const subject = currentSubject.subject;
    const comparisonData = currentSubject.age_group_statistics || {};
    
    // For now, create a simple comparison chart
    // In a real implementation, this would use actual comparison data
    const mockComparisonData = {
        ageGroupMean: subject.raw_metrics.snr * 0.9,
        overallMean: subject.raw_metrics.snr * 0.8
    };
    
    subjectCharts.comparison = charts.createMetricComparisonChart(
        'metric-comparison-chart',
        subject.raw_metrics,
        mockComparisonData,
        'SNR'
    );
}

/**
 * Create percentile radar chart
 */
function createPercentileRadarChart() {
    const subject = currentSubject.subject;
    
    if (subject.normalized_metrics && subject.normalized_metrics.percentiles) {
        subjectCharts.percentiles = charts.createPercentileRadarChart(
            'percentile-chart',
            subject.normalized_metrics.percentiles,
            subject
        );
    }
}

/**
 * Render quality flags
 */
function renderQualityFlags() {
    const flagsDiv = document.getElementById('quality-flags');
    if (!flagsDiv) return;
    
    const assessment = currentSubject.subject.quality_assessment;
    const flags = assessment.flags || [];
    
    if (flags.length === 0) {
        flagsDiv.innerHTML = `
            <div class="text-center text-success py-4">
                <i class="bi bi-check-circle display-4"></i>
                <h5 class="mt-2">No Quality Issues Detected</h5>
                <p class="text-muted mb-0">All quality metrics are within acceptable ranges.</p>
            </div>
        `;
        return;
    }
    
    const flagsHTML = `
        <div class="alert alert-warning">
            <h6><i class="bi bi-flag"></i> Quality Flags Detected</h6>
            <ul class="mb-0">
                ${flags.map(flag => `<li>${flag}</li>`).join('')}
            </ul>
        </div>
        
        <div class="mt-3">
            <h6>Recommended Actions:</h6>
            <div class="list-group list-group-flush">
                ${getRecommendedActions(flags).map(action => `
                    <div class="list-group-item border-0 px-0">
                        <i class="bi bi-arrow-right text-primary me-2"></i>
                        ${action}
                    </div>
                `).join('')}
            </div>
        </div>
    `;
    
    flagsDiv.innerHTML = flagsHTML;
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Metric view radio buttons
    const metricViewRadios = document.querySelectorAll('input[name="metric-view"]');
    metricViewRadios.forEach(radio => {
        radio.addEventListener('change', handleMetricViewChange);
    });
    
    // Comparison type selector
    const comparisonSelect = document.getElementById('comparison-type');
    if (comparisonSelect) {
        comparisonSelect.addEventListener('change', handleComparisonTypeChange);
    }
}

/**
 * Handle metric view change
 */
function handleMetricViewChange(event) {
    const viewType = event.target.id;
    
    switch (viewType) {
        case 'raw-view':
            renderRawMetrics(
                currentSubject.subject.raw_metrics,
                currentSubject.subject.normalized_metrics
            );
            break;
        case 'normalized-view':
            renderNormalizedMetrics();
            break;
        case 'percentile-view':
            renderPercentileMetrics();
            break;
    }
}

/**
 * Handle comparison type change
 */
function handleComparisonTypeChange(event) {
    const comparisonType = event.target.value;
    // In a real implementation, this would update the comparison chart
    console.log('Comparison type changed to:', comparisonType);
}

/**
 * Get Z-score color class
 */
function getZScoreColor(zScore) {
    const absZ = Math.abs(zScore);
    if (absZ <= 1) return 'text-success';
    if (absZ <= 2) return 'text-warning';
    return 'text-danger';
}

/**
 * Get Z-score interpretation
 */
function getZScoreInterpretation(zScore) {
    const absZ = Math.abs(zScore);
    if (absZ <= 1) return 'Normal range';
    if (absZ <= 2) return 'Borderline';
    return 'Outlier';
}

/**
 * Get percentile color class
 */
function getPercentileColor(percentile) {
    if (percentile >= 25 && percentile <= 75) return 'text-success';
    if (percentile >= 10 && percentile <= 90) return 'text-warning';
    return 'text-danger';
}

/**
 * Get percentile progress bar color
 */
function getPercentileProgressColor(percentile) {
    if (percentile >= 25 && percentile <= 75) return 'bg-success';
    if (percentile >= 10 && percentile <= 90) return 'bg-warning';
    return 'bg-danger';
}

/**
 * Get percentile interpretation
 */
function getPercentileInterpretation(percentile) {
    if (percentile >= 75) return 'Above average';
    if (percentile >= 25) return 'Average range';
    if (percentile >= 10) return 'Below average';
    return 'Well below average';
}

/**
 * Get recommended actions for quality flags
 */
function getRecommendedActions(flags) {
    const actions = [];
    
    if (flags.some(f => f.includes('motion'))) {
        actions.push('Review motion parameters and consider motion correction');
    }
    
    if (flags.some(f => f.includes('artifact'))) {
        actions.push('Inspect images for artifacts and consider exclusion');
    }
    
    if (flags.some(f => f.includes('SNR'))) {
        actions.push('Check acquisition parameters and scanner performance');
    }
    
    if (actions.length === 0) {
        actions.push('Manual review recommended');
    }
    
    return actions;
}

/**
 * Export subject report
 */
async function exportSubjectReport() {
    if (!currentSubject) return;
    
    try {
        showLoading(true, 'Generating report...');
        
        const subjectId = currentSubject.subject.subject_info.subject_id;
        const blob = await api.exportPDF([subjectId], 'individual');
        
        utils.downloadBlob(blob, `subject_${subjectId}_report.pdf`);
        utils.showAlert('Report exported successfully', 'success');
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export report');
    } finally {
        showLoading(false);
    }
}

/**
 * Export subject data
 */
async function exportSubjectData() {
    if (!currentSubject) return;
    
    try {
        showLoading(true, 'Exporting data...');
        
        const subjectId = currentSubject.subject.subject_info.subject_id;
        const blob = await api.exportCSV([subjectId]);
        
        utils.downloadBlob(blob, `subject_${subjectId}_data.csv`);
        utils.showAlert('Data exported successfully', 'success');
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export data');
    } finally {
        showLoading(false);
    }
}

/**
 * Update quality status
 */
function updateQualityStatus(newStatus) {
    const modal = new bootstrap.Modal(document.getElementById('quality-update-modal'));
    const subjectIdSpan = document.getElementById('update-subject-id');
    const statusSelect = document.getElementById('new-quality-status');
    
    if (subjectIdSpan) {
        subjectIdSpan.textContent = currentSubject.subject.subject_info.subject_id;
    }
    
    if (statusSelect) {
        statusSelect.value = newStatus;
    }
    
    modal.show();
}

/**
 * Confirm quality update
 */
async function confirmQualityUpdate() {
    const newStatus = document.getElementById('new-quality-status').value;
    const reason = document.getElementById('update-reason').value;
    
    if (!newStatus) {
        utils.showAlert('Please select a quality status', 'warning');
        return;
    }
    
    try {
        showLoading(true, 'Updating quality status...');
        
        const subjectId = currentSubject.subject.subject_info.subject_id;
        await api.updateSubjectQuality(subjectId, newStatus, reason);
        
        utils.showAlert('Quality status updated successfully', 'success');
        
        // Reload subject data
        await loadSubjectData(subjectId);
        renderSubjectDetails();
        
        // Hide modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('quality-update-modal'));
        modal.hide();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to update quality status');
    } finally {
        showLoading(false);
    }
}

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    // Destroy charts
    Object.values(subjectCharts).forEach(chart => {
        if (chart) charts.destroyChart(chart);
    });
});

// Export functions for global use
window.exportSubjectReport = exportSubjectReport;
window.exportSubjectData = exportSubjectData;
window.updateQualityStatus = updateQualityStatus;
window.confirmQualityUpdate = confirmQualityUpdate;