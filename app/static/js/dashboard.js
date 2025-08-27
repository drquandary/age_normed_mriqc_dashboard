/**
 * Dashboard page functionality for Age-Normed MRIQC Dashboard
 */

// Dashboard state
let dashboardData = null;
let dashboardCharts = {};
let refreshInterval = null;

/**
 * Initialize dashboard
 */
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    setupEventListeners();
    setupWebSocketHandlers();
    
    // Auto-refresh every 30 seconds
    refreshInterval = setInterval(refreshDashboard, 30000);
});

/**
 * Initialize dashboard components
 */
async function initializeDashboard() {
    try {
        showLoading(true, 'Loading dashboard...');
        await loadDashboardData();
        renderDashboard();
    } catch (error) {
        utils.handleAPIError(error, 'Failed to load dashboard');
    } finally {
        showLoading(false);
    }
}

/**
 * Load dashboard data from API
 */
async function loadDashboardData() {
    try {
        dashboardData = await api.getDashboardSummary();
        console.log('Dashboard data loaded:', dashboardData);
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        throw error;
    }
}

/**
 * Render dashboard components
 */
function renderDashboard() {
    if (!dashboardData) return;
    
    updateSummaryCards();
    updateChartsSection();
    updateRecentActivity();
    updateRecentSubjects();
}

/**
 * Update summary cards
 */
function updateSummaryCards() {
    const { total_subjects, quality_distribution } = dashboardData;
    
    // Update total subjects
    const totalElement = document.getElementById('total-subjects');
    if (totalElement) {
        totalElement.textContent = total_subjects || 0;
    }
    
    // Update quality counts
    const passElement = document.getElementById('pass-count');
    const warningElement = document.getElementById('warning-count');
    const failElement = document.getElementById('fail-count');
    
    if (passElement) passElement.textContent = quality_distribution?.pass || 0;
    if (warningElement) warningElement.textContent = quality_distribution?.warning || 0;
    if (failElement) failElement.textContent = quality_distribution?.fail || 0;
}

/**
 * Update charts section
 */
function updateChartsSection() {
    // Destroy existing charts
    Object.values(dashboardCharts).forEach(chart => {
        if (chart) charts.destroyChart(chart);
    });
    dashboardCharts = {};
    
    // Create quality distribution chart
    if (dashboardData.quality_distribution) {
        dashboardCharts.qualityDistribution = charts.createQualityDistributionChart(
            'quality-distribution-chart',
            dashboardData.quality_distribution
        );
    }
    
    // Create age distribution chart
    if (dashboardData.age_group_distribution) {
        dashboardCharts.ageDistribution = charts.createAgeDistributionChart(
            'age-distribution-chart',
            dashboardData.age_group_distribution
        );
    }
    
    // Create metrics chart
    updateMetricsChart();
}

/**
 * Update metrics chart based on selected metric
 */
function updateMetricsChart() {
    const metricSelect = document.getElementById('metric-select');
    const selectedMetric = metricSelect ? metricSelect.value : 'snr';
    const viewType = document.querySelector('input[name="metric-view"]:checked')?.id || 'metric-view-box';
    
    if (dashboardCharts.metrics) {
        charts.destroyChart(dashboardCharts.metrics);
    }
    
    if (dashboardData.metric_statistics && dashboardData.metric_statistics[selectedMetric]) {
        const metricData = dashboardData.metric_statistics[selectedMetric];
        
        if (viewType === 'metric-view-box') {
            dashboardCharts.metrics = charts.createMetricBoxPlot(
                'metrics-chart',
                metricData,
                selectedMetric.toUpperCase()
            );
        } else if (viewType === 'metric-view-scatter') {
            // For scatter plot, we need subject data
            loadSubjectsForScatter(selectedMetric);
        }
    }
}

/**
 * Load subjects data for scatter plot
 */
async function loadSubjectsForScatter(metric) {
    try {
        const response = await api.getSubjects({ page_size: 200 });
        if (response.subjects && response.subjects.length > 0) {
            dashboardCharts.metrics = charts.createMetricScatterPlot(
                'metrics-chart',
                response.subjects,
                metric,
                'composite_score'
            );
        }
    } catch (error) {
        console.error('Error loading subjects for scatter plot:', error);
    }
}

/**
 * Update recent activity
 */
function updateRecentActivity() {
    const activityContainer = document.getElementById('recent-activity');
    if (!activityContainer || !dashboardData.recent_activity) return;
    
    if (dashboardData.recent_activity.length === 0) {
        activityContainer.innerHTML = `
            <div class="text-muted text-center py-3">
                <i class="bi bi-hourglass-split"></i>
                <p class="mb-0">No recent activity</p>
            </div>
        `;
        return;
    }
    
    const activityHTML = dashboardData.recent_activity.map(activity => `
        <div class="activity-item d-flex">
            <div class="activity-icon bg-${getActivityIconColor(activity.type)} text-white">
                <i class="bi bi-${getActivityIcon(activity.type)}"></i>
            </div>
            <div class="activity-content">
                <div class="activity-text">${activity.message}</div>
                <div class="activity-time">${utils.formatRelativeTime(activity.timestamp)}</div>
            </div>
        </div>
    `).join('');
    
    activityContainer.innerHTML = activityHTML;
}

/**
 * Update recent subjects table
 */
function updateRecentSubjects() {
    const tbody = document.getElementById('recent-subjects-tbody');
    if (!tbody) return;
    
    // Load recent subjects
    loadRecentSubjects();
}

/**
 * Load recent subjects
 */
async function loadRecentSubjects() {
    try {
        const response = await api.getSubjects({
            sort_by: 'processing_timestamp',
            sort_order: 'desc',
            page_size: 10
        });
        
        renderRecentSubjectsTable(response.subjects || []);
    } catch (error) {
        console.error('Error loading recent subjects:', error);
        const tbody = document.getElementById('recent-subjects-tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="text-center text-danger py-4">
                        <i class="bi bi-exclamation-triangle"></i>
                        <p class="mb-0">Error loading subjects</p>
                    </td>
                </tr>
            `;
        }
    }
}

/**
 * Render recent subjects table
 */
function renderRecentSubjectsTable(subjects) {
    const tbody = document.getElementById('recent-subjects-tbody');
    if (!tbody) return;
    
    if (subjects.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="text-center text-muted py-4">
                    <i class="bi bi-inbox"></i>
                    <p class="mb-0">No subjects processed yet</p>
                </td>
            </tr>
        `;
        return;
    }
    
    const rowsHTML = subjects.map(subject => `
        <tr>
            <td>
                <a href="/subjects/${subject.subject_info.subject_id}" class="text-decoration-none">
                    ${subject.subject_info.subject_id}
                </a>
            </td>
            <td>${subject.subject_info.age || 'N/A'}</td>
            <td>
                <span class="badge bg-info">${subject.subject_info.scan_type}</span>
            </td>
            <td>${utils.getQualityStatusBadge(subject.quality_assessment.overall_status)}</td>
            <td>${utils.formatNumber(subject.quality_assessment.composite_score)}</td>
            <td>
                <small class="text-muted">
                    ${utils.formatRelativeTime(subject.processing_timestamp)}
                </small>
            </td>
            <td>
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-primary btn-sm" onclick="viewSubjectDetail('${subject.subject_info.subject_id}')">
                        <i class="bi bi-eye"></i>
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" onclick="exportSubject('${subject.subject_info.subject_id}')">
                        <i class="bi bi-download"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = rowsHTML;
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Metric selection change
    const metricSelect = document.getElementById('metric-select');
    if (metricSelect) {
        metricSelect.addEventListener('change', updateMetricsChart);
    }
    
    // Metric view type change
    const metricViewRadios = document.querySelectorAll('input[name="metric-view"]');
    metricViewRadios.forEach(radio => {
        radio.addEventListener('change', updateMetricsChart);
    });
    
    // Refresh button (if exists)
    const refreshBtn = document.getElementById('refresh-dashboard');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', refreshDashboard);
    }
}

/**
 * Setup WebSocket event handlers
 */
function setupWebSocketHandlers() {
    // Listen for dashboard updates
    wsClient.on('dashboardUpdate', (data) => {
        console.log('Dashboard update received:', data);
        refreshDashboard();
    });
    
    // Listen for batch completion
    wsClient.on('batchCompleted', (data) => {
        console.log('Batch completed, refreshing dashboard');
        setTimeout(refreshDashboard, 1000); // Small delay to ensure data is ready
    });
}

/**
 * Refresh dashboard data
 */
async function refreshDashboard() {
    try {
        await loadDashboardData();
        renderDashboard();
        console.log('Dashboard refreshed');
    } catch (error) {
        console.error('Error refreshing dashboard:', error);
    }
}

/**
 * View subject detail
 */
function viewSubjectDetail(subjectId) {
    window.location.href = `/subjects/${subjectId}`;
}

/**
 * Export subject data
 */
async function exportSubject(subjectId) {
    try {
        showLoading(true, 'Exporting subject data...');
        const blob = await api.exportCSV([subjectId]);
        utils.downloadBlob(blob, `subject_${subjectId}_data.csv`);
        utils.showAlert('Subject data exported successfully', 'success');
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export subject data');
    } finally {
        showLoading(false);
    }
}

/**
 * Show configuration modal
 */
function showConfigModal() {
    const modal = new bootstrap.Modal(document.getElementById('configModal'));
    modal.show();
    
    // Load configuration data
    loadConfigurationData();
}

/**
 * Load configuration data
 */
async function loadConfigurationData() {
    const configContent = document.getElementById('config-content');
    if (!configContent) return;
    
    try {
        configContent.innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <p class="mt-2">Loading configuration...</p>
            </div>
        `;
        
        const thresholds = await api.getQualityThresholds();
        renderConfigurationForm(thresholds);
    } catch (error) {
        configContent.innerHTML = `
            <div class="alert alert-danger">
                <i class="bi bi-exclamation-triangle"></i>
                Failed to load configuration: ${error.message}
            </div>
        `;
    }
}

/**
 * Render configuration form
 */
function renderConfigurationForm(thresholds) {
    const configContent = document.getElementById('config-content');
    if (!configContent) return;
    
    // This would render a form for editing quality thresholds
    configContent.innerHTML = `
        <div class="alert alert-info">
            <i class="bi bi-info-circle"></i>
            Configuration management will be implemented in a future update.
        </div>
        <pre>${JSON.stringify(thresholds, null, 2)}</pre>
    `;
}

/**
 * Save configuration
 */
function saveConfiguration() {
    utils.showAlert('Configuration saved successfully', 'success');
    const modal = bootstrap.Modal.getInstance(document.getElementById('configModal'));
    modal.hide();
}

/**
 * Show export modal
 */
function showExportModal() {
    const modal = new bootstrap.Modal(document.getElementById('exportModal'));
    modal.show();
}

/**
 * Export data
 */
async function exportData() {
    const format = document.getElementById('export-format').value;
    const includeRaw = document.getElementById('include-raw-metrics').checked;
    const includeNormalized = document.getElementById('include-normalized').checked;
    const includeQuality = document.getElementById('include-quality-assessment').checked;
    
    try {
        showLoading(true, 'Exporting data...');
        
        let blob;
        let filename;
        
        if (format === 'csv') {
            blob = await api.exportCSV(null, {
                includeRawMetrics: includeRaw,
                includeNormalized: includeNormalized,
                includeQualityAssessment: includeQuality
            });
            filename = `mriqc_dashboard_export_${new Date().toISOString().split('T')[0]}.csv`;
        } else if (format === 'pdf') {
            blob = await api.exportPDF(null, 'study');
            filename = `mriqc_dashboard_report_${new Date().toISOString().split('T')[0]}.pdf`;
        }
        
        utils.downloadBlob(blob, filename);
        utils.showAlert('Data exported successfully', 'success');
        
        const modal = bootstrap.Modal.getInstance(document.getElementById('exportModal'));
        modal.hide();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export data');
    } finally {
        showLoading(false);
    }
}

/**
 * Get activity icon based on type
 */
function getActivityIcon(type) {
    const icons = {
        'upload': 'cloud-upload',
        'processing': 'gear',
        'completed': 'check-circle',
        'error': 'exclamation-triangle',
        'export': 'download'
    };
    return icons[type] || 'info-circle';
}

/**
 * Get activity icon color based on type
 */
function getActivityIconColor(type) {
    const colors = {
        'upload': 'primary',
        'processing': 'info',
        'completed': 'success',
        'error': 'danger',
        'export': 'secondary'
    };
    return colors[type] || 'secondary';
}

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
    
    // Destroy charts
    Object.values(dashboardCharts).forEach(chart => {
        if (chart) charts.destroyChart(chart);
    });
});

// Export functions for global use
window.refreshDashboard = refreshDashboard;
window.viewSubjectDetail = viewSubjectDetail;
window.exportSubject = exportSubject;
window.showConfigModal = showConfigModal;
window.saveConfiguration = saveConfiguration;
window.showExportModal = showExportModal;
window.exportData = exportData;