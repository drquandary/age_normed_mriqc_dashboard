/**
 * Dashboard page functionality for Age-Normed MRIQC Dashboard
 */

// Dashboard state
let dashboardData = null;
let dashboardCharts = {};
let refreshInterval = null;

// Advanced dashboard features state
let dashboardFilters = {};
let dashboardSort = { sort_by: 'processing_timestamp', sort_order: 'desc' };
let dashboardPage = 1;
let dashboardPageSize = 10;
let dashboardSubjects = [];
let totalDashboardSubjects = 0;
let selectedDashboardSubjects = new Set();
let dashboardViewSettings = {
    widgets: {
        summaryCards: true,
        qualityChart: true,
        ageChart: true,
        metricsChart: true,
        recentActivity: true,
        subjectsTable: true
    },
    columns: {
        subjectId: true,
        age: true,
        ageGroup: true,
        scanType: true,
        qualityStatus: true,
        score: true,
        metrics: true,
        processed: true
    },
    refreshInterval: 30,
    defaultPageSize: 10,
    defaultSort: 'processing_timestamp_desc'
};

/**
 * Initialize dashboard
 */
document.addEventListener('DOMContentLoaded', function() {
    loadViewSettings();
    initializeDashboard();
    setupEventListeners();
    setupWebSocketHandlers();
    setupAdvancedFilters();
    
    // Setup auto-refresh based on settings
    setupAutoRefresh();
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
    // Load dashboard subjects with current filters
    loadDashboardSubjects();
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

/**
 * Load view settings from localStorage
 */
function loadViewSettings() {
    const savedSettings = utils.storage.get('dashboard_view_settings');
    if (savedSettings) {
        dashboardViewSettings = { ...dashboardViewSettings, ...savedSettings };
    }
    
    // Apply settings to UI
    applyViewSettings();
}

/**
 * Apply view settings to UI
 */
function applyViewSettings() {
    // Apply widget visibility
    Object.entries(dashboardViewSettings.widgets).forEach(([widget, visible]) => {
        const element = document.querySelector(`[data-widget="${widget}"]`);
        if (element) {
            element.style.display = visible ? 'block' : 'none';
        }
    });
    
    // Apply page size
    dashboardPageSize = dashboardViewSettings.defaultPageSize;
    const pageSizeSelect = document.getElementById('dashboard-page-size');
    if (pageSizeSelect) {
        pageSizeSelect.value = dashboardPageSize.toString();
    }
    
    // Apply default sort
    const [sortBy, sortOrder] = dashboardViewSettings.defaultSort.split('_');
    dashboardSort = { sort_by: sortBy, sort_order: sortOrder };
}

/**
 * Setup auto-refresh based on settings
 */
function setupAutoRefresh() {
    if (refreshInterval) {
        clearInterval(refreshInterval);
    }
    
    if (dashboardViewSettings.refreshInterval > 0) {
        refreshInterval = setInterval(refreshDashboard, dashboardViewSettings.refreshInterval * 1000);
    }
}

/**
 * Setup advanced filters
 */
function setupAdvancedFilters() {
    // Score threshold slider
    const scoreThreshold = document.getElementById('dashboard-score-threshold');
    const scoreValue = document.getElementById('score-threshold-value');
    
    if (scoreThreshold && scoreValue) {
        scoreThreshold.addEventListener('input', function() {
            scoreValue.textContent = this.value;
        });
    }
    
    // Multi-select filters
    const multiSelects = ['dashboard-quality-filter', 'dashboard-age-group-filter'];
    multiSelects.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            // Add event listener for multi-select changes
            element.addEventListener('change', function() {
                // Update filter state when selections change
                updateDashboardFilters();
            });
        }
    });
    
    // Date range inputs
    const dateInputs = ['dashboard-date-from', 'dashboard-date-to'];
    dateInputs.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', updateDashboardFilters);
        }
    });
    
    // Search input with debounce
    const searchInput = document.getElementById('dashboard-search');
    if (searchInput) {
        searchInput.addEventListener('input', utils.debounce(updateDashboardFilters, 500));
    }
}

/**
 * Update dashboard filters from form inputs
 */
function updateDashboardFilters() {
    dashboardFilters = {};
    
    // Search text
    const searchText = document.getElementById('dashboard-search')?.value.trim();
    if (searchText) {
        dashboardFilters.search_text = searchText;
    }
    
    // Quality status (multi-select)
    const qualitySelect = document.getElementById('dashboard-quality-filter');
    if (qualitySelect) {
        const selectedOptions = Array.from(qualitySelect.selectedOptions).map(opt => opt.value);
        if (selectedOptions.length > 0) {
            dashboardFilters.quality_status = selectedOptions;
        }
    }
    
    // Age group (multi-select)
    const ageGroupSelect = document.getElementById('dashboard-age-group-filter');
    if (ageGroupSelect) {
        const selectedOptions = Array.from(ageGroupSelect.selectedOptions).map(opt => opt.value);
        if (selectedOptions.length > 0) {
            dashboardFilters.age_group = selectedOptions;
        }
    }
    
    // Date range
    const dateFrom = document.getElementById('dashboard-date-from')?.value;
    const dateTo = document.getElementById('dashboard-date-to')?.value;
    if (dateFrom || dateTo) {
        dashboardFilters.date_range = {};
        if (dateFrom) dashboardFilters.date_range.start = dateFrom;
        if (dateTo) dashboardFilters.date_range.end = dateTo;
    }
    
    // Score threshold
    const scoreThreshold = document.getElementById('dashboard-score-threshold')?.value;
    if (scoreThreshold && parseFloat(scoreThreshold) > 0) {
        dashboardFilters.min_composite_score = parseFloat(scoreThreshold);
    }
}

/**
 * Toggle advanced filters panel
 */
function toggleAdvancedFilters() {
    const panel = document.getElementById('advanced-filters-panel');
    const icon = document.getElementById('advanced-filter-icon');
    const text = document.getElementById('advanced-filter-text');
    
    if (panel && icon && text) {
        if (panel.classList.contains('d-none')) {
            panel.classList.remove('d-none');
            icon.className = 'bi bi-sliders-fill';
            text.textContent = 'Hide Filters';
        } else {
            panel.classList.add('d-none');
            icon.className = 'bi bi-sliders';
            text.textContent = 'Show Filters';
        }
    }
}

/**
 * Apply dashboard filters
 */
async function applyDashboardFilters() {
    updateDashboardFilters();
    dashboardPage = 1;
    await loadDashboardSubjects();
    updateSubjectsTableTitle();
}

/**
 * Clear dashboard filters
 */
async function clearDashboardFilters() {
    // Reset form inputs
    const inputs = [
        'dashboard-search',
        'dashboard-date-from',
        'dashboard-date-to'
    ];
    
    inputs.forEach(id => {
        const element = document.getElementById(id);
        if (element) element.value = '';
    });
    
    // Reset multi-selects
    const multiSelects = ['dashboard-quality-filter', 'dashboard-age-group-filter'];
    multiSelects.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            Array.from(element.options).forEach(option => option.selected = false);
        }
    });
    
    // Reset score threshold
    const scoreThreshold = document.getElementById('dashboard-score-threshold');
    const scoreValue = document.getElementById('score-threshold-value');
    if (scoreThreshold && scoreValue) {
        scoreThreshold.value = '0';
        scoreValue.textContent = '0';
    }
    
    // Clear filters and reload
    dashboardFilters = {};
    dashboardPage = 1;
    await loadDashboardSubjects();
    updateSubjectsTableTitle();
}

/**
 * Save dashboard filter preset
 */
function saveDashboardPreset() {
    const presetName = prompt('Enter a name for this filter preset:');
    if (presetName) {
        updateDashboardFilters();
        const presets = utils.storage.get('dashboard_filter_presets', {});
        presets[presetName] = { ...dashboardFilters };
        utils.storage.set('dashboard_filter_presets', presets);
        utils.showAlert(`Filter preset "${presetName}" saved successfully`, 'success');
        
        // Refresh presets list if modal is open
        loadFilterPresets();
    }
}

/**
 * Show filter presets modal
 */
function showFilterPresets() {
    const modal = new bootstrap.Modal(document.getElementById('filter-presets-modal'));
    modal.show();
    loadFilterPresets();
}

/**
 * Load filter presets
 */
function loadFilterPresets() {
    const presetsList = document.getElementById('saved-presets-list');
    if (!presetsList) return;
    
    const presets = utils.storage.get('dashboard_filter_presets', {});
    
    if (Object.keys(presets).length === 0) {
        presetsList.innerHTML = `
            <div class="text-muted text-center py-3">
                <i class="bi bi-bookmark"></i>
                <p class="mb-0">No saved presets</p>
            </div>
        `;
        return;
    }
    
    const presetsHTML = Object.entries(presets).map(([name, filters]) => `
        <div class="list-group-item d-flex justify-content-between align-items-center">
            <div>
                <strong>${name}</strong>
                <br>
                <small class="text-muted">${getFilterSummary(filters)}</small>
            </div>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-primary" onclick="applyFilterPreset('${name}')">
                    <i class="bi bi-play"></i>
                </button>
                <button class="btn btn-outline-danger" onclick="deleteFilterPreset('${name}')">
                    <i class="bi bi-trash"></i>
                </button>
            </div>
        </div>
    `).join('');
    
    presetsList.innerHTML = presetsHTML;
}

/**
 * Get filter summary text
 */
function getFilterSummary(filters) {
    const parts = [];
    
    if (filters.quality_status) {
        parts.push(`Quality: ${filters.quality_status.join(', ')}`);
    }
    
    if (filters.age_group) {
        parts.push(`Age: ${filters.age_group.join(', ')}`);
    }
    
    if (filters.search_text) {
        parts.push(`Search: "${filters.search_text}"`);
    }
    
    if (filters.date_range) {
        parts.push('Date range');
    }
    
    if (filters.min_composite_score) {
        parts.push(`Score > ${filters.min_composite_score}`);
    }
    
    return parts.length > 0 ? parts.join(', ') : 'No filters';
}

/**
 * Apply filter preset
 */
async function applyFilterPreset(presetName) {
    const presets = utils.storage.get('dashboard_filter_presets', {});
    const preset = presets[presetName];
    
    if (!preset) {
        utils.showAlert('Preset not found', 'error');
        return;
    }
    
    // Apply preset to form
    dashboardFilters = { ...preset };
    applyFiltersToForm(preset);
    
    // Apply filters
    dashboardPage = 1;
    await loadDashboardSubjects();
    updateSubjectsTableTitle();
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('filter-presets-modal'));
    modal.hide();
    
    utils.showAlert(`Applied preset "${presetName}"`, 'success');
}

/**
 * Apply filters to form elements
 */
function applyFiltersToForm(filters) {
    // Search text
    const searchInput = document.getElementById('dashboard-search');
    if (searchInput) {
        searchInput.value = filters.search_text || '';
    }
    
    // Quality status
    const qualitySelect = document.getElementById('dashboard-quality-filter');
    if (qualitySelect && filters.quality_status) {
        Array.from(qualitySelect.options).forEach(option => {
            option.selected = filters.quality_status.includes(option.value);
        });
    }
    
    // Age group
    const ageGroupSelect = document.getElementById('dashboard-age-group-filter');
    if (ageGroupSelect && filters.age_group) {
        Array.from(ageGroupSelect.options).forEach(option => {
            option.selected = filters.age_group.includes(option.value);
        });
    }
    
    // Date range
    if (filters.date_range) {
        const dateFrom = document.getElementById('dashboard-date-from');
        const dateTo = document.getElementById('dashboard-date-to');
        if (dateFrom) dateFrom.value = filters.date_range.start || '';
        if (dateTo) dateTo.value = filters.date_range.end || '';
    }
    
    // Score threshold
    if (filters.min_composite_score) {
        const scoreThreshold = document.getElementById('dashboard-score-threshold');
        const scoreValue = document.getElementById('score-threshold-value');
        if (scoreThreshold && scoreValue) {
            scoreThreshold.value = filters.min_composite_score.toString();
            scoreValue.textContent = filters.min_composite_score.toString();
        }
    }
}

/**
 * Delete filter preset
 */
function deleteFilterPreset(presetName) {
    if (confirm(`Are you sure you want to delete the preset "${presetName}"?`)) {
        const presets = utils.storage.get('dashboard_filter_presets', {});
        delete presets[presetName];
        utils.storage.set('dashboard_filter_presets', presets);
        loadFilterPresets();
        utils.showAlert(`Preset "${presetName}" deleted`, 'success');
    }
}

/**
 * Apply quick preset
 */
async function applyQuickPreset(presetType) {
    let filters = {};
    
    switch (presetType) {
        case 'failed_subjects':
            filters.quality_status = ['fail'];
            break;
        case 'warning_subjects':
            filters.quality_status = ['warning'];
            break;
        case 'pediatric_subjects':
            filters.age_group = ['pediatric'];
            break;
        case 'recent_subjects':
            const sevenDaysAgo = new Date();
            sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
            filters.date_range = {
                start: sevenDaysAgo.toISOString().split('T')[0]
            };
            break;
        case 'high_quality':
            filters.min_composite_score = 0.8;
            break;
    }
    
    dashboardFilters = filters;
    applyFiltersToForm(filters);
    
    dashboardPage = 1;
    await loadDashboardSubjects();
    updateSubjectsTableTitle();
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('filter-presets-modal'));
    modal.hide();
    
    utils.showAlert('Quick preset applied', 'success');
}

/**
 * Show customize view modal
 */
function showCustomizeView() {
    const modal = new bootstrap.Modal(document.getElementById('customize-view-modal'));
    modal.show();
    
    // Load current settings into form
    loadCustomizeViewForm();
}

/**
 * Load customize view form with current settings
 */
function loadCustomizeViewForm() {
    // Widget checkboxes
    Object.entries(dashboardViewSettings.widgets).forEach(([widget, visible]) => {
        const checkbox = document.getElementById(`show-${widget.replace(/([A-Z])/g, '-$1').toLowerCase()}`);
        if (checkbox) {
            checkbox.checked = visible;
        }
    });
    
    // Column checkboxes
    Object.entries(dashboardViewSettings.columns).forEach(([column, visible]) => {
        const checkbox = document.getElementById(`show-col-${column.replace(/([A-Z])/g, '-$1').toLowerCase()}`);
        if (checkbox) {
            checkbox.checked = visible;
        }
    });
    
    // Other settings
    const refreshInterval = document.getElementById('refresh-interval');
    if (refreshInterval) {
        refreshInterval.value = dashboardViewSettings.refreshInterval.toString();
    }
    
    const defaultPageSize = document.getElementById('default-page-size');
    if (defaultPageSize) {
        defaultPageSize.value = dashboardViewSettings.defaultPageSize.toString();
    }
    
    const defaultSort = document.getElementById('default-sort');
    if (defaultSort) {
        defaultSort.value = dashboardViewSettings.defaultSort;
    }
}

/**
 * Save view settings
 */
function saveViewSettings() {
    // Collect widget settings
    const widgets = {};
    Object.keys(dashboardViewSettings.widgets).forEach(widget => {
        const checkbox = document.getElementById(`show-${widget.replace(/([A-Z])/g, '-$1').toLowerCase()}`);
        if (checkbox) {
            widgets[widget] = checkbox.checked;
        }
    });
    
    // Collect column settings
    const columns = {};
    Object.keys(dashboardViewSettings.columns).forEach(column => {
        const checkbox = document.getElementById(`show-col-${column.replace(/([A-Z])/g, '-$1').toLowerCase()}`);
        if (checkbox) {
            columns[column] = checkbox.checked;
        }
    });
    
    // Collect other settings
    const refreshInterval = parseInt(document.getElementById('refresh-interval')?.value || '30');
    const defaultPageSize = parseInt(document.getElementById('default-page-size')?.value || '10');
    const defaultSort = document.getElementById('default-sort')?.value || 'processing_timestamp_desc';
    
    // Update settings
    dashboardViewSettings = {
        widgets,
        columns,
        refreshInterval,
        defaultPageSize,
        defaultSort
    };
    
    // Save to localStorage
    utils.storage.set('dashboard_view_settings', dashboardViewSettings);
    
    // Apply settings
    applyViewSettings();
    setupAutoRefresh();
    
    // Refresh dashboard
    refreshDashboard();
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('customize-view-modal'));
    modal.hide();
    
    utils.showAlert('View settings saved successfully', 'success');
}

/**
 * Reset view settings to default
 */
function resetViewSettings() {
    if (confirm('Are you sure you want to reset all view settings to default?')) {
        utils.storage.remove('dashboard_view_settings');
        
        // Reset to default settings
        dashboardViewSettings = {
            widgets: {
                summaryCards: true,
                qualityChart: true,
                ageChart: true,
                metricsChart: true,
                recentActivity: true,
                subjectsTable: true
            },
            columns: {
                subjectId: true,
                age: true,
                ageGroup: true,
                scanType: true,
                qualityStatus: true,
                score: true,
                metrics: true,
                processed: true
            },
            refreshInterval: 30,
            defaultPageSize: 10,
            defaultSort: 'processing_timestamp_desc'
        };
        
        // Update form
        loadCustomizeViewForm();
        
        utils.showAlert('Settings reset to default', 'success');
    }
}

/**
 * Load dashboard subjects with filtering
 */
async function loadDashboardSubjects() {
    try {
        let response;
        
        if (Object.keys(dashboardFilters).length > 0) {
            // Use advanced filtering
            response = await api.filterSubjects(
                dashboardFilters,
                dashboardSort,
                dashboardPage,
                dashboardPageSize
            );
        } else {
            // Use simple filtering
            const params = {
                ...dashboardSort,
                page: dashboardPage,
                page_size: dashboardPageSize
            };
            response = await api.getSubjects(params);
        }
        
        dashboardSubjects = response.subjects || [];
        totalDashboardSubjects = response.total_count || 0;
        
        renderDashboardSubjectsTable();
        updateDashboardPagination();
        updateFilteredSubjectCount();
        
    } catch (error) {
        console.error('Error loading dashboard subjects:', error);
        const tbody = document.getElementById('dashboard-subjects-tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="10" class="text-center text-danger py-4">
                        <i class="bi bi-exclamation-triangle"></i>
                        <p class="mb-0">Error loading subjects</p>
                    </td>
                </tr>
            `;
        }
    }
}

/**
 * Render dashboard subjects table
 */
function renderDashboardSubjectsTable() {
    const tbody = document.getElementById('dashboard-subjects-tbody');
    if (!tbody) return;
    
    if (dashboardSubjects.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="10" class="text-center text-muted py-4">
                    <i class="bi bi-inbox"></i>
                    <p class="mb-0">No subjects found</p>
                    ${Object.keys(dashboardFilters).length > 0 ? 
                        '<small>Try adjusting your filters</small>' : 
                        '<small>Upload some MRIQC data to get started</small>'
                    }
                </td>
            </tr>
        `;
        return;
    }
    
    const rowsHTML = dashboardSubjects.map(subject => `
        <tr>
            <td>
                <input type="checkbox" class="form-check-input dashboard-subject-checkbox" 
                       value="${subject.subject_info.subject_id}"
                       ${selectedDashboardSubjects.has(subject.subject_info.subject_id) ? 'checked' : ''}>
            </td>
            <td>
                <a href="/subjects/${subject.subject_info.subject_id}" class="text-decoration-none fw-medium">
                    ${subject.subject_info.subject_id}
                </a>
            </td>
            <td>${subject.subject_info.age || 'N/A'}</td>
            <td>
                ${subject.normalized_metrics ? 
                    utils.getAgeGroupBadge(subject.normalized_metrics.age_group) : 
                    '<span class="text-muted">N/A</span>'
                }
            </td>
            <td>
                <span class="badge bg-info">${subject.subject_info.scan_type}</span>
            </td>
            <td>${utils.getQualityStatusBadge(subject.quality_assessment.overall_status)}</td>
            <td>
                <span class="fw-medium ${getScoreColor(subject.quality_assessment.composite_score)}">
                    ${utils.formatNumber(subject.quality_assessment.composite_score)}
                </span>
            </td>
            <td>
                <div class="d-flex gap-1">
                    ${renderKeyMetrics(subject.raw_metrics)}
                </div>
            </td>
            <td>
                <small class="text-muted">
                    ${utils.formatRelativeTime(subject.processing_timestamp)}
                </small>
            </td>
            <td>
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-outline-primary btn-sm" 
                            onclick="viewSubjectDetail('${subject.subject_info.subject_id}')"
                            title="View Details">
                        <i class="bi bi-eye"></i>
                    </button>
                    <button class="btn btn-outline-secondary btn-sm" 
                            onclick="exportSubject('${subject.subject_info.subject_id}')"
                            title="Export Data">
                        <i class="bi bi-download"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = rowsHTML;
    
    // Update select all checkbox
    updateDashboardSelectAllCheckbox();
    
    // Setup checkbox event listeners
    setupDashboardCheckboxListeners();
}

/**
 * Setup dashboard checkbox event listeners
 */
function setupDashboardCheckboxListeners() {
    // Select all checkbox
    const selectAllCheckbox = document.getElementById('dashboard-select-all');
    if (selectAllCheckbox) {
        selectAllCheckbox.removeEventListener('change', handleDashboardSelectAllChange);
        selectAllCheckbox.addEventListener('change', handleDashboardSelectAllChange);
    }
    
    // Individual checkboxes
    document.querySelectorAll('.dashboard-subject-checkbox').forEach(checkbox => {
        checkbox.removeEventListener('change', handleDashboardSubjectCheckboxChange);
        checkbox.addEventListener('change', handleDashboardSubjectCheckboxChange);
    });
}

/**
 * Handle dashboard select all checkbox change
 */
function handleDashboardSelectAllChange(event) {
    const isChecked = event.target.checked;
    const checkboxes = document.querySelectorAll('.dashboard-subject-checkbox');
    
    checkboxes.forEach(checkbox => {
        checkbox.checked = isChecked;
        const subjectId = checkbox.value;
        
        if (isChecked) {
            selectedDashboardSubjects.add(subjectId);
        } else {
            selectedDashboardSubjects.delete(subjectId);
        }
    });
    
    updateDashboardBulkActionButtons();
}

/**
 * Handle dashboard subject checkbox change
 */
function handleDashboardSubjectCheckboxChange(event) {
    const subjectId = event.target.value;
    
    if (event.target.checked) {
        selectedDashboardSubjects.add(subjectId);
    } else {
        selectedDashboardSubjects.delete(subjectId);
    }
    
    updateDashboardSelectAllCheckbox();
    updateDashboardBulkActionButtons();
}

/**
 * Update dashboard select all checkbox state
 */
function updateDashboardSelectAllCheckbox() {
    const selectAllCheckbox = document.getElementById('dashboard-select-all');
    const subjectCheckboxes = document.querySelectorAll('.dashboard-subject-checkbox');
    
    if (!selectAllCheckbox || subjectCheckboxes.length === 0) return;
    
    const checkedCount = Array.from(subjectCheckboxes).filter(cb => cb.checked).length;
    
    if (checkedCount === 0) {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    } else if (checkedCount === subjectCheckboxes.length) {
        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
    } else {
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = true;
    }
}

/**
 * Update dashboard bulk action buttons
 */
function updateDashboardBulkActionButtons() {
    const selectedCount = selectedDashboardSubjects.size;
    
    // Update selected count in bulk update modal
    const selectedCountSpan = document.getElementById('dashboard-selected-count');
    if (selectedCountSpan) {
        selectedCountSpan.textContent = selectedCount.toString();
    }
}

/**
 * Update dashboard pagination
 */
function updateDashboardPagination() {
    const paginationInfo = document.getElementById('dashboard-pagination-info');
    const pagination = document.getElementById('dashboard-pagination');
    
    if (!pagination) return;
    
    const startIdx = (dashboardPage - 1) * dashboardPageSize + 1;
    const endIdx = Math.min(dashboardPage * dashboardPageSize, totalDashboardSubjects);
    
    // Update pagination info
    if (paginationInfo) {
        paginationInfo.textContent = `Showing ${startIdx} - ${endIdx} of ${totalDashboardSubjects} subjects`;
    }
    
    // Calculate pagination
    const totalPages = Math.ceil(totalDashboardSubjects / dashboardPageSize);
    const maxVisiblePages = 3;
    
    let startPage = Math.max(1, dashboardPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage + 1 < maxVisiblePages) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    // Build pagination HTML
    let paginationHTML = '';
    
    // Previous button
    paginationHTML += `
        <li class="page-item ${dashboardPage === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changeDashboardPage(${dashboardPage - 1})">
                <i class="bi bi-chevron-left"></i>
            </a>
        </li>
    `;
    
    // Page numbers
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <li class="page-item ${i === dashboardPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="changeDashboardPage(${i})">${i}</a>
            </li>
        `;
    }
    
    // Next button
    paginationHTML += `
        <li class="page-item ${dashboardPage === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changeDashboardPage(${dashboardPage + 1})">
                <i class="bi bi-chevron-right"></i>
            </a>
        </li>
    `;
    
    pagination.innerHTML = paginationHTML;
}

/**
 * Update filtered subject count
 */
function updateFilteredSubjectCount() {
    const countElement = document.getElementById('filtered-subject-count');
    if (countElement) {
        countElement.textContent = totalDashboardSubjects.toString();
    }
}

/**
 * Update subjects table title
 */
function updateSubjectsTableTitle() {
    const titleElement = document.getElementById('subjects-table-title');
    const summaryElement = document.getElementById('subjects-filter-summary');
    
    if (titleElement) {
        if (Object.keys(dashboardFilters).length > 0) {
            titleElement.textContent = 'Filtered Subjects';
        } else {
            titleElement.textContent = 'Recent Subjects';
        }
    }
    
    if (summaryElement) {
        if (Object.keys(dashboardFilters).length > 0) {
            summaryElement.textContent = `Showing filtered results: ${getFilterSummary(dashboardFilters)}`;
        } else {
            summaryElement.textContent = 'Showing recent subjects';
        }
    }
}

/**
 * Dashboard pagination and sorting functions
 */
function changeDashboardPage(page) {
    if (page < 1 || page > Math.ceil(totalDashboardSubjects / dashboardPageSize)) return;
    
    dashboardPage = page;
    loadDashboardSubjects();
}

function previousDashboardPage() {
    changeDashboardPage(dashboardPage - 1);
}

function nextDashboardPage() {
    changeDashboardPage(dashboardPage + 1);
}

function changeDashboardPageSize() {
    const pageSizeSelect = document.getElementById('dashboard-page-size');
    if (pageSizeSelect) {
        dashboardPageSize = parseInt(pageSizeSelect.value);
        dashboardPage = 1;
        loadDashboardSubjects();
    }
}

function sortDashboardSubjects(sortBy) {
    if (dashboardSort.sort_by === sortBy) {
        // Toggle sort order
        dashboardSort.sort_order = dashboardSort.sort_order === 'asc' ? 'desc' : 'asc';
    } else {
        // New sort field
        dashboardSort.sort_by = sortBy;
        dashboardSort.sort_order = 'asc';
    }
    
    dashboardPage = 1;
    loadDashboardSubjects();
}

/**
 * Dashboard bulk operations
 */
function selectAllDashboardSubjects() {
    const checkboxes = document.querySelectorAll('.dashboard-subject-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
        selectedDashboardSubjects.add(checkbox.value);
    });
    
    updateDashboardSelectAllCheckbox();
    updateDashboardBulkActionButtons();
}

function deselectAllDashboardSubjects() {
    const checkboxes = document.querySelectorAll('.dashboard-subject-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
        selectedDashboardSubjects.delete(checkbox.value);
    });
    
    updateDashboardSelectAllCheckbox();
    updateDashboardBulkActionButtons();
}

async function bulkExportSelected() {
    if (selectedDashboardSubjects.size === 0) {
        utils.showAlert('Please select subjects to export', 'warning');
        return;
    }
    
    try {
        showLoading(true, 'Exporting selected subjects...');
        
        const subjectIds = Array.from(selectedDashboardSubjects);
        const blob = await api.exportCSV(subjectIds);
        
        const filename = `dashboard_export_${new Date().toISOString().split('T')[0]}.csv`;
        utils.downloadBlob(blob, filename);
        
        utils.showAlert(`Exported ${subjectIds.length} subjects successfully`, 'success');
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export subjects');
    } finally {
        showLoading(false);
    }
}

function bulkUpdateQualityDashboard() {
    if (selectedDashboardSubjects.size === 0) {
        utils.showAlert('Please select subjects to update', 'warning');
        return;
    }
    
    const modal = new bootstrap.Modal(document.getElementById('dashboard-bulk-update-modal'));
    modal.show();
}

async function confirmDashboardBulkUpdate() {
    const qualityStatus = document.getElementById('dashboard-bulk-quality-status').value;
    const reason = document.getElementById('dashboard-bulk-update-reason').value;
    
    if (!qualityStatus) {
        utils.showAlert('Please select a quality status', 'warning');
        return;
    }
    
    try {
        showLoading(true, 'Updating subjects...');
        
        await api.bulkUpdateQuality(Array.from(selectedDashboardSubjects), qualityStatus, reason);
        
        utils.showAlert(`Updated ${selectedDashboardSubjects.size} subjects successfully`, 'success');
        
        // Refresh dashboard
        await refreshDashboard();
        
        // Clear selection
        selectedDashboardSubjects.clear();
        updateDashboardSelectAllCheckbox();
        updateDashboardBulkActionButtons();
        
        // Hide modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('dashboard-bulk-update-modal'));
        modal.hide();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to update subjects');
    } finally {
        showLoading(false);
    }
}

function refreshDashboardSubjects() {
    loadDashboardSubjects();
}

function clearDashboardSearch() {
    const searchInput = document.getElementById('dashboard-search');
    if (searchInput) {
        searchInput.value = '';
        updateDashboardFilters();
        dashboardPage = 1;
        loadDashboardSubjects();
        updateSubjectsTableTitle();
    }
}

// Export functions for global use
window.refreshDashboard = refreshDashboard;
window.viewSubjectDetail = viewSubjectDetail;
window.exportSubject = exportSubject;
window.showConfigModal = showConfigModal;
window.saveConfiguration = saveConfiguration;
window.showExportModal = showExportModal;
window.exportData = exportData;

// Advanced dashboard functions
window.toggleAdvancedFilters = toggleAdvancedFilters;
window.applyDashboardFilters = applyDashboardFilters;
window.clearDashboardFilters = clearDashboardFilters;
window.saveDashboardPreset = saveDashboardPreset;
window.showFilterPresets = showFilterPresets;
window.applyFilterPreset = applyFilterPreset;
window.deleteFilterPreset = deleteFilterPreset;
window.applyQuickPreset = applyQuickPreset;
window.showCustomizeView = showCustomizeView;
window.saveViewSettings = saveViewSettings;
window.resetViewSettings = resetViewSettings;
window.changeDashboardPage = changeDashboardPage;
window.previousDashboardPage = previousDashboardPage;
window.nextDashboardPage = nextDashboardPage;
window.changeDashboardPageSize = changeDashboardPageSize;
window.sortDashboardSubjects = sortDashboardSubjects;
window.selectAllDashboardSubjects = selectAllDashboardSubjects;
window.deselectAllDashboardSubjects = deselectAllDashboardSubjects;
window.bulkExportSelected = bulkExportSelected;
window.bulkUpdateQualityDashboard = bulkUpdateQualityDashboard;
window.confirmDashboardBulkUpdate = confirmDashboardBulkUpdate;
window.refreshDashboardSubjects = refreshDashboardSubjects;
window.clearDashboardSearch = clearDashboardSearch;