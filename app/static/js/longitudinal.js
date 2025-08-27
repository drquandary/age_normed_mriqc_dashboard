/**
 * Longitudinal data visualization and management for MRIQC Dashboard
 */

class LongitudinalManager {
    constructor() {
        this.currentSubject = null;
        this.timeSeriesCharts = {};
        this.trendData = {};
        this.initializeEventListeners();
    }

    initializeEventListeners() {
        // Subject selection
        document.addEventListener('change', (e) => {
            if (e.target.id === 'longitudinal-subject-select') {
                this.loadSubjectData(e.target.value);
            }
        });

        // Metric selection for trend analysis
        document.addEventListener('change', (e) => {
            if (e.target.classList.contains('metric-checkbox')) {
                this.updateTrendVisualization();
            }
        });

        // Export buttons
        document.addEventListener('click', (e) => {
            if (e.target.id === 'export-longitudinal-csv') {
                this.exportData('csv');
            } else if (e.target.id === 'export-longitudinal-json') {
                this.exportData('json');
            }
        });
    }

    async loadLongitudinalSubjects(studyName = null) {
        try {
            showLoading('Loading longitudinal subjects...');
            
            const params = new URLSearchParams();
            if (studyName) params.append('study_name', studyName);
            
            const response = await fetch(`/api/longitudinal/subjects?${params}`);
            if (!response.ok) throw new Error('Failed to load subjects');
            
            const data = await response.json();
            this.renderSubjectsList(data.subjects);
            
        } catch (error) {
            console.error('Error loading longitudinal subjects:', error);
            showError('Failed to load longitudinal subjects');
        } finally {
            hideLoading();
        }
    }

    renderSubjectsList(subjects) {
        const container = document.getElementById('longitudinal-subjects-list');
        if (!container) return;

        if (subjects.length === 0) {
            container.innerHTML = '<p class="text-muted">No longitudinal subjects found.</p>';
            return;
        }

        const html = subjects.map(subject => `
            <div class="card mb-3">
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-8">
                            <h6 class="card-title">${subject.subject_id}</h6>
                            <p class="card-text">
                                <small class="text-muted">
                                    ${subject.timepoint_count} timepoints
                                    ${subject.age_range ? 
                                        `| Age range: ${subject.age_range.min.toFixed(1)} - ${subject.age_range.max.toFixed(1)} years` : 
                                        ''}
                                    ${subject.follow_up_days ? 
                                        `| Follow-up: ${subject.follow_up_days} days` : 
                                        ''}
                                </small>
                            </p>
                        </div>
                        <div class="col-md-4 text-end">
                            <button class="btn btn-primary btn-sm" 
                                    onclick="longitudinalManager.loadSubjectData('${subject.subject_id}')">
                                View Details
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `).join('');

        container.innerHTML = html;
    }    as
ync loadSubjectData(subjectId) {
        try {
            showLoading(`Loading data for ${subjectId}...`);
            this.currentSubject = subjectId;

            // Load subject details and trends
            const [subjectResponse, trendsResponse] = await Promise.all([
                fetch(`/api/longitudinal/subjects/${subjectId}`),
                fetch(`/api/longitudinal/subjects/${subjectId}/trends`)
            ]);

            if (!subjectResponse.ok || !trendsResponse.ok) {
                throw new Error('Failed to load subject data');
            }

            const subjectData = await subjectResponse.json();
            const trendsData = await trendsResponse.json();

            this.renderSubjectDetails(subjectData);
            this.renderTrendsAnalysis(trendsData);
            this.createTimeSeriesCharts(subjectData, trendsData);

        } catch (error) {
            console.error('Error loading subject data:', error);
            showError(`Failed to load data for ${subjectId}`);
        } finally {
            hideLoading();
        }
    }

    renderSubjectDetails(subjectData) {
        const container = document.getElementById('subject-details');
        if (!container) return;

        const html = `
            <div class="card">
                <div class="card-header">
                    <h5>Subject: ${subjectData.subject_id}</h5>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <p><strong>Baseline Age:</strong> ${subjectData.baseline_age?.toFixed(1) || 'N/A'} years</p>
                            <p><strong>Sex:</strong> ${subjectData.sex || 'N/A'}</p>
                            <p><strong>Study:</strong> ${subjectData.study_name || 'N/A'}</p>
                        </div>
                        <div class="col-md-6">
                            <p><strong>Number of Timepoints:</strong> ${subjectData.num_timepoints}</p>
                            <p><strong>Age Range:</strong> ${
                                subjectData.age_range ? 
                                `${subjectData.age_range.min.toFixed(1)} - ${subjectData.age_range.max.toFixed(1)} years` : 
                                'N/A'
                            }</p>
                            <p><strong>Follow-up Duration:</strong> ${
                                subjectData.follow_up_duration_days ? 
                                `${subjectData.follow_up_duration_days} days` : 
                                'N/A'
                            }</p>
                        </div>
                    </div>
                    
                    <h6 class="mt-3">Timepoints</h6>
                    <div class="table-responsive">
                        <table class="table table-sm">
                            <thead>
                                <tr>
                                    <th>Session</th>
                                    <th>Age</th>
                                    <th>Days from Baseline</th>
                                    <th>Quality Status</th>
                                    <th>Composite Score</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${subjectData.timepoints.map(tp => `
                                    <tr>
                                        <td>${tp.session || 'N/A'}</td>
                                        <td>${tp.age_at_scan?.toFixed(1) || 'N/A'}</td>
                                        <td>${tp.days_from_baseline || 0}</td>
                                        <td>
                                            <span class="badge bg-${this.getStatusColor(tp.processed_subject.quality_assessment.overall_status)}">
                                                ${tp.processed_subject.quality_assessment.overall_status}
                                            </span>
                                        </td>
                                        <td>${tp.processed_subject.quality_assessment.composite_score.toFixed(1)}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;

        container.innerHTML = html;
    }

    renderTrendsAnalysis(trendsData) {
        const container = document.getElementById('trends-analysis');
        if (!container) return;

        if (trendsData.length === 0) {
            container.innerHTML = '<p class="text-muted">No trend data available.</p>';
            return;
        }

        const html = `
            <div class="card">
                <div class="card-header">
                    <h5>Quality Metric Trends</h5>
                </div>
                <div class="card-body">
                    <div class="row">
                        ${trendsData.map(trend => `
                            <div class="col-md-6 mb-3">
                                <div class="border rounded p-3">
                                    <h6>${trend.metric_name.toUpperCase()}</h6>
                                    <p class="mb-1">
                                        <strong>Trend:</strong> 
                                        <span class="badge bg-${this.getTrendColor(trend.trend_direction)}">
                                            ${trend.trend_direction}
                                        </span>
                                    </p>
                                    ${trend.trend_slope !== null ? 
                                        `<p class="mb-1"><strong>Slope:</strong> ${trend.trend_slope.toFixed(4)} units/day</p>` : 
                                        ''
                                    }
                                    ${trend.trend_r_squared !== null ? 
                                        `<p class="mb-1"><strong>R²:</strong> ${trend.trend_r_squared.toFixed(3)}</p>` : 
                                        ''
                                    }
                                    ${trend.trend_p_value !== null ? 
                                        `<p class="mb-1"><strong>p-value:</strong> ${trend.trend_p_value.toFixed(3)}</p>` : 
                                        ''
                                    }
                                    ${trend.age_group_changes.length > 0 ? 
                                        `<p class="mb-0"><small class="text-info">${trend.age_group_changes.join(', ')}</small></p>` : 
                                        ''
                                    }
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
            </div>
        `;

        container.innerHTML = html;
        this.trendData = trendsData;
    }

    createTimeSeriesCharts(subjectData, trendsData) {
        const container = document.getElementById('timeseries-charts');
        if (!container) return;

        // Clear existing charts
        Object.values(this.timeSeriesCharts).forEach(chart => chart.destroy());
        this.timeSeriesCharts = {};

        // Create chart container
        container.innerHTML = `
            <div class="card">
                <div class="card-header">
                    <h5>Time Series Visualization</h5>
                    <div class="mt-2">
                        <label class="form-label">Select metrics to display:</label>
                        <div id="metric-checkboxes" class="d-flex flex-wrap gap-2">
                            ${trendsData.map(trend => `
                                <div class="form-check">
                                    <input class="form-check-input metric-checkbox" 
                                           type="checkbox" 
                                           value="${trend.metric_name}" 
                                           id="metric-${trend.metric_name}"
                                           ${trendsData.indexOf(trend) < 4 ? 'checked' : ''}>
                                    <label class="form-check-label" for="metric-${trend.metric_name}">
                                        ${trend.metric_name.toUpperCase()}
                                    </label>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                </div>
                <div class="card-body">
                    <div id="charts-container"></div>
                </div>
            </div>
        `;

        this.updateTrendVisualization();
    }

    updateTrendVisualization() {
        const selectedMetrics = Array.from(document.querySelectorAll('.metric-checkbox:checked'))
            .map(cb => cb.value);

        const chartsContainer = document.getElementById('charts-container');
        if (!chartsContainer || !this.trendData) return;

        // Clear existing charts
        Object.values(this.timeSeriesCharts).forEach(chart => chart.destroy());
        this.timeSeriesCharts = {};

        // Create charts for selected metrics
        chartsContainer.innerHTML = selectedMetrics.map(metric => 
            `<div class="mb-4">
                <canvas id="chart-${metric}" width="400" height="200"></canvas>
            </div>`
        ).join('');

        selectedMetrics.forEach(metric => {
            this.createMetricChart(metric);
        });
    }

    createMetricChart(metricName) {
        const trendData = this.trendData.find(t => t.metric_name === metricName);
        if (!trendData) return;

        const ctx = document.getElementById(`chart-${metricName}`);
        if (!ctx) return;

        const data = trendData.values_over_time.map(point => ({
            x: point.days_from_baseline,
            y: point.value
        }));

        // Create trend line data if slope is available
        let trendLineData = [];
        if (trendData.trend_slope !== null && data.length > 1) {
            const minDays = Math.min(...data.map(d => d.x));
            const maxDays = Math.max(...data.map(d => d.x));
            const baseValue = data.find(d => d.x === minDays)?.y || data[0].y;
            
            trendLineData = [
                { x: minDays, y: baseValue },
                { x: maxDays, y: baseValue + trendData.trend_slope * (maxDays - minDays) }
            ];
        }

        this.timeSeriesCharts[metricName] = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [
                    {
                        label: metricName.toUpperCase(),
                        data: data,
                        borderColor: this.getMetricColor(metricName),
                        backgroundColor: this.getMetricColor(metricName, 0.1),
                        borderWidth: 2,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        fill: false
                    },
                    ...(trendLineData.length > 0 ? [{
                        label: 'Trend Line',
                        data: trendLineData,
                        borderColor: 'rgba(255, 99, 132, 0.8)',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        pointRadius: 0,
                        fill: false
                    }] : [])
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    title: {
                        display: true,
                        text: `${metricName.toUpperCase()} - ${trendData.trend_direction} trend`
                    },
                    legend: {
                        display: trendLineData.length > 0
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Days from Baseline'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Metric Value'
                        }
                    }
                },
                interaction: {
                    intersect: false,
                    mode: 'index'
                }
            }
        });
    }

    async exportData(format) {
        try {
            showLoading(`Exporting longitudinal data as ${format.toUpperCase()}...`);
            
            const studyName = document.getElementById('study-name-filter')?.value || null;
            const params = new URLSearchParams();
            if (studyName) params.append('study_name', studyName);
            params.append('format', format);
            
            const response = await fetch(`/api/longitudinal/export?${params}`, {
                method: 'POST'
            });
            
            if (!response.ok) throw new Error('Export failed');
            
            // Download the file
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `longitudinal_data_${new Date().toISOString().split('T')[0]}.${format}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            
            showSuccess(`Longitudinal data exported successfully as ${format.toUpperCase()}`);
            
        } catch (error) {
            console.error('Export error:', error);
            showError(`Failed to export data: ${error.message}`);
        } finally {
            hideLoading();
        }
    }

    // Utility methods
    getStatusColor(status) {
        const colors = {
            'pass': 'success',
            'warning': 'warning',
            'fail': 'danger',
            'uncertain': 'secondary'
        };
        return colors[status] || 'secondary';
    }

    getTrendColor(trend) {
        const colors = {
            'improving': 'success',
            'stable': 'info',
            'declining': 'warning',
            'variable': 'secondary'
        };
        return colors[trend] || 'secondary';
    }

    getMetricColor(metric, alpha = 1) {
        const colors = {
            'snr': `rgba(54, 162, 235, ${alpha})`,
            'cnr': `rgba(255, 99, 132, ${alpha})`,
            'fber': `rgba(255, 205, 86, ${alpha})`,
            'efc': `rgba(75, 192, 192, ${alpha})`,
            'fwhm_avg': `rgba(153, 102, 255, ${alpha})`,
            'qi1': `rgba(255, 159, 64, ${alpha})`,
            'cjv': `rgba(199, 199, 199, ${alpha})`
        };
        return colors[metric] || `rgba(100, 100, 100, ${alpha})`;
    }
}

// Initialize longitudinal manager
const longitudinalManager = new LongitudinalManager();

// Utility functions for loading states
function showLoading(message = 'Loading...') {
    const loader = document.getElementById('loading-indicator');
    if (loader) {
        loader.textContent = message;
        loader.style.display = 'block';
    }
}

function hideLoading() {
    const loader = document.getElementById('loading-indicator');
    if (loader) {
        loader.style.display = 'none';
    }
}

function showSuccess(message) {
    showAlert(message, 'success');
}

function showError(message) {
    showAlert(message, 'danger');
}

function showAlert(message, type) {
    const alertContainer = document.getElementById('alert-container');
    if (!alertContainer) return;

    const alert = document.createElement('div');
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    alertContainer.appendChild(alert);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        if (alert.parentNode) {
            alert.remove();
        }
    }, 5000);
}