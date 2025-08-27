/**
 * Chart utilities for Age-Normed MRIQC Dashboard
 */

/**
 * Chart configuration defaults
 */
const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
        legend: {
            position: 'top',
        },
        tooltip: {
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            titleColor: 'white',
            bodyColor: 'white',
            borderColor: 'rgba(255, 255, 255, 0.1)',
            borderWidth: 1,
            cornerRadius: 6,
            displayColors: true,
        }
    },
    scales: {
        x: {
            grid: {
                color: 'rgba(0, 0, 0, 0.1)',
            },
            ticks: {
                color: '#666'
            }
        },
        y: {
            grid: {
                color: 'rgba(0, 0, 0, 0.1)',
            },
            ticks: {
                color: '#666'
            }
        }
    }
};

/**
 * Color palettes
 */
const colorPalettes = {
    quality: {
        pass: '#198754',
        warning: '#ffc107',
        fail: '#dc3545',
        uncertain: '#6c757d'
    },
    ageGroups: {
        pediatric: '#0066cc',
        adolescent: '#e65100',
        young_adult: '#2e7d32',
        middle_age: '#7b1fa2',
        elderly: '#c2185b'
    },
    metrics: [
        '#0d6efd', '#198754', '#dc3545', '#ffc107', '#20c997',
        '#6f42c1', '#fd7e14', '#e91e63', '#795548', '#607d8b'
    ]
};

/**
 * Create quality distribution pie chart
 */
function createQualityDistributionChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const chartData = {
        labels: Object.keys(data).map(key => key.charAt(0).toUpperCase() + key.slice(1)),
        datasets: [{
            data: Object.values(data),
            backgroundColor: Object.keys(data).map(key => colorPalettes.quality[key]),
            borderWidth: 2,
            borderColor: '#fff'
        }]
    };

    const config = {
        type: 'doughnut',
        data: chartData,
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                legend: {
                    position: 'bottom',
                    labels: {
                        padding: 20,
                        usePointStyle: true
                    }
                },
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const percentage = ((context.parsed / total) * 100).toFixed(1);
                            return `${context.label}: ${context.parsed} (${percentage}%)`;
                        }
                    }
                }
            },
            cutout: '60%'
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create age group distribution bar chart
 */
function createAgeDistributionChart(canvasId, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const chartData = {
        labels: Object.keys(data).map(key => utils.getAgeGroupName(key)),
        datasets: [{
            label: 'Subjects',
            data: Object.values(data),
            backgroundColor: Object.keys(data).map(key => colorPalettes.ageGroups[key]),
            borderWidth: 1,
            borderColor: '#fff'
        }]
    };

    const config = {
        type: 'bar',
        data: chartData,
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                legend: {
                    display: false
                },
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(context) {
                            return `${context.label}: ${context.parsed.y} subjects`;
                        }
                    }
                }
            },
            scales: {
                ...chartDefaults.scales,
                y: {
                    ...chartDefaults.scales.y,
                    beginAtZero: true,
                    ticks: {
                        ...chartDefaults.scales.y.ticks,
                        stepSize: 1
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create metric box plot chart
 */
function createMetricBoxPlot(canvasId, metricData, metricName) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    // Process data for box plot visualization
    const processedData = processBoxPlotData(metricData);

    const chartData = {
        labels: processedData.labels,
        datasets: [{
            label: metricName,
            data: processedData.data,
            backgroundColor: 'rgba(13, 110, 253, 0.2)',
            borderColor: '#0d6efd',
            borderWidth: 2,
            pointBackgroundColor: '#0d6efd',
            pointBorderColor: '#fff',
            pointBorderWidth: 2,
            pointRadius: 4
        }]
    };

    const config = {
        type: 'line',
        data: chartData,
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(context) {
                            return `${metricName}: ${context.parsed.y.toFixed(3)}`;
                        }
                    }
                }
            },
            scales: {
                ...chartDefaults.scales,
                x: {
                    ...chartDefaults.scales.x,
                    title: {
                        display: true,
                        text: 'Age Group'
                    }
                },
                y: {
                    ...chartDefaults.scales.y,
                    title: {
                        display: true,
                        text: metricName
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create metric scatter plot
 */
function createMetricScatterPlot(canvasId, subjects, xMetric, yMetric) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const datasets = [];
    const qualityGroups = ['pass', 'warning', 'fail', 'uncertain'];

    qualityGroups.forEach(quality => {
        const filteredSubjects = subjects.filter(s => s.quality_assessment.overall_status === quality);
        const data = filteredSubjects.map(subject => ({
            x: subject.raw_metrics[xMetric] || 0,
            y: subject.raw_metrics[yMetric] || 0,
            subject: subject
        }));

        if (data.length > 0) {
            datasets.push({
                label: quality.charAt(0).toUpperCase() + quality.slice(1),
                data: data,
                backgroundColor: colorPalettes.quality[quality],
                borderColor: colorPalettes.quality[quality],
                pointRadius: 6,
                pointHoverRadius: 8
            });
        }
    });

    const config = {
        type: 'scatter',
        data: { datasets },
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        title: function(context) {
                            return context[0].raw.subject.subject_info.subject_id;
                        },
                        label: function(context) {
                            const point = context.raw;
                            return [
                                `${xMetric}: ${point.x.toFixed(3)}`,
                                `${yMetric}: ${point.y.toFixed(3)}`,
                                `Quality: ${point.subject.quality_assessment.overall_status}`
                            ];
                        }
                    }
                }
            },
            scales: {
                ...chartDefaults.scales,
                x: {
                    ...chartDefaults.scales.x,
                    title: {
                        display: true,
                        text: xMetric.toUpperCase()
                    }
                },
                y: {
                    ...chartDefaults.scales.y,
                    title: {
                        display: true,
                        text: yMetric.toUpperCase()
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create percentile radar chart
 */
function createPercentileRadarChart(canvasId, percentileData, subjectData) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const metrics = Object.keys(percentileData);
    const subjectPercentiles = metrics.map(metric => percentileData[metric]);
    const ageGroupAverage = metrics.map(() => 50); // 50th percentile as reference

    const chartData = {
        labels: metrics.map(m => m.toUpperCase()),
        datasets: [
            {
                label: 'Subject Percentiles',
                data: subjectPercentiles,
                backgroundColor: 'rgba(13, 110, 253, 0.2)',
                borderColor: '#0d6efd',
                borderWidth: 2,
                pointBackgroundColor: '#0d6efd',
                pointBorderColor: '#fff',
                pointBorderWidth: 2
            },
            {
                label: 'Age Group Average (50th percentile)',
                data: ageGroupAverage,
                backgroundColor: 'rgba(108, 117, 125, 0.1)',
                borderColor: '#6c757d',
                borderWidth: 1,
                borderDash: [5, 5],
                pointBackgroundColor: '#6c757d',
                pointBorderColor: '#fff',
                pointBorderWidth: 1
            }
        ]
    };

    const config = {
        type: 'radar',
        data: chartData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                },
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${context.parsed.r.toFixed(1)}th percentile`;
                        }
                    }
                }
            },
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: {
                        stepSize: 25,
                        callback: function(value) {
                            return value + 'th';
                        }
                    },
                    grid: {
                        color: 'rgba(0, 0, 0, 0.1)'
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create metric comparison bar chart
 */
function createMetricComparisonChart(canvasId, subjectMetrics, comparisonData, metricName) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const chartData = {
        labels: ['Subject', 'Age Group Mean', 'All Subjects Mean'],
        datasets: [{
            label: metricName,
            data: [
                subjectMetrics[metricName] || 0,
                comparisonData.ageGroupMean || 0,
                comparisonData.overallMean || 0
            ],
            backgroundColor: [
                '#0d6efd',
                'rgba(13, 110, 253, 0.6)',
                'rgba(13, 110, 253, 0.3)'
            ],
            borderColor: '#0d6efd',
            borderWidth: 1
        }]
    };

    const config = {
        type: 'bar',
        data: chartData,
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                legend: {
                    display: false
                },
                tooltip: {
                    ...chartDefaults.plugins.tooltip,
                    callbacks: {
                        label: function(context) {
                            return `${context.label}: ${context.parsed.y.toFixed(3)}`;
                        }
                    }
                }
            },
            scales: {
                ...chartDefaults.scales,
                y: {
                    ...chartDefaults.scales.y,
                    title: {
                        display: true,
                        text: metricName
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Create processing progress chart
 */
function createProgressChart(canvasId, progressData) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;

    const chartData = {
        labels: progressData.labels,
        datasets: [{
            label: 'Processing Progress',
            data: progressData.values,
            backgroundColor: 'rgba(13, 110, 253, 0.2)',
            borderColor: '#0d6efd',
            borderWidth: 2,
            fill: true,
            tension: 0.4
        }]
    };

    const config = {
        type: 'line',
        data: chartData,
        options: {
            ...chartDefaults,
            plugins: {
                ...chartDefaults.plugins,
                legend: {
                    display: false
                }
            },
            scales: {
                ...chartDefaults.scales,
                x: {
                    ...chartDefaults.scales.x,
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    ...chartDefaults.scales.y,
                    beginAtZero: true,
                    max: 100,
                    title: {
                        display: true,
                        text: 'Progress (%)'
                    }
                }
            }
        }
    };

    return new Chart(ctx, config);
}

/**
 * Process data for box plot visualization
 */
function processBoxPlotData(metricData) {
    const labels = [];
    const data = [];

    Object.keys(metricData).forEach(ageGroup => {
        const values = metricData[ageGroup];
        if (values && values.length > 0) {
            labels.push(utils.getAgeGroupName(ageGroup));
            
            // Calculate median for line chart representation
            const sorted = values.sort((a, b) => a - b);
            const median = sorted[Math.floor(sorted.length / 2)];
            data.push(median);
        }
    });

    return { labels, data };
}

/**
 * Update chart data
 */
function updateChart(chart, newData) {
    if (!chart) return;

    chart.data = newData;
    chart.update('active');
}

/**
 * Destroy chart safely
 */
function destroyChart(chart) {
    if (chart && typeof chart.destroy === 'function') {
        chart.destroy();
    }
}

/**
 * Resize chart
 */
function resizeChart(chart) {
    if (chart && typeof chart.resize === 'function') {
        chart.resize();
    }
}

/**
 * Export chart as image
 */
function exportChartAsImage(chart, filename = 'chart.png') {
    if (!chart) return;

    const url = chart.toBase64Image();
    const link = document.createElement('a');
    link.download = filename;
    link.href = url;
    link.click();
}

// Chart management object
const chartManager = {
    charts: new Map(),
    
    register(id, chart) {
        if (this.charts.has(id)) {
            this.destroy(id);
        }
        this.charts.set(id, chart);
    },
    
    get(id) {
        return this.charts.get(id);
    },
    
    update(id, newData) {
        const chart = this.charts.get(id);
        if (chart) {
            updateChart(chart, newData);
        }
    },
    
    destroy(id) {
        const chart = this.charts.get(id);
        if (chart) {
            destroyChart(chart);
            this.charts.delete(id);
        }
    },
    
    destroyAll() {
        this.charts.forEach((chart, id) => {
            destroyChart(chart);
        });
        this.charts.clear();
    },
    
    resize(id) {
        const chart = this.charts.get(id);
        if (chart) {
            resizeChart(chart);
        }
    },
    
    resizeAll() {
        this.charts.forEach(chart => {
            resizeChart(chart);
        });
    }
};

// Handle window resize
window.addEventListener('resize', utils.debounce(() => {
    chartManager.resizeAll();
}, 250));

// Export chart utilities
window.charts = {
    createQualityDistributionChart,
    createAgeDistributionChart,
    createMetricBoxPlot,
    createMetricScatterPlot,
    createPercentileRadarChart,
    createMetricComparisonChart,
    createProgressChart,
    updateChart,
    destroyChart,
    resizeChart,
    exportChartAsImage,
    chartManager,
    colorPalettes,
    chartDefaults
};