/**
 * API communication module for Age-Normed MRIQC Dashboard
 */

class APIClient {
    constructor(baseURL = '/api') {
        this.baseURL = baseURL;
        this.defaultHeaders = {
            'Content-Type': 'application/json',
        };
    }

    /**
     * Make HTTP request with error handling
     */
    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const config = {
            headers: { ...this.defaultHeaders, ...options.headers },
            ...options
        };

        try {
            const response = await fetch(url, config);
            
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                throw new APIError(
                    errorData.message || `HTTP ${response.status}: ${response.statusText}`,
                    response.status,
                    errorData
                );
            }

            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return await response.json();
            } else {
                return await response.blob();
            }
        } catch (error) {
            if (error instanceof APIError) {
                throw error;
            }
            throw new APIError(`Network error: ${error.message}`, 0, { originalError: error });
        }
    }

    /**
     * GET request
     */
    async get(endpoint, params = {}) {
        const url = new URL(`${this.baseURL}${endpoint}`, window.location.origin);
        Object.keys(params).forEach(key => {
            if (params[key] !== null && params[key] !== undefined && params[key] !== '') {
                url.searchParams.append(key, params[key]);
            }
        });
        
        return this.request(url.pathname + url.search, { method: 'GET' });
    }

    /**
     * POST request
     */
    async post(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    /**
     * PUT request
     */
    async put(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    }

    /**
     * DELETE request
     */
    async delete(endpoint) {
        return this.request(endpoint, { method: 'DELETE' });
    }

    /**
     * Upload file
     */
    async uploadFile(endpoint, file, onProgress = null) {
        const formData = new FormData();
        formData.append('file', file);

        const config = {
            method: 'POST',
            body: formData,
            headers: {} // Let browser set Content-Type for FormData
        };

        // Add progress tracking if callback provided
        if (onProgress && typeof onProgress === 'function') {
            return new Promise((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                
                xhr.upload.addEventListener('progress', (event) => {
                    if (event.lengthComputable) {
                        const percentComplete = (event.loaded / event.total) * 100;
                        onProgress(percentComplete);
                    }
                });

                xhr.addEventListener('load', () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            const response = JSON.parse(xhr.responseText);
                            resolve(response);
                        } catch (e) {
                            resolve(xhr.responseText);
                        }
                    } else {
                        try {
                            const errorData = JSON.parse(xhr.responseText);
                            reject(new APIError(errorData.message || `HTTP ${xhr.status}`, xhr.status, errorData));
                        } catch (e) {
                            reject(new APIError(`HTTP ${xhr.status}: ${xhr.statusText}`, xhr.status));
                        }
                    }
                });

                xhr.addEventListener('error', () => {
                    reject(new APIError('Network error during file upload', 0));
                });

                xhr.open('POST', `${this.baseURL}${endpoint}`);
                xhr.send(formData);
            });
        }

        return this.request(endpoint, config);
    }

    // Specific API methods

    /**
     * Health check
     */
    async healthCheck() {
        return this.get('/health');
    }

    /**
     * Upload MRIQC file
     */
    async uploadMRIQCFile(file, onProgress = null) {
        return this.uploadFile('/upload', file, onProgress);
    }

    /**
     * Process uploaded file
     */
    async processFile(fileId, options = {}) {
        return this.post('/process', {
            file_id: fileId,
            apply_quality_assessment: options.applyQualityAssessment ?? true,
            custom_thresholds: options.customThresholds || null
        });
    }

    /**
     * Get batch processing status
     */
    async getBatchStatus(batchId) {
        return this.get(`/batch/${batchId}/status`);
    }

    /**
     * Get subjects list
     */
    async getSubjects(params = {}) {
        return this.get('/subjects', params);
    }

    /**
     * Advanced subject filtering
     */
    async filterSubjects(filterCriteria, sortCriteria = null, page = 1, pageSize = 50) {
        const data = {
            filter_criteria: filterCriteria,
            sort_criteria: sortCriteria,
            page: page,
            page_size: pageSize
        };
        return this.post('/subjects/filter', data);
    }

    /**
     * Get subject details
     */
    async getSubjectDetails(subjectId) {
        return this.get(`/subjects/${subjectId}`);
    }

    /**
     * Get dashboard summary
     */
    async getDashboardSummary(batchId = null) {
        const params = batchId ? { batch_id: batchId } : {};
        return this.get('/dashboard/summary', params);
    }

    /**
     * Export data
     */
    async exportData(format, options = {}) {
        const params = { format, ...options };
        return this.get('/export', params);
    }

    /**
     * Export CSV
     */
    async exportCSV(subjectIds = null, includeOptions = {}) {
        const data = {
            subject_ids: subjectIds,
            include_raw_metrics: includeOptions.includeRawMetrics ?? true,
            include_normalized: includeOptions.includeNormalized ?? true,
            include_quality_assessment: includeOptions.includeQualityAssessment ?? true
        };
        return this.post('/export/csv', data);
    }

    /**
     * Export PDF report
     */
    async exportPDF(subjectIds = null, reportType = 'individual') {
        const data = {
            subject_ids: subjectIds,
            report_type: reportType
        };
        return this.post('/export/pdf', data);
    }

    /**
     * Update subject quality status
     */
    async updateSubjectQuality(subjectId, qualityStatus, reason = '') {
        return this.put(`/subjects/${subjectId}/quality`, {
            quality_status: qualityStatus,
            reason: reason
        });
    }

    /**
     * Bulk update subjects quality status
     */
    async bulkUpdateQuality(subjectIds, qualityStatus, reason = '') {
        return this.post('/subjects/bulk-update', {
            subject_ids: subjectIds,
            quality_status: qualityStatus,
            reason: reason
        });
    }

    /**
     * Get quality thresholds configuration
     */
    async getQualityThresholds() {
        return this.get('/config/thresholds');
    }

    /**
     * Update quality thresholds configuration
     */
    async updateQualityThresholds(thresholds) {
        return this.put('/config/thresholds', thresholds);
    }

    /**
     * Get normative data
     */
    async getNormativeData(ageGroup = null, metric = null) {
        const params = {};
        if (ageGroup) params.age_group = ageGroup;
        if (metric) params.metric = metric;
        return this.get('/normative-data', params);
    }
}

/**
 * Custom API Error class
 */
class APIError extends Error {
    constructor(message, status = 0, data = {}) {
        super(message);
        this.name = 'APIError';
        this.status = status;
        this.data = data;
    }

    get isNetworkError() {
        return this.status === 0;
    }

    get isClientError() {
        return this.status >= 400 && this.status < 500;
    }

    get isServerError() {
        return this.status >= 500;
    }
}

// Global API client instance
const api = new APIClient();

// Export for use in other modules
window.APIClient = APIClient;
window.APIError = APIError;
window.api = api;