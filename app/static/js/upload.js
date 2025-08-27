/**
 * Upload page functionality for Age-Normed MRIQC Dashboard
 */

// Upload state
let selectedFile = null;
let currentBatchId = null;
let uploadHistory = [];

/**
 * Initialize upload page
 */
document.addEventListener('DOMContentLoaded', function() {
    setupFileUpload();
    setupEventListeners();
    setupWebSocketHandlers();
    loadUploadHistory();
});

/**
 * Setup file upload functionality
 */
function setupFileUpload() {
    const uploadArea = document.getElementById('upload-area');
    const fileInput = document.getElementById('file-input');
    
    if (!uploadArea || !fileInput) return;
    
    // Drag and drop handlers
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleDrop);
    uploadArea.addEventListener('click', () => fileInput.click());
    
    // File input change handler
    fileInput.addEventListener('change', handleFileSelect);
}

/**
 * Handle drag over event
 */
function handleDragOver(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.add('dragover');
}

/**
 * Handle drag leave event
 */
function handleDragLeave(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragover');
}

/**
 * Handle drop event
 */
function handleDrop(event) {
    event.preventDefault();
    event.stopPropagation();
    event.currentTarget.classList.remove('dragover');
    
    const files = event.dataTransfer.files;
    if (files.length > 0) {
        handleFileSelection(files[0]);
    }
}

/**
 * Handle file select from input
 */
function handleFileSelect(event) {
    const files = event.target.files;
    if (files.length > 0) {
        handleFileSelection(files[0]);
    }
}

/**
 * Handle file selection
 */
function handleFileSelection(file) {
    // Validate file
    if (!validateFile(file)) {
        return;
    }
    
    selectedFile = file;
    displayFileInfo(file);
    showProcessingOptions();
    enableActionButtons();
}

/**
 * Validate selected file
 */
function validateFile(file) {
    // Check file type
    if (!file.name.toLowerCase().endsWith('.csv')) {
        utils.showAlert('Please select a CSV file.', 'danger');
        return false;
    }
    
    // Check file size (50MB limit)
    const maxSize = 50 * 1024 * 1024; // 50MB
    if (file.size > maxSize) {
        utils.showAlert('File size exceeds 50MB limit.', 'danger');
        return false;
    }
    
    return true;
}

/**
 * Display file information
 */
function displayFileInfo(file) {
    const fileInfoDiv = document.getElementById('file-info');
    const fileName = document.getElementById('file-name');
    const fileSize = document.getElementById('file-size');
    const fileStatus = document.getElementById('file-status');
    
    if (fileInfoDiv) fileInfoDiv.classList.remove('d-none');
    if (fileName) fileName.textContent = file.name;
    if (fileSize) fileSize.textContent = utils.formatFileSize(file.size);
    if (fileStatus) {
        fileStatus.textContent = 'Ready';
        fileStatus.className = 'badge bg-success';
    }
    
    // Validate file format and get subject count
    validateFileFormat(file);
}

/**
 * Validate file format and get subject count
 */
async function validateFileFormat(file) {
    const fileStatus = document.getElementById('file-status');
    const subjectsCount = document.getElementById('subjects-count');
    
    try {
        if (fileStatus) {
            fileStatus.textContent = 'Validating...';
            fileStatus.className = 'badge bg-warning';
        }
        
        // Upload file for validation
        const response = await api.uploadMRIQCFile(file);
        
        if (fileStatus) {
            fileStatus.textContent = 'Valid';
            fileStatus.className = 'badge bg-success';
        }
        
        if (subjectsCount) {
            subjectsCount.textContent = response.subjects_count || 'Unknown';
        }
        
        // Store file ID for processing
        selectedFile.fileId = response.file_id;
        
    } catch (error) {
        if (fileStatus) {
            fileStatus.textContent = 'Invalid';
            fileStatus.className = 'badge bg-danger';
        }
        
        if (subjectsCount) {
            subjectsCount.textContent = 'N/A';
        }
        
        // Show validation modal with error details
        showValidationModal(error);
        
        // Disable processing
        disableActionButtons();
    }
}

/**
 * Show validation modal with error details
 */
function showValidationModal(error) {
    const modal = new bootstrap.Modal(document.getElementById('validation-modal'));
    const validationContent = document.getElementById('validation-content');
    
    if (validationContent) {
        validationContent.innerHTML = `
            <div class="alert alert-danger">
                <h6><i class="bi bi-exclamation-triangle"></i> File Validation Failed</h6>
                <p class="mb-2">${error.message}</p>
                ${error.data && error.data.details ? `
                    <hr>
                    <small class="text-muted">
                        <strong>Details:</strong><br>
                        ${Array.isArray(error.data.details) ? error.data.details.join('<br>') : error.data.details}
                    </small>
                ` : ''}
            </div>
            <div class="alert alert-info">
                <h6><i class="bi bi-info-circle"></i> Requirements</h6>
                <ul class="mb-0">
                    <li>File must be in CSV format</li>
                    <li>Must contain standard MRIQC columns</li>
                    <li>Subject IDs must be present</li>
                    <li>Numeric metrics must be valid</li>
                </ul>
            </div>
        `;
    }
    
    modal.show();
}

/**
 * Show processing options
 */
function showProcessingOptions() {
    const optionsDiv = document.getElementById('processing-options');
    if (optionsDiv) {
        optionsDiv.classList.remove('d-none');
    }
}

/**
 * Enable action buttons
 */
function enableActionButtons() {
    const clearBtn = document.getElementById('clear-file');
    const processBtn = document.getElementById('process-file');
    
    if (clearBtn) clearBtn.disabled = false;
    if (processBtn) processBtn.disabled = false;
}

/**
 * Disable action buttons
 */
function disableActionButtons() {
    const clearBtn = document.getElementById('clear-file');
    const processBtn = document.getElementById('process-file');
    
    if (clearBtn) clearBtn.disabled = true;
    if (processBtn) processBtn.disabled = true;
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Clear file button
    const clearBtn = document.getElementById('clear-file');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearFile);
    }
    
    // Process file button
    const processBtn = document.getElementById('process-file');
    if (processBtn) {
        processBtn.addEventListener('click', processFile);
    }
    
    // Cancel processing button
    const cancelBtn = document.getElementById('cancel-processing');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', cancelProcessing);
    }
    
    // View results button
    const viewResultsBtn = document.getElementById('view-results');
    if (viewResultsBtn) {
        viewResultsBtn.addEventListener('click', viewResults);
    }
    
    // Go to dashboard button
    const dashboardBtn = document.getElementById('go-to-dashboard');
    if (dashboardBtn) {
        dashboardBtn.addEventListener('click', goToDashboard);
    }
}

/**
 * Setup WebSocket event handlers
 */
function setupWebSocketHandlers() {
    // Listen for batch progress updates
    wsClient.on('batchProgressUpdate', (data) => {
        if (data.batch_id === currentBatchId) {
            updateProcessingProgress(data);
        }
    });
    
    // Listen for batch completion
    wsClient.on('batchCompleted', (data) => {
        if (data.batch_id === currentBatchId) {
            handleProcessingCompletion(data);
        }
    });
    
    // Listen for batch failure
    wsClient.on('batchFailed', (data) => {
        if (data.batch_id === currentBatchId) {
            handleProcessingFailure(data);
        }
    });
    
    // Listen for processing errors
    wsClient.on('processingError', (data) => {
        if (data.batch_id === currentBatchId) {
            handleProcessingError(data);
        }
    });
}

/**
 * Clear selected file
 */
function clearFile() {
    selectedFile = null;
    currentBatchId = null;
    
    // Reset file input
    const fileInput = document.getElementById('file-input');
    if (fileInput) fileInput.value = '';
    
    // Hide file info and options
    const fileInfoDiv = document.getElementById('file-info');
    const optionsDiv = document.getElementById('processing-options');
    const progressDiv = document.getElementById('processing-progress');
    
    if (fileInfoDiv) fileInfoDiv.classList.add('d-none');
    if (optionsDiv) optionsDiv.classList.add('d-none');
    if (progressDiv) progressDiv.classList.add('d-none');
    
    // Disable buttons
    disableActionButtons();
    
    // Reset upload area
    const uploadArea = document.getElementById('upload-area');
    if (uploadArea) uploadArea.classList.remove('dragover');
}

/**
 * Process selected file
 */
async function processFile() {
    if (!selectedFile || !selectedFile.fileId) {
        utils.showAlert('No file selected or file not validated.', 'danger');
        return;
    }
    
    try {
        // Get processing options
        const applyQualityAssessment = document.getElementById('apply-quality-assessment')?.checked ?? true;
        const useAgeNormalization = document.getElementById('use-age-normalization')?.checked ?? true;
        
        // Start processing
        showLoading(true, 'Starting file processing...');
        
        const response = await api.processFile(selectedFile.fileId, {
            applyQualityAssessment,
            useAgeNormalization
        });
        
        currentBatchId = response.batch_id;
        
        // Show processing progress
        showProcessingProgress();
        
        // Connect WebSocket for real-time updates
        if (!wsClient.isConnected) {
            await wsClient.connect(currentBatchId);
        }
        
        // Add to upload history
        addToUploadHistory({
            filename: selectedFile.name,
            subjects: response.subjects_processed,
            status: 'processing',
            timestamp: new Date().toISOString(),
            batchId: currentBatchId
        });
        
        utils.showAlert('Processing started successfully!', 'success');
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to start processing');
    } finally {
        showLoading(false);
    }
}

/**
 * Show processing progress section
 */
function showProcessingProgress() {
    const progressDiv = document.getElementById('processing-progress');
    if (progressDiv) {
        progressDiv.classList.remove('d-none');
    }
    
    // Initialize progress
    updateProcessingProgress({
        progress: { completed: 0, total: 0, progress_percent: 0 }
    });
    
    // Enable cancel button
    const cancelBtn = document.getElementById('cancel-processing');
    if (cancelBtn) cancelBtn.disabled = false;
}

/**
 * Update processing progress
 */
function updateProcessingProgress(data) {
    const progressBar = document.getElementById('progress-bar');
    const progressPercentage = document.getElementById('progress-percentage');
    const processedCount = document.getElementById('processed-count');
    const totalCount = document.getElementById('total-count');
    
    if (data.progress) {
        const percent = Math.round(data.progress.progress_percent || 0);
        
        if (progressBar) {
            progressBar.style.width = `${percent}%`;
            progressBar.setAttribute('aria-valuenow', percent);
        }
        
        if (progressPercentage) {
            progressPercentage.textContent = `${percent}%`;
        }
        
        if (processedCount) {
            processedCount.textContent = data.progress.completed || 0;
        }
        
        if (totalCount) {
            totalCount.textContent = data.progress.total || 0;
        }
    }
    
    // Update current status
    if (data.current_subject) {
        updateProcessingLog(`Processing: ${data.current_subject}`);
    }
}

/**
 * Handle processing completion
 */
function handleProcessingCompletion(data) {
    // Update progress to 100%
    updateProcessingProgress({
        progress: { completed: data.subjects_processed, total: data.subjects_processed, progress_percent: 100 }
    });
    
    // Update status
    const statusElement = document.getElementById('current-status');
    if (statusElement) {
        statusElement.innerHTML = `
            <i class="bi bi-check-circle"></i>
            Processing completed successfully! ${data.subjects_processed} subjects processed.
        `;
        statusElement.className = 'alert alert-success';
    }
    
    // Enable result buttons
    const viewResultsBtn = document.getElementById('view-results');
    const dashboardBtn = document.getElementById('go-to-dashboard');
    const cancelBtn = document.getElementById('cancel-processing');
    
    if (viewResultsBtn) viewResultsBtn.disabled = false;
    if (dashboardBtn) dashboardBtn.disabled = false;
    if (cancelBtn) cancelBtn.disabled = true;
    
    // Update upload history
    updateUploadHistoryStatus(currentBatchId, 'completed');
    
    updateProcessingLog('Processing completed successfully!');
}

/**
 * Handle processing failure
 */
function handleProcessingFailure(data) {
    const statusElement = document.getElementById('current-status');
    if (statusElement) {
        statusElement.innerHTML = `
            <i class="bi bi-x-circle"></i>
            Processing failed: ${data.error_message}
        `;
        statusElement.className = 'alert alert-danger';
    }
    
    // Disable cancel button
    const cancelBtn = document.getElementById('cancel-processing');
    if (cancelBtn) cancelBtn.disabled = true;
    
    // Update upload history
    updateUploadHistoryStatus(currentBatchId, 'failed');
    
    updateProcessingLog(`Processing failed: ${data.error_message}`);
}

/**
 * Handle processing error
 */
function handleProcessingError(data) {
    const errorCount = document.getElementById('error-count');
    if (errorCount) {
        const currentErrors = parseInt(errorCount.textContent) || 0;
        errorCount.textContent = currentErrors + 1;
    }
    
    updateProcessingLog(`Error: ${data.error.message} (Subject: ${data.subject_id})`);
}

/**
 * Update processing log
 */
function updateProcessingLog(message) {
    const logContent = document.getElementById('log-content');
    if (logContent) {
        const timestamp = new Date().toLocaleTimeString();
        const logEntry = document.createElement('div');
        logEntry.innerHTML = `<span class="text-muted">[${timestamp}]</span> ${message}`;
        logContent.appendChild(logEntry);
        
        // Auto-scroll to bottom
        logContent.scrollTop = logContent.scrollHeight;
        
        // Limit log entries
        const entries = logContent.children;
        if (entries.length > 100) {
            logContent.removeChild(entries[0]);
        }
    }
}

/**
 * Toggle processing log visibility
 */
function toggleProcessingLog() {
    const logDiv = document.getElementById('processing-log');
    const toggleIcon = document.getElementById('log-toggle-icon');
    const toggleText = document.getElementById('log-toggle-text');
    
    if (logDiv && toggleIcon && toggleText) {
        if (logDiv.classList.contains('d-none')) {
            logDiv.classList.remove('d-none');
            toggleIcon.className = 'bi bi-chevron-up';
            toggleText.textContent = 'Hide Details';
        } else {
            logDiv.classList.add('d-none');
            toggleIcon.className = 'bi bi-chevron-down';
            toggleText.textContent = 'Show Details';
        }
    }
}

/**
 * Cancel processing
 */
function cancelProcessing() {
    if (confirm('Are you sure you want to cancel processing?')) {
        // In a real implementation, this would send a cancel request to the server
        utils.showAlert('Processing cancellation requested.', 'info');
        
        const cancelBtn = document.getElementById('cancel-processing');
        if (cancelBtn) cancelBtn.disabled = true;
    }
}

/**
 * View processing results
 */
function viewResults() {
    if (currentBatchId) {
        window.location.href = `/subjects?batch_id=${currentBatchId}`;
    } else {
        window.location.href = '/subjects';
    }
}

/**
 * Go to dashboard
 */
function goToDashboard() {
    window.location.href = '/';
}

/**
 * Load upload history
 */
function loadUploadHistory() {
    // Load from localStorage
    uploadHistory = utils.storage.get('upload_history', []);
    renderUploadHistory();
}

/**
 * Add to upload history
 */
function addToUploadHistory(entry) {
    uploadHistory.unshift(entry);
    
    // Keep only last 10 entries
    if (uploadHistory.length > 10) {
        uploadHistory = uploadHistory.slice(0, 10);
    }
    
    // Save to localStorage
    utils.storage.set('upload_history', uploadHistory);
    
    renderUploadHistory();
}

/**
 * Update upload history status
 */
function updateUploadHistoryStatus(batchId, status) {
    const entry = uploadHistory.find(h => h.batchId === batchId);
    if (entry) {
        entry.status = status;
        utils.storage.set('upload_history', uploadHistory);
        renderUploadHistory();
    }
}

/**
 * Render upload history table
 */
function renderUploadHistory() {
    const tbody = document.getElementById('upload-history-tbody');
    if (!tbody) return;
    
    if (uploadHistory.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-muted py-3">
                    <i class="bi bi-inbox"></i>
                    No recent uploads
                </td>
            </tr>
        `;
        return;
    }
    
    const rowsHTML = uploadHistory.map(entry => `
        <tr>
            <td>
                <span class="text-truncate" style="max-width: 200px;" title="${entry.filename}">
                    ${entry.filename}
                </span>
            </td>
            <td>${entry.subjects || 'N/A'}</td>
            <td>
                <span class="badge bg-${getStatusColor(entry.status)}">
                    ${entry.status.charAt(0).toUpperCase() + entry.status.slice(1)}
                </span>
            </td>
            <td>
                <small class="text-muted">
                    ${utils.formatRelativeTime(entry.timestamp)}
                </small>
            </td>
            <td>
                <div class="btn-group btn-group-sm">
                    ${entry.status === 'completed' ? `
                        <button class="btn btn-outline-primary btn-sm" onclick="viewBatchResults('${entry.batchId}')">
                            <i class="bi bi-eye"></i>
                        </button>
                    ` : ''}
                    <button class="btn btn-outline-secondary btn-sm" onclick="removeBatchFromHistory('${entry.batchId}')">
                        <i class="bi bi-trash"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = rowsHTML;
}

/**
 * Get status color for badge
 */
function getStatusColor(status) {
    const colors = {
        'processing': 'primary',
        'completed': 'success',
        'failed': 'danger',
        'cancelled': 'secondary'
    };
    return colors[status] || 'secondary';
}

/**
 * View batch results
 */
function viewBatchResults(batchId) {
    window.location.href = `/subjects?batch_id=${batchId}`;
}

/**
 * Remove batch from history
 */
function removeBatchFromHistory(batchId) {
    uploadHistory = uploadHistory.filter(h => h.batchId !== batchId);
    utils.storage.set('upload_history', uploadHistory);
    renderUploadHistory();
}

// Export functions for global use
window.toggleProcessingLog = toggleProcessingLog;
window.viewBatchResults = viewBatchResults;
window.removeBatchFromHistory = removeBatchFromHistory;