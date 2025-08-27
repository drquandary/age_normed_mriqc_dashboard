/**
 * WebSocket client for real-time updates in Age-Normed MRIQC Dashboard
 */

class WebSocketClient {
    constructor() {
        this.socket = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // Start with 1 second
        this.maxReconnectDelay = 30000; // Max 30 seconds
        this.isConnecting = false;
        this.isManualClose = false;
        this.subscriptions = new Map();
        this.messageHandlers = new Map();
        
        // Bind methods
        this.connect = this.connect.bind(this);
        this.disconnect = this.disconnect.bind(this);
        this.onOpen = this.onOpen.bind(this);
        this.onMessage = this.onMessage.bind(this);
        this.onClose = this.onClose.bind(this);
        this.onError = this.onError.bind(this);
    }

    /**
     * Connect to WebSocket server
     */
    connect(batchId = null) {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            return Promise.resolve();
        }

        if (this.isConnecting) {
            return Promise.resolve();
        }

        this.isConnecting = true;
        this.isManualClose = false;

        return new Promise((resolve, reject) => {
            try {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const host = window.location.host;
                let wsUrl = `${protocol}//${host}/ws`;
                
                if (batchId) {
                    wsUrl += `?batch_id=${batchId}`;
                }

                this.socket = new WebSocket(wsUrl);
                
                this.socket.onopen = (event) => {
                    this.onOpen(event);
                    resolve();
                };
                
                this.socket.onmessage = this.onMessage;
                this.socket.onclose = this.onClose;
                this.socket.onerror = (event) => {
                    this.onError(event);
                    reject(new Error('WebSocket connection failed'));
                };

            } catch (error) {
                this.isConnecting = false;
                reject(error);
            }
        });
    }

    /**
     * Disconnect from WebSocket server
     */
    disconnect() {
        this.isManualClose = true;
        this.reconnectAttempts = 0;
        
        if (this.socket) {
            this.socket.close(1000, 'Manual disconnect');
            this.socket = null;
        }
    }

    /**
     * Handle WebSocket open event
     */
    onOpen(event) {
        console.log('WebSocket connected');
        this.isConnecting = false;
        this.reconnectAttempts = 0;
        this.reconnectDelay = 1000;
        
        // Emit connection event
        this.emit('connected', { event });
        
        // Show connection status
        this.updateConnectionStatus(true);
    }

    /**
     * Handle WebSocket message event
     */
    onMessage(event) {
        try {
            const data = JSON.parse(event.data);
            console.log('WebSocket message received:', data);
            
            // Handle different message types
            switch (data.type) {
                case 'batch_status_update':
                    this.handleBatchStatusUpdate(data);
                    break;
                case 'batch_progress_update':
                    this.handleBatchProgressUpdate(data);
                    break;
                case 'batch_completed':
                    this.handleBatchCompleted(data);
                    break;
                case 'batch_failed':
                    this.handleBatchFailed(data);
                    break;
                case 'processing_error':
                    this.handleProcessingError(data);
                    break;
                case 'dashboard_update':
                    this.handleDashboardUpdate(data);
                    break;
                default:
                    console.log('Unknown message type:', data.type);
            }
            
            // Emit generic message event
            this.emit('message', data);
            
        } catch (error) {
            console.error('Error parsing WebSocket message:', error);
        }
    }

    /**
     * Handle WebSocket close event
     */
    onClose(event) {
        console.log('WebSocket disconnected:', event.code, event.reason);
        this.isConnecting = false;
        this.socket = null;
        
        // Update connection status
        this.updateConnectionStatus(false);
        
        // Emit disconnection event
        this.emit('disconnected', { event });
        
        // Attempt reconnection if not manual close
        if (!this.isManualClose && this.reconnectAttempts < this.maxReconnectAttempts) {
            this.scheduleReconnect();
        } else if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            console.error('Max reconnection attempts reached');
            utils.showAlert('Connection lost. Please refresh the page.', 'warning');
        }
    }

    /**
     * Handle WebSocket error event
     */
    onError(event) {
        console.error('WebSocket error:', event);
        this.emit('error', { event });
    }

    /**
     * Schedule reconnection attempt
     */
    scheduleReconnect() {
        this.reconnectAttempts++;
        
        console.log(`Scheduling reconnection attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts} in ${this.reconnectDelay}ms`);
        
        setTimeout(() => {
            if (!this.isManualClose) {
                this.connect().catch(error => {
                    console.error('Reconnection failed:', error);
                });
            }
        }, this.reconnectDelay);
        
        // Exponential backoff with jitter
        this.reconnectDelay = Math.min(
            this.reconnectDelay * 2 + Math.random() * 1000,
            this.maxReconnectDelay
        );
    }

    /**
     * Send message to server
     */
    send(message) {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify(message));
            return true;
        } else {
            console.warn('WebSocket not connected, cannot send message:', message);
            return false;
        }
    }

    /**
     * Subscribe to specific events
     */
    on(eventType, handler) {
        if (!this.messageHandlers.has(eventType)) {
            this.messageHandlers.set(eventType, []);
        }
        this.messageHandlers.get(eventType).push(handler);
    }

    /**
     * Unsubscribe from events
     */
    off(eventType, handler) {
        if (this.messageHandlers.has(eventType)) {
            const handlers = this.messageHandlers.get(eventType);
            const index = handlers.indexOf(handler);
            if (index > -1) {
                handlers.splice(index, 1);
            }
        }
    }

    /**
     * Emit event to subscribers
     */
    emit(eventType, data) {
        if (this.messageHandlers.has(eventType)) {
            this.messageHandlers.get(eventType).forEach(handler => {
                try {
                    handler(data);
                } catch (error) {
                    console.error(`Error in event handler for ${eventType}:`, error);
                }
            });
        }
    }

    /**
     * Handle batch status update
     */
    handleBatchStatusUpdate(data) {
        console.log('Batch status update:', data);
        this.emit('batchStatusUpdate', data);
        
        // Update UI elements
        this.updateBatchStatus(data);
    }

    /**
     * Handle batch progress update
     */
    handleBatchProgressUpdate(data) {
        console.log('Batch progress update:', data);
        this.emit('batchProgressUpdate', data);
        
        // Update progress indicators
        this.updateBatchProgress(data);
    }

    /**
     * Handle batch completion
     */
    handleBatchCompleted(data) {
        console.log('Batch completed:', data);
        this.emit('batchCompleted', data);
        
        // Show completion notification
        utils.showAlert(
            `Processing completed! ${data.subjects_processed} subjects processed.`,
            'success'
        );
        
        // Update UI
        this.updateBatchCompletion(data);
    }

    /**
     * Handle batch failure
     */
    handleBatchFailed(data) {
        console.log('Batch failed:', data);
        this.emit('batchFailed', data);
        
        // Show error notification
        utils.showAlert(
            `Processing failed: ${data.error_message}`,
            'danger'
        );
    }

    /**
     * Handle processing error
     */
    handleProcessingError(data) {
        console.log('Processing error:', data);
        this.emit('processingError', data);
        
        // Show error notification
        utils.showAlert(
            `Error processing ${data.subject_id}: ${data.error.message}`,
            'warning',
            8000
        );
    }

    /**
     * Handle dashboard update
     */
    handleDashboardUpdate(data) {
        console.log('Dashboard update:', data);
        this.emit('dashboardUpdate', data);
        
        // Refresh dashboard data if on dashboard page
        if (window.location.pathname === '/' || window.location.pathname === '/dashboard') {
            // Trigger dashboard refresh
            if (typeof refreshDashboard === 'function') {
                refreshDashboard();
            }
        }
    }

    /**
     * Update connection status indicator
     */
    updateConnectionStatus(connected) {
        const indicator = document.getElementById('connection-status');
        if (indicator) {
            if (connected) {
                indicator.className = 'badge bg-success';
                indicator.textContent = 'Connected';
            } else {
                indicator.className = 'badge bg-danger';
                indicator.textContent = 'Disconnected';
            }
        }
    }

    /**
     * Update batch status in UI
     */
    updateBatchStatus(data) {
        const statusElement = document.getElementById('current-status');
        if (statusElement) {
            statusElement.innerHTML = `
                <i class="bi bi-info-circle"></i>
                ${data.status === 'processing' ? 'Processing...' : data.status}
            `;
            statusElement.className = `alert alert-${data.status === 'processing' ? 'info' : 'secondary'}`;
        }
    }

    /**
     * Update batch progress in UI
     */
    updateBatchProgress(data) {
        const progressBar = document.getElementById('progress-bar');
        const progressPercentage = document.getElementById('progress-percentage');
        const processedCount = document.getElementById('processed-count');
        const totalCount = document.getElementById('total-count');

        if (progressBar && data.progress) {
            const percent = data.progress.progress_percent || 0;
            progressBar.style.width = `${percent}%`;
            progressBar.setAttribute('aria-valuenow', percent);
        }

        if (progressPercentage && data.progress) {
            progressPercentage.textContent = `${Math.round(data.progress.progress_percent || 0)}%`;
        }

        if (processedCount && data.progress) {
            processedCount.textContent = data.progress.completed || 0;
        }

        if (totalCount && data.progress) {
            totalCount.textContent = data.progress.total || 0;
        }

        // Update processing log
        this.updateProcessingLog(`Processing subject: ${data.current_subject || 'Unknown'}`);
    }

    /**
     * Update batch completion in UI
     */
    updateBatchCompletion(data) {
        const statusElement = document.getElementById('current-status');
        if (statusElement) {
            statusElement.innerHTML = `
                <i class="bi bi-check-circle"></i>
                Processing completed successfully!
            `;
            statusElement.className = 'alert alert-success';
        }

        // Enable result viewing buttons
        const viewResultsBtn = document.getElementById('view-results');
        const goToDashboardBtn = document.getElementById('go-to-dashboard');
        
        if (viewResultsBtn) viewResultsBtn.disabled = false;
        if (goToDashboardBtn) goToDashboardBtn.disabled = false;
    }

    /**
     * Update processing log
     */
    updateProcessingLog(message) {
        const logContent = document.getElementById('log-content');
        if (logContent) {
            const timestamp = new Date().toLocaleTimeString();
            const logEntry = document.createElement('div');
            logEntry.innerHTML = `<span class="text-muted">[${timestamp}]</span> ${message}`;
            logContent.appendChild(logEntry);
            
            // Auto-scroll to bottom
            logContent.scrollTop = logContent.scrollHeight;
            
            // Limit log entries to prevent memory issues
            const entries = logContent.children;
            if (entries.length > 100) {
                logContent.removeChild(entries[0]);
            }
        }
    }

    /**
     * Get connection state
     */
    get isConnected() {
        return this.socket && this.socket.readyState === WebSocket.OPEN;
    }

    /**
     * Get connection state string
     */
    get connectionState() {
        if (!this.socket) return 'disconnected';
        
        switch (this.socket.readyState) {
            case WebSocket.CONNECTING: return 'connecting';
            case WebSocket.OPEN: return 'connected';
            case WebSocket.CLOSING: return 'closing';
            case WebSocket.CLOSED: return 'disconnected';
            default: return 'unknown';
        }
    }
}

// Global WebSocket client instance
const wsClient = new WebSocketClient();

// Auto-connect on page load if needed
document.addEventListener('DOMContentLoaded', function() {
    // Check if we're on a page that needs WebSocket connection
    const needsWebSocket = [
        '/',
        '/dashboard',
        '/upload',
        '/subjects'
    ].includes(window.location.pathname);

    if (needsWebSocket) {
        // Try to connect with batch ID from URL if available
        const batchId = utils.urlParams.get('batch_id');
        wsClient.connect(batchId).catch(error => {
            console.warn('Failed to establish WebSocket connection:', error);
        });
    }
});

// Disconnect on page unload
window.addEventListener('beforeunload', function() {
    wsClient.disconnect();
});

// Export WebSocket client
window.wsClient = wsClient;
window.WebSocketClient = WebSocketClient;