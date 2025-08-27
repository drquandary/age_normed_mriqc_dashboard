/**
 * Utility functions for Age-Normed MRIQC Dashboard
 */

/**
 * Show alert message
 */
function showAlert(message, type = 'info', duration = 5000) {
    const alertContainer = document.getElementById('alert-container');
    if (!alertContainer) return;

    const alertId = 'alert-' + Date.now();
    const alertHTML = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert" id="${alertId}">
            <i class="bi bi-${getAlertIcon(type)}"></i>
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;

    alertContainer.insertAdjacentHTML('beforeend', alertHTML);

    // Auto-dismiss after duration
    if (duration > 0) {
        setTimeout(() => {
            const alert = document.getElementById(alertId);
            if (alert) {
                const bsAlert = new bootstrap.Alert(alert);
                bsAlert.close();
            }
        }, duration);
    }
}

/**
 * Get Bootstrap icon for alert type
 */
function getAlertIcon(type) {
    const icons = {
        'success': 'check-circle',
        'danger': 'exclamation-triangle',
        'warning': 'exclamation-triangle',
        'info': 'info-circle',
        'primary': 'info-circle',
        'secondary': 'info-circle'
    };
    return icons[type] || 'info-circle';
}

/**
 * Show/hide loading overlay
 */
function showLoading(show = true, message = 'Processing...') {
    const overlay = document.getElementById('loading-overlay');
    if (!overlay) return;

    if (show) {
        overlay.querySelector('.loading-spinner div:last-child').textContent = message;
        overlay.classList.remove('d-none');
    } else {
        overlay.classList.add('d-none');
    }
}

/**
 * Format file size
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Format date/time
 */
function formatDateTime(dateString, options = {}) {
    const date = new Date(dateString);
    const defaultOptions = {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    };
    
    return date.toLocaleDateString('en-US', { ...defaultOptions, ...options });
}

/**
 * Format relative time (e.g., "2 hours ago")
 */
function formatRelativeTime(dateString) {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffSecs = Math.floor(diffMs / 1000);
    const diffMins = Math.floor(diffSecs / 60);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffSecs < 60) return 'Just now';
    if (diffMins < 60) return `${diffMins} minute${diffMins !== 1 ? 's' : ''} ago`;
    if (diffHours < 24) return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
    if (diffDays < 7) return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
    
    return formatDateTime(dateString);
}

/**
 * Format number with appropriate precision
 */
function formatNumber(value, decimals = 2) {
    if (value === null || value === undefined) return 'N/A';
    if (typeof value !== 'number') return value;
    
    return value.toFixed(decimals);
}

/**
 * Format percentage
 */
function formatPercentage(value, decimals = 1) {
    if (value === null || value === undefined) return 'N/A';
    return `${formatNumber(value, decimals)}%`;
}

/**
 * Get quality status badge HTML
 */
function getQualityStatusBadge(status) {
    const badges = {
        'pass': '<span class="badge bg-success">Pass</span>',
        'warning': '<span class="badge bg-warning text-dark">Warning</span>',
        'fail': '<span class="badge bg-danger">Fail</span>',
        'uncertain': '<span class="badge bg-secondary">Uncertain</span>'
    };
    return badges[status] || '<span class="badge bg-light text-dark">Unknown</span>';
}

/**
 * Get age group badge HTML
 */
function getAgeGroupBadge(ageGroup) {
    const badges = {
        'pediatric': '<span class="badge age-group-badge pediatric">Pediatric</span>',
        'adolescent': '<span class="badge age-group-badge adolescent">Adolescent</span>',
        'young_adult': '<span class="badge age-group-badge young_adult">Young Adult</span>',
        'middle_age': '<span class="badge age-group-badge middle_age">Middle Age</span>',
        'elderly': '<span class="badge age-group-badge elderly">Elderly</span>'
    };
    return badges[ageGroup] || '<span class="badge bg-light text-dark">Unknown</span>';
}

/**
 * Get age group display name
 */
function getAgeGroupName(ageGroup) {
    const names = {
        'pediatric': 'Pediatric (6-12)',
        'adolescent': 'Adolescent (13-17)',
        'young_adult': 'Young Adult (18-35)',
        'middle_age': 'Middle Age (36-65)',
        'elderly': 'Elderly (65+)'
    };
    return names[ageGroup] || 'Unknown';
}

/**
 * Debounce function
 */
function debounce(func, wait, immediate = false) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            timeout = null;
            if (!immediate) func(...args);
        };
        const callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func(...args);
    };
}

/**
 * Throttle function
 */
function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

/**
 * Deep clone object
 */
function deepClone(obj) {
    if (obj === null || typeof obj !== 'object') return obj;
    if (obj instanceof Date) return new Date(obj.getTime());
    if (obj instanceof Array) return obj.map(item => deepClone(item));
    if (typeof obj === 'object') {
        const clonedObj = {};
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                clonedObj[key] = deepClone(obj[key]);
            }
        }
        return clonedObj;
    }
}

/**
 * Generate unique ID
 */
function generateId(prefix = 'id') {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Validate email format
 */
function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

/**
 * Sanitize HTML to prevent XSS
 */
function sanitizeHTML(str) {
    const temp = document.createElement('div');
    temp.textContent = str;
    return temp.innerHTML;
}

/**
 * Download file from blob
 */
function downloadBlob(blob, filename) {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
}

/**
 * Copy text to clipboard
 */
async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        showAlert('Copied to clipboard', 'success', 2000);
        return true;
    } catch (err) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = text;
        document.body.appendChild(textArea);
        textArea.focus();
        textArea.select();
        try {
            document.execCommand('copy');
            showAlert('Copied to clipboard', 'success', 2000);
            return true;
        } catch (err) {
            showAlert('Failed to copy to clipboard', 'danger');
            return false;
        } finally {
            document.body.removeChild(textArea);
        }
    }
}

/**
 * Handle API errors with user-friendly messages
 */
function handleAPIError(error, context = '') {
    console.error('API Error:', error);
    
    let message = 'An unexpected error occurred';
    
    if (error instanceof APIError) {
        if (error.isNetworkError) {
            message = 'Network error. Please check your connection and try again.';
        } else if (error.status === 400) {
            message = error.message || 'Invalid request. Please check your input.';
        } else if (error.status === 401) {
            message = 'Authentication required. Please log in.';
        } else if (error.status === 403) {
            message = 'Access denied. You do not have permission for this action.';
        } else if (error.status === 404) {
            message = 'Resource not found.';
        } else if (error.status === 429) {
            message = 'Too many requests. Please wait and try again.';
        } else if (error.isServerError) {
            message = 'Server error. Please try again later.';
        } else {
            message = error.message || message;
        }
    } else if (error.message) {
        message = error.message;
    }
    
    if (context) {
        message = `${context}: ${message}`;
    }
    
    showAlert(message, 'danger');
}

/**
 * Local storage helpers
 */
const storage = {
    set(key, value) {
        try {
            localStorage.setItem(key, JSON.stringify(value));
            return true;
        } catch (e) {
            console.warn('Failed to save to localStorage:', e);
            return false;
        }
    },
    
    get(key, defaultValue = null) {
        try {
            const item = localStorage.getItem(key);
            return item ? JSON.parse(item) : defaultValue;
        } catch (e) {
            console.warn('Failed to read from localStorage:', e);
            return defaultValue;
        }
    },
    
    remove(key) {
        try {
            localStorage.removeItem(key);
            return true;
        } catch (e) {
            console.warn('Failed to remove from localStorage:', e);
            return false;
        }
    },
    
    clear() {
        try {
            localStorage.clear();
            return true;
        } catch (e) {
            console.warn('Failed to clear localStorage:', e);
            return false;
        }
    }
};

/**
 * URL parameter helpers
 */
const urlParams = {
    get(param) {
        const urlParams = new URLSearchParams(window.location.search);
        return urlParams.get(param);
    },
    
    set(param, value) {
        const url = new URL(window.location);
        url.searchParams.set(param, value);
        window.history.pushState({}, '', url);
    },
    
    remove(param) {
        const url = new URL(window.location);
        url.searchParams.delete(param);
        window.history.pushState({}, '', url);
    },
    
    getAll() {
        const urlParams = new URLSearchParams(window.location.search);
        const params = {};
        for (const [key, value] of urlParams) {
            params[key] = value;
        }
        return params;
    }
};

/**
 * Initialize tooltips and popovers
 */
function initializeBootstrapComponents() {
    // Initialize tooltips
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize popovers
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });
}

/**
 * Set active navigation item
 */
function setActiveNavItem() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('.navbar-nav .nav-link');
    
    navLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        }
    });
}

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', function() {
    initializeBootstrapComponents();
    setActiveNavItem();
});

// Export utilities for global use
window.utils = {
    showAlert,
    showLoading,
    formatFileSize,
    formatDateTime,
    formatRelativeTime,
    formatNumber,
    formatPercentage,
    getQualityStatusBadge,
    getAgeGroupBadge,
    getAgeGroupName,
    debounce,
    throttle,
    deepClone,
    generateId,
    isValidEmail,
    sanitizeHTML,
    downloadBlob,
    copyToClipboard,
    handleAPIError,
    storage,
    urlParams
};