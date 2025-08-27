/**
 * Subjects page functionality for Age-Normed MRIQC Dashboard
 */

// Subjects page state
let currentSubjects = [];
let currentFilters = {};
let currentSort = { sort_by: 'processing_timestamp', sort_order: 'desc' };
let currentPage = 1;
let pageSize = 50;
let totalSubjects = 0;
let selectedSubjects = new Set();

/**
 * Initialize subjects page
 */
document.addEventListener('DOMContentLoaded', function() {
    initializeSubjectsPage();
    setupEventListeners();
    setupWebSocketHandlers();
});

/**
 * Initialize subjects page
 */
async function initializeSubjectsPage() {
    try {
        showLoading(true, 'Loading subjects...');
        
        // Load initial filters from URL
        loadFiltersFromURL();
        
        // Load subjects
        await loadSubjects();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to load subjects');
    } finally {
        showLoading(false);
    }
}

/**
 * Load filters from URL parameters
 */
function loadFiltersFromURL() {
    const urlParams = utils.urlParams.getAll();
    
    // Apply URL filters to form elements
    if (urlParams.batch_id) {
        currentFilters.batch_ids = [urlParams.batch_id];
    }
    
    if (urlParams.quality_status) {
        const qualityFilter = document.getElementById('quality-filter');
        if (qualityFilter) qualityFilter.value = urlParams.quality_status;
        currentFilters.quality_status = [urlParams.quality_status];
    }
    
    if (urlParams.age_group) {
        const ageGroupFilter = document.getElementById('age-group-filter');
        if (ageGroupFilter) ageGroupFilter.value = urlParams.age_group;
        currentFilters.age_group = [urlParams.age_group];
    }
    
    if (urlParams.scan_type) {
        const scanTypeFilter = document.getElementById('scan-type-filter');
        if (scanTypeFilter) scanTypeFilter.value = urlParams.scan_type;
        currentFilters.scan_type = [urlParams.scan_type];
    }
    
    if (urlParams.search) {
        const searchInput = document.getElementById('search-input');
        if (searchInput) searchInput.value = urlParams.search;
        currentFilters.search_text = urlParams.search;
    }
    
    if (urlParams.page) {
        currentPage = parseInt(urlParams.page) || 1;
    }
    
    if (urlParams.page_size) {
        pageSize = parseInt(urlParams.page_size) || 50;
        const pageSizeSelect = document.getElementById('page-size-select');
        if (pageSizeSelect) pageSizeSelect.value = pageSize.toString();
    }
}

/**
 * Load subjects from API
 */
async function loadSubjects() {
    try {
        let response;
        
        if (Object.keys(currentFilters).length > 0) {
            // Use advanced filtering
            response = await api.filterSubjects(
                currentFilters,
                currentSort,
                currentPage,
                pageSize
            );
        } else {
            // Use simple filtering
            const params = {
                ...currentSort,
                page: currentPage,
                page_size: pageSize
            };
            response = await api.getSubjects(params);
        }
        
        currentSubjects = response.subjects || [];
        totalSubjects = response.total_count || 0;
        
        renderSubjectsTable();
        updatePagination();
        updateSubjectCount();
        updateFilterSummary(response.filters_applied || {});
        
    } catch (error) {
        console.error('Error loading subjects:', error);
        throw error;
    }
}

/**
 * Render subjects table
 */
function renderSubjectsTable() {
    const tbody = document.getElementById('subjects-tbody');
    if (!tbody) return;
    
    if (currentSubjects.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="10" class="text-center text-muted py-5">
                    <i class="bi bi-inbox"></i>
                    <p class="mb-0">No subjects found</p>
                    ${Object.keys(currentFilters).length > 0 ? 
                        '<small>Try adjusting your filters</small>' : 
                        '<small>Upload some MRIQC data to get started</small>'
                    }
                </td>
            </tr>
        `;
        return;
    }
    
    const rowsHTML = currentSubjects.map(subject => `
        <tr>
            <td>
                <input type="checkbox" class="form-check-input subject-checkbox" 
                       value="${subject.subject_info.subject_id}"
                       ${selectedSubjects.has(subject.subject_info.subject_id) ? 'checked' : ''}>
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
                            onclick="exportSubjectData('${subject.subject_info.subject_id}')"
                            title="Export Data">
                        <i class="bi bi-download"></i>
                    </button>
                </div>
            </td>
        </tr>
    `).join('');
    
    tbody.innerHTML = rowsHTML;
    
    // Update select all checkbox
    updateSelectAllCheckbox();
}

/**
 * Render key metrics badges
 */
function renderKeyMetrics(metrics) {
    const keyMetrics = ['snr', 'cnr', 'fber'];
    return keyMetrics.map(metric => {
        const value = metrics[metric];
        if (value !== null && value !== undefined) {
            return `<small class="badge bg-light text-dark" title="${metric.toUpperCase()}: ${value}">
                ${metric.toUpperCase()}: ${utils.formatNumber(value, 1)}
            </small>`;
        }
        return '';
    }).filter(Boolean).join('');
}

/**
 * Get score color class
 */
function getScoreColor(score) {
    if (score >= 0.8) return 'text-success';
    if (score >= 0.6) return 'text-warning';
    return 'text-danger';
}

/**
 * Update pagination
 */
function updatePagination() {
    const paginationInfo = document.getElementById('pagination-info');
    const pagination = document.getElementById('pagination');
    
    if (!pagination) return;
    
    const startIdx = (currentPage - 1) * pageSize + 1;
    const endIdx = Math.min(currentPage * pageSize, totalSubjects);
    
    // Update pagination info
    if (paginationInfo) {
        paginationInfo.textContent = `Showing ${startIdx} - ${endIdx} of ${totalSubjects} subjects`;
    }
    
    // Calculate pagination
    const totalPages = Math.ceil(totalSubjects / pageSize);
    const maxVisiblePages = 5;
    
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage + 1 < maxVisiblePages) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    // Build pagination HTML
    let paginationHTML = '';
    
    // Previous button
    paginationHTML += `
        <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage - 1})">
                <i class="bi bi-chevron-left"></i>
            </a>
        </li>
    `;
    
    // First page
    if (startPage > 1) {
        paginationHTML += `
            <li class="page-item">
                <a class="page-link" href="#" onclick="changePage(1)">1</a>
            </li>
        `;
        if (startPage > 2) {
            paginationHTML += '<li class="page-item disabled"><span class="page-link">...</span></li>';
        }
    }
    
    // Page numbers
    for (let i = startPage; i <= endPage; i++) {
        paginationHTML += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="changePage(${i})">${i}</a>
            </li>
        `;
    }
    
    // Last page
    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            paginationHTML += '<li class="page-item disabled"><span class="page-link">...</span></li>';
        }
        paginationHTML += `
            <li class="page-item">
                <a class="page-link" href="#" onclick="changePage(${totalPages})">${totalPages}</a>
            </li>
        `;
    }
    
    // Next button
    paginationHTML += `
        <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="changePage(${currentPage + 1})">
                <i class="bi bi-chevron-right"></i>
            </a>
        </li>
    `;
    
    pagination.innerHTML = paginationHTML;
}

/**
 * Update subject count
 */
function updateSubjectCount() {
    const subjectCount = document.getElementById('subject-count');
    if (subjectCount) {
        subjectCount.textContent = totalSubjects.toString();
    }
}

/**
 * Update filter summary
 */
function updateFilterSummary(appliedFilters) {
    const filterSummary = document.getElementById('filter-summary');
    if (!filterSummary) return;
    
    const filterCount = Object.keys(appliedFilters).length;
    
    if (filterCount === 0) {
        filterSummary.textContent = 'Showing all subjects';
    } else {
        const filterTexts = [];
        
        if (appliedFilters.quality_status) {
            filterTexts.push(`Quality: ${appliedFilters.quality_status}`);
        }
        
        if (appliedFilters.age_group) {
            filterTexts.push(`Age Group: ${appliedFilters.age_group}`);
        }
        
        if (appliedFilters.scan_type) {
            filterTexts.push(`Scan Type: ${appliedFilters.scan_type}`);
        }
        
        if (appliedFilters.search_text) {
            filterTexts.push(`Search: "${appliedFilters.search_text}"`);
        }
        
        filterSummary.textContent = `Filtered by: ${filterTexts.join(', ')}`;
    }
}

/**
 * Setup event listeners
 */
function setupEventListeners() {
    // Search input with debounce
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        searchInput.addEventListener('input', utils.debounce(handleSearchChange, 500));
    }
    
    // Filter dropdowns
    const filterElements = [
        'quality-filter',
        'age-group-filter',
        'scan-type-filter'
    ];
    
    filterElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', handleFilterChange);
        }
    });
    
    // Age range inputs
    const ageMinInput = document.getElementById('age-min');
    const ageMaxInput = document.getElementById('age-max');
    
    if (ageMinInput) ageMinInput.addEventListener('change', handleFilterChange);
    if (ageMaxInput) ageMaxInput.addEventListener('change', handleFilterChange);
    
    // Page size selector
    const pageSizeSelect = document.getElementById('page-size-select');
    if (pageSizeSelect) {
        pageSizeSelect.addEventListener('change', handlePageSizeChange);
    }
    
    // Select all checkbox
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', handleSelectAllChange);
    }
    
    // Subject checkboxes (delegated event handling)
    document.addEventListener('change', function(event) {
        if (event.target.classList.contains('subject-checkbox')) {
            handleSubjectCheckboxChange(event.target);
        }
    });
}

/**
 * Setup WebSocket event handlers
 */
function setupWebSocketHandlers() {
    // Listen for dashboard updates that might affect subjects
    wsClient.on('dashboardUpdate', () => {
        refreshSubjects();
    });
    
    // Listen for batch completion
    wsClient.on('batchCompleted', () => {
        setTimeout(refreshSubjects, 1000);
    });
}

/**
 * Handle search input change
 */
function handleSearchChange(event) {
    const searchText = event.target.value.trim();
    
    if (searchText) {
        currentFilters.search_text = searchText;
        utils.urlParams.set('search', searchText);
    } else {
        delete currentFilters.search_text;
        utils.urlParams.remove('search');
    }
    
    currentPage = 1;
    loadSubjects();
}

/**
 * Handle filter change
 */
function handleFilterChange() {
    // Collect all filter values
    const qualityFilter = document.getElementById('quality-filter')?.value;
    const ageGroupFilter = document.getElementById('age-group-filter')?.value;
    const scanTypeFilter = document.getElementById('scan-type-filter')?.value;
    const ageMin = document.getElementById('age-min')?.value;
    const ageMax = document.getElementById('age-max')?.value;
    
    // Reset filters
    currentFilters = {};
    
    // Apply filters
    if (qualityFilter) {
        currentFilters.quality_status = [qualityFilter];
        utils.urlParams.set('quality_status', qualityFilter);
    } else {
        utils.urlParams.remove('quality_status');
    }
    
    if (ageGroupFilter) {
        currentFilters.age_group = [ageGroupFilter];
        utils.urlParams.set('age_group', ageGroupFilter);
    } else {
        utils.urlParams.remove('age_group');
    }
    
    if (scanTypeFilter) {
        currentFilters.scan_type = [scanTypeFilter];
        utils.urlParams.set('scan_type', scanTypeFilter);
    } else {
        utils.urlParams.remove('scan_type');
    }
    
    if (ageMin || ageMax) {
        currentFilters.age_range = {};
        if (ageMin) currentFilters.age_range.min = parseFloat(ageMin);
        if (ageMax) currentFilters.age_range.max = parseFloat(ageMax);
    }
    
    currentPage = 1;
    loadSubjects();
}

/**
 * Handle page size change
 */
function handlePageSizeChange(event) {
    pageSize = parseInt(event.target.value);
    currentPage = 1;
    utils.urlParams.set('page_size', pageSize.toString());
    loadSubjects();
}

/**
 * Handle select all checkbox change
 */
function handleSelectAllChange(event) {
    const isChecked = event.target.checked;
    const checkboxes = document.querySelectorAll('.subject-checkbox');
    
    checkboxes.forEach(checkbox => {
        checkbox.checked = isChecked;
        const subjectId = checkbox.value;
        
        if (isChecked) {
            selectedSubjects.add(subjectId);
        } else {
            selectedSubjects.delete(subjectId);
        }
    });
    
    updateBulkActionButtons();
}

/**
 * Handle individual subject checkbox change
 */
function handleSubjectCheckboxChange(checkbox) {
    const subjectId = checkbox.value;
    
    if (checkbox.checked) {
        selectedSubjects.add(subjectId);
    } else {
        selectedSubjects.delete(subjectId);
    }
    
    updateSelectAllCheckbox();
    updateBulkActionButtons();
}

/**
 * Update select all checkbox state
 */
function updateSelectAllCheckbox() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    const subjectCheckboxes = document.querySelectorAll('.subject-checkbox');
    
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
 * Update bulk action buttons
 */
function updateBulkActionButtons() {
    const selectedCount = selectedSubjects.size;
    
    // Update selected count in bulk update modal
    const selectedCountSpan = document.getElementById('selected-count');
    if (selectedCountSpan) {
        selectedCountSpan.textContent = selectedCount.toString();
    }
    
    // Enable/disable bulk action buttons based on selection
    const bulkButtons = document.querySelectorAll('[data-requires-selection]');
    bulkButtons.forEach(button => {
        button.disabled = selectedCount === 0;
    });
}

/**
 * Toggle filters panel
 */
function toggleFilters() {
    const filterPanel = document.getElementById('filter-panel');
    const toggleIcon = document.getElementById('filter-toggle-icon');
    const toggleText = document.getElementById('filter-toggle-text');
    
    if (filterPanel && toggleIcon && toggleText) {
        if (filterPanel.classList.contains('d-none')) {
            filterPanel.classList.remove('d-none');
            toggleIcon.className = 'bi bi-chevron-up';
            toggleText.textContent = 'Hide Filters';
        } else {
            filterPanel.classList.add('d-none');
            toggleIcon.className = 'bi bi-chevron-down';
            toggleText.textContent = 'Show Filters';
        }
    }
}

/**
 * Toggle advanced filters
 */
function toggleAdvancedFilters() {
    const advancedFilters = document.getElementById('advanced-filters');
    if (advancedFilters) {
        advancedFilters.classList.toggle('d-none');
    }
}

/**
 * Apply filters
 */
function applyFilters() {
    handleFilterChange();
}

/**
 * Clear all filters
 */
function clearFilters() {
    // Reset form elements
    const filterElements = [
        'search-input',
        'quality-filter',
        'age-group-filter',
        'scan-type-filter',
        'age-min',
        'age-max',
        'snr-min',
        'snr-max',
        'cnr-min',
        'cnr-max',
        'date-from',
        'date-to'
    ];
    
    filterElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.value = '';
        }
    });
    
    // Clear filters and URL params
    currentFilters = {};
    ['search', 'quality_status', 'age_group', 'scan_type'].forEach(param => {
        utils.urlParams.remove(param);
    });
    
    currentPage = 1;
    loadSubjects();
}

/**
 * Save filter preset
 */
function saveFilterPreset() {
    const presetName = prompt('Enter a name for this filter preset:');
    if (presetName) {
        const presets = utils.storage.get('filter_presets', {});
        presets[presetName] = { ...currentFilters };
        utils.storage.set('filter_presets', presets);
        utils.showAlert(`Filter preset "${presetName}" saved successfully`, 'success');
    }
}

/**
 * Change page
 */
function changePage(page) {
    if (page < 1 || page > Math.ceil(totalSubjects / pageSize)) return;
    
    currentPage = page;
    utils.urlParams.set('page', page.toString());
    loadSubjects();
}

/**
 * Previous page
 */
function previousPage() {
    changePage(currentPage - 1);
}

/**
 * Next page
 */
function nextPage() {
    changePage(currentPage + 1);
}

/**
 * Change page size
 */
function changePageSize() {
    handlePageSizeChange({ target: document.getElementById('page-size-select') });
}

/**
 * Sort subjects
 */
function sortSubjects(sortBy) {
    if (currentSort.sort_by === sortBy) {
        // Toggle sort order
        currentSort.sort_order = currentSort.sort_order === 'asc' ? 'desc' : 'asc';
    } else {
        // New sort field
        currentSort.sort_by = sortBy;
        currentSort.sort_order = 'asc';
    }
    
    currentPage = 1;
    loadSubjects();
}

/**
 * Refresh subjects
 */
async function refreshSubjects() {
    try {
        await loadSubjects();
        utils.showAlert('Subjects refreshed', 'success', 2000);
    } catch (error) {
        utils.handleAPIError(error, 'Failed to refresh subjects');
    }
}

/**
 * Export filtered subjects
 */
async function exportFilteredSubjects() {
    try {
        showLoading(true, 'Exporting subjects...');
        
        // Get all subjects matching current filters (not just current page)
        const allSubjects = [];
        let page = 1;
        let hasMore = true;
        
        while (hasMore) {
            const response = await api.filterSubjects(
                currentFilters,
                currentSort,
                page,
                1000 // Large page size
            );
            
            allSubjects.push(...response.subjects);
            hasMore = response.subjects.length === 1000;
            page++;
        }
        
        const subjectIds = allSubjects.map(s => s.subject_info.subject_id);
        const blob = await api.exportCSV(subjectIds);
        
        const filename = `subjects_export_${new Date().toISOString().split('T')[0]}.csv`;
        utils.downloadBlob(blob, filename);
        
        utils.showAlert(`Exported ${subjectIds.length} subjects successfully`, 'success');
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to export subjects');
    } finally {
        showLoading(false);
    }
}

/**
 * Select all subjects
 */
function selectAllSubjects() {
    const checkboxes = document.querySelectorAll('.subject-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
        selectedSubjects.add(checkbox.value);
    });
    
    updateSelectAllCheckbox();
    updateBulkActionButtons();
}

/**
 * Deselect all subjects
 */
function deselectAllSubjects() {
    const checkboxes = document.querySelectorAll('.subject-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
        selectedSubjects.delete(checkbox.value);
    });
    
    updateSelectAllCheckbox();
    updateBulkActionButtons();
}

/**
 * Bulk update quality
 */
function bulkUpdateQuality() {
    if (selectedSubjects.size === 0) {
        utils.showAlert('Please select subjects to update', 'warning');
        return;
    }
    
    const modal = new bootstrap.Modal(document.getElementById('bulk-update-modal'));
    modal.show();
}

/**
 * Confirm bulk update
 */
async function confirmBulkUpdate() {
    const qualityStatus = document.getElementById('bulk-quality-status').value;
    const reason = document.getElementById('bulk-update-reason').value;
    
    if (!qualityStatus) {
        utils.showAlert('Please select a quality status', 'warning');
        return;
    }
    
    try {
        showLoading(true, 'Updating subjects...');
        
        await api.bulkUpdateQuality(Array.from(selectedSubjects), qualityStatus, reason);
        
        utils.showAlert(`Updated ${selectedSubjects.size} subjects successfully`, 'success');
        
        // Refresh subjects list
        await loadSubjects();
        
        // Clear selection
        selectedSubjects.clear();
        updateSelectAllCheckbox();
        updateBulkActionButtons();
        
        // Hide modal
        const modal = bootstrap.Modal.getInstance(document.getElementById('bulk-update-modal'));
        modal.hide();
        
    } catch (error) {
        utils.handleAPIError(error, 'Failed to update subjects');
    } finally {
        showLoading(false);
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
async function exportSubjectData(subjectId) {
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
 * Clear search
 */
function clearSearch() {
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        searchInput.value = '';
        handleSearchChange({ target: searchInput });
    }
}

// Export functions for global use
window.toggleFilters = toggleFilters;
window.toggleAdvancedFilters = toggleAdvancedFilters;
window.applyFilters = applyFilters;
window.clearFilters = clearFilters;
window.saveFilterPreset = saveFilterPreset;
window.changePage = changePage;
window.previousPage = previousPage;
window.nextPage = nextPage;
window.changePageSize = changePageSize;
window.sortSubjects = sortSubjects;
window.refreshSubjects = refreshSubjects;
window.exportFilteredSubjects = exportFilteredSubjects;
window.selectAllSubjects = selectAllSubjects;
window.deselectAllSubjects = deselectAllSubjects;
window.bulkUpdateQuality = bulkUpdateQuality;
window.confirmBulkUpdate = confirmBulkUpdate;
window.viewSubjectDetail = viewSubjectDetail;
window.exportSubjectData = exportSubjectData;
window.clearSearch = clearSearch;