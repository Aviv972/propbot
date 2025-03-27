// Table Filtering and Sorting Functionality
document.addEventListener('DOMContentLoaded', function() {
    // Initialize variables
    const table = document.querySelector('.property-table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    
    // Add data attributes to each cell for easier filtering and sorting
    processTableData();
    
    // Create filter controls
    createFilterControls();
    
    // Initialize table sorting
    initTableSorting();
    
    // Create button to reset filters
    createResetButton();
    
    // Initialize highlights for top deals
    highlightTopDeals();
    
    // Add scroll event listener to manage sticky header appearance
    initStickyHeaderBehavior();
    
    // ====== Initialization Functions ======
    
    // Process table data to add data attributes
    function processTableData() {
        // Get all headers
        const headers = Array.from(table.querySelectorAll('th')).map(th => th.textContent.trim());
        
        // Process each row
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            
            // Add data attributes to each cell based on header
            cells.forEach((cell, index) => {
                if (index < headers.length) {
                    const header = headers[index];
                    
                    // Extract numeric value from the cell
                    let rawValue = cell.textContent.trim();
                    
                    // Handle special cases with currency symbols or percentages
                    if (rawValue.includes('€')) {
                        // Remove euro symbol and commas, then convert to number
                        const numericValue = parseFloat(rawValue.replace('€', '').replace(/,/g, ''));
                        cell.setAttribute('data-value', isNaN(numericValue) ? -1 : numericValue);
                    } else if (rawValue.includes('%')) {
                        // Remove percent symbol and convert to number
                        const numericValue = parseFloat(rawValue.replace('%', ''));
                        cell.setAttribute('data-value', isNaN(numericValue) ? -1 : numericValue);
                    } else if (!isNaN(parseFloat(rawValue))) {
                        // It's already a number
                        cell.setAttribute('data-value', parseFloat(rawValue));
                    } else {
                        // Text value
                        cell.setAttribute('data-value', rawValue.toLowerCase());
                    }
                }
            });
        });
    }
    
    // Create filter controls above the table
    function createFilterControls() {
        // Create container for filter controls
        const filterContainer = document.createElement('div');
        filterContainer.className = 'filter-controls';
        
        // Add title
        const filterTitle = document.createElement('h3');
        filterTitle.textContent = 'Filter Properties';
        filterContainer.appendChild(filterTitle);
        
        // Create filter form
        const filterForm = document.createElement('form');
        filterForm.className = 'filter-form';
        filterForm.addEventListener('submit', function(e) {
            e.preventDefault();
            applyFilters();
        });
        
        // Add key metrics filters
        const metricsContainer = document.createElement('div');
        metricsContainer.className = 'filter-group';
        
        const metricsTitle = document.createElement('h4');
        metricsTitle.textContent = 'Key Financial Metrics';
        metricsContainer.appendChild(metricsTitle);
        
        // Add filters for key metrics
        const metrics = [
            { name: 'price', label: 'Max Price (€)', default: '' },
            { name: 'gross_yield', label: 'Min Gross Yield (%)', default: '' },
            { name: 'cap_rate', label: 'Min Cap Rate (%)', default: '' },
            { name: 'cash_flow', label: 'Min Monthly Cash Flow (€)', default: '' },
            { name: 'price_per_sqm', label: 'Max Price per sqm (€)', default: '' }
        ];
        
        metrics.forEach(metric => {
            const group = document.createElement('div');
            group.className = 'input-group';
            
            const label = document.createElement('label');
            label.setAttribute('for', `filter-${metric.name}`);
            label.textContent = metric.label;
            
            const input = document.createElement('input');
            input.type = 'number';
            input.id = `filter-${metric.name}`;
            input.name = metric.name;
            input.placeholder = metric.default;
            input.step = 'any';
            
            group.appendChild(label);
            group.appendChild(input);
            metricsContainer.appendChild(group);
        });
        
        filterForm.appendChild(metricsContainer);
        
        // Add neighborhood filter
        const neighborhoodContainer = document.createElement('div');
        neighborhoodContainer.className = 'filter-group';
        
        const neighborhoodTitle = document.createElement('h4');
        neighborhoodTitle.textContent = 'Neighborhood';
        neighborhoodContainer.appendChild(neighborhoodTitle);
        
        const neighborhoodGroup = document.createElement('div');
        neighborhoodGroup.className = 'input-group';
        
        const neighborhoodLabel = document.createElement('label');
        neighborhoodLabel.setAttribute('for', 'filter-neighborhood');
        neighborhoodLabel.textContent = 'Neighborhood';
        
        const neighborhoodSelect = document.createElement('select');
        neighborhoodSelect.id = 'filter-neighborhood';
        neighborhoodSelect.name = 'neighborhood';
        
        // Get unique neighborhoods from the table
        const neighborhoods = new Set();
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 15) { // Index 14 is the neighborhood column (0-based)
                neighborhoods.add(cells[14].textContent.trim());
            }
        });
        
        // Add all option
        const allOption = document.createElement('option');
        allOption.value = '';
        allOption.textContent = 'All Neighborhoods';
        neighborhoodSelect.appendChild(allOption);
        
        // Add options for each neighborhood
        Array.from(neighborhoods).sort().forEach(neighborhood => {
            const option = document.createElement('option');
            option.value = neighborhood;
            option.textContent = neighborhood;
            neighborhoodSelect.appendChild(option);
        });
        
        neighborhoodGroup.appendChild(neighborhoodLabel);
        neighborhoodGroup.appendChild(neighborhoodSelect);
        neighborhoodContainer.appendChild(neighborhoodGroup);
        
        filterForm.appendChild(neighborhoodContainer);
        
        // Add price classification filter
        const classificationContainer = document.createElement('div');
        classificationContainer.className = 'filter-group';
        
        const classificationTitle = document.createElement('h4');
        classificationTitle.textContent = 'Price Classification';
        classificationContainer.appendChild(classificationTitle);
        
        const classificationGroup = document.createElement('div');
        classificationGroup.className = 'checkbox-group';
        
        const classifications = [
            { value: 'Below Average', label: 'Below Average (Good Deal)' },
            { value: 'Average', label: 'Average' },
            { value: 'Above Average', label: 'Above Average (Premium)' }
        ];
        
        classifications.forEach(classification => {
            const checkboxGroup = document.createElement('div');
            checkboxGroup.className = 'checkbox-item';
            
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `filter-classification-${classification.value.replace(' ', '-').toLowerCase()}`;
            checkbox.name = 'classification';
            checkbox.value = classification.value;
            checkbox.checked = true; // All checked by default
            
            const label = document.createElement('label');
            label.setAttribute('for', `filter-classification-${classification.value.replace(' ', '-').toLowerCase()}`);
            label.textContent = classification.label;
            
            checkboxGroup.appendChild(checkbox);
            checkboxGroup.appendChild(label);
            classificationGroup.appendChild(checkboxGroup);
        });
        
        classificationContainer.appendChild(classificationGroup);
        filterForm.appendChild(classificationContainer);
        
        // Add submit button
        const submitContainer = document.createElement('div');
        submitContainer.className = 'filter-actions';
        
        const submitButton = document.createElement('button');
        submitButton.type = 'submit';
        submitButton.className = 'btn btn-primary';
        submitButton.textContent = 'Apply Filters';
        
        submitContainer.appendChild(submitButton);
        filterForm.appendChild(submitContainer);
        
        // Add filter form to container
        filterContainer.appendChild(filterForm);
        
        // Insert filter container before table
        table.parentNode.insertBefore(filterContainer, table);
    }
    
    // Initialize table sorting
    function initTableSorting() {
        const headers = table.querySelectorAll('th');
        
        headers.forEach((header, index) => {
            // Avoid sorting for property column (first column)
            if (index === 0) return;
            
            // Make headers clickable for sorting
            header.style.cursor = 'pointer';
            header.setAttribute('data-sort-direction', 'none');
            
            // Add click event for sorting
            header.addEventListener('click', function() {
                const direction = this.getAttribute('data-sort-direction');
                
                // Reset all headers
                headers.forEach(h => {
                    h.setAttribute('data-sort-direction', 'none');
                    h.classList.remove('sort-asc', 'sort-desc');
                });
                
                // Set new direction
                let newDirection = 'asc';
                if (direction === 'asc') {
                    newDirection = 'desc';
                }
                
                this.setAttribute('data-sort-direction', newDirection);
                this.classList.add(`sort-${newDirection}`);
                
                // Sort the table
                sortTable(index, newDirection);
            });
        });
        
        // Add sort indicators to headers
        const style = document.createElement('style');
        style.textContent = `
            th.sort-asc::after {
                content: ' ▲';
                font-size: 0.8em;
            }
            th.sort-desc::after {
                content: ' ▼';
                font-size: 0.8em;
            }
        `;
        document.head.appendChild(style);
    }
    
    // Initialize sticky header behavior
    function initStickyHeaderBehavior() {
        const tableContainer = document.querySelector('.responsive-table');
        const thead = table.querySelector('thead');
        
        // Add class to show active sticky state when scrolling
        tableContainer.addEventListener('scroll', function() {
            if (tableContainer.scrollTop > 0) {
                thead.classList.add('sticky-active');
            } else {
                thead.classList.remove('sticky-active');
            }
        });
        
        // Add the viewport height constraint to ensure scrolling works
        const viewportHeight = window.innerHeight;
        const tableTop = tableContainer.getBoundingClientRect().top;
        const maxHeight = viewportHeight - tableTop - 100; // Leave some space at bottom
        
        // Set the max height if it's reasonable
        if (maxHeight > 300) {
            tableContainer.style.maxHeight = `${maxHeight}px`;
        }
    }
    
    // Create reset button
    function createResetButton() {
        const resetButton = document.createElement('button');
        resetButton.className = 'btn btn-secondary reset-filters';
        resetButton.textContent = 'Reset All Filters';
        resetButton.addEventListener('click', function() {
            // Reset form inputs
            document.querySelector('.filter-form').reset();
            
            // Show all rows
            rows.forEach(row => {
                row.style.display = '';
                row.classList.remove('highlighted-deal');
            });
            
            // Reset sorting
            const headers = table.querySelectorAll('th');
            headers.forEach(h => {
                h.setAttribute('data-sort-direction', 'none');
                h.classList.remove('sort-asc', 'sort-desc');
            });
            
            // Show deals counter
            updateDealsCounter(rows.length);
        });
        
        // Add reset button to the filter actions
        const filterActions = document.querySelector('.filter-actions');
        filterActions.appendChild(resetButton);
    }
    
    // ====== Filtering Functions ======
    
    // Apply filters to the table
    function applyFilters() {
        // Get filter values
        const maxPrice = parseFloat(document.getElementById('filter-price').value) || Infinity;
        const minGrossYield = parseFloat(document.getElementById('filter-gross_yield').value) || 0;
        const minCapRate = parseFloat(document.getElementById('filter-cap_rate').value) || 0;
        const minCashFlow = parseFloat(document.getElementById('filter-cash_flow').value) || -Infinity;
        const maxPricePerSqm = parseFloat(document.getElementById('filter-price_per_sqm').value) || Infinity;
        const selectedNeighborhood = document.getElementById('filter-neighborhood').value;
        
        // Get selected classifications
        const selectedClassifications = Array.from(document.querySelectorAll('input[name="classification"]:checked'))
            .map(checkbox => checkbox.value);
        
        // Filter rows
        let visibleRows = 0;
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            
            // Skip rows with not enough cells
            if (cells.length < 19) {
                row.style.display = 'none';
                return;
            }
            
            // Get cell values
            const price = parseFloat(cells[1].getAttribute('data-value')) || Infinity;
            const grossYield = parseFloat(cells[10].getAttribute('data-value')) || 0;
            const capRate = parseFloat(cells[9].getAttribute('data-value')) || 0;
            const cashFlow = parseFloat(cells[12].getAttribute('data-value')) || -Infinity;
            const pricePerSqm = parseFloat(cells[13].getAttribute('data-value')) || Infinity;
            const neighborhood = cells[14].textContent.trim();
            const classification = cells[18].textContent.trim();
            
            // Apply filters
            const meetsMaxPrice = price <= maxPrice;
            const meetsMinGrossYield = grossYield >= minGrossYield;
            const meetsMinCapRate = capRate >= minCapRate;
            const meetsMinCashFlow = cashFlow >= minCashFlow;
            const meetsMaxPricePerSqm = pricePerSqm <= maxPricePerSqm;
            const meetsNeighborhood = !selectedNeighborhood || neighborhood === selectedNeighborhood;
            const meetsClassification = selectedClassifications.includes(classification);
            
            // Show/hide row based on filters
            if (meetsMaxPrice && meetsMinGrossYield && meetsMinCapRate && meetsMinCashFlow && 
                meetsMaxPricePerSqm && meetsNeighborhood && meetsClassification) {
                row.style.display = '';
                visibleRows++;
            } else {
                row.style.display = 'none';
            }
        });
        
        // Update deals counter
        updateDealsCounter(visibleRows);
        
        // Highlight top deals if there are visible rows
        if (visibleRows > 0) {
            highlightTopDeals();
        }
    }
    
    // ====== Sorting Functions ======
    
    // Sort the table by column index and direction
    function sortTable(columnIndex, direction) {
        const sortedRows = rows.sort((rowA, rowB) => {
            const cellA = rowA.querySelectorAll('td')[columnIndex];
            const cellB = rowB.querySelectorAll('td')[columnIndex];
            
            if (!cellA || !cellB) return 0;
            
            const valueA = cellA.getAttribute('data-value');
            const valueB = cellB.getAttribute('data-value');
            
            // Handle numeric values
            if (!isNaN(valueA) && !isNaN(valueB)) {
                return direction === 'asc' 
                    ? parseFloat(valueA) - parseFloat(valueB)
                    : parseFloat(valueB) - parseFloat(valueA);
            }
            
            // Handle text values
            return direction === 'asc'
                ? valueA.localeCompare(valueB)
                : valueB.localeCompare(valueA);
        });
        
        // Remove all rows from tbody
        while (tbody.firstChild) {
            tbody.removeChild(tbody.firstChild);
        }
        
        // Append sorted rows to tbody
        sortedRows.forEach(row => {
            tbody.appendChild(row);
        });
        
        // Force a repaint of the sticky headers to ensure they look correct
        const tableContainer = document.querySelector('.responsive-table');
        if (tableContainer.scrollTop > 0) {
            // Toggle a class to force repaint
            const thead = table.querySelector('thead');
            thead.classList.add('force-repaint');
            setTimeout(() => thead.classList.remove('force-repaint'), 0);
        }
    }
    
    // ====== Deal Highlighting Functions ======
    
    // Highlight top deals based on key metrics
    function highlightTopDeals() {
        // Reset all highlights
        rows.forEach(row => {
            row.classList.remove('highlighted-deal');
        });
        
        // Get visible rows
        const visibleRows = rows.filter(row => row.style.display !== 'none');
        
        // Skip if no visible rows
        if (visibleRows.length === 0) return;
        
        // Highlight top 10% of properties by gross yield
        const sortByGrossYield = [...visibleRows].sort((a, b) => {
            const yieldA = parseFloat(a.querySelectorAll('td')[10].getAttribute('data-value')) || 0;
            const yieldB = parseFloat(b.querySelectorAll('td')[10].getAttribute('data-value')) || 0;
            return yieldB - yieldA; // Descending order
        });
        
        const topYieldCount = Math.max(1, Math.ceil(visibleRows.length * 0.1)); // At least 1, up to 10%
        sortByGrossYield.slice(0, topYieldCount).forEach(row => {
            row.classList.add('highlighted-deal');
        });
        
        // Add pulsing animation to highlighted deals
        if (!document.querySelector('.highlight-animation-style')) {
            const style = document.createElement('style');
            style.className = 'highlight-animation-style';
            style.textContent = `
                .highlighted-deal {
                    background-color: rgba(46, 204, 113, 0.1) !important;
                    animation: pulse 2s infinite;
                }
                
                @keyframes pulse {
                    0% {
                        background-color: rgba(46, 204, 113, 0.1);
                    }
                    50% {
                        background-color: rgba(46, 204, 113, 0.3);
                    }
                    100% {
                        background-color: rgba(46, 204, 113, 0.1);
                    }
                }
                
                .sticky-active th {
                    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
                }
            `;
            document.head.appendChild(style);
        }
    }
    
    // Update the deals counter
    function updateDealsCounter(count) {
        // Create or update deals counter
        let counterDiv = document.querySelector('.deals-counter');
        
        if (!counterDiv) {
            counterDiv = document.createElement('div');
            counterDiv.className = 'deals-counter';
            const filterControls = document.querySelector('.filter-controls');
            filterControls.appendChild(counterDiv);
        }
        
        counterDiv.textContent = `Showing ${count} properties`;
    }
    
    // Handle window resize to adjust table container height
    window.addEventListener('resize', function() {
        const tableContainer = document.querySelector('.responsive-table');
        const viewportHeight = window.innerHeight;
        const tableTop = tableContainer.getBoundingClientRect().top;
        const maxHeight = viewportHeight - tableTop - 100; // Leave some space at bottom
        
        // Set the max height if it's reasonable
        if (maxHeight > 300) {
            tableContainer.style.maxHeight = `${maxHeight}px`;
        }
    });
}); 