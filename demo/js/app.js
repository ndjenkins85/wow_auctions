// Data storage
let priceHistory = {};
let holdings = [];
let inventory = [];
let sellPolicy = [];
let marketSnapshot = [];

// Charts
let holdingsChart = null;
let priceChart = null;
let inventoryChart = null;

// Format gold values
function formatGold(copper) {
    const gold = Math.floor(copper / 10000);
    return gold.toLocaleString() + 'g';
}

function formatCopper(copper) {
    return copper.toLocaleString() + 'c';
}

// Load all data
async function loadData() {
    try {
        const [priceData, holdingsData, inventoryData, sellPolicyData, marketData] = await Promise.all([
            fetch('data/price_history.json').then(r => r.json()),
            fetch('data/holdings.json').then(r => r.json()),
            fetch('data/inventory.json').then(r => r.json()),
            fetch('data/sell_policy.json').then(r => r.json()),
            fetch('data/market_snapshot.json').then(r => r.json())
        ]);

        priceHistory = priceData;
        holdings = holdingsData;
        inventory = inventoryData;
        sellPolicy = sellPolicyData;
        marketSnapshot = marketData;

        initializeApp();
    } catch (error) {
        console.error('Error loading data:', error);
    }
}

// Initialize the app
function initializeApp() {
    setupTabs();
    renderStats();
    renderHoldingsChart();
    renderPriceChart('Greater Mana Potion');
    renderInventoryChart();
    renderTables();
    setupItemSelector();
}

// Tab switching
function setupTabs() {
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetTab = btn.dataset.tab;

            tabBtns.forEach(b => b.classList.remove('active'));
            tabContents.forEach(c => c.classList.remove('active'));

            btn.classList.add('active');
            document.getElementById(targetTab).classList.add('active');
        });
    });
}

// Render overview stats
function renderStats() {
    const latest = holdings[holdings.length - 1];
    document.getElementById('total-gold').textContent = formatGold(latest.gold);
    document.getElementById('inventory-value').textContent = formatGold(latest.inventory_value);
    document.getElementById('total-holdings').textContent = formatGold(latest.total);
    
    // Calculate profit margin from sell policy
    const avgMargin = sellPolicy.reduce((sum, item) => {
        return sum + (item.profit_per_item / item.min_list_price * 100);
    }, 0) / sellPolicy.length;
    document.getElementById('profit-margin').textContent = '+' + avgMargin.toFixed(1) + '%';
}

// Render holdings chart
function renderHoldingsChart() {
    const ctx = document.getElementById('holdingsChart');
    
    if (holdingsChart) {
        holdingsChart.destroy();
    }

    holdingsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: holdings.map(h => h.date),
            datasets: [
                {
                    label: 'Total Holdings',
                    data: holdings.map(h => h.total / 10000),
                    borderColor: '#ffd700',
                    backgroundColor: 'rgba(255, 215, 0, 0.1)',
                    borderWidth: 3,
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Liquid Gold',
                    data: holdings.map(h => h.gold / 10000),
                    borderColor: '#8b5cf6',
                    backgroundColor: 'rgba(139, 92, 246, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Inventory Value',
                    data: holdings.map(h => h.inventory_value / 10000),
                    borderColor: '#ff8c00',
                    backgroundColor: 'rgba(255, 140, 0, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e0e0e0',
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': ' + context.parsed.y.toLocaleString() + 'g';
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: '#a0a0a0' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                },
                y: {
                    ticks: {
                        color: '#a0a0a0',
                        callback: function(value) {
                            return value.toLocaleString() + 'g';
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                }
            }
        }
    });
}

// Render price chart
function renderPriceChart(itemName) {
    const ctx = document.getElementById('priceChart');
    const itemData = priceHistory[itemName];
    
    if (!itemData) return;

    if (priceChart) {
        priceChart.destroy();
    }

    priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: itemData.map(d => d.date),
            datasets: [{
                label: itemName,
                data: itemData.map(d => d.price / 100),
                borderColor: '#ffd700',
                backgroundColor: 'rgba(255, 215, 0, 0.2)',
                borderWidth: 3,
                tension: 0.4,
                fill: true,
                pointRadius: 4,
                pointHoverRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e0e0e0',
                        font: { size: 14 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.parsed.y.toFixed(2) + 'g';
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: { color: '#a0a0a0' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                },
                y: {
                    ticks: {
                        color: '#a0a0a0',
                        callback: function(value) {
                            return value.toFixed(0) + 'g';
                        }
                    },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                }
            }
        }
    });
}

// Render inventory chart
function renderInventoryChart() {
    const ctx = document.getElementById('inventoryChart');
    
    if (inventoryChart) {
        inventoryChart.destroy();
    }

    // Group by location
    const locations = {};
    inventory.forEach(item => {
        if (!locations[item.location]) {
            locations[item.location] = 0;
        }
        locations[item.location] += item.value;
    });

    const colors = {
        'Auctions': '#ffd700',
        'Bank': '#8b5cf6',
        'Inventory': '#ff8c00',
        'Storage': '#10b981'
    };

    inventoryChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: Object.keys(locations),
            datasets: [{
                data: Object.values(locations).map(v => v / 10000),
                backgroundColor: Object.keys(locations).map(loc => colors[loc] || '#666'),
                borderColor: '#1a1a2e',
                borderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#e0e0e0',
                        font: { size: 12 },
                        padding: 20
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const label = context.label || '';
                            const value = context.parsed || 0;
                            return label + ': ' + value.toLocaleString() + 'g';
                        }
                    }
                }
            }
        }
    });
}

// Render all tables
function renderTables() {
    renderMarketPricesTable();
    renderInventoryTable();
    renderSellPolicyTable();
    renderMarketSnapshotTable();
}

// Render market prices table
function renderMarketPricesTable() {
    const tbody = document.getElementById('market-prices-table');
    tbody.innerHTML = '';

    sellPolicy.forEach(item => {
        const materialCost = item.min_list_price - 1000; // Approximate
        const margin = ((item.profit_per_item / item.min_list_price) * 100).toFixed(1);
        
        const row = `
            <tr>
                <td class="highlight">${item.item}</td>
                <td>${formatCopper(item.market_price)}</td>
                <td>${formatCopper(materialCost)}</td>
                <td class="success">${formatCopper(item.profit_per_item)}</td>
                <td class="success">+${margin}%</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// Render inventory table
function renderInventoryTable() {
    const tbody = document.getElementById('inventory-table');
    tbody.innerHTML = '';

    inventory.forEach(item => {
        const row = `
            <tr>
                <td class="highlight">${item.item}</td>
                <td>${item.count}</td>
                <td>${formatGold(item.value)}</td>
                <td>${item.location}</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// Render sell policy table
function renderSellPolicyTable() {
    const tbody = document.getElementById('sell-policy-table');
    tbody.innerHTML = '';

    sellPolicy.forEach(item => {
        const undercutClass = item.undercut_count > 10 ? 'warning' : 'success';
        
        const row = `
            <tr>
                <td class="highlight">${item.item}</td>
                <td>${formatCopper(item.sell_price)}</td>
                <td class="success">${formatCopper(item.profit_per_item)}</td>
                <td>${item.stack}</td>
                <td class="success">${item.auction_leads}</td>
                <td class="${undercutClass}">${item.undercut_count}</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// Render market snapshot table
function renderMarketSnapshotTable() {
    const tbody = document.getElementById('market-snapshot-table');
    tbody.innerHTML = '';

    marketSnapshot.forEach(auction => {
        const sellerClass = auction.seller === 'Amazona' ? 'highlight' : '';
        
        const row = `
            <tr>
                <td>${auction.item}</td>
                <td class="${sellerClass}">${auction.seller}</td>
                <td>${formatCopper(auction.price_per)}</td>
                <td>${auction.count}</td>
                <td class="success">${formatCopper(auction.buyout)}</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// Setup item selector buttons
function setupItemSelector() {
    const itemBtns = document.querySelectorAll('.item-btn');
    
    itemBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            itemBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            
            const itemName = btn.dataset.item;
            renderPriceChart(itemName);
        });
    });
}

// Initialize on load
document.addEventListener('DOMContentLoaded', loadData);
