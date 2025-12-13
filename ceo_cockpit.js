/**
 * static/js/ceo_cockpit.js
 * Logic hiển thị biểu đồ và tương tác cho CEO Cockpit
 */

document.addEventListener('DOMContentLoaded', function() {
    // 1. LẤY DỮ LIỆU TỪ DOM (Data Context)
    const contextEl = document.getElementById('ceo-context');
    if (!contextEl) {
        console.error("Lỗi: Không tìm thấy phần tử #ceo-context");
        return;
    }

    let invData, financialData, catData, funnelData;

    try {
        invData = JSON.parse(contextEl.dataset.inventory || '{}');
        financialData = JSON.parse(contextEl.dataset.financial || '{}');
        catData = JSON.parse(contextEl.dataset.category || '{}');
        funnelData = JSON.parse(contextEl.dataset.funnel || '{}');
    } catch (e) {
        console.error("Lỗi parse dữ liệu biểu đồ:", e);
        return;
    }

    // 2. KHỞI TẠO CÁC BIỂU ĐỒ (RENDER CHARTS)
    
    // --- Chart 1: Sức khỏe Tồn kho (Donut) ---
    if (invData.series && invData.series.length > 0) {
        var invOptions = {
            series: invData.series,
            labels: invData.labels,
            chart: {
                type: 'donut', height: 320, fontFamily: 'Inter, sans-serif',
                events: {
                    dataPointSelection: function(event, chartContext, config) {
                        const label = config.w.config.labels[config.dataPointIndex];
                        const details = invData.drilldown ? invData.drilldown[label] : [];
                        showInventoryModal(label, details);
                    }
                }
            },
            colors: ['#05CD99', '#4318FF', '#FFB547', '#FF8F00', '#E31A1A'],
            plotOptions: {
                pie: { 
                    donut: { 
                        size: '60%', 
                        labels: { 
                            show: true, 
                            total: { 
                                show: true, 
                                label: 'Tổng Tồn', 
                                formatter: (w) => {
                                    const total = w.globals.seriesTotals.reduce((a, b) => a + b, 0);
                                    return (total / 1000000).toFixed(0) + 'M';
                                } 
                            } 
                        } 
                    } 
                }
            },
            dataLabels: { enabled: false },
            legend: { position: 'bottom', fontSize: '11px' },
            tooltip: { y: { formatter: (val) => new Intl.NumberFormat('vi-VN').format(val) + " VNĐ" } }
        };
        new ApexCharts(document.querySelector("#inventoryChart"), invOptions).render();
    }

    // --- Chart 2: Hiệu quả Nhóm hàng (Mixed) ---
    if (catData.revenue && catData.revenue.length > 0) {
        var catOptions = {
            series: [
                { name: 'Doanh thu', type: 'column', data: catData.revenue.map(x=>(x/1000000).toFixed(0)) },
                { name: 'Lợi nhuận gộp', type: 'column', data: catData.profit.map(x=>(x/1000000).toFixed(0)) },
                { name: '% Margin', type: 'line', data: catData.margin }
            ],
            chart: { height: 320, type: 'line', toolbar: {show:false}, fontFamily: 'Inter' },
            stroke: { width: [0, 0, 3], curve: 'smooth' },
            colors: ['#4318FF', '#05CD99', '#FFB547'],
            xaxis: { 
                categories: catData.categories,
                labels: { rotate: -45, style: { fontSize: '10px' } }
            },
            yaxis: [
                { title: {text: 'Triệu VNĐ'}, labels: {formatter: (val) => val} }, 
                { opposite: true, title: {text: '%'}, labels: {formatter: (val) => val} }
            ],
            plotOptions: { bar: { columnWidth: '60%', borderRadius: 3 } },
            dataLabels: { 
                enabled: true, 
                enabledOnSeries: [2], 
                formatter: (val) => val + "%",
                style: { colors: ['#B71C1C'] } 
            },
            legend: { position: 'top' },
            tooltip: {
                shared: true, intersect: false,
                y: {
                    formatter: function (y, { seriesIndex }) {
                        if(seriesIndex === 2) return y + " %";
                        return new Intl.NumberFormat('vi-VN').format(y) + " Triệu";
                    }
                }
            }
        };
        new ApexCharts(document.querySelector("#categoryChart"), catOptions).render();
    }

    // --- Chart 3: Xu hướng Tài chính (Multi-Axis) ---
    if (financialData.revenue && financialData.revenue.length > 0) {
        var finOptions = {
            series: [
                { name: 'Doanh thu', type: 'column', data: financialData.revenue },
                { name: 'Chi phí', type: 'column', data: financialData.expenses },
                { name: 'Lợi nhuận Ròng', type: 'line', data: financialData.net_profit }
            ],
            chart: { height: 350, type: 'line', toolbar: {show:false}, fontFamily: 'Inter' },
            stroke: { width: [0, 0, 4], curve: 'smooth' },
            colors: ['#4318FF', '#FFB547', '#05CD99'], 
            xaxis: { categories: financialData.categories },
            yaxis: { title: { text: 'Tỷ VNĐ' } },
            plotOptions: { bar: { columnWidth: '55%', borderRadius: 2 } },
            dataLabels: { enabled: false },
            legend: { position: 'top' },
            tooltip: { y: { formatter: (val) => val + " Tỷ" } }
        };
        new ApexCharts(document.querySelector("#financialTrendChart"), finOptions).render();
    }

    // --- Chart 4: Phễu Kinh doanh (Bar) ---
    if (funnelData.quotes && funnelData.quotes.length > 0) {
        var funnelOptions = {
            series: [
                { name: 'Số lượng Chào giá', type: 'column', data: funnelData.quotes },
                { name: 'Đơn hàng thành công', type: 'column', data: funnelData.orders },
                { name: 'Doanh số (Tỷ)', type: 'line', data: funnelData.revenue }
            ],
            chart: { height: 350, type: 'line', toolbar: {show:false}, fontFamily: 'Inter' },
            stroke: { width: [0, 0, 3], curve: 'monotoneCubic' },
            colors: ['#A3AED0', '#4318FF', '#05CD99'], 
            xaxis: { categories: funnelData.categories },
            yaxis: [
                { title: { text: 'Số lượng Phiếu' }, labels: { formatter: (val) => val.toFixed(0) } },
                { opposite: true, title: { text: 'Doanh số (Tỷ)' }, labels: { formatter: (val) => val } }
            ],
            plotOptions: { bar: { columnWidth: '60%', borderRadius: 2 } },
            legend: { position: 'top' },
            tooltip: {
                shared: true, intersect: false,
                y: {
                    formatter: function (y, { seriesIndex }) {
                        if(seriesIndex === 2) return y + " Tỷ";
                        return y + " Phiếu";
                    }
                }
            }
        };
        new ApexCharts(document.querySelector("#funnelChart"), funnelOptions).render();
    }
});

// --- HELPER FUNCTIONS (EXPOSED TO GLOBAL IF NEEDED) ---

// Hàm hiển thị Modal Drill-down Tồn kho
function showInventoryModal(title, items) {
    document.getElementById('invModalTitle').innerText = title;
    const tbody = document.querySelector('#invDetailTable tbody');
    tbody.innerHTML = '';
    
    if(items && items.length > 0) {
        items.forEach(i => {
            const row = `<tr>
                <td class="ps-4 fw-medium">${i.name}</td>
                <td class="text-end pe-4 fw-bold font-monospace">${new Intl.NumberFormat('vi-VN').format(i.value)}</td>
            </tr>`;
            tbody.insertAdjacentHTML('beforeend', row);
        });
    } else {
        tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted py-3">Không có dữ liệu chi tiết.</td></tr>';
    }
    
    new bootstrap.Modal(document.getElementById('inventoryDetailModal')).show();
}