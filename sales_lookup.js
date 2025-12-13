/**
 * static/js/sales_lookup.js
 * Logic xử lý cho Dashboard Tra Cứu Bán Hàng
 */

// Biến toàn cục
let block1Table = null;
let backorderModalInstance = null;

// Hàm định dạng tiền tệ
function formatCurrency(num) {
    return new Intl.NumberFormat('vi-VN').format(num || 0);
}

// Hàm khởi tạo DataTables cho Block 1
function initializeBlock1DataTable() {
    // Hủy instance cũ nếu tồn tại
    if ($.fn.DataTable.isDataTable('#block1-table')) {
        $('#block1-table').DataTable().destroy();
    }

    block1Table = $('#block1-table').DataTable({
        "paging": true,
        "pageLength": 15,
        "lengthChange": false, // Ẩn "Show X entries"
        "searching": true,
        "ordering": true,
        "info": true,
        "order": [[0, 'asc']],
        // DOM: Info, Filter, Table, Pagination
        "dom": '<"row"<"col-sm-12 col-md-6"i><"col-sm-12 col-md-6"f>>' +
               '<"row"<"col-sm-12"tr>>' +
               '<"row"<"col-sm-12"p>>',
        "language": {
            "search": "Lọc nhanh bảng này:",
            "zeroRecords": "Không tìm thấy mặt hàng nào.",
            "info": "Trang _PAGE_/_PAGES_ (Tổng _TOTAL_ mặt hàng)",
            "infoEmpty": "Không có dữ liệu",
            "infoFiltered": "",
            "paginate": { "next": "Sau", "previous": "Trước" },
            "lengthMenu": "Hiển thị _MENU_ dòng"
        }
    });

    // Di chuyển ô lọc vào vị trí tùy chỉnh trên giao diện
    const searchInput = $('#block1-table_filter');
    $('#block1-filter-toolbar').html(searchInput);
    $('#block1-table_filter input').addClass('form-control-sm');
}

// Hàm debounce để hạn chế gọi API liên tục khi gõ phím
function debounce(func, timeout = 300) {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => { func.apply(this, args); }, timeout);
    };
}

// Hàm tìm kiếm khách hàng (Autocomplete)
function timKhachHang() {
    const term = $('#kh_search_input').val().trim();
    const statusMsg = $('#kh_status');
    const resultsDropdown = $('#kh_search_results');

    resultsDropdown.hide().empty();
    $('#kh_ma_selected').val('');
    $('#kh_display_selected').val('');

    if (term.length < 2) {
        statusMsg.text('Nhập ít nhất 2 ký tự.');
        return;
    }

    statusMsg.text('Đang tìm kiếm...');

    fetch(`/sales/api/khachhang/${encodeURIComponent(term)}`)
        .then(response => response.json())
        .then(data => {
            if (data && data.length > 0) {
                resultsDropdown.empty(); // Xóa cũ
                data.forEach(kh => {
                    const option = document.createElement('option');
                    option.value = kh.ID;
                    option.textContent = `${kh.FullName} (${kh.ID})`;
                    // Lưu data vào dataset để dùng khi chọn
                    option.dataset.fullname = kh.FullName;
                    resultsDropdown.append(option);
                });
                resultsDropdown.show().attr('size', Math.max(Math.min(data.length, 5), 2));
                statusMsg.html(`Tìm thấy ${data.length} kết quả. Vui lòng chọn.`);
            } else {
                statusMsg.text('Không tìm thấy kết quả nào.');
            }
        })
        .catch(error => {
            console.error(error);
            statusMsg.text(`Lỗi tra cứu: ${error.message}`);
        });
}

// Hàm chọn khách hàng từ Dropdown
// Gán vào window để HTML có thể gọi (vì onclick="chonKhachHang()" trong HTML)
window.chonKhachHang = function() {
    const selectedOption = $('#kh_search_results').find('option:selected');

    if (selectedOption.length) {
        const id = selectedOption.val();
        const fullName = selectedOption.data('fullname') || selectedOption.attr('data-fullname');
        const displayText = id + ' - ' + fullName;

        $('#kh_search_results').hide();
        $('#kh_ma_selected').val(id);
        $('#kh_display_selected').val(displayText);
        $('#kh_search_input').val(displayText);
        $('#kh_status').html('✅ Khách hàng đã được chọn và xác nhận.');
    }
};

// Hàm hiển thị Modal BackOrder
window.showBackorderModal = function(inventoryId, inventoryName) {
    if (!backorderModalInstance) {
        backorderModalInstance = new bootstrap.Modal(document.getElementById('backorderModal'));
    }

    const modalTitle = document.getElementById('backorderModalTitle');
    const modalBody = document.getElementById('backorderModalBody');

    modalTitle.innerText = `Chi tiết BackOrder: ${inventoryName} (${inventoryId})`;
    modalBody.innerHTML = '<p class="text-center"><i class="fas fa-spinner fa-spin me-2"></i> Đang tải chi tiết PO...</p>';

    backorderModalInstance.show();

    fetch(`/sales/api/backorder_details/${inventoryId}`)
        .then(response => {
            if (!response.ok) throw new Error('Không thể tải dữ liệu BackOrder.');
            return response.json();
        })
        .then(data => {
            if (!data || data.length === 0) {
                modalBody.innerHTML = '<p class="alert alert-info text-center">Không có dữ liệu BackOrder (PO) nào cho mã hàng này.</p>';
                return;
            }

            let tableHtml = '<div class="table-responsive"><table class="table table-sm table-striped table-bordered small mb-0">';
            tableHtml += '<thead class="table-primary"><tr>';
            tableHtml += '<th>Mã PO (VoucherNo)</th>';
            tableHtml += '<th>Ngày PO (OrderDate)</th>';
            tableHtml += '<th class="text-end">SL Còn (con)</th>';
            tableHtml += '<th>Ngày Dự kiến Về (ShipDate)</th>';
            tableHtml += '</tr></thead><tbody>';

            data.forEach(item => {
                tableHtml += `<tr>
                                <td>${item.VoucherNo}</td>
                                <td>${item.OrderDate}</td>
                                <td class="text-end fw-bold">${formatCurrency(item.con)}</td>
                                <td class="fw-bold ${item.ShipDate === '—' ? 'text-danger' : 'text-success'}">${item.ShipDate}</td>
                              </tr>`;
            });

            tableHtml += '</tbody></table></div>';
            modalBody.innerHTML = tableHtml;
        })
        .catch(error => {
            modalBody.innerHTML = `<p class="alert alert-danger text-center">Lỗi: ${error.message}</p>`;
        });
};

// --- MAIN INITIALIZATION ---
$(document).ready(function() {

    // 1. Kiểm tra trạng thái Khách hàng ban đầu
    if ($('#kh_display_selected').val()) {
         $('#kh_status').html('✅ Khách hàng đã được chọn và xác nhận.');
    }

    // 2. Gắn sự kiện Input tìm KH
    $('#kh_search_input').on('input', debounce(timKhachHang, 300));

    // 3. Chặn Enter trên các ô input search (tránh submit form ngoài ý muốn)
    $('#item_search_input, #kh_search_input').on('keydown', function(e) {
         if (e.key === 'Enter') { e.preventDefault(); }
    });

    // 4. Ẩn dropdown khi click ra ngoài
    $(document).on('click', function(e) {
        if (!$(e.target).closest('.search-container').length) {
            $('#kh_search_results').hide();
        }
    });

    // 5. Khởi tạo DataTables nếu có dữ liệu
    if ($('#block1-table tbody tr').length > 0) {
        initializeBlock1DataTable();
    }

    // 6. Custom Search cho DataTables (Lọc Tồn > 0)
    $.fn.dataTable.ext.search.push(
        function(settings, data, dataIndex) {
            if (settings.nTable.id !== 'block1-table') {
                return true;
            }
            const showOnlyStock = $('#toggleTonKho').is(':checked');
            if (!showOnlyStock) {
                return true;
            }
            // Cột Tồn kho là cột index 2
            const tonKhoStr = data[2] || '0';
            const tonKho = parseFloat(tonKhoStr.replace(/\./g, '').replace(/,/g, '')) || 0; // Fix parse cho cả dấu chấm/phẩy
            return tonKho > 0;
        }
    );

    // Sự kiện toggle switch Tồn kho
    $('#toggleTonKho').on('change', function() {
        if (block1Table) {
            block1Table.draw();
        }
    });

    // 7. Xử lý nút "Tra nhanh Tồn" (Gọi API Multi Lookup)
    $('#quickLookupBtn').on('click', function() {
        const itemSearchTerm = $('#item_search_input').val().trim();
        const button = $(this);

        if (!itemSearchTerm) {
            alert('Vui lòng nhập Tên hoặc Mã Mặt hàng để tra cứu nhanh.');
            return;
        }

        button.prop('disabled', true).html('<i class="fas fa-spinner fa-spin"></i>');

        // Ẩn Block 2, 3 và hiện Block 1
        $('#block2-container, #block3-container').hide();
        $('#block1-container').show();

        const formData = new FormData();
        formData.append('item_search', itemSearchTerm);

        fetch('/sales/api/multi_lookup', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (!response.ok) { return response.json().then(err => { throw new Error(err.message || 'Lỗi server'); }); }
            return response.json();
        })
        .then(data => {
            // Hủy bảng cũ nếu có
            if (block1Table) {
                block1Table.destroy();
                $('#block1-filter-toolbar').empty();
                block1Table = null;
            }

            const tbody = $('#block1-table tbody');
            tbody.empty();

            $('#block1-container .card-header-custom span').text(`1. Tra cứu Tồn kho Nhanh (Khớp với '${itemSearchTerm}')`);

            if (data && data.length > 0) {
                data.forEach(item => {
                    // Tạo dòng HTML
                    const row = $(`<tr style="cursor: pointer;">
                        <td class="text-start">${item.InventoryID}</td>
                        <td class="text-start">${item.InventoryName}</td>
                        <td class="text-end">${formatCurrency(item.Ton)}</td>
                        <td class="text-end">${formatCurrency(item.BackOrder)}</td>
                        <td class="text-end">${formatCurrency(item.GiaBanQuyDinh)}</td>
                        <td class="text-end">—</td>
                        <td class="text-end">—</td>
                    </tr>`);

                    // Gắn sự kiện click
                    row.on('click', function() {
                        window.showBackorderModal(item.InventoryID, item.InventoryName);
                    });

                    tbody.append(row);
                });
            } else {
                tbody.append('<tr><td colspan="7" class="text-center">Không tìm thấy dữ liệu tra cứu nhanh.</td></tr>');
            }

            // Re-init DataTable
            initializeBlock1DataTable();
        })
        .catch(error => {
            alert('Lỗi Tra cứu nhanh: ' + error.message);
        })
        .finally(() => {
            button.prop('disabled', false).html('<i class="fas fa-bolt"></i> Tra nhanh Tồn');
        });
    });

    // 8. Chặn in ấn/Copy (Security)
    document.addEventListener('keydown', function(e) {
        if ((e.ctrlKey || e.metaKey) && e.key === 'p') {
            e.preventDefault();
            e.stopPropagation();
            alert("Chức năng in đã bị khóa trên trang này.");
        }
    }, true);

    window.print = function() {
        alert("Chức năng in đã bị khóa trên trang này.");
    };

    document.addEventListener('contextmenu', e => e.preventDefault());
    document.addEventListener('copy', e => { e.preventDefault(); alert("Sao chép nội dung bị cấm."); });

});