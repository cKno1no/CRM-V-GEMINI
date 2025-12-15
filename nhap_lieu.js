document.addEventListener('DOMContentLoaded', function() {
    
    // --- VARIABLES ---
    let allDefaults = {};
    let activeEditorId = 'editor_noi_dung_1'; 
    let activeTagGroup = '1'; 
    let searchTimeout = null;

    // --- ELEMENTS ---
    const inpSearchKh = document.getElementById('kh_ten_tat');
    const khSearchResults = document.getElementById('kh_search_results');
    const ddlLoaiBaoCao = document.getElementById('ddl_loai_bao_cao');
    const reportForm = document.getElementById('reportForm');

    // --- 1. CUSTOMER SEARCH LOGIC ---
    if(inpSearchKh) {
        inpSearchKh.addEventListener('input', function() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(timKhachHang, 300);
        });
        
        inpSearchKh.addEventListener('keydown', function(e) { 
            if (e.key === 'Enter') { e.preventDefault(); timKhachHang(); } 
        });
    }

    if(khSearchResults) {
        khSearchResults.addEventListener('change', chonKhachHang);
    }

    function timKhachHang() {
        const ten_tat = inpSearchKh.value.trim();
        khSearchResults.style.display = 'none'; 
        khSearchResults.innerHTML = '';
        
        if (ten_tat.length < 2) return;
        
        fetch(`/sales/api/khachhang/${encodeURIComponent(ten_tat)}`)
            .then(r => r.json())
            .then(data => {
                if (data && data.length > 0) {
                    data.forEach(kh => {
                        const option = document.createElement('option');
                        option.value = kh.ID; 
                        option.textContent = `${kh.FullName} (${kh.ID})`;
                        option.setAttribute('data-fullname', kh.FullName);
                        option.setAttribute('data-diachi', kh.Address || 'N/A');
                        khSearchResults.appendChild(option);
                    });
                    khSearchResults.style.display = 'block'; 
                    khSearchResults.size = Math.min(data.length, 5) + 1;
                }
            }).catch(console.error);
    }

    function chonKhachHang() {
        if (khSearchResults.selectedIndex === -1) return;
        const selectedOption = khSearchResults.options[khSearchResults.selectedIndex];
        if (selectedOption) {
            const maDoiTuong = selectedOption.value;
            document.getElementById('kh_ma_doi_tuong').value = maDoiTuong;
            document.getElementById('kh_ten_day_du').value = selectedOption.getAttribute('data-fullname');
            document.getElementById('ref_diachi').textContent = selectedOption.getAttribute('data-diachi');
            khSearchResults.style.display = 'none';
            
            // Trigger extra fetches
            fetchReferenceCount(maDoiTuong); 
            fetchNhansuDropdownData(maDoiTuong); 
        }
    }

    function fetchReferenceCount(maDoiTuong) {
        const nlhObject = document.getElementById('ref_nlh_count');
        if(nlhObject) nlhObject.textContent = '...';
        fetch(`/api/khachhang/ref/${maDoiTuong}`)
            .then(r => r.json())
            .then(data => { 
                if(nlhObject) nlhObject.textContent = `Đã liên hệ ${data.CountNLH || 0} người.`; 
            })
            .catch(() => { if(nlhObject) nlhObject.textContent = `Lỗi tải.`; });
    }

    // --- 2. STAFF DROPDOWN LOGIC ---
    function fetchNhansuDropdownData(maDoiTuong) {
        fetch(`/api/nhansu_ddl_by_khachhang/${maDoiTuong}`)
            .then(r => r.json())
            .then(data => {
                const ddl1 = document.getElementById('nhansu_hengap_1');
                const ddl2 = document.getElementById('nhansu_hengap_2');
                if(!ddl1 || !ddl2) return;

                ddl1.innerHTML = '<option value="">-- Chọn Nhân sự --</option>';
                ddl2.innerHTML = '<option value="">-- Chọn (Không bắt buộc) --</option>';
                
                if (data && data.length > 0) {
                    data.forEach(item => {
                        ddl1.appendChild(new Option(item.text, item.id));
                        ddl2.appendChild(new Option(item.text, item.id));
                    });
                }
            }).catch(console.error);
    }

    // Make global for onclick in HTML
    window.openNhansuForm = function(event) {
        event.preventDefault();
        const maDoiTuong = document.getElementById('kh_ma_doi_tuong').value;
        if (maDoiTuong.trim() !== '') { 
            window.open(`/nhansu_nhaplieu?kh_code=${maDoiTuong}`, '_blank'); 
        } else { 
            alert("Vui lòng chọn Khách hàng trước."); 
        }
    };

    // --- 3. EDITOR & TAG LOGIC ---
    
    // Create Tag Button DOM
    function createTagButton(tagName, tagTemplate, isGlobal = false) {
        const btn = document.createElement('div');
        btn.className = isGlobal ? 'tag-btn global-tag' : 'tag-btn';
        const icon = isGlobal ? 'fa-star' : 'fa-plus-circle';
        btn.innerHTML = `<i class="fas ${icon}"></i> ${tagName}`;
        
        btn.onclick = function() {
            injectHeaderOnly(tagName);
            updateSuggestionPreview(tagTemplate);
        };
        return btn;
    }

    // Insert Header into ContentEditable
    function injectHeaderOnly(tagName) {
        const editor = document.getElementById(activeEditorId);
        if (!editor) return;

        const htmlTemplate = `
            <div class="injected-header-p" contenteditable="false">
                <strong style="flex-grow: 1;">${tagName}:</strong>
                <span class="delete-header-btn" onclick="this.parentElement.nextElementSibling?.remove(); this.parentElement.remove();">&times;</span>
            </div>
            <div><br></div> 
        `;
        
        editor.focus();
        
        // Move cursor to end
        const range = document.createRange();
        range.selectNodeContents(editor);
        range.collapse(false); 
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);

        // Insert
        document.execCommand('insertHTML', false, htmlTemplate);
        editor.scrollTop = editor.scrollHeight;
    }

    function updateSuggestionPreview(tagTemplate) {
        const previewContent = document.getElementById('suggestion-preview-content');
        if(!previewContent) return;

        const templateLines = (tagTemplate || '').split('\n').filter(line => line.trim() !== '');
        
        if (templateLines.length === 0) {
            previewContent.innerHTML = '<p class="text-muted small">Không có nội dung gợi ý.</p>';
            return;
        }

        let html = '<ul style="padding-left: 20px; margin-bottom: 0;">';
        templateLines.forEach(line => {
            let clean = line.trim();
            if (clean.startsWith('*')) clean = clean.substring(1).trim();
            clean = clean.replace(/\\F058/gi, '').trim();
            clean = clean.replace(/"/g, '');
            html += `<li>${clean}</li>`;
        });
        html += '</ul>';
        previewContent.innerHTML = html;
    }

    // --- 4. CONFIG & DROPDOWNS ---
    
    function resetTabsAndEditors() {
        const tabA = document.getElementById('lbl_tab_A');
        const tabB = document.getElementById('lbl_tab_B');
        const tabC = document.getElementById('lbl_tab_C');
        if(tabA) tabA.textContent = 'Tab A';
        if(tabB) tabB.textContent = 'Tab B';
        if(tabC) tabC.textContent = 'Tab C';

        const liB = document.getElementById('li_tab_b');
        const liC = document.getElementById('li_tab_c');
        if(liB) liB.classList.remove('hidden-field');
        if(liC) liC.classList.remove('hidden-field');
        
        document.getElementById('editor_noi_dung_1').innerHTML = '';
        document.getElementById('editor_noi_dung_2').innerHTML = '';
        document.getElementById('editor_danh_gia_1').innerHTML = '';

        updateDropdown(null, '4', 'noi_dung_4', 'Mục đích *');
        updateDropdown(null, '5', 'noi_dung_5', 'Kết quả *');
        updateDropdown(null, '6', 'danh_gia_4', 'Hành động *');
    }

    function updateDropdown(prefix, grp, name, defLabel) {
        const sel = document.querySelector(`select[name="${name}"]`);
        if(!sel) return;
        
        const parentDiv = sel.closest('div[class^="col-"]');
        const lbl = parentDiv.querySelector('label');
        
        sel.innerHTML = '<option value="">-- Vui lòng chọn --</option>';
        if (lbl) lbl.textContent = defLabel;
        
        if (!prefix) { parentDiv.classList.add('hidden-field'); return; }

        const labelKey = prefix + grp + '1H';
        if (allDefaults[labelKey] && lbl) lbl.textContent = allDefaults[labelKey] + ' *';

        const opts = Object.keys(allDefaults).filter(k => k.length === 4 && k.startsWith(prefix + grp) && k.endsWith('M')).sort();
        
        if (opts.length === 0) { parentDiv.classList.add('hidden-field'); return; }
        
        parentDiv.classList.remove('hidden-field');
        opts.forEach(k => {
            const s = allDefaults[k];
            if (s) {
                const p = s.split(':');
                const val = p.length > 1 ? p[1] : p[0];
                sel.appendChild(new Option(val, p[0]));
            }
        });
    }

    function updateTagPool(prefix) {
        const sidebar = document.getElementById('tag-pool-sidebar-body');
        sidebar.innerHTML = '';
        if (!prefix) { sidebar.innerHTML = '<p class="text-muted small text-center mt-4">Vui lòng chọn Loại Báo cáo.</p>'; return; }
        
        let hasTags = false;
        // Global Tag
        const gM = prefix + '00M'; const gH = prefix + '00H';
        if (allDefaults[gM] || allDefaults[gH]) {
            sidebar.appendChild(createTagButton(allDefaults[gH] || "Việc quan trọng", allDefaults[gM], true));
            hasTags = true;
        }
        
        // Tab Tags
        const keys = Object.keys(allDefaults).filter(k => k.length === 4 && k.startsWith(prefix) && k.endsWith('H') && k.substring(1, 3) !== '00');
        keys.forEach(k => {
            const group = k.substring(1, 2);
            if (group === activeTagGroup) {
                const mKey = k.replace('H', 'M');
                sidebar.appendChild(createTagButton(allDefaults[k], allDefaults[mKey], false));
                hasTags = true;
            }
        });
        
        if (!hasTags) sidebar.innerHTML = '<p class="text-muted small text-center mt-4">Không có gợi ý.</p>';
    }

    function updateTabs(prefix) {
        const tabB = document.getElementById('li_tab_b');
        const tabC = document.getElementById('li_tab_c');
        const keys = Object.keys(allDefaults).filter(k => k.length === 3 && k.startsWith(prefix) && (k.endsWith('AH') || k.endsWith('BH') || k.endsWith('CH')));
        let hasB = false, hasC = false;
        
        keys.forEach(k => {
            const stt = k.substring(1, 2);
            const lbl = document.getElementById('lbl_tab_' + stt);
            if (lbl) lbl.textContent = allDefaults[k];
            if (stt === 'B') hasB = true;
            if (stt === 'C') hasC = true;
        });
        
        if (!hasB && tabB) tabB.classList.add('hidden-field');
        if (!hasC && tabC) tabC.classList.add('hidden-field');
    }

    // --- EVENTS ---
    if(ddlLoaiBaoCao) {
        ddlLoaiBaoCao.addEventListener('change', function() {
            const prefix = this.value;
            const sidebar = document.getElementById('tag-pool-sidebar-body');
            allDefaults = {}; 
            resetTabsAndEditors(); 
            sidebar.innerHTML = '<p class="text-muted small text-center mt-4"><i class="fas fa-spinner fa-spin"></i> Đang tải...</p>';
            
            if (prefix) {
                fetch(`/api/defaults/${prefix}`).then(r => r.json()).then(d => {
                    allDefaults = d;
                    updateTabs(prefix); 
                    updateTagPool(prefix);
                    updateDropdown(prefix, '4', 'noi_dung_4', 'Mục đích *');
                    updateDropdown(prefix, '5', 'noi_dung_5', 'Kết quả *');
                    updateDropdown(prefix, '6', 'danh_gia_4', 'Hành động *');
                }).catch(e => {
                    console.error(e);
                    sidebar.innerHTML = '<p class="text-danger small text-center">Lỗi kết nối.</p>';
                });
            } else { updateTagPool(''); }
        });
    }

    // Tab Change
    document.querySelectorAll('button[data-bs-toggle="pill"]').forEach(t => {
        t.addEventListener('shown.bs.tab', function(e) {
            const id = e.target.id;
            if (id === 'tab-a-tab') { activeEditorId = 'editor_noi_dung_1'; activeTagGroup = '1'; }
            else if (id === 'tab-b-tab') { activeEditorId = 'editor_noi_dung_2'; activeTagGroup = '2'; }
            else { activeEditorId = 'editor_danh_gia_1'; activeTagGroup = '3'; }
            
            if(ddlLoaiBaoCao) updateTagPool(ddlLoaiBaoCao.value);
        });
    });

    // Form Submit (Map Editor -> Hidden)
    if(reportForm) {
        reportForm.addEventListener('submit', function(e) {
            try {
                document.getElementById('hidden_danh_gia_2').value = document.getElementById('editor_noi_dung_1').innerHTML;
                document.getElementById('hidden_noi_dung_2').value = document.getElementById('editor_noi_dung_2').innerHTML;
                document.getElementById('hidden_noi_dung_1').value = document.getElementById('editor_danh_gia_1').innerHTML;
            } catch (err) { e.preventDefault(); alert("Lỗi lấy nội dung."); }
        });
    }

    // Init
    resetTabsAndEditors();
    updateTagPool('');
});