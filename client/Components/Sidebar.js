document.addEventListener("DOMContentLoaded", function () {
    const sidebarHTML = `
    <button id="hamburger-btn" class="hamburger-btn">☰</button>
    <div id="sidebar-overlay" class="sidebar-overlay"></div>
    <div id="sidebar-container" class="sidebar-container">
        <div class="sidebar-header">
            <h2>Menu</h2>
            <button id="close-btn" class="close-btn">✖</button>
        </div>
        <nav class="sidebar-nav">
            <a href="/" class="nav-item">📊 Dashboard</a>
            <a href="/cleaner" class="nav-item">🚚 Cleaner</a>
            
            <div class="nav-dropdown">
                <button class="nav-item dropdown-btn" id="audit-dropdown-btn">🛰️ Audit Corner ▾</button>
                <div class="dropdown-container" id="audit-dropdown-content">
                    <a href="/audit/mcd" class="sub-nav-item">↳ MCD Audit</a>
                    <a href="/audit/alt-vehicle" class="sub-nav-item">↳ Alt Vehicle</a>
                    <a href="/audit/toll" class="sub-nav-item">↳ Toll Trips</a>
                    <a href="/b2b-maker" class="nav-item">🔗 B2B Maker</a>
                    <a href="/audit/gps" class="sub-nav-item">↳ Gps Audit</a>
                    <a href="/audit/incomplete" class="sub-nav-item">↳ Incomplete</a>
                </div>
            </div>

            <a href="/locality-manager" class="nav-item">🏙️ Locality Manager</a>
            <a href="/operation-manager" class="nav-item">⬇️ Downloads</a>
            <a href="/admin" class="nav-item">🔧 Admin</a>
            <a href="/logout" class="nav-item logout-item">🚪 Logout</a>
        </nav>
    </div>
    `;

    document.body.insertAdjacentHTML('afterbegin', sidebarHTML);

    const hamburgerBtn = document.getElementById('hamburger-btn');
    const closeBtn = document.getElementById('close-btn');
    const overlay = document.getElementById('sidebar-overlay');
    const sidebar = document.getElementById('sidebar-container');
    const auditBtn = document.getElementById('audit-dropdown-btn');
    const auditContent = document.getElementById('audit-dropdown-content');

    // 1. Toggle Sidebar Logic
    function toggleSidebar() {
        sidebar.classList.toggle('open');
        overlay.classList.toggle('active');
    }
    function closeSidebar() {
        sidebar.classList.remove('open');
        overlay.classList.remove('active');
    }

    // 2. Dropdown Logic
    if (auditBtn) {
        auditBtn.addEventListener('click', () => {
            auditContent.classList.toggle('show');
            auditBtn.classList.toggle('dropdown-active');
        });
    }

    if (hamburgerBtn) hamburgerBtn.addEventListener('click', toggleSidebar);
    if (closeBtn) closeBtn.addEventListener('click', closeSidebar);
    if (overlay) overlay.addEventListener('click', closeSidebar);

    // 3. Highlight Active Link
    const currentPath = window.location.pathname;
    document.querySelectorAll('.nav-item, .sub-nav-item').forEach(link => {
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
            // If it's a sub-item, open the parent dropdown automatically
            if (link.classList.contains('sub-nav-item')) {
                auditContent.classList.add('show');
            }
        }
    });
});