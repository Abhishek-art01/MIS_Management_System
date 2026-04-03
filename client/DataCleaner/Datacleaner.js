/**
 * Datacleaner.js — handles file upload, processing and result display.
 */
(function () {
  "use strict";

  // ── DOM refs ─────────────────────────────────────────────────────────────
  const dropZone   = document.getElementById("drop-zone");
  const fileInput  = document.getElementById("file-input");
  const fileList   = document.getElementById("file-list");
  const actionRow  = document.getElementById("action-row");
  const processBtn = document.getElementById("process-btn");
  const clearBtn   = document.getElementById("clear-btn");
  const btnLabel   = document.getElementById("btn-label");
  const btnSpinner = document.getElementById("btn-spinner");
  const resultArea = document.getElementById("result-area");
  const countBadge = document.getElementById("file-count-badge");
  const dropTitle  = document.getElementById("drop-title");
  const dropSub    = document.getElementById("drop-sub");
  const dropIcon   = document.getElementById("drop-icon");

  let files = [];

  // ── Helpers ───────────────────────────────────────────────────────────────
  function fmtSize(bytes) {
    if (bytes < 1024)       return bytes + " B";
    if (bytes < 1024 ** 2)  return (bytes / 1024).toFixed(1) + " KB";
    return (bytes / 1024 ** 2).toFixed(1) + " MB";
  }

  function getType() {
    return document.querySelector('input[name="cleanerType"]:checked')?.value ?? "client";
  }

  function isFastag() {
    return getType() === "fastag";
  }

  function setLoading(on) {
    processBtn.disabled = on;
    btnLabel.style.display   = on ? "none"         : "inline";
    btnSpinner.style.display = on ? "inline-block"  : "none";
  }

  // ── Drop zone UI — sync to selected cleaner type ──────────────────────────
  function updateDropZoneUI() {
    if (isFastag()) {
      fileInput.accept  = ".pdf";
      fileInput.multiple = true;
      dropTitle.textContent = "Drop PDF files here";
      dropSub.innerHTML = `or <span class="drop-link">browse to upload</span> &nbsp;·&nbsp; .pdf supported`;
      dropIcon.textContent  = "◈";
    } else {
      fileInput.accept   = ".xlsx,.xls";
      fileInput.multiple = true;
      dropTitle.textContent = "Drop Excel files here";
      dropSub.innerHTML = `or <span class="drop-link">browse to upload</span> &nbsp;·&nbsp; .xlsx, .xls supported`;
      dropIcon.textContent  = "↑";
    }
  }

  // ── Cleaner type change — clear queue + refresh UI ────────────────────────
  document.querySelectorAll('input[name="cleanerType"]').forEach(radio => {
    radio.addEventListener("change", () => {
      files = [];
      renderFiles();
      resultArea.innerHTML = "";
      updateDropZoneUI();
    });
  });

  // ── Render file list ──────────────────────────────────────────────────────
  function renderFiles() {
    if (files.length === 0) {
      fileList.style.display   = "none";
      actionRow.style.display  = "none";
      countBadge.style.display = "none";
      return;
    }
    fileList.style.display   = "flex";
    actionRow.style.display  = "flex";
    countBadge.style.display = "inline-flex";
    countBadge.textContent   = files.length + (files.length === 1 ? " file" : " files");

    fileList.innerHTML = files.map((f, i) => `
      <div class="file-item">
        <span class="file-item-icon">${f.name.match(/\.pdf$/i) ? "◈" : "◈"}</span>
        <span class="file-item-name" title="${f.name}">${f.name}</span>
        <span class="file-item-size">${fmtSize(f.size)}</span>
        <button class="file-item-remove" data-index="${i}" title="Remove">✕</button>
      </div>
    `).join("");

    fileList.querySelectorAll(".file-item-remove").forEach(btn => {
      btn.addEventListener("click", () => {
        files.splice(parseInt(btn.dataset.index), 1);
        renderFiles();
      });
    });
  }

  // ── Add files ─────────────────────────────────────────────────────────────
  function addFiles(incoming) {
    const excelMimes = [
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
    ];
    const pdfMime = "application/pdf";

    for (const f of incoming) {
      const isExcel = excelMimes.includes(f.type) || f.name.match(/\.xlsx?$/i);
      const isPdf   = f.type === pdfMime          || f.name.match(/\.pdf$/i);

      // Gate by cleaner type — silently skip wrong file types
      if (isFastag()) {
        if (!isPdf) continue;
      } else {
        if (!isExcel) continue;
      }

      // Deduplicate by name + size
      if (files.some(x => x.name === f.name && x.size === f.size)) continue;
      files.push(f);
    }
    renderFiles();
    resultArea.innerHTML = "";
  }

  // ── Drop zone events ──────────────────────────────────────────────────────
  dropZone.addEventListener("click",    () => fileInput.click());
  dropZone.addEventListener("dragover", e => { e.preventDefault(); dropZone.classList.add("drag-over"); });
  dropZone.addEventListener("dragleave", ()  => dropZone.classList.remove("drag-over"));
  dropZone.addEventListener("drop", e => {
    e.preventDefault();
    dropZone.classList.remove("drag-over");
    addFiles(Array.from(e.dataTransfer.files));
  });
  fileInput.addEventListener("change", () => {
    addFiles(Array.from(fileInput.files));
    fileInput.value = "";
  });

  clearBtn.addEventListener("click", () => {
    files = [];
    renderFiles();
    resultArea.innerHTML = "";
  });

  // ── Process ───────────────────────────────────────────────────────────────
  processBtn.addEventListener("click", async () => {
    if (files.length === 0) return;

    setLoading(true);
    resultArea.innerHTML = `
      <div class="card" style="margin-top:0;">
        <div class="progress-bar"><div class="progress-fill" id="pbar" style="width:0%"></div></div>
        <p style="margin-top:14px; font-size:.875rem; color:var(--text-muted);">Processing files…</p>
      </div>`;

    const pbar = document.getElementById("pbar");
    let prog = 0;
    const tick = setInterval(() => {
      prog = Math.min(prog + Math.random() * 8, 85);
      pbar.style.width = prog + "%";
    }, 180);

    try {
      const fd = new FormData();
      files.forEach(f => fd.append("files", f));
      fd.append("cleanerType", getType());

      const res = await fetch("/clean-data", { method: "POST", body: fd });
      clearInterval(tick);
      pbar.style.width = "100%";

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        showError(err.detail ?? "Server error");
        return;
      }

      const ct = res.headers.get("content-type") ?? "";

      if (ct.includes("json")) {
        const data = await res.json();
        showSuccess(data);
      } else {
        const blob  = await res.blob();
        const fname = getFilename(res) || "output.xlsx";
        showDownload(blob, fname);
      }

    } catch (e) {
      clearInterval(tick);
      showError(e.message ?? "Network error");
    } finally {
      setLoading(false);
    }
  });

  // ── Result renderers ──────────────────────────────────────────────────────
  function getFilename(response) {
    const cd = response.headers.get("content-disposition") ?? "";
    const m  = cd.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
    return m ? m[1].replace(/['"]/g, "") : null;
  }

  function showError(msg) {
    resultArea.innerHTML = `
      <div class="alert alert-error" style="margin-top:16px;">
        <span>⚠</span>
        <span>${escHtml(msg)}</span>
      </div>`;
  }

  function showSuccess(data) {
    // Check if the backend sent a file_url. If so, create a download button using your existing download API.
    const downloadSection = data.file_url
      ? `<div>
           <div class="type-name">${escHtml(data.file_url)}</div>
           <div class="type-desc">Excel file ready</div>
         </div>
         <a href="/download/${escHtml(data.file_url)}" download="${escHtml(data.file_url)}" class="btn btn-primary btn-sm">
           ↓ Download File
         </a>`
      : `<span class="badge badge-gray">No download file generated</span>`;

    resultArea.innerHTML = `
      <div class="result-card">
        <div class="result-header">
          <div class="section-title" style="margin:0;">Processing Complete</div>
          <span class="badge badge-green">✓ Success</span>
        </div>
        <div class="result-stats">
          ${statBlock("Rows Processed",   data.rows_processed ?? data.total       ?? "—")}
          ${statBlock("Rows Saved",       data.db_rows_added  ?? data.rows_saved  ?? "—")}
          ${statBlock("Addresses Synced", data.new_addresses  ?? "—")}
          ${statBlock("Source",           escHtml(data.source ?? data.cleanerType ?? "—"))}
        </div>
        <div class="result-download">
          ${downloadSection}
        </div>
      </div>`;
  }

  function showDownload(blob, fname) {
    const url    = URL.createObjectURL(blob);
    const sizeKB = (blob.size / 1024).toFixed(1);
    resultArea.innerHTML = `
      <div class="result-card">
        <div class="result-header">
          <div class="section-title" style="margin:0;">File Ready</div>
          <span class="badge badge-green">✓ Done</span>
        </div>
        <div class="result-download">
          <div>
            <div class="type-name">${escHtml(fname)}</div>
            <div class="type-desc">${sizeKB} KB · Excel file</div>
          </div>
          <a href="${url}" download="${escHtml(fname)}" class="btn btn-primary btn-sm" id="dl-btn">
            ↓ Download File
          </a>
        </div>
      </div>`;

    setTimeout(() => document.getElementById("dl-btn")?.click(), 400);
  }

  function statBlock(label, value) {
    return `
      <div class="result-stat">
        <div class="result-stat-val">${value}</div>
        <div class="result-stat-lbl">${label}</div>
      </div>`;
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // ── Init ──────────────────────────────────────────────────────────────────
  updateDropZoneUI();

})();