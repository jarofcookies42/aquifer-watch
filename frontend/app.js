/**
 * AquiferWatch — Frontend Application
 * Leaflet map + API integration for data center & well visualization.
 */

const API = '';  // same origin

// ---------------------------------------------------------------------------
// Map setup
// ---------------------------------------------------------------------------

const map = L.map('map', {
    center: [34.2, -101.3],  // West Texas center
    zoom: 7,
    zoomControl: true,
});

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a>',
    maxZoom: 19,
}).addTo(map);

// Layer groups
const sitesLayer = L.layerGroup().addTo(map);
const wellsLayer = L.layerGroup().addTo(map);
const ercotLayer = L.layerGroup().addTo(map);

// Site marker icons
const siteIcon = (status) => {
    const colors = {
        rumored: '#6b7280',
        filing_detected: '#a855f7',
        permitted: '#f59e0b',
        under_construction: '#3b82f6',
        operational: '#22c55e',
        paused: '#ef4444',
        cancelled: '#374151',
    };
    const color = colors[status] || '#6b7280';
    return L.divIcon({
        className: '',
        html: `<div style="
            width: 18px; height: 18px;
            background: ${color};
            border: 2px solid #fff;
            border-radius: 50%;
            box-shadow: 0 0 8px ${color}88;
        "></div>`,
        iconSize: [18, 18],
        iconAnchor: [9, 9],
    });
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let sites = [];
let activeSiteId = null;

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

async function fetchJSON(url) {
    const resp = await fetch(API + url);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
}

// ---------------------------------------------------------------------------
// Load dashboard stats
// ---------------------------------------------------------------------------

async function loadDashboard() {
    const data = await fetchJSON('/api/dashboard');

    document.getElementById('stat-sites').textContent = data.sites.length;
    document.getElementById('stat-wells').textContent = data.total_wells.toLocaleString();
    document.getElementById('stat-counties').textContent = data.counties_covered;
    document.getElementById('stat-ercot').textContent =
        data.ercot_total_mw ? Math.round(data.ercot_total_mw / 1000) + 'K' : '-';

    return data;
}

// ---------------------------------------------------------------------------
// Load and render sites
// ---------------------------------------------------------------------------

async function loadSites() {
    sites = await fetchJSON('/api/sites');

    sitesLayer.clearLayers();
    const listEl = document.getElementById('site-list');
    listEl.innerHTML = '';

    sites.forEach(site => {
        // Map marker
        if (site.lat && site.lon) {
            const marker = L.marker([site.lat, site.lon], { icon: siteIcon(site.status) })
                .bindPopup(`
                    <strong>${site.name}</strong><br>
                    ${site.operator || 'Unknown operator'}<br>
                    ${site.county} County<br>
                    ${site.capacity_mw ? site.capacity_mw.toLocaleString() + ' MW' : 'Capacity TBD'}<br>
                    <em>${formatStatus(site.status)}</em>
                `)
                .on('click', () => selectSite(site.id));
            sitesLayer.addLayer(marker);
        }

        // Sidebar card
        const card = document.createElement('div');
        card.className = 'site-card';
        card.dataset.siteId = site.id;
        card.innerHTML = `
            <h3>${site.name} <span class="status-badge status-${site.status}">${formatStatus(site.status)}</span></h3>
            <div class="meta">
                <span>${site.county} Co.</span>
                ${site.capacity_mw ? `<span>${site.capacity_mw.toLocaleString()} MW</span>` : ''}
                ${site.operator ? `<span>${site.operator}</span>` : ''}
            </div>
        `;
        card.addEventListener('click', () => selectSite(site.id));
        listEl.appendChild(card);
    });
}

function formatStatus(s) {
    return (s || '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ---------------------------------------------------------------------------
// Site selection — load nearby wells
// ---------------------------------------------------------------------------

async function selectSite(siteId) {
    activeSiteId = siteId;

    // Highlight active card
    document.querySelectorAll('.site-card').forEach(c => {
        c.classList.toggle('active', Number(c.dataset.siteId) === siteId);
    });

    const site = sites.find(s => s.id === siteId);
    if (site && site.lat && site.lon) {
        map.flyTo([site.lat, site.lon], 9, { duration: 1 });
    }

    // Load wells, water level chart, and county water breakdown
    const county = site ? site.county : null;
    await Promise.all([
        loadWells(siteId),
        loadWaterLevelChart(siteId),
        loadCountyWaterBreakdown(county),
    ]);
}

async function loadWells(siteId) {
    wellsLayer.clearLayers();

    const url = siteId != null
        ? `/api/wells/geojson?site_id=${siteId}&limit=2000`
        : '/api/wells/geojson?limit=5000';

    const geojson = await fetchJSON(url);

    L.geoJSON(geojson, {
        pointToLayer: (feature, latlng) => {
            const depth = feature.properties.depth_ft;
            const radius = depth ? Math.min(Math.max(depth / 80, 2), 6) : 3;
            return L.circleMarker(latlng, {
                radius: radius,
                fillColor: wellColor(depth),
                color: '#ffffff22',
                weight: 0.5,
                fillOpacity: 0.7,
            });
        },
        onEachFeature: (feature, layer) => {
            const p = feature.properties;
            layer.bindPopup(`
                <strong>Well ${p.swn}</strong><br>
                ${p.county} County<br>
                Depth: ${p.depth_ft ? p.depth_ft + ' ft' : 'Unknown'}<br>
                Aquifer: ${p.aquifer || 'Ogallala'}<br>
                ${p.distance_mi ? `Distance: ${p.distance_mi} mi from site` : ''}
            `);
        },
    }).addTo(wellsLayer);
}

function wellColor(depthFt) {
    if (!depthFt) return '#6b7280';
    if (depthFt < 150) return '#22d3ee';   // shallow — cyan
    if (depthFt < 300) return '#3b82f6';   // medium — blue
    if (depthFt < 500) return '#6366f1';   // deep — indigo
    return '#a855f7';                       // very deep — purple
}

// ---------------------------------------------------------------------------
// Water level time-series chart (canvas)
// ---------------------------------------------------------------------------

async function loadWaterLevelChart(siteId) {
    const section = document.getElementById('chart-section');

    if (siteId == null) {
        section.style.display = 'none';
        return;
    }

    try {
        const data = await fetchJSON(`/api/water-levels?site_id=${siteId}`);
        if (!data.length) {
            section.style.display = 'none';
            return;
        }

        section.style.display = 'block';
        drawChart(data);
    } catch (err) {
        console.error('Chart error:', err);
        section.style.display = 'none';
    }
}

function drawChart(data) {
    const canvas = document.getElementById('wl-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    // Size canvas to container
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = 180 * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = '180px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = 180;
    const pad = { top: 20, right: 16, bottom: 30, left: 50 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    // Clear
    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    if (!data.length) return;

    // Data range — focus on the average trend with some padding
    const depths = data.map(d => d.avg_depth_ft);
    const avgMin = Math.min(...depths);
    const avgMax = Math.max(...depths);
    const rangePad = Math.max((avgMax - avgMin) * 0.3, 10);
    const yMin = Math.floor((avgMin - rangePad) / 5) * 5;
    const yMax = Math.ceil((avgMax + rangePad) / 5) * 5;

    const xScale = (i) => pad.left + (i / (data.length - 1)) * plotW;
    // Invert Y: deeper water = higher on chart (worse = up)
    const yScale = (v) => pad.top + ((v - yMin) / (yMax - yMin)) * plotH;

    // Grid lines
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 1;
    const ySteps = 5;
    const yStep = (yMax - yMin) / ySteps;
    ctx.fillStyle = '#64748b';
    ctx.font = '11px -apple-system, sans-serif';
    ctx.textAlign = 'right';

    for (let i = 0; i <= ySteps; i++) {
        const val = yMin + yStep * i;
        const y = yScale(val);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText(Math.round(val) + ' ft', pad.left - 6, y + 4);
    }

    // Y axis label
    ctx.save();
    ctx.translate(12, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.fillStyle = '#94a3b8';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.fillText('Depth to Water', 0, 0);
    ctx.restore();

    // X axis labels (years)
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '11px -apple-system, sans-serif';
    data.forEach((d, i) => {
        if (i % 3 === 0 || i === data.length - 1) {
            ctx.fillText(d.year, xScale(i), H - pad.bottom + 16);
        }
    });

    // Range band (narrow spread around average to show variability)
    const spread = (avgMax - avgMin) * 0.25;
    ctx.fillStyle = '#38bdf822';
    ctx.beginPath();
    data.forEach((d, i) => {
        const x = xScale(i);
        ctx[i === 0 ? 'moveTo' : 'lineTo'](x, yScale(d.avg_depth_ft - spread));
    });
    for (let i = data.length - 1; i >= 0; i--) {
        ctx.lineTo(xScale(i), yScale(data[i].avg_depth_ft + spread));
    }
    ctx.closePath();
    ctx.fill();

    // Average line
    ctx.strokeStyle = '#38bdf8';
    ctx.lineWidth = 2.5;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    data.forEach((d, i) => {
        const x = xScale(i);
        const y = yScale(d.avg_depth_ft);
        ctx[i === 0 ? 'moveTo' : 'lineTo'](x, y);
    });
    ctx.stroke();

    // Dots
    data.forEach((d, i) => {
        ctx.fillStyle = '#38bdf8';
        ctx.beginPath();
        ctx.arc(xScale(i), yScale(d.avg_depth_ft), 3, 0, Math.PI * 2);
        ctx.fill();
    });

    // Trend annotation
    const first = data[0].avg_depth_ft;
    const last = data[data.length - 1].avg_depth_ft;
    const change = last - first;
    const years = data.length;
    ctx.fillStyle = change > 0 ? '#ef4444' : '#22c55e';
    ctx.font = 'bold 12px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(
        `${change > 0 ? '+' : ''}${change.toFixed(1)} ft over ${years} yrs`,
        W - pad.right,
        pad.top - 4
    );
}


// ---------------------------------------------------------------------------
// Water budget stacked area chart (Phase 5)
// ---------------------------------------------------------------------------

const WU_COLORS = {
    irrigation:     '#3b82f6',   // blue — dominant
    municipal:      '#22d3ee',   // cyan
    manufacturing:  '#f59e0b',   // amber
    mining:         '#a78bfa',   // violet
    livestock:      '#4ade80',   // green
    steam_electric: '#f87171',   // red
};

const WU_LABELS = {
    irrigation:     'Irrigation',
    municipal:      'Municipal',
    manufacturing:  'Manufacturing',
    mining:         'Mining',
    livestock:      'Livestock',
    steam_electric: 'Steam Electric',
};

const WU_CATEGORIES = Object.keys(WU_COLORS);

async function loadWaterBudget() {
    try {
        const trends = await fetchJSON('/api/water-usage/trends?source_type=total');

        if (!trends.length) {
            document.getElementById('water-budget-section').style.display = 'none';
            return;
        }

        drawWaterBudgetChart(trends);
        renderWaterBudgetLegend(trends);
    } catch (err) {
        console.warn('Water budget load error:', err);
        document.getElementById('water-budget-section').style.display = 'none';
    }
}

function drawWaterBudgetChart(data) {
    const canvas = document.getElementById('wb-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.parentElement.getBoundingClientRect();
    const W = rect.width;
    const H = 180;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width = W + 'px';
    canvas.style.height = H + 'px';
    ctx.scale(dpr, dpr);

    const pad = { top: 20, right: 16, bottom: 30, left: 52 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    // Compute stacked totals per year for y-axis range
    const totals = data.map(d => {
        return WU_CATEGORIES.reduce((sum, cat) => sum + (d[cat + '_af'] || 0), 0);
    });
    const yMax = Math.ceil(Math.max(...totals) / 100000) * 100000 || 1;

    const xScale = (i) => pad.left + (i / Math.max(data.length - 1, 1)) * plotW;
    const yScale = (v) => pad.top + plotH - (v / yMax) * plotH;

    // Grid lines
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 1;
    const ySteps = 4;
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    for (let i = 0; i <= ySteps; i++) {
        const val = (yMax / ySteps) * i;
        const y = yScale(val);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText(
            val >= 1000000 ? (val / 1000000).toFixed(1) + 'M' :
            val >= 1000    ? Math.round(val / 1000) + 'K' : Math.round(val),
            pad.left - 6, y + 4
        );
    }

    // Y-axis label
    ctx.save();
    ctx.translate(11, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.fillStyle = '#94a3b8';
    ctx.font = '9px -apple-system, sans-serif';
    ctx.fillText('Acre-Feet', 0, 0);
    ctx.restore();

    // X-axis labels
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    data.forEach((d, i) => {
        if (i % Math.ceil(data.length / 6) === 0 || i === data.length - 1) {
            ctx.fillText(d.year, xScale(i), H - pad.bottom + 14);
        }
    });

    // Draw stacked areas from bottom up
    WU_CATEGORIES.forEach((cat, catIdx) => {
        // Compute cumulative base for this category
        const baseline = data.map(d => {
            return WU_CATEGORIES.slice(0, catIdx)
                .reduce((s, c) => s + (d[c + '_af'] || 0), 0);
        });
        const tops = data.map((d, i) => baseline[i] + (d[cat + '_af'] || 0));

        const color = WU_COLORS[cat];

        // Fill area
        ctx.fillStyle = color + '55';  // semi-transparent fill
        ctx.beginPath();
        data.forEach((_, i) => {
            ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(baseline[i]));
        });
        for (let i = data.length - 1; i >= 0; i--) {
            ctx.lineTo(xScale(i), yScale(tops[i]));
        }
        ctx.closePath();
        ctx.fill();

        // Top stroke line
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        tops.forEach((v, i) => {
            ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(v));
        });
        ctx.stroke();
    });
}

function renderWaterBudgetLegend(data) {
    const el = document.getElementById('wb-legend');
    if (!el) return;

    // Get average values for each category to show in legend
    const avgs = {};
    WU_CATEGORIES.forEach(cat => {
        const vals = data.map(d => d[cat + '_af'] || 0).filter(v => v > 0);
        avgs[cat] = vals.length ? Math.round(vals.reduce((a, b) => a + b, 0) / vals.length) : 0;
    });

    el.innerHTML = WU_CATEGORIES
        .filter(cat => avgs[cat] > 0)
        .map(cat => `
            <span>
                <span class="wb-dot" style="background:${WU_COLORS[cat]}"></span>
                ${WU_LABELS[cat]}
            </span>
        `).join('');
}

async function loadCountyWaterBreakdown(county) {
    const section = document.getElementById('county-water-section');
    if (!county) {
        section.style.display = 'none';
        return;
    }

    try {
        const data = await fetchJSON(
            `/api/water-usage?county=${encodeURIComponent(county)}&source_type=total&limit=200`
        );
        if (!data.length) {
            section.style.display = 'none';
            return;
        }

        // Get most recent year's data
        const maxYear = Math.max(...data.map(d => d.year));
        const latestRows = data.filter(d => d.year === maxYear);

        const total = latestRows.reduce((s, r) => s + (r.volume_acre_ft || 0), 0);
        if (!total) { section.style.display = 'none'; return; }

        section.style.display = 'block';
        const barsEl = document.getElementById('county-bars');
        barsEl.innerHTML = `
            <div style="font-size:0.7rem;color:#64748b;margin-bottom:8px">${county} County — ${maxYear} totals</div>
        `;

        latestRows
            .sort((a, b) => (b.volume_acre_ft || 0) - (a.volume_acre_ft || 0))
            .forEach(row => {
                const vol = row.volume_acre_ft || 0;
                const pct = Math.round((vol / total) * 100);
                const color = WU_COLORS[row.category] || '#6b7280';
                const label = WU_LABELS[row.category] || row.category;
                barsEl.innerHTML += `
                    <div class="county-bar-wrap">
                        <div class="county-bar-label">
                            <span>${label}</span>
                            <span style="color:#e2e8f0">${vol.toLocaleString()} af (${pct}%)</span>
                        </div>
                        <div class="county-bar-track">
                            <div class="county-bar-fill" style="width:${pct}%;background:${color}"></div>
                        </div>
                    </div>
                `;
            });
    } catch (err) {
        console.warn('County water breakdown error:', err);
        section.style.display = 'none';
    }
}

// ---------------------------------------------------------------------------
// Agriculture irrigated acreage chart (Phase 5)
// ---------------------------------------------------------------------------

const AG_CROP_COLORS = {
    COTTON:   '#f59e0b',
    WHEAT:    '#84cc16',
    CORN:     '#facc15',
    SORGHUM:  '#fb923c',
    SOYBEANS: '#4ade80',
    HAY:      '#a3e635',
};

async function loadAgricultureData() {
    try {
        const summary = await fetchJSON('/api/agriculture/summary');

        if (!summary.trend || !summary.trend.length) {
            return;
        }

        drawAgChart(summary.trend);
        renderAgCropGrid(summary.by_crop);
    } catch (err) {
        console.warn('Agriculture data load error:', err);
    }
}

function drawAgChart(trend) {
    const canvas = document.getElementById('ag-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.parentElement.getBoundingClientRect();
    const W = rect.width;
    const H = 160;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    canvas.style.width = W + 'px';
    canvas.style.height = H + 'px';
    ctx.scale(dpr, dpr);

    const pad = { top: 20, right: 16, bottom: 28, left: 52 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    const data = trend.filter(d => d.total_irrigated_acres > 0);
    if (!data.length) return;

    const yMax = Math.ceil(Math.max(...data.map(d => d.total_irrigated_acres)) / 500000) * 500000 || 1;
    const xScale = (i) => pad.left + (i / Math.max(data.length - 1, 1)) * plotW;
    const yScale = (v) => pad.top + plotH - (v / yMax) * plotH;

    // Grid
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    [0, 0.25, 0.5, 0.75, 1].forEach(frac => {
        const val = yMax * frac;
        const y = yScale(val);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText(
            val >= 1000000 ? (val / 1000000).toFixed(1) + 'M' :
            val >= 1000    ? Math.round(val / 1000) + 'K' : Math.round(val),
            pad.left - 6, y + 4
        );
    });

    // Y label
    ctx.save();
    ctx.translate(11, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.fillStyle = '#94a3b8';
    ctx.font = '9px -apple-system, sans-serif';
    ctx.fillText('Acres', 0, 0);
    ctx.restore();

    // X labels
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    data.forEach((d, i) => {
        if (i % Math.ceil(data.length / 5) === 0 || i === data.length - 1) {
            ctx.fillText(d.year, xScale(i), H - pad.bottom + 12);
        }
    });

    // Gradient area fill
    const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + plotH);
    grad.addColorStop(0, '#4ade8066');
    grad.addColorStop(1, '#4ade8011');
    ctx.fillStyle = grad;
    ctx.beginPath();
    data.forEach((d, i) => {
        ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.total_irrigated_acres));
    });
    ctx.lineTo(xScale(data.length - 1), pad.top + plotH);
    ctx.lineTo(xScale(0), pad.top + plotH);
    ctx.closePath();
    ctx.fill();

    // Line
    ctx.strokeStyle = '#4ade80';
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    data.forEach((d, i) => {
        ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.total_irrigated_acres));
    });
    ctx.stroke();

    // Trend annotation
    const first = data[0].total_irrigated_acres;
    const last = data[data.length - 1].total_irrigated_acres;
    const pctChange = ((last - first) / first * 100).toFixed(1);
    ctx.fillStyle = last < first ? '#22c55e' : '#ef4444';  // less irrigation = good for aquifer
    ctx.font = 'bold 11px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(
        `${pctChange > 0 ? '+' : ''}${pctChange}% since ${data[0].year}`,
        W - pad.right, pad.top - 4
    );
}

function renderAgCropGrid(byCrop) {
    const el = document.getElementById('ag-crop-grid');
    if (!el || !byCrop.length) return;

    el.innerHTML = byCrop.slice(0, 6).map(item => {
        const crop = item.crop_type || 'Other';
        const acres = item.total_acres ? Math.round(parseFloat(item.total_acres)).toLocaleString() : 'N/A';
        const color = AG_CROP_COLORS[crop] || '#6b7280';
        return `
            <div class="ag-crop-card">
                <div class="crop-name" style="color:${color}">${crop}</div>
                <div class="crop-acres">${acres}</div>
                <div class="crop-label">irrigated acres</div>
            </div>
        `;
    }).join('');
}

// ---------------------------------------------------------------------------
// ERCOT generation queue breakdown
// ---------------------------------------------------------------------------

const FUEL_COLORS = {
    Solar: '#facc15',
    Wind: '#38bdf8',
    Battery: '#a78bfa',
    Gas: '#f87171',
    Other: '#6b7280',
};

async function loadErcotMapLayer() {
    try {
        const geojson = await fetchJSON('/api/ercot/geojson');

        ercotLayer.clearLayers();

        L.geoJSON(geojson, {
            pointToLayer: (feature, latlng) => {
                const fuel = feature.properties.fuel;
                const mw = feature.properties.mw || 0;
                const color = FUEL_COLORS[fuel] || FUEL_COLORS.Other;
                const radius = Math.min(Math.max(Math.sqrt(mw) / 4, 4), 14);

                return L.circleMarker(latlng, {
                    radius: radius,
                    fillColor: color,
                    color: '#ffffff44',
                    weight: 1,
                    fillOpacity: 0.6,
                });
            },
            onEachFeature: (feature, layer) => {
                const p = feature.properties;
                layer.bindPopup(`
                    <strong>${p.name || p.inr}</strong><br>
                    ${p.fuel} — ${p.mw.toLocaleString()} MW<br>
                    ${p.county} County<br>
                    Status: ${p.status}
                `);
            },
        }).addTo(ercotLayer);
    } catch (err) {
        console.error('ERCOT map layer error:', err);
    }
}


async function loadErcotSummary() {
    try {
        const data = await fetchJSON('/api/ercot/summary');
        const barEl = document.getElementById('fuel-bar');
        const legendEl = document.getElementById('fuel-legend');
        const totalMW = data.total_mw || 1;

        barEl.innerHTML = '';
        legendEl.innerHTML = '';

        data.by_fuel.forEach(item => {
            const fuel = item.fuel_type || 'Other';
            const mw = parseFloat(item.total_mw) || 0;
            const pct = (mw / totalMW) * 100;
            const color = FUEL_COLORS[fuel] || FUEL_COLORS.Other;

            const seg = document.createElement('div');
            seg.style.width = pct + '%';
            seg.style.background = color;
            seg.textContent = pct > 8 ? Math.round(mw).toLocaleString() : '';
            seg.title = `${fuel}: ${Math.round(mw).toLocaleString()} MW (${item.project_count} projects)`;
            barEl.appendChild(seg);

            const legend = document.createElement('span');
            legend.innerHTML = `<span class="fuel-dot" style="background:${color}"></span>${fuel}: ${Math.round(mw).toLocaleString()} MW`;
            legendEl.appendChild(legend);
        });
    } catch (err) {
        console.error('ERCOT load error:', err);
    }
}


// ---------------------------------------------------------------------------
// Water impact calculator
// ---------------------------------------------------------------------------

async function updateCalc() {
    const mw = parseFloat(document.getElementById('calc-mw').value) || 0;
    const cooling = document.getElementById('calc-cooling').value;

    if (mw <= 0) {
        document.getElementById('calc-gpd').textContent = '-';
        document.getElementById('calc-detail').textContent = '';
        return;
    }

    const data = await fetchJSON(`/api/water-impact?capacity_mw=${mw}&cooling=${cooling}`);

    document.getElementById('calc-gpd').textContent =
        data.gallons_per_day.toLocaleString() + ' gal/day';
    document.getElementById('calc-detail').textContent =
        `${data.gallons_per_year.toLocaleString()} gal/year | ${data.acre_feet_per_year} acre-ft/year`;
}

document.getElementById('calc-mw').addEventListener('input', updateCalc);
document.getElementById('calc-cooling').addEventListener('change', updateCalc);

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

(async () => {
    try {
        await loadDashboard();
        await loadSites();
        await Promise.all([
            loadWells(null),
            loadErcotSummary(),
            loadErcotMapLayer(),
            loadWaterBudget(),
            loadAgricultureData(),
        ]);
        updateCalc();

        // Layer toggle control
        L.control.layers(null, {
            'Ogallala Wells': wellsLayer,
            'ERCOT Gen Queue': ercotLayer,
            'DC Sites': sitesLayer,
        }, { collapsed: false, position: 'bottomleft' }).addTo(map);
    } catch (err) {
        console.error('Failed to load:', err);
    }
})();
