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
const reservoirsLayer = L.layerGroup().addTo(map);
const droughtLayer = L.layerGroup().addTo(map);

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
let currentView = 'full';
let layerControl = null;

// Cache loaded view data so we don't re-fetch on every tab switch
const viewCache = {};

// ---------------------------------------------------------------------------
// View configuration
// ---------------------------------------------------------------------------

const VIEW_CONFIG = {
    full: {
        panels: ['panel-sites', 'panel-chart', 'panel-ercot', 'panel-calc'],
        mapLayers: { wells: true, ercot: true, sites: true },
        statsMode: 'full',
        loader: null,
    },
    policy: {
        panels: ['panel-policy-overview', 'panel-policy-pipeline', 'panel-policy-aquifer', 'panel-sites'],
        mapLayers: { wells: true, ercot: false, sites: true },
        statsMode: 'policy',
        loader: loadPolicyView,
    },
    industry: {
        panels: ['panel-industry-energy', 'panel-industry-water', 'panel-site-comparison', 'panel-ercot'],
        mapLayers: { wells: false, ercot: true, sites: true },
        statsMode: 'industry',
        loader: loadIndustryView,
    },
    public: {
        panels: ['panel-public-facts', 'panel-public-city', 'panel-public-comparisons', 'panel-calc', 'panel-public-glossary'],
        mapLayers: { wells: false, ercot: false, sites: true },
        statsMode: 'public',
        loader: loadPublicView,
    },
};

// All panel IDs (in sidebar order)
const ALL_PANELS = [
    'panel-sites',
    'panel-chart',
    'panel-ercot',
    'panel-calc',
    'panel-policy-overview',
    'panel-policy-pipeline',
    'panel-policy-aquifer',
    'panel-industry-energy',
    'panel-industry-water',
    'panel-site-comparison',
    'panel-public-facts',
    'panel-public-city',
    'panel-public-comparisons',
    'panel-public-glossary',
];

// ---------------------------------------------------------------------------
// View switcher
// ---------------------------------------------------------------------------

async function switchView(viewName) {
    if (viewName === currentView) return;
    currentView = viewName;

    const config = VIEW_CONFIG[viewName];

    document.querySelectorAll('.view-tab').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    ALL_PANELS.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        if (id === 'panel-chart') return;
        el.style.display = config.panels.includes(id) ? '' : 'none';
    });

    if (config.mapLayers.wells) {
        if (!map.hasLayer(wellsLayer)) wellsLayer.addTo(map);
    } else {
        if (map.hasLayer(wellsLayer)) map.removeLayer(wellsLayer);
    }
    if (config.mapLayers.ercot) {
        if (!map.hasLayer(ercotLayer)) ercotLayer.addTo(map);
    } else {
        if (map.hasLayer(ercotLayer)) map.removeLayer(ercotLayer);
    }
    if (!map.hasLayer(sitesLayer)) sitesLayer.addTo(map);

    renderStatsBar(config.statsMode);

    if (config.loader && !viewCache[viewName]) {
        try {
            await config.loader();
        } catch (err) {
            console.error(`Failed to load ${viewName} view:`, err);
        }
    }
}

// ---------------------------------------------------------------------------
// Stats bar rendering
// ---------------------------------------------------------------------------

let dashboardData = null;
let policyData = null;
let industryData = null;
let publicData = null;

function renderStatsBar(mode) {
    const bar = document.getElementById('stats-bar');
    if (!bar) return;

    const stat = (value, label) =>
        `<div class="stat"><div class="stat-value">${value}</div><div class="stat-label">${label}</div></div>`;

    if (mode === 'full' && dashboardData) {
        bar.innerHTML = `
            ${stat(dashboardData.sites.length, 'Sites Tracked')}
            ${stat(dashboardData.total_wells.toLocaleString(), 'Ogallala Wells')}
            ${stat(dashboardData.counties_covered, 'Counties')}
            ${stat(dashboardData.ercot_total_mw ? Math.round(dashboardData.ercot_total_mw / 1000) + 'K' : '-', 'ERCOT MW')}
        `;
    } else if (mode === 'policy' && policyData) {
        const mw = policyData.total_capacity_mw
            ? Math.round(policyData.total_capacity_mw).toLocaleString() + ' MW'
            : '-';
        const gpd = policyData.total_water_demand_gpd
            ? (policyData.total_water_demand_gpd / 1_000_000).toFixed(1) + 'M gal/day'
            : '-';
        const rate = policyData.aquifer_depletion_rate_ft_per_yr != null
            ? (policyData.aquifer_depletion_rate_ft_per_yr > 0 ? '+' : '') + policyData.aquifer_depletion_rate_ft_per_yr + ' ft/yr'
            : '-';
        bar.innerHTML = `
            ${stat(policyData.total_projects, 'Active DC Projects')}
            ${stat(mw, 'Total Proposed')}
            ${stat(gpd, 'Est. Water Demand')}
            ${stat(rate, 'Aquifer Change')}
        `;
    } else if (mode === 'industry' && industryData) {
        const mw = industryData.ercot_total_mw
            ? Math.round(industryData.ercot_total_mw / 1000) + 'K MW'
            : '-';
        bar.innerHTML = `
            ${stat(mw, 'ERCOT Queue')}
            ${stat(industryData.renewable_pct + '%', 'Renewable')}
            ${stat(industryData.tracked_sites.length, 'DC Sites')}
            ${stat('-', 'Avg $/MWh')}
        `;
    } else if (mode === 'public' && publicData) {
        const depth = publicData.avg_aquifer_depth_ft != null
            ? publicData.avg_aquifer_depth_ft + ' ft'
            : '-';
        const gpd = publicData.total_water_demand_gpd
            ? (publicData.total_water_demand_gpd / 1_000_000).toFixed(1) + 'M gal/day'
            : '-';
        bar.innerHTML = `
            ${stat(publicData.tracked_sites, 'Sites Tracked')}
            ${stat(gpd, 'Est. Water Use')}
            ${stat(publicData.comparisons.households_equivalent.toLocaleString(), 'Households Equiv.')}
            ${stat(depth, 'Avg. Aquifer Depth')}
        `;
    }
}

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
    dashboardData = await fetchJSON('/api/dashboard');
    renderStatsBar('full');
    return dashboardData;
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

    // Load wells and water level chart
    await Promise.all([loadWells(siteId), loadWaterLevelChart(siteId)]);
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
// Reservoir layer + summary panel
// ---------------------------------------------------------------------------

let activeReservoirId = null;

/**
 * Return a colour based on percent-full value for consistent visual coding.
 * Green ≥ 70%, amber 40-70%, orange 20-40%, red < 20%, grey = unknown.
 */
function reservoirColor(pct) {
    if (pct == null) return '#6b7280';
    if (pct >= 70)   return '#22c55e';
    if (pct >= 40)   return '#f59e0b';
    if (pct >= 20)   return '#f97316';
    return '#ef4444';
}

function pctClass(pct) {
    if (pct == null) return 'pct-unknown';
    if (pct >= 70)   return 'pct-full';
    if (pct >= 50)   return 'pct-good';
    if (pct >= 30)   return 'pct-medium';
    if (pct >= 15)   return 'pct-low';
    return 'pct-critical';
}

/** Reservoir icon — diamond shape, coloured by percent-full. */
function reservoirIcon(pct) {
    const color = reservoirColor(pct);
    return L.divIcon({
        className: '',
        html: `<div style="
            width: 16px; height: 16px;
            background: ${color};
            border: 2px solid #fff;
            border-radius: 3px;
            transform: rotate(45deg);
            box-shadow: 0 0 8px ${color}99;
        "></div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8],
    });
}

async function loadReservoirSummary() {
    let data;
    try {
        data = await fetchJSON('/api/reservoir-summary');
    } catch (err) {
        console.error('Reservoir summary error:', err);
        return;
    }

    const { reservoirs, summary } = data;

    // Update aggregate stat row
    const summaryRow = document.getElementById('res-summary-row');
    if (summary.statewide_pct_full != null) {
        document.getElementById('res-stat-pct').textContent =
            summary.statewide_pct_full.toFixed(1) + '%';
    }
    if (summary.total_current_storage_acft) {
        const acft = summary.total_current_storage_acft;
        document.getElementById('res-stat-storage').textContent =
            acft >= 1000000
                ? (acft / 1000000).toFixed(2) + 'M'
                : Math.round(acft / 1000) + 'K';
    }
    document.getElementById('res-stat-count').textContent = summary.count;
    summaryRow.style.display = 'flex';

    // Build sidebar cards
    const listEl = document.getElementById('reservoir-list');
    listEl.innerHTML = '';
    reservoirsLayer.clearLayers();

    reservoirs.forEach(res => {
        const pct = res.percent_full != null ? parseFloat(res.percent_full) : null;
        const color = reservoirColor(pct);
        const cls   = pctClass(pct);
        const pctDisplay = pct != null ? pct.toFixed(1) + '%' : '—';

        // --- Map marker ---
        if (res.lat && res.lon) {
            const marker = L.marker([res.lat, res.lon], {
                icon: reservoirIcon(pct),
                zIndexOffset: -100,
            });

            const dateStr = res.measured_at
                ? new Date(res.measured_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
                : 'No data';
            const storageStr = res.current_storage_acft
                ? Math.round(res.current_storage_acft).toLocaleString() + ' ac-ft'
                : 'Unknown';

            marker.bindPopup(`
                <strong>${res.name}</strong><br>
                ${res.managing_authority || 'Managing authority unknown'}<br>
                ${res.county} County<br>
                <span style="color:${color};font-weight:700">${pctDisplay} full</span>
                ${pct != null ? `<div style="margin:4px 0;height:4px;background:#1e293b;border-radius:2px"><div style="width:${Math.min(pct,100)}%;height:4px;background:${color};border-radius:2px"></div></div>` : ''}
                Storage: ${storageStr}<br>
                <em style="color:#64748b;font-size:0.78rem">As of ${dateStr}</em>
            `);

            marker.on('click', () => selectReservoir(res.id, res.name));
            reservoirsLayer.addLayer(marker);
        }

        // --- Sidebar card ---
        const card = document.createElement('div');
        card.className = 'reservoir-card';
        card.dataset.resId = res.id;
        card.innerHTML = `
            <div class="res-info">
                <div class="res-name">${res.name}</div>
                <div class="res-meta">${res.county} Co. · ${res.managing_authority ? res.managing_authority.split('/')[0].trim() : 'Unknown'}</div>
                ${pct != null ? `<div class="pct-bar-wrap"><div class="pct-bar" style="width:${Math.min(pct,100)}%;background:${color}"></div></div>` : ''}
            </div>
            <div class="pct-badge">
                <div class="pct-value ${cls}">${pctDisplay}</div>
                <div class="pct-label">full</div>
            </div>
        `;
        card.addEventListener('click', () => selectReservoir(res.id, res.name));
        listEl.appendChild(card);
    });
}

async function selectReservoir(reservoirId, name) {
    activeReservoirId = reservoirId;

    // Highlight active card
    document.querySelectorAll('.reservoir-card').forEach(c => {
        c.classList.toggle('active', Number(c.dataset.resId) === reservoirId);
    });

    // Fly to reservoir
    try {
        const res = await fetchJSON(`/api/reservoirs/${reservoirId}`);
        if (res.lat && res.lon) {
            map.flyTo([res.lat, res.lon], 10, { duration: 1 });
        }
    } catch (err) {
        console.error('Reservoir detail error:', err);
    }

    // Load level trend chart
    await loadReservoirChart(reservoirId, name);
}

async function loadReservoirChart(reservoirId, name) {
    const section = document.getElementById('res-chart-section');
    try {
        const levels = await fetchJSON(
            `/api/reservoirs/${reservoirId}/levels?resolution=monthly`
        );
        if (!levels.length) {
            section.style.display = 'none';
            return;
        }
        document.getElementById('res-chart-title').textContent =
            name + ' — Storage Trend';
        section.style.display = 'block';
        drawReservoirChart(levels);
    } catch (err) {
        console.error('Reservoir chart error:', err);
        section.style.display = 'none';
    }
}

function drawReservoirChart(data) {
    const canvas = document.getElementById('res-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width  = rect.width * dpr;
    canvas.height = 160 * dpr;
    canvas.style.width  = rect.width + 'px';
    canvas.style.height = '160px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = 160;
    const pad = { top: 20, right: 16, bottom: 28, left: 44 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    if (!data.length) return;

    const pcts = data.map(d => d.avg_pct_full).filter(v => v != null);
    if (!pcts.length) return;

    const yMin = 0;
    const yMax = 100;

    const xScale = i => pad.left + (i / (data.length - 1 || 1)) * plotW;
    const yScale = v  => pad.top + ((yMax - v) / (yMax - yMin)) * plotH;

    // Grid lines at 25%, 50%, 75%, 100%
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    [0, 25, 50, 75, 100].forEach(val => {
        const y = yScale(val);
        ctx.strokeStyle = val === 50 ? '#334155' : '#1e293b';
        ctx.lineWidth = val === 50 ? 1.5 : 1;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillStyle = '#475569';
        ctx.fillText(val + '%', pad.left - 4, y + 3);
    });

    // Area fill under curve
    ctx.fillStyle = '#06b6d422';
    ctx.beginPath();
    let first = true;
    data.forEach((d, i) => {
        if (d.avg_pct_full == null) return;
        const x = xScale(i);
        const y = yScale(d.avg_pct_full);
        if (first) { ctx.moveTo(x, y); first = false; }
        else ctx.lineTo(x, y);
    });
    // Close path along bottom
    ctx.lineTo(xScale(data.length - 1), yScale(0));
    ctx.lineTo(xScale(0), yScale(0));
    ctx.closePath();
    ctx.fill();

    // Line
    ctx.strokeStyle = '#06b6d4';
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    first = true;
    data.forEach((d, i) => {
        if (d.avg_pct_full == null) return;
        const x = xScale(i);
        const y = yScale(d.avg_pct_full);
        if (first) { ctx.moveTo(x, y); first = false; }
        else ctx.lineTo(x, y);
    });
    ctx.stroke();

    // X-axis date labels (monthly data → show year transitions)
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    let lastYear = null;
    data.forEach((d, i) => {
        if (!d.period) return;
        const yr = d.period.slice(0, 4);
        if (yr !== lastYear && (i === 0 || i === data.length - 1 || lastYear !== null)) {
            ctx.fillText(yr, xScale(i), H - pad.bottom + 14);
            lastYear = yr;
        }
    });

    // Latest value annotation
    const lastWithData = [...data].reverse().find(d => d.avg_pct_full != null);
    if (lastWithData) {
        const pct = lastWithData.avg_pct_full;
        const color = reservoirColor(pct);
        ctx.fillStyle = color;
        ctx.font = 'bold 12px -apple-system, sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText(pct.toFixed(1) + '% full', W - pad.right, pad.top - 4);
    }
}

// ---------------------------------------------------------------------------
// Energy Market panel
// ---------------------------------------------------------------------------

async function loadEnergyPanel() {
    try {
        const summary = await fetchJSON('/api/energy/summary');
        const hasData = summary.data_loaded.pricing_rows > 0 || summary.data_loaded.generation_rows > 0;

        if (!hasData) {
            document.getElementById('energy-no-data').style.display = 'block';
            document.getElementById('energy-charts').style.display = 'none';
            return;
        }

        document.getElementById('energy-no-data').style.display = 'none';
        document.getElementById('energy-charts').style.display = 'block';

        const c = summary.current;

        // Current price — highlight negative in purple
        const priceEl = document.getElementById('en-price');
        const price = c.hb_west_price;
        if (price != null) {
            priceEl.textContent = '$' + price.toFixed(2);
            priceEl.className = 'value' + (price < 0 ? ' negative' : '');
        }

        // Today average
        const avg = summary.today_hb_west.avg_price;
        const avgEl = document.getElementById('en-avg');
        if (avg != null) {
            avgEl.textContent = '$' + avg.toFixed(2);
            avgEl.className = 'value' + (avg < 0 ? ' negative' : '');
        }

        // Wind & solar
        if (c.wind_mw != null) {
            document.getElementById('en-wind').textContent =
                (c.wind_mw / 1000).toFixed(1) + ' GW';
        }
        if (c.solar_mw != null) {
            document.getElementById('en-solar').textContent =
                (c.solar_mw / 1000).toFixed(1) + ' GW';
        }

        // Load chart data in parallel
        await Promise.all([loadPricingChart(), loadGenerationChart()]);

    } catch (err) {
        console.error('Energy panel error:', err);
    }
}

async function loadPricingChart() {
    try {
        const resp = await fetchJSON('/api/energy/pricing?zone=HB_WEST&days=7&resolution=hourly');
        if (!resp.data.length) return;
        drawPricingChart(resp.data);
    } catch (err) {
        console.error('Pricing chart error:', err);
    }
}

async function loadGenerationChart() {
    try {
        const resp = await fetchJSON('/api/energy/generation?days=7');
        if (!resp.data.length) return;
        drawGenerationChart(resp.data);
    } catch (err) {
        console.error('Generation chart error:', err);
    }
}

function drawPricingChart(data) {
    const canvas = document.getElementById('en-price-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = 140 * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = '140px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = 140;
    const pad = { top: 16, right: 12, bottom: 24, left: 44 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    if (!data.length) return;

    const prices = data.map(d => parseFloat(d.price_per_mwh));
    const rawMin = Math.min(...prices);
    const rawMax = Math.max(...prices);
    const rangePad = Math.max((rawMax - rawMin) * 0.15, 5);
    const yMin = rawMin - rangePad;
    const yMax = rawMax + rangePad;

    const xScale = i => pad.left + (i / (data.length - 1)) * plotW;
    const yScale = v => pad.top + (1 - (v - yMin) / (yMax - yMin)) * plotH;
    const zeroY = yScale(0);

    // Zero line (shows when prices go negative)
    if (yMin < 0 && yMax > 0) {
        ctx.strokeStyle = '#475569';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(pad.left, zeroY);
        ctx.lineTo(W - pad.right, zeroY);
        ctx.stroke();
        ctx.setLineDash([]);
    }

    // Y grid + labels
    const ySteps = 4;
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillStyle = '#64748b';
    for (let i = 0; i <= ySteps; i++) {
        const val = yMin + ((yMax - yMin) / ySteps) * i;
        const y = yScale(val);
        ctx.strokeStyle = '#1e293b';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText('$' + Math.round(val), pad.left - 4, y + 3);
    }

    // X axis time labels (show ~4 evenly spaced)
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    const step = Math.max(1, Math.floor(data.length / 4));
    data.forEach((d, i) => {
        if (i % step === 0 || i === data.length - 1) {
            const label = new Date(d.ts).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric' });
            ctx.fillText(label, xScale(i), H - pad.bottom + 14);
        }
    });

    // Fill: negative regions in purple, positive in blue
    for (let i = 0; i < data.length - 1; i++) {
        const x0 = xScale(i);
        const x1 = xScale(i + 1);
        const p0 = prices[i];
        const p1 = prices[i + 1];
        const isNeg = p0 < 0 || p1 < 0;
        ctx.fillStyle = isNeg ? '#a855f722' : '#38bdf812';
        ctx.fillRect(x0, Math.min(yScale(p0), yScale(p1)), x1 - x0,
            Math.max(Math.abs(yScale(p0) - yScale(p1)), 1));
    }

    // Price line — color by sign
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    data.forEach((d, i) => {
        if (i === 0) return;
        const isNeg = prices[i] < 0;
        ctx.strokeStyle = isNeg ? '#a855f7' : '#38bdf8';
        ctx.beginPath();
        ctx.moveTo(xScale(i - 1), yScale(prices[i - 1]));
        ctx.lineTo(xScale(i), yScale(prices[i]));
        ctx.stroke();
    });

    // Annotation: negative interval count
    const negCount = prices.filter(p => p < 0).length;
    if (negCount > 0) {
        ctx.fillStyle = '#a855f7';
        ctx.font = 'bold 10px -apple-system, sans-serif';
        ctx.textAlign = 'right';
        ctx.fillText(`${negCount} negative intervals`, W - pad.right, pad.top - 2);
    }
}

function drawGenerationChart(data) {
    const canvas = document.getElementById('en-gen-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = 140 * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = '140px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = 140;
    const pad = { top: 16, right: 12, bottom: 24, left: 44 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    if (!data.length) return;

    // Split into wind and solar series keyed by ts
    const tsSet = [...new Set(data.map(d => d.ts))].sort();
    const windByTs = Object.fromEntries(
        data.filter(d => d.fuel_type === 'Wind').map(d => [d.ts, d.output_mw])
    );
    const solarByTs = Object.fromEntries(
        data.filter(d => d.fuel_type === 'Solar').map(d => [d.ts, d.output_mw])
    );

    const windVals = tsSet.map(t => windByTs[t] ?? null);
    const solarVals = tsSet.map(t => solarByTs[t] ?? null);
    const allVals = [...windVals, ...solarVals].filter(v => v != null);
    if (!allVals.length) return;

    const yMax = Math.max(...allVals) * 1.1;
    const yMin = 0;

    const xScale = i => pad.left + (i / Math.max(tsSet.length - 1, 1)) * plotW;
    const yScale = v => pad.top + (1 - (v - yMin) / (yMax - yMin)) * plotH;

    // Y grid + labels
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillStyle = '#64748b';
    const ySteps = 3;
    for (let i = 0; i <= ySteps; i++) {
        const val = (yMax / ySteps) * i;
        const y = yScale(val);
        ctx.strokeStyle = '#1e293b';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText(Math.round(val / 1000) + 'k', pad.left - 4, y + 3);
    }

    // X axis time labels
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    const step = Math.max(1, Math.floor(tsSet.length / 4));
    tsSet.forEach((ts, i) => {
        if (i % step === 0 || i === tsSet.length - 1) {
            const label = new Date(ts).toLocaleDateString('en-US', { month: 'numeric', day: 'numeric' });
            ctx.fillText(label, xScale(i), H - pad.bottom + 14);
        }
    });

    function drawSeries(vals, color) {
        // Area fill
        ctx.fillStyle = color + '18';
        ctx.beginPath();
        let started = false;
        vals.forEach((v, i) => {
            if (v == null) return;
            const x = xScale(i);
            const y = yScale(v);
            if (!started) { ctx.moveTo(x, yScale(0)); ctx.lineTo(x, y); started = true; }
            else ctx.lineTo(x, y);
        });
        // Close path back to baseline (right to left)
        for (let i = vals.length - 1; i >= 0; i--) {
            if (vals[i] != null) ctx.lineTo(xScale(i), yScale(0));
        }
        ctx.closePath();
        ctx.fill();

        // Line
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.lineJoin = 'round';
        ctx.beginPath();
        let first = true;
        vals.forEach((v, i) => {
            if (v == null) { first = true; return; }
            if (first) { ctx.moveTo(xScale(i), yScale(v)); first = false; }
            else ctx.lineTo(xScale(i), yScale(v));
        });
        ctx.stroke();
    }

    drawSeries(solarVals, '#facc15');
    drawSeries(windVals, '#38bdf8');

    // Legend
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'left';
    ctx.fillStyle = '#38bdf8';
    ctx.fillRect(pad.left + 2, pad.top, 10, 3);
    ctx.fillText('Wind', pad.left + 16, pad.top + 4);
    ctx.fillStyle = '#facc15';
    ctx.fillRect(pad.left + 52, pad.top, 10, 3);
    ctx.fillText('Solar', pad.left + 66, pad.top + 4);
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
        const baseline = data.map(d => {
            return WU_CATEGORIES.slice(0, catIdx)
                .reduce((s, c) => s + (d[c + '_af'] || 0), 0);
        });
        const tops = data.map((d, i) => baseline[i] + (d[cat + '_af'] || 0));

        const color = WU_COLORS[cat];

        ctx.fillStyle = color + '55';
        ctx.beginPath();
        data.forEach((_, i) => {
            ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(baseline[i]));
        });
        for (let i = data.length - 1; i >= 0; i--) {
            ctx.lineTo(xScale(i), yScale(tops[i]));
        }
        ctx.closePath();
        ctx.fill();

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

    ctx.save();
    ctx.translate(11, H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.fillStyle = '#94a3b8';
    ctx.font = '9px -apple-system, sans-serif';
    ctx.fillText('Acres', 0, 0);
    ctx.restore();

    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '10px -apple-system, sans-serif';
    data.forEach((d, i) => {
        if (i % Math.ceil(data.length / 5) === 0 || i === data.length - 1) {
            ctx.fillText(d.year, xScale(i), H - pad.bottom + 12);
        }
    });

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

    ctx.strokeStyle = '#4ade80';
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    data.forEach((d, i) => {
        ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.total_irrigated_acres));
    });
    ctx.stroke();

    const first = data[0].total_irrigated_acres;
    const last = data[data.length - 1].total_irrigated_acres;
    const pctChange = ((last - first) / first * 100).toFixed(1);
    ctx.fillStyle = last < first ? '#22c55e' : '#ef4444';
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
// Drought color scale
// ---------------------------------------------------------------------------

// D0-D4 colors match US Drought Monitor convention (yellows → reds → dark brown)
const DROUGHT_COLORS = {
    None: '#1e293b',  // no drought — dark slate (map background)
    D0:   '#ffff00',  // abnormally dry — yellow
    D1:   '#fcd34d',  // moderate — amber
    D2:   '#f97316',  // severe — orange
    D3:   '#dc2626',  // extreme — red
    D4:   '#7f1d1d',  // exceptional — dark red
};

const DROUGHT_LABELS = {
    None: 'No Drought',
    D0:   'D0 Abnormally Dry',
    D1:   'D1 Moderate',
    D2:   'D2 Severe',
    D3:   'D3 Extreme',
    D4:   'D4 Exceptional',
};

// Badge colors for the sidebar worst-category badge
const DROUGHT_BADGE_STYLE = {
    None: { bg: '#374151', color: '#9ca3af' },
    D0:   { bg: '#854d0e', color: '#fef08a' },
    D1:   { bg: '#92400e', color: '#fcd34d' },
    D2:   { bg: '#c2410c', color: '#fed7aa' },
    D3:   { bg: '#991b1b', color: '#fca5a5' },
    D4:   { bg: '#7f1d1d', color: '#fca5a5' },
};

// ---------------------------------------------------------------------------
// Drought county overlay (choropleth)
// ---------------------------------------------------------------------------

// FIPS → worst drought category from latest_drought API response
let droughtByFips = {};

async function loadDroughtOverlay() {
    // Fetch current drought status
    let droughtData;
    try {
        droughtData = await fetchJSON('/api/drought/current');
    } catch (err) {
        console.error('Drought API error:', err);
        return;
    }

    // Build fips → record lookup
    droughtByFips = {};
    for (const rec of droughtData) {
        droughtByFips[rec.county_fips] = rec;
    }

    if (Object.keys(droughtByFips).length === 0) return;

    // Fetch Texas county boundaries from Census TIGERweb for tracked FIPS codes
    const fipsList = Object.keys(droughtByFips).map(f => `'${f}'`).join(',');
    const tigerUrl =
        'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/1/query' +
        `?where=STATEFP%3D'48'+AND+GEOID+IN+(${encodeURIComponent(fipsList)})` +
        '&outFields=GEOID,NAME&outSR=4326&f=geojson';

    let countyGeo;
    try {
        const resp = await fetch(tigerUrl);
        countyGeo = await resp.json();
    } catch (err) {
        console.error('County boundary fetch error:', err);
        return;
    }

    droughtLayer.clearLayers();

    L.geoJSON(countyGeo, {
        style: (feature) => {
            const fips = feature.properties.GEOID;
            const rec = droughtByFips[fips];
            const category = rec ? (rec.worst_category || 'None') : 'None';
            const color = DROUGHT_COLORS[category] || DROUGHT_COLORS.None;
            return {
                fillColor: color,
                fillOpacity: category === 'None' ? 0.08 : 0.45,
                color: '#334155',
                weight: 0.8,
            };
        },
        onEachFeature: (feature, layer) => {
            const fips = feature.properties.GEOID;
            const rec = droughtByFips[fips];
            const name = feature.properties.NAME;
            if (rec) {
                const d1plus = ((rec.d1_pct || 0) + (rec.d2_pct || 0) +
                                (rec.d3_pct || 0) + (rec.d4_pct || 0)).toFixed(1);
                layer.bindPopup(
                    `<strong>${name} County</strong><br>` +
                    `As of: ${rec.valid_date}<br>` +
                    `Worst: <b>${rec.worst_category || 'None'}</b><br>` +
                    `D1+ coverage: ${d1plus}%<br>` +
                    `D0 ${(rec.d0_pct || 0).toFixed(1)}% | ` +
                    `D1 ${(rec.d1_pct || 0).toFixed(1)}% | ` +
                    `D2 ${(rec.d2_pct || 0).toFixed(1)}%`
                );
            } else {
                layer.bindPopup(`<strong>${name} County</strong><br>No drought data`);
            }
        },
    }).addTo(droughtLayer);

    // Add legend to map (bottom right)
    addDroughtMapLegend();
}

function addDroughtMapLegend() {
    if (window._droughtLegendControl) {
        map.removeControl(window._droughtLegendControl);
    }

    const legend = L.control({ position: 'bottomright' });
    legend.onAdd = () => {
        const div = L.DomUtil.create('div', 'drought-legend');
        div.innerHTML = '<h4>Drought</h4>' +
            Object.entries(DROUGHT_LABELS).map(([cat, label]) =>
                `<div><i style="background:${cat === 'None' ? '#475569' : DROUGHT_COLORS[cat]}"></i>${label}</div>`
            ).join('');
        return div;
    };
    legend.addTo(map);
    window._droughtLegendControl = legend;
}

// ---------------------------------------------------------------------------
// Drought sidebar panel
// ---------------------------------------------------------------------------

async function loadDroughtSummary() {
    let data;
    try {
        data = await fetchJSON('/api/drought/summary');
    } catch (err) {
        console.error('Drought summary error:', err);
        return;
    }

    const metaEl = document.getElementById('drought-meta');
    const barEl = document.getElementById('drought-bar');
    const legendEl = document.getElementById('drought-legend-inline');
    const badgeEl = document.getElementById('drought-worst-badge');

    if (!data.valid_date) {
        metaEl.textContent = 'No drought data available.';
        return;
    }

    // Format date
    const d = new Date(data.valid_date);
    const dateStr = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
    metaEl.textContent = `${data.county_count} counties · week of ${dateStr}`;

    // Worst badge
    const worst = data.worst_regional || 'None';
    const bs = DROUGHT_BADGE_STYLE[worst] || DROUGHT_BADGE_STYLE.None;
    badgeEl.textContent = worst;
    badgeEl.style.background = bs.bg;
    badgeEl.style.color = bs.color;

    // Stacked bar
    const avg = data.regional_avg || {};
    const segments = [
        { key: 'no_drought_pct', color: '#1e3a5f', label: 'None' },
        { key: 'd0_pct',         color: DROUGHT_COLORS.D0, label: 'D0' },
        { key: 'd1_pct',         color: DROUGHT_COLORS.D1, label: 'D1' },
        { key: 'd2_pct',         color: DROUGHT_COLORS.D2, label: 'D2' },
        { key: 'd3_pct',         color: DROUGHT_COLORS.D3, label: 'D3' },
        { key: 'd4_pct',         color: DROUGHT_COLORS.D4, label: 'D4' },
    ];

    barEl.innerHTML = '';
    legendEl.innerHTML = '';

    for (const seg of segments) {
        const pct = avg[seg.key] || 0;
        if (pct < 0.1) continue;

        const div = document.createElement('div');
        div.style.width = pct + '%';
        div.style.background = seg.color;
        div.title = `${seg.label}: ${pct.toFixed(1)}%`;
        barEl.appendChild(div);

        const span = document.createElement('span');
        span.innerHTML =
            `<span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${seg.color};margin-right:3px;vertical-align:middle"></span>` +
            `${seg.label} ${pct.toFixed(1)}%`;
        legendEl.appendChild(span);
    }
}

// ---------------------------------------------------------------------------
// Drought trend chart (% of counties in D1+ each week)
// ---------------------------------------------------------------------------

async function loadDroughtTrendChart() {
    let history;
    try {
        history = await fetchJSON('/api/drought/history?weeks=52');
    } catch (err) {
        console.error('Drought history error:', err);
        return;
    }

    if (!history.length) return;

    // Aggregate by valid_date: average D1+ across all counties
    const byDate = {};
    for (const rec of history) {
        const d = rec.valid_date;
        if (!byDate[d]) byDate[d] = { d1plus: [], d2plus: [] };
        const d1p = (rec.d1_pct || 0) + (rec.d2_pct || 0) + (rec.d3_pct || 0) + (rec.d4_pct || 0);
        const d2p = (rec.d2_pct || 0) + (rec.d3_pct || 0) + (rec.d4_pct || 0);
        byDate[d].d1plus.push(d1p);
        byDate[d].d2plus.push(d2p);
    }

    const dates = Object.keys(byDate).sort();
    const chartData = dates.map(d => ({
        date: d,
        d1plus: byDate[d].d1plus.reduce((a, b) => a + b, 0) / byDate[d].d1plus.length,
        d2plus: byDate[d].d2plus.reduce((a, b) => a + b, 0) / byDate[d].d2plus.length,
    }));

    drawDroughtChart(chartData);
}

function drawDroughtChart(data) {
    const canvas = document.getElementById('drought-chart');
    if (!canvas || !data.length) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = 160 * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = '160px';
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = 160;
    const pad = { top: 16, right: 12, bottom: 28, left: 40 };
    const plotW = W - pad.left - pad.right;
    const plotH = H - pad.top - pad.bottom;

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    const xScale = (i) => pad.left + (i / Math.max(data.length - 1, 1)) * plotW;
    const yScale = (v) => pad.top + plotH - (v / 100) * plotH;

    // Grid lines at 25, 50, 75, 100%
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#475569';
    ctx.font = `${10 * dpr / dpr}px -apple-system, sans-serif`;
    ctx.textAlign = 'right';
    for (const pct of [0, 25, 50, 75, 100]) {
        const y = yScale(pct);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(W - pad.right, y);
        ctx.stroke();
        ctx.fillText(pct + '%', pad.left - 4, y + 4);
    }

    // X axis labels — show ~6 evenly spaced dates
    ctx.fillStyle = '#475569';
    ctx.textAlign = 'center';
    const step = Math.max(1, Math.floor(data.length / 6));
    data.forEach((d, i) => {
        if (i % step === 0 || i === data.length - 1) {
            const dateStr = new Date(d.date + 'T00:00:00Z')
                .toLocaleDateString('en-US', { month: 'short', year: '2-digit', timeZone: 'UTC' });
            ctx.fillText(dateStr, xScale(i), H - pad.bottom + 14);
        }
    });

    // D1+ filled area
    ctx.fillStyle = '#f9731620';
    ctx.beginPath();
    data.forEach((d, i) => ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.d1plus)));
    ctx.lineTo(xScale(data.length - 1), yScale(0));
    ctx.lineTo(xScale(0), yScale(0));
    ctx.closePath();
    ctx.fill();

    // D1+ line
    ctx.strokeStyle = '#f97316';
    ctx.lineWidth = 2;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    data.forEach((d, i) => ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.d1plus)));
    ctx.stroke();

    // D2+ filled area (darker)
    ctx.fillStyle = '#dc262620';
    ctx.beginPath();
    data.forEach((d, i) => ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.d2plus)));
    ctx.lineTo(xScale(data.length - 1), yScale(0));
    ctx.lineTo(xScale(0), yScale(0));
    ctx.closePath();
    ctx.fill();

    // D2+ line
    ctx.strokeStyle = '#dc2626';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    data.forEach((d, i) => ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.d2plus)));
    ctx.stroke();

    // Legend in top-right corner of chart
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'left';
    const lx = W - pad.right - 80;
    const ly = pad.top + 4;
    ctx.strokeStyle = '#f97316'; ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(lx, ly + 5); ctx.lineTo(lx + 14, ly + 5); ctx.stroke();
    ctx.fillStyle = '#94a3b8'; ctx.fillText('D1+', lx + 17, ly + 9);
    ctx.strokeStyle = '#dc2626'; ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.moveTo(lx + 36, ly + 5); ctx.lineTo(lx + 50, ly + 5); ctx.stroke();
    ctx.fillText('D2+', lx + 53, ly + 9);
}

// ---------------------------------------------------------------------------
// Weather panel
// ---------------------------------------------------------------------------

async function loadWeatherPanel() {
    let data;
    try {
        data = await fetchJSON('/api/weather/current');
    } catch (err) {
        console.error('Weather API error:', err);
        return;
    }

    const gridEl = document.getElementById('weather-grid');
    if (!data.length) {
        gridEl.innerHTML = '<div style="font-size:0.8rem;color:#64748b">No weather data available.</div>';
        return;
    }

    const stationLabels = {
        KLBB: 'Lubbock',
        KAMA: 'Amarillo',
        KCDS: 'Childress',
        KBPG: 'Big Spring',
    };

    gridEl.innerHTML = data.map(obs => {
        const label = stationLabels[obs.station_id] || obs.station_id;
        const temp = obs.temperature_f != null ? Math.round(obs.temperature_f) : '--';
        const rh = obs.humidity_pct != null ? Math.round(obs.humidity_pct) : '--';
        const wind = obs.wind_speed_mph != null ? Math.round(obs.wind_speed_mph) : '--';
        const cond = obs.conditions || '';

        return `
            <div class="weather-station">
                <div class="ws-name">${label}</div>
                <div class="ws-temp">${temp}<span>°F</span></div>
                <div class="ws-detail">RH ${rh}% · Wind ${wind} mph</div>
                <div class="ws-cond">${cond}</div>
            </div>
        `;
    }).join('');
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
// Policy view
// ---------------------------------------------------------------------------

async function loadPolicyView() {
    policyData = await fetchJSON('/api/views/policy-summary');
    viewCache['policy'] = true;

    const fmtMW = v => v ? Math.round(v).toLocaleString() + ' MW' : '-';
    const fmtGPD = v => v ? (v / 1_000_000).toFixed(1) + 'M gal/day' : '-';
    const fmtAcft = v => v ? Math.round(v).toLocaleString() + ' ac-ft' : '-';

    setText('pm-total-mw', fmtMW(policyData.total_capacity_mw));
    setText('pm-total-gpd', fmtGPD(policyData.total_water_demand_gpd));
    setText('pm-acft', fmtAcft(policyData.total_water_demand_acft_yr));

    const pct = policyData.dc_pct_of_regional_ag_water;
    setText('pm-pct-ag', pct != null ? pct.toFixed(3) + '%' : '-');

    const rate = policyData.aquifer_depletion_rate_ft_per_yr;
    const ttd = policyData.time_to_depletion_yrs;
    if (rate != null) {
        const rateStr = (rate > 0 ? '+' : '') + rate + ' ft/yr';
        const rateColor = rate > 0 ? 'danger' : 'ok';
        const valEl = document.getElementById('pm-depletion-val');
        if (valEl) {
            valEl.textContent = rateStr;
            valEl.className = `metric-val ${rateColor}`;
        }
        setText('pm-depletion-label', 'Aquifer Depletion Rate');
        setText('pm-depletion-sub',
            ttd ? `At this rate, remaining saturated thickness (~100 ft avg) exhausted in ~${Math.round(ttd).toLocaleString()} years`
                : 'Based on TWDB well measurements over past 10 years');
    }

    const tbody = document.getElementById('policy-pipeline-rows');
    if (tbody) {
        tbody.innerHTML = '';
        (policyData.pipeline_by_status || []).forEach(row => {
            const tr = document.createElement('tr');
            const gpd = row.total_gpd > 0 ? (row.total_gpd / 1_000_000).toFixed(1) + 'M' : '—';
            tr.innerHTML = `
                <td><span class="status-badge status-${row.status}">${formatStatus(row.status)}</span></td>
                <td style="text-align:center">${row.count}</td>
                <td style="text-align:right">${row.total_mw ? Math.round(row.total_mw).toLocaleString() : '—'}</td>
                <td style="text-align:right">${gpd}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    const trend = policyData.aquifer_trend;
    let aqHtml = '';
    if (trend && trend.length >= 2) {
        const first = trend[0];
        const last = trend[trend.length - 1];
        const change = last.avg_depth_ft - first.avg_depth_ft;
        const direction = change > 0 ? 'deepened' : 'shallowed';
        const changeAbs = Math.abs(change).toFixed(1);
        aqHtml = `
            <div style="margin-bottom:10px">
                The regional water table has <strong style="color:${change > 0 ? '#ef4444' : '#22c55e'}">${direction} by ${changeAbs} ft</strong>
                from ${first.year} to ${last.year}, based on ${policyData.monitoring_wells.toLocaleString()} TWDB monitoring wells.
            </div>
            <div style="font-size:0.75rem;color:#64748b">
                Most recent avg depth: <strong style="color:#e2e8f0">${last.avg_depth_ft} ft</strong> (${last.year})
            </div>
        `;
    } else {
        aqHtml = '<div style="color:#64748b">Insufficient trend data in database.</div>';
    }
    const aqEl = document.getElementById('policy-aquifer-content');
    if (aqEl) aqEl.innerHTML = aqHtml;

    if (currentView === 'policy') renderStatsBar('policy');
}

// ---------------------------------------------------------------------------
// Industry & Economic view
// ---------------------------------------------------------------------------

async function loadIndustryView() {
    const [indData, compareData] = await Promise.all([
        fetchJSON('/api/views/industry-summary'),
        fetchJSON('/api/compare/sites'),
    ]);
    industryData = indData;
    viewCache['industry'] = true;

    setText('ind-ercot-mw',
        indData.ercot_total_mw ? Math.round(indData.ercot_total_mw / 1000) + 'K MW' : '-');
    setText('ind-ercot-projects', `${indData.ercot_total_projects} projects in queue`);
    setText('ind-renewable-pct', indData.renewable_pct + '%');
    setText('ind-renewable-mw',
        indData.renewable_mw ? Math.round(indData.renewable_mw).toLocaleString() + ' MW solar+wind+battery' : '-');

    const fuelEl = document.getElementById('ind-fuel-breakdown');
    if (fuelEl) {
        fuelEl.innerHTML = '';
        (indData.ercot_by_fuel || []).forEach(f => {
            const color = FUEL_COLORS[f.fuel] || FUEL_COLORS.Other;
            const mwStr = Math.round(f.mw).toLocaleString();
            fuelEl.innerHTML += `
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;font-size:0.75rem">
                    <span class="fuel-dot" style="background:${color}"></span>
                    <span style="min-width:60px;color:#e2e8f0">${f.fuel}</span>
                    <div style="flex:1;background:#0f172a;border-radius:3px;height:6px;overflow:hidden">
                        <div style="width:${f.pct}%;background:${color};height:100%;border-radius:3px"></div>
                    </div>
                    <span style="color:#94a3b8;min-width:80px;text-align:right">${mwStr} MW (${f.pct}%)</span>
                </div>
            `;
        });
    }

    const waterEl = document.getElementById('ind-water-avail');
    if (waterEl) {
        waterEl.innerHTML = '';
        (indData.county_water_availability || []).slice(0, 10).forEach(c => {
            const score = c.avail_score;
            const cls = score >= 70 ? 'high' : score >= 50 ? 'medium' : 'low';
            const depthStr = c.avg_depth_ft != null ? c.avg_depth_ft + ' ft' : 'no data';
            waterEl.innerHTML += `
                <div class="avail-score-row">
                    <span class="avail-county">${c.county}</span>
                    <span class="avail-depth">${depthStr}</span>
                    <div class="avail-bar-wrap">
                        <div class="avail-bar">
                            <div class="avail-fill ${cls}" style="width:${score}%"></div>
                        </div>
                    </div>
                    <span class="avail-num ${cls}">${score}</span>
                </div>
            `;
        });
    }

    const tbody = document.getElementById('compare-rows');
    if (tbody) {
        tbody.innerHTML = '';
        compareData.forEach(s => {
            const mw = s.capacity_mw ? s.capacity_mw.toLocaleString() : '—';
            const gpd = s.water_demand_gpd ? (s.water_demand_gpd / 1_000_000).toFixed(1) + 'M' : '—';
            const gpdMw = s.water_intensity_gpd_per_mw ? Math.round(s.water_intensity_gpd_per_mw).toLocaleString() : '—';
            const depth = s.avg_aquifer_depth_nearby_ft ? s.avg_aquifer_depth_nearby_ft + ' ft' : '—';
            const tr = document.createElement('tr');
            tr.style.cursor = 'pointer';
            tr.innerHTML = `
                <td style="white-space:nowrap;font-weight:600;color:#f1f5f9">${s.name}</td>
                <td>${s.county || '—'}</td>
                <td><span class="status-badge status-${s.status}">${formatStatus(s.status)}</span></td>
                <td class="num">${mw}</td>
                <td class="num">${gpd}</td>
                <td class="num">${gpdMw}</td>
                <td class="num">${s.nearby_monitoring_wells ?? '—'}</td>
                <td class="num">${depth}</td>
            `;
            tr.addEventListener('click', () => {
                if (s.lat && s.lon) map.flyTo([s.lat, s.lon], 9, { duration: 1 });
            });
            tbody.appendChild(tr);
        });
    }

    if (currentView === 'industry') renderStatsBar('industry');
}

// ---------------------------------------------------------------------------
// Public / Learn view
// ---------------------------------------------------------------------------

const GLOSSARY_TERMS = [
    { term: 'Aquifer', def: 'An underground layer of permeable rock, sediment, or soil that contains and transmits groundwater. Aquifers are the source of water for wells and springs.' },
    { term: 'Ogallala Aquifer', def: 'One of the world\'s largest aquifers, stretching beneath 8 states from South Dakota to Texas. It supplies about 30% of all groundwater used for irrigation in the U.S. In West Texas, it is being depleted much faster than it recharges.' },
    { term: 'Acre-foot', def: 'A unit of water volume equal to one acre covered one foot deep — about 325,851 gallons. A family of four uses roughly 1 acre-foot of water per year. A large data center can use thousands of acre-feet per year.' },
    { term: 'Depth to water', def: 'How far underground you must drill to reach the water table. As an aquifer is depleted, this number gets larger — meaning water is harder and more expensive to access.' },
    { term: 'Evaporative cooling', def: 'A cooling method used in data centers and industrial facilities that removes heat through water evaporation. It is highly effective but consumes significant water — roughly 7,500 gallons per day per megawatt of computing capacity.' },
    { term: 'Hybrid cooling', def: 'A mix of air-cooled and evaporative cooling that uses less water than pure evaporative systems, typically around 3,000 gallons per day per MW. Some companies promise hybrid systems but may not deploy them fully for years.' },
    { term: 'ERCOT', def: 'Electric Reliability Council of Texas — the grid operator managing electricity for most of Texas. ERCOT operates an interconnection queue listing power generation projects waiting to connect to the grid.' },
    { term: 'Interconnection queue', def: 'The list of power generation projects (solar, wind, gas, etc.) waiting for approval to connect to the electric grid. A long queue signals strong interest in building in the region.' },
    { term: 'Negative electricity price', def: 'When electricity supply exceeds demand, ERCOT prices can go negative — meaning generators pay to put power on the grid. This happens in West Texas when wind turbines produce more electricity than the region can use, usually late at night. Data centers can benefit from these low-cost periods.' },
    { term: 'TCEQ permit', def: 'A permit from the Texas Commission on Environmental Quality, required for facilities that emit air pollutants or use certain water resources. Air permits for large data centers (especially those with backup diesel generators) must be approved by TCEQ.' },
    { term: 'Settlement point price', def: 'The real-time price of electricity at a specific geographic node on the ERCOT grid, determined every 5 minutes. Prices vary widely across West Texas.' },
    { term: 'Recharge rate', def: 'How quickly water is naturally replenished in an aquifer. The Ogallala recharges less than 1 inch per year in most of West Texas, while pumping can draw down 1–3 feet per year.' },
];

async function loadPublicView() {
    publicData = await fetchJSON('/api/views/public-summary');
    viewCache['public'] = true;

    setText('pub-sites', publicData.tracked_sites);
    const mwStr = publicData.total_capacity_mw
        ? Math.round(publicData.total_capacity_mw).toLocaleString() + ' MW'
        : '-';
    setText('pub-mw', mwStr);

    const homesEq = publicData.total_capacity_mw
        ? Math.round(publicData.total_capacity_mw * 750).toLocaleString()
        : '-';
    setText('pub-homes', homesEq);

    const gpdStr = publicData.total_water_demand_gpd
        ? (publicData.total_water_demand_gpd / 1_000_000).toFixed(1) + ' million gallons'
        : '-';
    setText('pub-gpd', gpdStr);

    const depthStr = publicData.avg_aquifer_depth_ft != null
        ? publicData.avg_aquifer_depth_ft + ' feet'
        : 'Data updating';
    setText('pub-depth', depthStr);

    const cmp = publicData.comparisons;
    setText('cmp-households', cmp.households_equivalent ? cmp.households_equivalent.toLocaleString() + ' households' : '-');
    setText('cmp-farms', cmp.farm_acres_equivalent ? cmp.farm_acres_equivalent.toLocaleString() + ' acres' : '-');
    setText('cmp-pools', cmp.olympic_pools_per_day ? cmp.olympic_pools_per_day.toLocaleString() + ' pools' : '-');

    buildGlossary();

    if (currentView === 'public') renderStatsBar('public');
}

function buildGlossary() {
    const el = document.getElementById('glossary-list');
    if (!el) return;
    el.innerHTML = '';
    GLOSSARY_TERMS.forEach(({ term, def }) => {
        const entry = document.createElement('div');
        entry.className = 'glossary-entry';
        entry.innerHTML = `
            <button class="glossary-term-btn" aria-expanded="false">
                ${term}
                <span class="g-arrow">&#9660;</span>
            </button>
            <div class="glossary-def">${def}</div>
        `;
        const btn = entry.querySelector('.glossary-term-btn');
        const defEl = entry.querySelector('.glossary-def');
        btn.addEventListener('click', () => {
            const isOpen = defEl.classList.toggle('open');
            btn.classList.toggle('open', isOpen);
            btn.setAttribute('aria-expanded', isOpen);
        });
        el.appendChild(entry);
    });
}

// ---------------------------------------------------------------------------
// City selector (Public view)
// ---------------------------------------------------------------------------

function setupCitySelector() {
    document.querySelectorAll('.city-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.city-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            renderCityDetails(btn.dataset.city, btn.dataset.county.split(','));
        });
    });
}

function renderCityDetails(city, counties) {
    if (!publicData) return;
    const matchingSites = (publicData.by_county || []).filter(r =>
        counties.some(c => c.trim().toLowerCase() === (r.county || '').toLowerCase())
    );

    const el = document.getElementById('city-details');
    if (!el) return;

    if (!matchingSites.length) {
        el.innerHTML = `<div style="color:#64748b">No tracked data center projects near ${city} in our database yet.</div>`;
        return;
    }

    const totalSites = matchingSites.reduce((a, b) => a + b.sites, 0);
    const totalMW = matchingSites.reduce((a, b) => a + (b.mw || 0), 0);
    const totalGPD = matchingSites.reduce((a, b) => a + (b.gpd || 0), 0);
    const hhEq = totalGPD ? Math.round(totalGPD / (80 * 2.53)) : 0;

    el.innerHTML = `
        <div class="city-stat"><span>Tracked DC projects near ${city}</span><span class="city-stat-val">${totalSites}</span></div>
        <div class="city-stat"><span>Total proposed capacity</span><span class="city-stat-val">${Math.round(totalMW).toLocaleString()} MW</span></div>
        <div class="city-stat"><span>Estimated daily water use</span><span class="city-stat-val">${(totalGPD / 1_000_000).toFixed(1)}M gal/day</span></div>
        <div class="city-stat"><span>Equivalent to supplying</span><span class="city-stat-val">${hhEq.toLocaleString()} households</span></div>
    `;

    const cityCoords = {
        Lubbock: [33.58, -101.85],
        Amarillo: [35.22, -101.83],
        Midland: [31.99, -102.07],
        Odessa: [31.84, -102.36],
        Dickens: [33.62, -100.84],
    };
    if (cityCoords[city]) map.flyTo(cityCoords[city], 9, { duration: 1 });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val ?? '-';
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

(async () => {
    try {
        // Apply initial panel visibility (Full Dashboard)
        const fullConfig = VIEW_CONFIG['full'];
        ALL_PANELS.forEach(id => {
            const el = document.getElementById(id);
            if (!el || id === 'panel-chart') return;
            el.style.display = fullConfig.panels.includes(id) ? '' : 'none';
        });

        await loadDashboard();
        await loadSites();
        await Promise.allSettled([
            loadWells(null),
            loadErcotSummary(),
            loadErcotMapLayer(),
            loadReservoirSummary(),
            loadEnergyPanel(),
            loadWeatherPanel(),
            loadDroughtSummary(),
            loadDroughtTrendChart(),
            loadDroughtOverlay(),
            loadWaterBudget(),
            loadAgricultureData(),
        ]);
        updateCalc();

    } catch (err) {
        console.error('Failed to load:', err);
    }

    // UI setup — must run regardless of data load success
    layerControl = L.control.layers(null, {
        'Drought Overlay': droughtLayer,
        'Ogallala Wells': wellsLayer,
        'ERCOT Gen Queue': ercotLayer,
        'DC Sites': sitesLayer,
        'Reservoirs': reservoirsLayer,
    }, { collapsed: false, position: 'bottomleft' }).addTo(map);

    // Map coverage note
    const coverageNote = L.control({ position: 'bottomleft' });
    coverageNote.onAdd = function () {
        const div = L.DomUtil.create('div', 'map-coverage-note');
        div.innerHTML = 'Wells shown within 25mi of tracked sites';
        div.style.cssText = 'color:#94a3b8;font-size:10px;padding:2px 6px;background:rgba(15,23,42,0.7);border-radius:3px;margin-top:4px;';
        return div;
    };
    coverageNote.addTo(map);

    // View tab click handlers
    document.querySelectorAll('.view-tab').forEach(btn => {
        btn.addEventListener('click', () => switchView(btn.dataset.view));
    });

    // City selector
    setupCitySelector();
})();
