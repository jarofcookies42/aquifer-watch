/**
 * AquiferWatch — Frontend Application
 * Leaflet map + API integration for data center & well visualization.
 * Phase 6: Multi-audience view switcher (Full, Policy, Industry, Public).
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

    // Update tab active state
    document.querySelectorAll('.view-tab').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    // Show/hide panels
    ALL_PANELS.forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        // panel-chart is managed by selectSite, keep its display separate
        if (id === 'panel-chart') return;
        el.style.display = config.panels.includes(id) ? '' : 'none';
    });

    // Map layer visibility
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
    // Sites always visible
    if (!map.hasLayer(sitesLayer)) sitesLayer.addTo(map);

    // Update header stats
    renderStatsBar(config.statsMode);

    // Load view data if not cached
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
    // Fallback: keep current content if data isn't loaded yet
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
// Load dashboard stats (Full view)
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

    // Chart is only in Full view
    if (currentView === 'full') {
        await Promise.all([loadWells(siteId), loadWaterLevelChart(siteId)]);
    } else {
        // Still load wells if the current view shows them
        const showWells = VIEW_CONFIG[currentView].mapLayers.wells;
        if (showWells) await loadWells(siteId);
    }
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
    const section = document.getElementById('panel-chart');

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
        // Only show chart in full view
        if (currentView === 'full') {
            section.style.display = 'block';
            drawChart(data);
        }
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

    ctx.fillStyle = '#0f172a';
    ctx.fillRect(0, 0, W, H);

    if (!data.length) return;

    const depths = data.map(d => d.avg_depth_ft);
    const avgMin = Math.min(...depths);
    const avgMax = Math.max(...depths);
    const rangePad = Math.max((avgMax - avgMin) * 0.3, 10);
    const yMin = Math.floor((avgMin - rangePad) / 5) * 5;
    const yMax = Math.ceil((avgMax + rangePad) / 5) * 5;

    const xScale = (i) => pad.left + (i / (data.length - 1)) * plotW;
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

    // X axis labels
    ctx.textAlign = 'center';
    ctx.fillStyle = '#64748b';
    ctx.font = '11px -apple-system, sans-serif';
    data.forEach((d, i) => {
        if (i % 3 === 0 || i === data.length - 1) {
            ctx.fillText(d.year, xScale(i), H - pad.bottom + 16);
        }
    });

    // Range band
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
        ctx[i === 0 ? 'moveTo' : 'lineTo'](xScale(i), yScale(d.avg_depth_ft));
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
    ctx.fillStyle = change > 0 ? '#ef4444' : '#22c55e';
    ctx.font = 'bold 12px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(
        `${change > 0 ? '+' : ''}${change.toFixed(1)} ft over ${data.length} yrs`,
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
// Policy & Planning view
// ---------------------------------------------------------------------------

async function loadPolicyView() {
    policyData = await fetchJSON('/api/views/policy-summary');
    viewCache['policy'] = true;

    // Big metrics
    const fmtMW = v => v ? Math.round(v).toLocaleString() + ' MW' : '-';
    const fmtGPD = v => v ? (v / 1_000_000).toFixed(1) + 'M gal/day' : '-';
    const fmtAcft = v => v ? Math.round(v).toLocaleString() + ' ac-ft' : '-';

    setText('pm-total-mw', fmtMW(policyData.total_capacity_mw));
    setText('pm-total-gpd', fmtGPD(policyData.total_water_demand_gpd));
    setText('pm-acft', fmtAcft(policyData.total_water_demand_acft_yr));

    const pct = policyData.dc_pct_of_regional_ag_water;
    setText('pm-pct-ag', pct != null ? pct.toFixed(3) + '%' : '-');

    // Aquifer depletion card
    const rate = policyData.aquifer_depletion_rate_ft_per_yr;
    const ttd = policyData.time_to_depletion_yrs;
    if (rate != null) {
        const rateStr = (rate > 0 ? '+' : '') + rate + ' ft/yr';
        const rateColor = rate > 0 ? 'danger' : 'ok';
        document.getElementById('pm-depletion-val').textContent = rateStr;
        document.getElementById('pm-depletion-val').className = `metric-val ${rateColor}`;
        setText('pm-depletion-label', 'Aquifer Depletion Rate');
        setText('pm-depletion-sub',
            ttd ? `At this rate, remaining saturated thickness (~100 ft avg) exhausted in ~${Math.round(ttd).toLocaleString()} years`
                : 'Based on TWDB well measurements over past 10 years');
    }

    // Pipeline table
    const tbody = document.getElementById('policy-pipeline-rows');
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

    // Aquifer status text
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
    document.getElementById('policy-aquifer-content').innerHTML = aqHtml;

    // Update stats bar if currently in policy view
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

    // Energy context
    setText('ind-ercot-mw',
        indData.ercot_total_mw ? Math.round(indData.ercot_total_mw / 1000) + 'K MW' : '-');
    setText('ind-ercot-projects', `${indData.ercot_total_projects} projects in queue`);
    setText('ind-renewable-pct', indData.renewable_pct + '%');
    setText('ind-renewable-mw',
        indData.renewable_mw ? Math.round(indData.renewable_mw).toLocaleString() + ' MW solar+wind+battery' : '-');

    // Fuel breakdown mini-bars
    const fuelEl = document.getElementById('ind-fuel-breakdown');
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

    // Water availability by county
    const waterEl = document.getElementById('ind-water-avail');
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

    // Site comparison table
    const tbody = document.getElementById('compare-rows');
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

    if (currentView === 'industry') renderStatsBar('industry');
}

// ---------------------------------------------------------------------------
// Public / Learn view
// ---------------------------------------------------------------------------

const GLOSSARY_TERMS = [
    {
        term: 'Aquifer',
        def: 'An underground layer of permeable rock, sediment, or soil that contains and transmits groundwater. Aquifers are the source of water for wells and springs.',
    },
    {
        term: 'Ogallala Aquifer',
        def: 'One of the world\'s largest aquifers, stretching beneath 8 states from South Dakota to Texas. It supplies about 30% of all groundwater used for irrigation in the U.S. In West Texas, it is being depleted much faster than it recharges.',
    },
    {
        term: 'Acre-foot',
        def: 'A unit of water volume equal to one acre covered one foot deep — about 325,851 gallons. A family of four uses roughly 1 acre-foot of water per year. A large data center can use thousands of acre-feet per year.',
    },
    {
        term: 'Depth to water',
        def: 'How far underground you must drill to reach the water table. As an aquifer is depleted, this number gets larger — meaning water is harder and more expensive to access.',
    },
    {
        term: 'Evaporative cooling',
        def: 'A cooling method used in data centers and industrial facilities that removes heat through water evaporation. It is highly effective but consumes significant water — roughly 7,500 gallons per day per megawatt of computing capacity.',
    },
    {
        term: 'Hybrid cooling',
        def: 'A mix of air-cooled and evaporative cooling that uses less water than pure evaporative systems, typically around 3,000 gallons per day per MW. Some companies promise hybrid systems but may not deploy them fully for years.',
    },
    {
        term: 'ERCOT',
        def: 'Electric Reliability Council of Texas — the grid operator managing electricity for most of Texas. ERCOT operates an interconnection queue listing power generation projects waiting to connect to the grid.',
    },
    {
        term: 'Interconnection queue',
        def: 'The list of power generation projects (solar, wind, gas, etc.) waiting for approval to connect to the electric grid. A long queue signals strong interest in building in the region.',
    },
    {
        term: 'Negative electricity price',
        def: 'When electricity supply exceeds demand, ERCOT prices can go negative — meaning generators pay to put power on the grid. This happens in West Texas when wind turbines produce more electricity than the region can use, usually late at night. Data centers can benefit from these low-cost periods.',
    },
    {
        term: 'TCEQ permit',
        def: 'A permit from the Texas Commission on Environmental Quality, required for facilities that emit air pollutants or use certain water resources. Air permits for large data centers (especially those with backup diesel generators) must be approved by TCEQ.',
    },
    {
        term: 'Settlement point price',
        def: 'The real-time price of electricity at a specific geographic node on the ERCOT grid, determined every 5 minutes. Prices vary widely across West Texas.',
    },
    {
        term: 'Recharge rate',
        def: 'How quickly water is naturally replenished in an aquifer. The Ogallala recharges less than 1 inch per year in most of West Texas, while pumping can draw down 1–3 feet per year.',
    },
];

async function loadPublicView() {
    publicData = await fetchJSON('/api/views/public-summary');
    viewCache['public'] = true;

    // Fact cards
    setText('pub-sites', publicData.tracked_sites);
    const mwStr = publicData.total_capacity_mw
        ? Math.round(publicData.total_capacity_mw).toLocaleString() + ' MW'
        : '-';
    setText('pub-mw', mwStr);

    // Homes equivalent: 1 MW powers ~750 homes
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

    // Comparisons
    const cmp = publicData.comparisons;
    setText('cmp-households', cmp.households_equivalent ? cmp.households_equivalent.toLocaleString() + ' households' : '-');
    setText('cmp-farms', cmp.farm_acres_equivalent ? cmp.farm_acres_equivalent.toLocaleString() + ' acres' : '-');
    setText('cmp-pools', cmp.olympic_pools_per_day ? cmp.olympic_pools_per_day.toLocaleString() + ' pools' : '-');

    // Build glossary
    buildGlossary();

    if (currentView === 'public') renderStatsBar('public');
}

function buildGlossary() {
    const el = document.getElementById('glossary-list');
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
    if (!matchingSites.length) {
        el.innerHTML = `<div style="color:#64748b">No tracked data center projects near ${city} in our database yet.</div>`;
        return;
    }

    const totalSites = matchingSites.reduce((a, b) => a + b.sites, 0);
    const totalMW = matchingSites.reduce((a, b) => a + (b.mw || 0), 0);
    const totalGPD = matchingSites.reduce((a, b) => a + (b.gpd || 0), 0);
    const hhEq = totalGPD ? Math.round(totalGPD / (80 * 2.53)) : 0;

    el.innerHTML = `
        <div class="city-stat">
            <span>Tracked DC projects near ${city}</span>
            <span class="city-stat-val">${totalSites}</span>
        </div>
        <div class="city-stat">
            <span>Total proposed capacity</span>
            <span class="city-stat-val">${Math.round(totalMW).toLocaleString()} MW</span>
        </div>
        <div class="city-stat">
            <span>Estimated daily water use</span>
            <span class="city-stat-val">${(totalGPD / 1_000_000).toFixed(1)}M gal/day</span>
        </div>
        <div class="city-stat">
            <span>Equivalent to supplying</span>
            <span class="city-stat-val">${hhEq.toLocaleString()} households</span>
        </div>
    `;

    // Fly to area on map
    const countyData = publicData.by_county.find(r =>
        counties.some(c => c.trim().toLowerCase() === (r.county || '').toLowerCase())
    );
    // Rough city coordinates for map zoom
    const cityCoords = {
        Lubbock: [33.58, -101.85],
        Amarillo: [35.22, -101.83],
        Midland: [31.99, -102.07],
        Odessa: [31.84, -102.36],
        Dickens: [33.62, -100.84],
    };
    if (cityCoords[city]) {
        map.flyTo(cityCoords[city], 9, { duration: 1 });
    }
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
        await Promise.all([loadWells(null), loadErcotSummary(), loadErcotMapLayer()]);
        updateCalc();

        // Layer toggle control
        layerControl = L.control.layers(null, {
            'Ogallala Wells': wellsLayer,
            'ERCOT Gen Queue': ercotLayer,
            'DC Sites': sitesLayer,
        }, { collapsed: false, position: 'bottomleft' }).addTo(map);

        // View tab click handlers
        document.querySelectorAll('.view-tab').forEach(btn => {
            btn.addEventListener('click', () => switchView(btn.dataset.view));
        });

        // City selector
        setupCitySelector();

    } catch (err) {
        console.error('Failed to load:', err);
    }
})();
