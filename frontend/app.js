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
            loadWeatherPanel(),
            loadDroughtSummary(),
            loadDroughtTrendChart(),
            loadDroughtOverlay(),
        ]);
        updateCalc();

        // Layer toggle control
        L.control.layers(null, {
            'Drought Overlay': droughtLayer,
            'Ogallala Wells': wellsLayer,
            'ERCOT Gen Queue': ercotLayer,
            'DC Sites': sitesLayer,
        }, { collapsed: false, position: 'bottomleft' }).addTo(map);
    } catch (err) {
        console.error('Failed to load:', err);
    }
})();
