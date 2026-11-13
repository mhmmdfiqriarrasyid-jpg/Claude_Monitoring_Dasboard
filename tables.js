// Long lists without a scroll inside the page (2.42.0).
// A table used to sit in a box 320–640px tall that scrolled on its own, inside a page that
// scrolls too: the mouse wheel moved one or the other, and the side scrollbar hid at the
// bottom of the box. Now, for the lists below:
//   · the box is as tall as its rows — only the page scrolls;
//   · past 50 rows the rest wait behind "Show 50 more" / "Show all" (search and filters
//     still look at everything; the limit stays while the list is redrawn);
//   · when the table fits the width, its header stays at the top while the page scrolls.
// It watches each list's <tbody>, so the code that draws the rows does not change.
'use strict';

const TW_STEP = 50;
// The lists, by their <tbody> id.
const TW_LISTS = ['detailBody', 'repairBody', 'damageBody', 'licenseBody', 'workLogBody', 'leaveBody', 'recapBody',
    'insPlanBody', 'insReportBody', 'deviceBody', 'stockBody', 'fuelBody', 'jobBody'];
const _twLimit = new Map();      // tbody id → rows shown
let _twQueued = new Set(), _twRaf = 0;

// A "row" is a main row with the detail rows that follow it (they open under it).
const _twIsDetail = tr => /(^|[\s-])(detail|sub|expand|child)([\s-]|$)/i.test(tr.className || '') || tr.hasAttribute('data-detail-of');

function twApply(tbody) {
    if (!tbody || !tbody.isConnected) return;
    const wrap = tbody.closest('.table-wrapper'), table = tbody.closest('table');
    if (!wrap || !table) return;
    wrap.classList.add('tw-page');
    const rows = [...tbody.children].filter(tr => tr.tagName === 'TR');
    const main = rows.filter(tr => !_twIsDetail(tr) && !tr.querySelector('td[colspan].adm-empty, td.empty-state'));
    const limit = _twLimit.get(tbody.id) || TW_STEP;
    let shown = 0;
    rows.forEach(tr => {
        if (!_twIsDetail(tr)) shown++;
        tr.classList.toggle('tw-hide', main.length > limit && shown > limit);
    });
    // The bar under the list.
    let bar = wrap.nextElementSibling && wrap.nextElementSibling.classList.contains('tw-more') ? wrap.nextElementSibling : null;
    if (main.length > limit) {
        if (!bar) { bar = document.createElement('div'); bar.className = 'tw-more'; wrap.after(bar); }
        const next = Math.min(TW_STEP, main.length - limit);
        bar.innerHTML = `<span class="tw-more__n">Showing ${limit} of ${main.length}</span>
            <button type="button" class="btn btn-secondary btn-sm" onclick="twMore('${tbody.id}', ${limit + TW_STEP})">Show ${next} more</button>
            <button type="button" class="btn btn-secondary btn-sm" onclick="twMore('${tbody.id}', Infinity)">Show all</button>`;
    } else if (bar) bar.remove();
    twFit(wrap, table);
}
// Fits the width: the box opens up, so its header can stay at the top of the page.
function twFit(wrap, table) {
    const fits = window.innerWidth > 1024 && table.scrollWidth <= wrap.clientWidth + 1;
    wrap.classList.toggle('tw-fit', fits);
    const card = wrap.closest('.table-card');
    if (card) card.classList.toggle('tw-card-open', fits);
}
function twMore(id, n) {
    _twLimit.set(id, n);
    const tb = document.getElementById(id);
    if (tb) twApply(tb);
}
function _twQueue(tb) {
    _twQueued.add(tb);
    if (_twRaf) return;
    _twRaf = requestAnimationFrame(() => { _twRaf = 0; const q = [..._twQueued]; _twQueued = new Set(); q.forEach(twApply); });
}
function twInit() {
    TW_LISTS.forEach(id => {
        const tb = document.getElementById(id);
        if (!tb) return;
        new MutationObserver(() => _twQueue(tb)).observe(tb, { childList: true });
        _twQueue(tb);
    });
    let t = 0;
    window.addEventListener('resize', () => { clearTimeout(t); t = setTimeout(() => TW_LISTS.forEach(id => { const tb = document.getElementById(id); if (tb) twApply(tb); }), 150); });
}
// A list drawn while its page was hidden measures 0 wide: measured again when a page or tab opens (navigateTo).
function twRefresh() { setTimeout(() => TW_LISTS.forEach(id => { const tb = document.getElementById(id); if (tb && tb.offsetParent) twApply(tb); }), 0); }
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', twInit); else twInit();
