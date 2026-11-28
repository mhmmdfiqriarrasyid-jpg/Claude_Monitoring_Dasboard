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
function twRefresh() { setTimeout(() => { TW_LISTS.forEach(id => { const tb = document.getElementById(id); if (tb && tb.offsetParent) twApply(tb); }); twPinAll(); }, 0); }

// ---- Pinned bars and columns (2.50.0) ----
const TW_PIN_BARS = '.view > .units-filters, .view .adm-filters, .view > .dmg-filters, .view > .bast-toolbar, .view .fp-filterbar';
// The header of the column that names a row, in the order they are looked for.
const TW_PIN_HEAD = /^(nickname|unit|device|no surat|member|item|vehicle|pr|profile|person|unit \/ row)$/i;
// The pinned bar of the page on screen: its height lets a sticky table header sit below it.
function twPinBar() {
    const bars = [...document.querySelectorAll(TW_PIN_BARS)].filter(el => el.offsetParent);
    return bars[0] || null;
}
function twPinMeasure() {
    // On a phone or tablet the page scrolls under the top bar: the bar stops right below it.
    const tb = document.querySelector('.topbar');
    if (window.innerWidth <= 900 && tb && tb.offsetHeight) document.documentElement.style.setProperty('--pin-top', tb.offsetHeight + 'px');
    else document.documentElement.style.removeProperty('--pin-top');
    const bar = twPinBar();
    document.documentElement.style.setProperty('--pin-h', bar ? bar.offsetHeight + 'px' : '0px');
    twPinStuck();
}
// A shadow under the bar once it is stuck at the top.
function twPinStuck() {
    const top = parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--pin-top')) || 0;
    document.querySelectorAll(TW_PIN_BARS).forEach(el => {
        if (!el.offsetParent) { el.classList.remove('pin-stuck'); return; }
        // Where a stuck bar stops: under the top bar (the page scrolls) or at the top of .main.
        const sc = el.closest('.main');
        const own = sc && getComputedStyle(sc).overflowY !== 'visible' && sc.scrollHeight > sc.clientHeight;
        const limit = own ? sc.getBoundingClientRect().top + (parseFloat(getComputedStyle(sc).paddingTop) || 0) + top : top;
        el.classList.toggle('pin-stuck', el.getBoundingClientRect().top <= limit + 1 && (own ? sc.scrollTop > 0 : window.scrollY > 0));
    });
}
// On a tablet or phone a wide table scrolls sideways: its naming column stays at the left.
// Off on a desktop, and where a phone shows the table as cards.
function twPinCol(table) {
    if (!table) return;
    const head = table.tHead && table.tHead.rows[0];
    table.querySelectorAll('.tw-pin-col').forEach(c => c.classList.remove('tw-pin-col'));
    if (!head) return;
    const w = window.innerWidth;
    if (w > 1024 || (w <= 700 && table.classList.contains('table--cardable'))) return;
    const wrap = table.closest('.table-wrapper');
    if (!wrap || table.scrollWidth <= wrap.clientWidth + 1) return;
    const n = head.cells.length;
    const idx = [...head.cells].findIndex(c => TW_PIN_HEAD.test((c.textContent || '').trim()));
    if (idx < 0) return;
    // A table that pins its own columns (the Units table, the roster) is left as it is.
    if (getComputedStyle(head.cells[idx]).position === 'sticky') return;
    head.cells[idx].classList.add('tw-pin-col');
    [...table.tBodies].forEach(tb => [...tb.rows].forEach(tr => { if (tr.cells.length === n && tr.cells[idx]) tr.cells[idx].classList.add('tw-pin-col'); }));
}
function twPinAll() {
    document.querySelectorAll('.table-wrapper table').forEach(t => { if (t.offsetParent) twPinCol(t); });
    twPinMeasure();
}
(function twPinInit() {
    const start = () => {
        const seen = new WeakSet();
        let raf = 0;
        const queue = () => { if (raf) return; raf = requestAnimationFrame(() => { raf = 0; twPinAll(); }); };
        document.querySelectorAll('.table-wrapper tbody').forEach(tb => {
            if (seen.has(tb)) return; seen.add(tb);
            new MutationObserver(queue).observe(tb, { childList: true });
        });
        let s = 0;
        const onScroll = () => { if (s) return; s = requestAnimationFrame(() => { s = 0; twPinStuck(); }); };
        window.addEventListener('scroll', onScroll, { passive: true });
        const main = document.querySelector('.main');
        if (main) main.addEventListener('scroll', onScroll, { passive: true });
        window.addEventListener('resize', queue);
        queue();
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', twInit); else twInit();
