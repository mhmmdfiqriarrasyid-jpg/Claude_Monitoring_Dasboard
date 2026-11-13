// When a scheduled notice is due, and which period a summary covers (2.20.0).
// All in local time (WIT on the server). tests/schedule_test.js.
'use strict';
const FREQS = ['daily', 'weekly', 'monthly'];
const pad = n => String(n).padStart(2, '0');
const iso = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

// { freq, hour, weekday (0 = Sunday … 6), monthday (1–28) } from stored fields,
// with the defaults each notice had before it could be scheduled.
function readSchedule(s, prefix, dflt) {
    const int = (v, lo, hi, d) => { const n = Number(v); return Number.isInteger(n) && n >= lo && n <= hi ? n : d; };
    return {
        freq: FREQS.includes(s[prefix + 'Freq']) ? s[prefix + 'Freq'] : dflt.freq,
        hour: int(s[prefix === 'send' ? 'sendHour' : prefix + 'Hour'], 0, 23, dflt.hour),
        weekday: int(s[prefix + 'Weekday'], 0, 6, dflt.weekday == null ? 1 : dflt.weekday),
        monthday: int(s[prefix + 'Monthday'], 1, 28, dflt.monthday == null ? 1 : dflt.monthday)
    };
}
function isDue(sched, now) {
    const d = new Date(now);
    if (d.getHours() !== sched.hour) return false;
    if (sched.freq === 'weekly') return d.getDay() === sched.weekday;
    if (sched.freq === 'monthly') return d.getDate() === sched.monthday;
    return true;
}
// The period a summary sent `now` reports on: yesterday, the 7 days before
// today, or the calendar month before this one.
function periodRange(freq, now) {
    const d = new Date(now);
    const today = new Date(d.getFullYear(), d.getMonth(), d.getDate());
    let start, end = today, name, label;
    const fmt = (x, o) => x.toLocaleDateString('en-GB', o);
    if (freq === 'daily') {
        start = new Date(today); start.setDate(start.getDate() - 1);
        name = 'Daily Summary'; label = fmt(start, { day: 'numeric', month: 'long', year: 'numeric' });
    } else if (freq === 'weekly') {
        start = new Date(today); start.setDate(start.getDate() - 7);
        const last = new Date(today); last.setDate(last.getDate() - 1);
        name = 'Weekly Summary'; label = `${fmt(start, { day: 'numeric', month: 'short' })} – ${fmt(last, { day: 'numeric', month: 'short', year: 'numeric' })}`;
    } else {
        start = new Date(d.getFullYear(), d.getMonth() - 1, 1);
        end = new Date(d.getFullYear(), d.getMonth(), 1);
        name = 'Monthly Report'; label = fmt(start, { month: 'long', year: 'numeric' });
    }
    return { start: start.getTime(), end: end.getTime(), from: iso(start), to: iso(end), name, label };
}
module.exports = { FREQS, readSchedule, isDue, periodRange };
