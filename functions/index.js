// Operation Technology Dashboard — Cloud Functions (Blaze).
//
//   notificationTick     every 10 minutes (WIT). Sends, as chosen in Admin →
//                        Notifications (settings/email):
//                          · the License Alert report, once a day at its hour
//                          · the Daily Report reminder, once a day at its hour
//                          · a Breakdown / damage alert for each new damage record
//   sendLicenseAlert     the "Send now" / Email Report button (callable).
//   sendNotificationNow  "Send test" (breakdown) / "Send now" (reminder) in
//                        Notification Settings — owner only (callable, 2.15.1).
//   deleteSignInAccount  removes the Firebase sign-in account of someone the
//                        owner has just removed or rejected (callable, 2.15.0).
//
// Mail goes out through Gmail with an App Password, kept in Secret Manager
// (GMAIL_USER, GMAIL_APP_PASSWORD). Deploy from Cloud Shell: functions/README.md.
'use strict';

process.env.TZ = 'Asia/Jayapura';   // "today" and every hour below are WIT, like the field teams

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onCall, HttpsError } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');
const logger = require('firebase-functions/logger');
const admin = require('firebase-admin');
const nodemailer = require('nodemailer');
const { composeEmail } = require('./licenseReport');
const { missingReports, composeReminder, composeDamage, shiftDates } = require('./teamNotices');

admin.initializeApp();
const db = admin.firestore();

const GMAIL_USER = defineSecret('GMAIL_USER');
const GMAIL_APP_PASSWORD = defineSecret('GMAIL_APP_PASSWORD');
const SECRETS = [GMAIL_USER, GMAIL_APP_PASSWORD];
const REGION = 'asia-southeast2';   // Jakarta
const OWNER_UIDS = ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72'];
const EMAIL_RE = /^[^\s@<>"',;]+@[^\s@<>"',;]+\.[^\s@<>"',;]+$/;

// Gmail refusing the sign-in (wrong App Password) is the one failure the owner
// can fix, so it gets its own code; everything else is 'internal'.
function mailError(err) {
    if (err && (err.code === 'EAUTH' || err.responseCode === 535))
        return new HttpsError('failed-precondition', 'gmail-auth');
    return new HttpsError('internal', 'The email could not be sent');
}
const cleanList = v => (Array.isArray(v) ? v : []).map(r => String(r || '').trim()).filter(r => EMAIL_RE.test(r)).slice(0, 10);
const cleanHour = (v, dflt) => { const h = Number(v); return Number.isInteger(h) && h >= 0 && h <= 23 ? h : dflt; };

async function emailSettings() {
    const snap = await db.doc('settings/email').get();
    const s = snap.exists ? snap.data() : {};
    return {
        recipients: cleanList(s.recipients),
        dailyEnabled: !!s.dailyEnabled,
        sendHour: cleanHour(s.sendHour, 7),
        reminderEnabled: !!s.reminderEnabled,
        reminderHour: cleanHour(s.reminderHour, 17),
        reminderRecipients: cleanList(s.reminderRecipients),
        damageEnabled: !!s.damageEnabled,
        damageRecipients: cleanList(s.damageRecipients)
    };
}

async function mail(to, m, log) {
    const user = GMAIL_USER.value().trim();
    const transport = nodemailer.createTransport({
        service: 'gmail',
        // Spaces (also the non-breaking ones a copy from Google's page brings)
        // and a stray newline are not part of an App Password.
        auth: { user, pass: GMAIL_APP_PASSWORD.value().replace(/\s+/g, '') }
    });
    await transport.sendMail({ from: `"OT Dashboard" <${user}>`, to: to.join(', '), subject: m.subject, text: m.text, html: m.html });
    await db.collection('mailLog').add({ ...log, recipients: to, subject: m.subject, at: admin.firestore.FieldValue.serverTimestamp() });
}

// ---- License Alert ----
async function sendReport(trigger, by) {
    const { recipients } = await emailSettings();
    if (!recipients.length) return { sent: false, reason: 'no-recipients' };
    const [units, stock] = await Promise.all([db.collection('units').get(), db.collection('licenseStock').get()]);
    const m = composeEmail(units.docs.map(d => d.data()), stock.docs.map(d => d.data()), { timeZone: 'Asia/Jayapura' });
    if (!m) return { sent: false, reason: 'nothing-to-report' };
    await mail(recipients, m, { kind: 'licenseAlert', trigger, by: by || '', items: m.report.total });
    return { sent: true, recipients: recipients.length, items: m.report.total };
}

// ---- Daily Report reminder ----
async function sendReminder(s, now, trigger, by) {
    if (!s.reminderRecipients.length) return { sent: false, reason: 'no-recipients' };
    const { today, yesterday } = shiftDates(now);
    const dates = [yesterday, today];
    const [shifts, logs, members, leaves] = await Promise.all([
        db.collection('shifts').where('date', 'in', dates).get(),
        db.collection('workLogs').where('date', 'in', dates).get(),
        db.collection('teamMembers').get(),
        db.collection('leaveRequests').where('approval', '==', 'approved').get()
    ]);
    const list = missingReports({ shifts: shifts.docs.map(d => d.data()), workLogs: logs.docs.map(d => d.data()),
        members: members.docs.map(d => ({ ...d.data(), id: d.id })), leaves: leaves.docs.map(d => d.data()), now });
    const m = composeReminder(list);
    if (!m) return { sent: false, reason: 'all-sent' };
    await mail(s.reminderRecipients, m, { kind: 'reportReminder', trigger: trigger || 'daily', by: by || '', items: list.length });
    return { sent: true, items: list.length };
}

// ---- Damage / Breakdown alerts ----
// Every tick looks at damage records changed in the last 3 days and mails
// the ones not mailed before (their ids are kept in mailState/damage). A
// record written offline and synced hours later is still caught. The very
// first run only takes note of what already exists.
const DAMAGE_WINDOW_MS = 3 * 86400000;
async function scanDamage(s, now) {
    const state = db.doc('mailState/damage');
    const prev = await state.get();
    const seen = new Set(prev.exists ? (prev.data().ids || []) : []);
    const snap = await db.collection('damageRecords').where('updatedAt', '>', now.getTime() - DAMAGE_WINDOW_MS).get();
    const fresh = snap.docs.filter(d => !seen.has(d.id));
    if (!fresh.length && prev.exists) return 0;
    let sent = 0;
    if (prev.exists && s.damageEnabled && s.damageRecipients.length) {
        for (const d of fresh) {
            const rec = d.data();
            // New records only: an old record edited today is not news.
            if (!rec.createdAt || rec.createdAt < now.getTime() - DAMAGE_WINDOW_MS) continue;
            const m = composeDamage(rec);
            if (!m) continue;
            await mail(s.damageRecipients, m, { kind: 'damageAlert', trigger: 'auto', by: '', items: 1, recordId: d.id });
            sent++;
        }
    }
    fresh.forEach(d => seen.add(d.id));
    await state.set({ ids: [...seen].slice(-800), at: admin.firestore.FieldValue.serverTimestamp() });
    return sent;
}

// Once a day per kind: mailState/<kind>.date is the WIT date it last ran.
async function oncePerDay(kind, today, fn) {
    const ref = db.doc(`mailState/${kind}`);
    const prev = await ref.get();
    if (prev.exists && prev.data().date === today) return;
    const r = await fn();
    await ref.set({ date: today, result: r.sent ? 'sent' : (r.reason || 'skipped'), at: admin.firestore.FieldValue.serverTimestamp() });
    logger.info(`${kind}`, r);
}

exports.notificationTick = onSchedule({
    schedule: '*/10 * * * *', timeZone: 'Asia/Jayapura', region: REGION, secrets: SECRETS, retryCount: 0
}, async () => {
    const now = new Date();
    const s = await emailSettings();
    const today = shiftDates(now).today;
    const hour = now.getHours();
    const jobs = [];
    if (s.dailyEnabled && hour === s.sendHour) jobs.push(oncePerDay('daily', today, () => sendReport('daily', '')));
    if (s.reminderEnabled && hour === s.reminderHour) jobs.push(oncePerDay('reminder', today, () => sendReminder(s, now)));
    jobs.push(scanDamage(s, now).then(n => { if (n) logger.info('damage alerts sent', { n }); }));
    const results = await Promise.allSettled(jobs);
    results.filter(r => r.status === 'rejected').forEach(r => logger.error('notification job failed', r.reason));
});

// ---- Who may do what (same rules as firestore.rules) ----
async function userDoc(uid) {
    const snap = await db.doc(`users/${uid}`).get();
    return snap.exists ? snap.data() : null;
}
function isOwnerDoc(uid, d) { return OWNER_UIDS.includes(uid) || (!!d && d.status === 'active' && d.role === 'owner'); }
async function mayEmail(uid) {
    const d = await userDoc(uid);
    if (isOwnerDoc(uid, d)) return true;
    if (!d || d.status !== 'active') return false;
    const access = d.access && typeof d.access === 'object' ? d.access : {};
    const scope = access.unitScope == null ? 'all' : access.unitScope;
    if (!['all', 'tractor'].includes(scope)) return false;
    const lvl = access.licenseStock != null ? access.licenseStock : (d.role === 'team' ? 'edit' : 'none');
    return lvl === 'view' || lvl === 'edit';
}

exports.sendLicenseAlert = onCall({ region: REGION, secrets: SECRETS, enforceAppCheck: true }, async req => {
    if (!req.auth) throw new HttpsError('unauthenticated', 'Sign in first');
    if (!(await mayEmail(req.auth.uid))) throw new HttpsError('permission-denied', 'No access to License Stock');
    try {
        return await sendReport('manual', req.auth.token.email || req.auth.uid);
    } catch (err) {
        logger.error('license alert failed', err);
        throw mailError(err);
    }
});

// 2.15.1: the per-section buttons in Notification Settings.
//   damage   — the newest damage record, as the alert would look, marked [Test]
//   reminder — who has not sent the report of an ended shift, right now
exports.sendNotificationNow = onCall({ region: REGION, secrets: SECRETS, enforceAppCheck: true }, async req => {
    if (!req.auth) throw new HttpsError('unauthenticated', 'Sign in first');
    if (!isOwnerDoc(req.auth.uid, await userDoc(req.auth.uid))) throw new HttpsError('permission-denied', 'Only the Super Admin');
    const kind = req.data && req.data.kind;
    const s = await emailSettings();
    const by = req.auth.token.email || req.auth.uid;
    try {
        if (kind === 'damage') {
            if (!s.damageRecipients.length) return { sent: false, reason: 'no-recipients' };
            const snap = await db.collection('damageRecords').orderBy('createdAt', 'desc').limit(1).get();
            if (snap.empty) return { sent: false, reason: 'no-records' };
            const m = composeDamage({ ...snap.docs[0].data(), resolved: false });
            m.subject = `[Test] ${m.subject}`;
            await mail(s.damageRecipients, m, { kind: 'damageAlert', trigger: 'manual', by, items: 1, recordId: snap.docs[0].id });
            return { sent: true, recipients: s.damageRecipients.length };
        }
        if (kind === 'reminder') {
            const r = await sendReminder(s, new Date(), 'manual', by);
            return r.sent ? { ...r, recipients: s.reminderRecipients.length } : r;
        }
    } catch (err) {
        logger.error('send now failed', err);
        throw mailError(err);
    }
    throw new HttpsError('invalid-argument', 'Unknown notification');
});

// 2.15.0: the owner removed or rejected someone — their dashboard record is
// already gone; this deletes the sign-in account too. Refused while the
// person still has a record (remove them first), and never for an owner.
exports.deleteSignInAccount = onCall({ region: REGION, enforceAppCheck: true }, async req => {
    if (!req.auth) throw new HttpsError('unauthenticated', 'Sign in first');
    const me = await userDoc(req.auth.uid);
    if (!isOwnerDoc(req.auth.uid, me)) throw new HttpsError('permission-denied', 'Only the Super Admin');
    const uid = String((req.data && req.data.uid) || '');
    if (!/^[A-Za-z0-9]{10,128}$/.test(uid)) throw new HttpsError('invalid-argument', 'No account given');
    if (OWNER_UIDS.includes(uid) || uid === req.auth.uid) throw new HttpsError('failed-precondition', 'The owner account cannot be deleted');
    if (await userDoc(uid)) throw new HttpsError('failed-precondition', 'Remove the person from the dashboard first');
    try {
        await admin.auth().deleteUser(uid);
    } catch (err) {
        if (err && err.code === 'auth/user-not-found') return { deleted: false, reason: 'not-found' };
        logger.error('delete sign-in account failed', err);
        throw new HttpsError('internal', 'The account could not be deleted');
    }
    await db.collection('mailLog').add({ kind: 'accountDeleted', by: req.auth.token.email || req.auth.uid, uid, at: admin.firestore.FieldValue.serverTimestamp() });
    logger.info('sign-in account deleted', { uid });
    return { deleted: true };
});
