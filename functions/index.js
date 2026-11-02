// Operation Technology Dashboard — Cloud Functions (2.14.0, Blaze).
//
//   dailyLicenseAlert  every morning 07:00 WIT: emails the licence alert
//                      report to the recipients in settings/email, when the
//                      daily email is on and something is expiring.
//   sendLicenseAlert   the dashboard's "Email Report" button (callable).
//
// Mail goes out through Gmail with an App Password, kept in Secret Manager:
//   firebase functions:secrets:set GMAIL_USER
//   firebase functions:secrets:set GMAIL_APP_PASSWORD
// Deploy from Cloud Shell: see functions/README.md.
'use strict';

process.env.TZ = 'Asia/Jayapura';   // "today" and expiry days in WIT, like the field teams

const { onSchedule } = require('firebase-functions/v2/scheduler');
const { onCall, HttpsError } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');
const logger = require('firebase-functions/logger');
const admin = require('firebase-admin');
const nodemailer = require('nodemailer');
const { composeEmail } = require('./licenseReport');

admin.initializeApp();
const db = admin.firestore();

const GMAIL_USER = defineSecret('GMAIL_USER');
const GMAIL_APP_PASSWORD = defineSecret('GMAIL_APP_PASSWORD');
const REGION = 'asia-southeast2';   // Jakarta
const OWNER_UIDS = ['vWZZ3jz1LTdzrU5c5Q1Gzco8nh72'];
const EMAIL_RE = /^[^\s@<>"',;]+@[^\s@<>"',;]+\.[^\s@<>"',;]+$/;

async function emailSettings() {
    const snap = await db.doc('settings/email').get();
    const s = snap.exists ? snap.data() : {};
    const recipients = (Array.isArray(s.recipients) ? s.recipients : [])
        .map(r => String(r || '').trim()).filter(r => EMAIL_RE.test(r)).slice(0, 10);
    return { recipients, dailyEnabled: !!s.dailyEnabled };
}

async function sendReport(trigger, by) {
    const { recipients } = await emailSettings();
    if (!recipients.length) return { sent: false, reason: 'no-recipients' };
    const [units, stock] = await Promise.all([db.collection('units').get(), db.collection('licenseStock').get()]);
    const mail = composeEmail(units.docs.map(d => d.data()), stock.docs.map(d => d.data()), { timeZone: 'Asia/Jayapura' });
    if (!mail) return { sent: false, reason: 'nothing-to-report' };
    const transport = nodemailer.createTransport({
        service: 'gmail',
        // Spaces (also the non-breaking ones a copy from Google's page brings)
        // and a stray newline are not part of an App Password.
        auth: { user: GMAIL_USER.value().trim(), pass: GMAIL_APP_PASSWORD.value().replace(/\s+/g, '') }
    });
    await transport.sendMail({
        from: `"OT Dashboard" <${GMAIL_USER.value().trim()}>`,
        to: recipients.join(', '),
        subject: mail.subject,
        text: mail.text,
        html: mail.html
    });
    await db.collection('mailLog').add({
        kind: 'licenseAlert', trigger, by: by || '', recipients, subject: mail.subject,
        items: mail.report.total, at: admin.firestore.FieldValue.serverTimestamp()
    });
    logger.info('license alert sent', { trigger, recipients: recipients.length, items: mail.report.total });
    return { sent: true, recipients: recipients.length, items: mail.report.total };
}

exports.dailyLicenseAlert = onSchedule({
    schedule: '0 7 * * *', timeZone: 'Asia/Jayapura', region: REGION,
    secrets: [GMAIL_USER, GMAIL_APP_PASSWORD], retryCount: 1
}, async () => {
    const { dailyEnabled } = await emailSettings();
    if (!dailyEnabled) { logger.info('daily license email is off'); return; }
    const r = await sendReport('daily', '');
    logger.info('daily license alert', r);
});

// Who may press "Email Report": the owner, or an active account that can see
// License Stock (the same rule firestore.rules applies to that area).
async function mayEmail(uid) {
    if (OWNER_UIDS.includes(uid)) return true;
    const snap = await db.doc(`users/${uid}`).get();
    if (!snap.exists) return false;
    const d = snap.data();
    if (d.status !== 'active') return false;
    if (d.role === 'owner') return true;
    const access = d.access && typeof d.access === 'object' ? d.access : {};
    const scope = access.unitScope == null ? 'all' : access.unitScope;
    if (!['all', 'tractor'].includes(scope)) return false;
    const lvl = access.licenseStock != null ? access.licenseStock : (d.role === 'team' ? 'edit' : 'none');
    return lvl === 'view' || lvl === 'edit';
}

exports.sendLicenseAlert = onCall({
    region: REGION, secrets: [GMAIL_USER, GMAIL_APP_PASSWORD], enforceAppCheck: true
}, async req => {
    if (!req.auth) throw new HttpsError('unauthenticated', 'Sign in first');
    if (!(await mayEmail(req.auth.uid))) throw new HttpsError('permission-denied', 'No access to License Stock');
    try {
        return await sendReport('manual', req.auth.token.email || req.auth.uid);
    } catch (err) {
        logger.error('license alert failed', err);
        throw new HttpsError('internal', 'The email could not be sent');
    }
});
