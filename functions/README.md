# Cloud Functions — OT Dashboard (2.14.0)

| Function | Trigger | What it does |
|---|---|---|
| `notificationTick` | every 10 minutes (WIT) | As set in Admin → Notifications (`settings/email`): the License Alert report once a day at its hour, the Daily Report reminder once a day at its hour (`mailState/daily`, `mailState/reminder`), and a Breakdown / damage email for each new damage record (`mailState/damage`) |
| *(push, 2.18.0)* | inside `notificationTick` | Push to phones (`pushTokens`): new damage → everyone who can see Damage; Daily Reports / leave / inspections waiting → their approvers. Switches `pushDamage`, `pushApprovals` |
| *(monthly, 2.18.0)* | inside `notificationTick` | Monthly Report on the 1st at `monthlyHour` to `monthlyRecipients` (`monthlyReport.js`); *Send now* via `sendNotificationNow` kind `monthly` |
| *(housekeeping, 2.19.0)* | inside `notificationTick`, 03:00 WIT | Error log entries > 30 days deleted; history > 2 years moved to Storage `archives/history/YYYY-MM.json` (`housekeeping.js`, `tests/housekeeping_e2e.js`) |
| `dailyMaintenance` *(2.35.0)* | every day 02:00 WIT | Backs up every Firestore collection to Storage `backups/<date>/<collection>.json.gz` with a manifest (kept 30 days, and the 1st of each month for a year; `backup.js`), then makes the photo thumbnails still missing and removes orphaned ones (`thumbs.js`, `sharp`). `tests/backup_e2e.js` |
| `serverBackups` *(2.35.0)* | callable | The owner's *Server Backups* window: `list`, `detail`, `now`, `restore` (adds only the records a collection is missing; never `users`). Owner only |
| *(ticket SLA, 2.35.0)* | inside `notificationTick` | Push when a ticket enters the last quarter of its time (PIC) and when it is overdue (PIC + OT team), repeated every 24 h for two weeks (`mailState/ticketSla`). Switch `pushTickets` |
| *(manager daily, 2.39.0)* | inside `notificationTick`, at `mgrHour` (default 07:00 WIT) | Yesterday's Daily Report for the manager to `mgrRecipients` (`mailState/mgrDaily`): the same deck as the dashboard's Weekly Report → Daily Report (`dailyCore.js`, shared with the browser), drawn on the server as PowerPoint + PDF (`dailyDeck.js`: pptxgenjs, pdfkit; company template in `assets/`), photos read from Storage (`dailySend.js`). *Send now* via `sendNotificationNow` kind `mgrDaily`. `tests/daily_core_test.js` |
| `sendLicenseAlert` | callable | "Send license report now" / Email Report. Owner, or an active account that can see License Stock |
| `sendNotificationNow` | callable | Notification Settings → *Send test* (breakdown: the newest damage record, marked [Test]) and *Send now* (Daily Report reminder, right now). Owner only |
| `deleteSignInAccount` | callable | After the owner removes or rejects someone: deletes their Firebase sign-in account. Owner only; refused while the person still has a `users` record, never for the owner |

The licence report is built by `licenseReport.js`; the reminder and damage email by `teamNotices.js` (`tests/team_notices_test.js`). The licence report uses, the same rules as the dashboard
(`tests/license_report_test.js` checks both give the same lines). Mail goes
through Gmail with an App Password; nothing about it is in the browser.

## One-time setup (Google Cloud Shell, as the project owner)

1. Google Account → Security → 2-Step Verification must be on, then create
   an **App Password** (myaccount.google.com/apppasswords), name "OT Dashboard".
2. In Cloud Shell:

```bash
git clone -b claude/build-monitoring-dashboard-uok4y https://github.com/mhmmdfiqriarrasyid-jpg/Claude_Monitoring_Dasboard.git ot
cd ot/functions && npm ci && cd ..
npx -y firebase-tools@latest login --no-localhost
npx -y firebase-tools@latest functions:secrets:set GMAIL_USER --project ot-monitoring-tractor
npx -y firebase-tools@latest functions:secrets:set GMAIL_APP_PASSWORD --project ot-monitoring-tractor
npx -y firebase-tools@latest deploy --only functions --project ot-monitoring-tractor
```

`GMAIL_USER` is the Gmail address the App Password belongs to. The first
deploy turns on the APIs it needs (Cloud Functions, Cloud Build, Artifact
Registry, Cloud Run, Cloud Scheduler, Eventarc, Secret Manager) and asks how
long to keep build images — answer **1** day.

## Changing the App Password

When the dashboard says *"Gmail refused the sign-in"* (log: `535 … Username and
Password not accepted`), make a new App Password for the `GMAIL_USER` account,
store it, and deploy so the functions use the new version:

```bash
npx -y firebase-tools@latest functions:secrets:set GMAIL_APP_PASSWORD --project ot-monitoring-tractor
npx -y firebase-tools@latest deploy --only functions --project ot-monitoring-tractor
```

## Updating later

```bash
cd ot && git pull && cd functions && npm ci && cd ..
npx -y firebase-tools@latest deploy --only functions --project ot-monitoring-tractor
```
