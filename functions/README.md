# Cloud Functions — OT Dashboard (2.14.0)

| Function | Trigger | What it does |
|---|---|---|
| `dailyLicenseAlert` | every day 07:00 WIT | Emails the License Alert report to the recipients in `settings/email`, when *daily* is on and a licence expires within 30 days |
| `sendLicenseAlert` | callable (Email Report button) | Same report, now. Owner, or an active account that can see License Stock |

The report is built by `licenseReport.js`, the same rules as the dashboard
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

## Updating later

```bash
cd ot && git pull && cd functions && npm ci && cd ..
npx -y firebase-tools@latest deploy --only functions --project ot-monitoring-tractor
```
