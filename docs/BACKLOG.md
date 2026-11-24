# Backlog — ideas not started yet

## Conversational BI (saved 2026-11-24, on hold)
Ask the dashboard's data in plain language ("berapa unit Breakdown di MNM minggu ini?") and get an
answer, a table or a chart. Three options were weighed:

- **A — through the Claude app (owner's choice to explore first; no API cost).** A scheduled export
  (Cloud Function) writes summary tables to a Google Sheet in the owner's Drive (Units, Damage,
  Warehouse/BAST, Tickets, Daily Report). The owner asks Claude in the Claude app with the Google
  Drive/Sheets connector; usage counts against the subscription. Limits: data as fresh as the last
  export; whoever can open the Sheet sees all of it (the dashboard's access areas do not apply).
  Open questions: which tables, and how often (hourly / daily).
- **B — guided questions in the dashboard (no AI, free).** Pick data, period, PT, status → numbers,
  table, chart (Chart.js is already vendored). Live data, access areas apply; no free-text questions.
- **C — free-text box in the dashboard with Claude (API).** Callable Cloud Function `askData`
  (App Check, per-user daily limit, question log); Claude gets tools (count / filter / group / trend)
  over the collections the asking account may read — never raw dumps; Haiku for routine questions,
  Sonnet for harder analysis. Needs an Anthropic API key in Secret Manager (set by the owner in
  Cloud Shell, never pasted in chat) and separate API billing.
