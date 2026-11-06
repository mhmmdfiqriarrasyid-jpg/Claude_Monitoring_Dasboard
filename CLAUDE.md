# Working notes for Claude — OT Dashboard

## Talking with the owner
- The owner writes Indonesian; answer in Indonesian. The app's UI text stays English.
- **Commands for the owner (Cloud Shell, terminal, Console steps): one command per code block,
  numbered, all sent in the same message.** Never chain them with `&&` in one block. Say what a
  successful result looks like after each, and what to answer to any prompt.
- Never use or repeat credentials, passwords or secret keys the owner pastes in chat. Only the
  public reCAPTCHA site key belongs in code.
- When `firestore.rules` changes, send the file and remind the owner to Publish it.

## Versioning
- Semver: patch for small changes, minor for features.
- Always bump `APP_VERSION` (script.js) and `CACHE_NAME` (service-worker.js) together.

## Tests
- Full suite: `PORT=8797 npm test` (all must pass before a push).
- Single suite: `BASE_URL=http://localhost:8811 node tests/<name>.js` with a local
  `python3 -m http.server 8811` running.
- Rules: run `tests/rules_test.js` in the Firestore emulator; Storage rules with
  `tests/storage_rules_test.js` (see README).

## Cloud Functions
- Code in `functions/`; the owner deploys from Cloud Shell (`functions/README.md`).

## Daily sugarcane fact (scheduled task, every morning ~05:00 WIT)
Adds ONE fact to `data/agri-facts.json`, shown on the Dashboard card "Sugarcane knowledge".
1. Work on branch `claude/build-monitoring-dashboard-uok4y` (`git fetch origin <branch>` then
   `git checkout -B <branch> origin/<branch>`). If today's date (Asia/Jayapura) already has a fact,
   stop — at most one per day.
2. Pick a sugarcane topic not already covered (compare `term` and `text` of every fact): cultivation,
   varieties, pests & diseases, nutrients & soil, irrigation, harvest & ripening, mill & by-products,
   machinery, precision agriculture. General knowledge, useful to field teams.
3. Source rules — the fact must be stated by the source:
   - Allowed: FAO, P3GI, Kementan / Ditjenbun / BRIN (*.go.id), universities (*.ac.id, *.edu and other
     universities), peer-reviewed journals (DOI, Garuda, ScienceDirect, Springer, MDPI, PMC),
     sugarcane research bodies (SASRI, SRA/BSES, ISSCT, LSU AgCenter, TNAU, ICAR-SBI), government
     agencies (EPA, USDA, DPIRD…).
   - Never: Wikipedia or any wiki, blogs, news, social media, vendor/marketing pages, AI summaries.
   - Verify with WebSearch (and WebFetch when the network allows): the claim and the URL must appear
     in the results. If nothing can be verified, add nothing that day.
4. Write it like the others: `term` (≤ 80), `text` in Indonesian, 1–3 sentences (≤ 400 chars),
   `source` = institution — title (≤ 120), `url` https, `addedAt` = today (YYYY-MM-DD, WIT),
   `id` = short kebab-case, unique. Append to `facts`; keep the file's 2-space JSON style.
5. Run `node tests/agri_bank_test.js` (must pass), then commit
   "Daily sugarcane fact: <term>" with the attribution lines and push to the branch.
   Do not touch any other file; do not bump the app version.
