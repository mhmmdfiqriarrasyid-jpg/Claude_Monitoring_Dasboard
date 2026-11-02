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
