# UI UX Pro Max: source and local changes

Copied from https://github.com/nextlevelbuilder/ui-ux-pro-max-skill, plugin
version 2.15.0 (commit 56ea9cf, 2026-10-14), MIT licence (see `LICENSE` in this
folder).

Only the `ui-ux-pro-max` skill is installed: `SKILL.md`, `references/`, the
four runtime scripts (`search.py`, `core.py`, `design_system.py`,
`reasoning_contract.py`) and the CSV data they read. The scripts use the
Python standard library only. They make no network calls, run no commands,
and write files only with `--persist`.

Left out on purpose:
- The other skills in the upstream plugin (`ui-styling`, `design`,
  `design-system`, `brand`, `slides`, `banner-design`). They target
  shadcn/Tailwind, logos, banners and slide decks, which this project does
  not use. Some of them call external image APIs.
- `scripts/validate_data.py`, `scripts/tests/`, and the four JSON files in
  `data/` (`catalog-summary`, `data-provenance`, `google-font-licenses`,
  `phosphor-icons-upstream`). Upstream uses them only to validate its own
  data, and the search does not read them.

Local changes, to re-apply when updating:
1. Script path: `python "${CLAUDE_PLUGIN_ROOT}/.claude/skills/ui-ux-pro-max/scripts/search.py"`
   becomes `python3 .claude/skills/ui-ux-pro-max/scripts/search.py`, run from
   the repository root. This is the same path the upstream CLI generates for
   a project install.
2. A new "In this repository" section in `SKILL.md` covers the stack, the
   existing design tokens, the users, and when not to use `--persist`.

To update, copy the same files from upstream again and re-apply both changes.
