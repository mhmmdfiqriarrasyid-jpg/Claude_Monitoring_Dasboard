# Ponytail skills: source and local changes

Copied from https://github.com/DietrichGebert/ponytail, release v5.2.0
(commit 2961b52), MIT licence (see `LICENSE` in this folder).

Only the six skills are installed: `ponytail`, `ponytail-review`,
`ponytail-audit`, `ponytail-debt`, `ponytail-gain` and `ponytail-help`. The
plugin's Node hooks (session start, subagent and prompt tracking, statusline)
are not.

Local changes, to re-apply when updating:

1. `ponytail/SKILL.md` adds `disable-model-invocation: true`. Ponytail mode
   runs only when someone types `/ponytail` and never switches itself on for
   an ordinary coding task.
2. `ponytail-review/SKILL.md` and `ponytail-audit/SKILL.md` report "in the
   language the user writes in" (Indonesian for this project) instead of
   "Very simple English".
3. `ponytail-help/SKILL.md` replaces the "Configure Default Mode" and
   "Update" sections (plugin and hook settings) with "In this repo" and
   "Update" sections that describe this install.
