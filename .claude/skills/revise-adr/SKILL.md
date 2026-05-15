---
name: revise-adr
description: Start a guided conversation with the user to revise, supersede, amend, or deprecate an existing ADR in docs/adr/. Use when the user says "revise/update/rewrite ADR", "supersede ADR-NNNN", "the decision in ADR-X changed", or otherwise wants to alter a recorded architecture decision in this repo.
---

# Revise ADR

You are helping the user change a recorded architecture decision. ADRs are load-bearing in this repo — the agent docs (`docs/agents/domain.md`) instruct other skills to read them before exploring code, and `CLAUDE.md` elevates `docs/adr/` as the canonical decision log. Treat every revision as a conversation, not a one-shot edit.

## Repo conventions you MUST follow

- **Location**: `docs/adr/` at repo root. File name `NNNN-kebab-case-title.md`, four-digit zero-padded, sequential.
- **Language**: Match the language of the ADR being changed (existing ADRs happen to be in Dutch, but new ones don't have to be). If the user starts the conversation in English, draft in English unless they ask otherwise. When superseding, the new ADR can switch languages — call it out in Step 5 so the user confirms.
- **Length**: Short. One paragraph for the decision, optionally one paragraph for explicitly rejected alternatives. No "Status / Context / Decision / Consequences" template — the body itself states the decision and reasoning.
- **Title**: H1 that *is* the decision in declarative form (e.g. "Silver leest Bronze via Change Data Feed + apply_changes" or "Silver reads Bronze via Change Data Feed + apply_changes"), not "ADR-0003: …".
- **CONTEXT.md coupling**: ADRs often correspond to a section in `CONTEXT.md`. When the decision changes, the related section in `CONTEXT.md` is almost certainly stale.

## Workflow

### Step 1 — Discover ADRs

Run `ls docs/adr/` and read each ADR file's H1 so you can list them with their *real* one-line summary, not just the filename slug. If the user already named the ADR (number or title), skip the list and confirm.

### Step 2 — Ask which ADR to change

Use `AskUserQuestion` with each existing ADR as a labelled option (label = number + short Dutch title). If there are more than 4, list them in a regular message and ask the user to type the number.

### Step 3 — Ask what kind of change

Use `AskUserQuestion` with these four options — the description column is critical, the user needs to understand the difference:

| Option | What it means | What you'll do |
|---|---|---|
| **Edit in place** | Wording is unclear or wrong, but the decision itself is unchanged. | Edit the existing file. Keep the title and decision direction. No new file. |
| **Supersede** | The decision actually changed. The old ADR is now history. | Create a new ADR with the next sequential number containing the new decision. Add a top line `> Supersedes ADR-NNNN — <reason>.` to the new file, and a top line `> **Superseded** by ADR-NNNN — <reason>.` to the old file. Both files stay in the repo (ADRs are append-only history). Match the language of the file you're writing the marker into. |
| **Amend** | Add nuance, a caveat, or a rejected alternative to a still-valid decision. | Append a paragraph to the existing file in the same language. Don't rewrite the original sentence. |
| **Deprecate** | The decision no longer applies and nothing replaces it. | Add a top line `> **Deprecated** — <reason>.` to the existing file, in the file's language. Leave the body intact. |

### Step 4 — Gather the substance

Now have a real conversation — don't try to extract everything in one structured question.

- **What changed and why?** Get the user to articulate the new decision in their own words. If they only describe the *symptom* (e.g. "DLT keeps failing"), keep asking until you have the *decision* (e.g. "we're moving Expectations from drop to warn for X").
- **Rejected alternative?** Ask whether there's an alternative worth recording as explicitly-rejected. ADR 0002 does this and it's the strongest pattern in this repo.
- **CONTEXT.md impact**: Ask whether a section in `CONTEXT.md` describes this same decision, and if so, flag that it will need a follow-up edit (don't auto-edit — surface it).

Push back when the reasoning is thin. An ADR with "we changed our mind" as the rationale is worse than no change at all.

### Step 5 — Draft, then confirm before writing

Output the proposed new/edited content as a fenced markdown block in the chat *first*. Wait for the user to approve or request changes. Only call `Write` / `Edit` after they've signed off.

For **supersede**, draft both files (new + the `Superseded` marker on the old one) in the same review block.

### Step 6 — Write and report

- For edit-in-place / amend / deprecate: use `Edit` on the existing file.
- For supersede: use `Write` for the new file, `Edit` for the marker line on the old file. Number the new file as `max(existing) + 1`, zero-padded.
- After writing, output:
  - The path(s) you changed
  - A reminder of any `CONTEXT.md` section that likely needs a manual follow-up edit
  - If the change is non-trivial, suggest the user open a PR (`gh pr create`) — don't run it.

## Anti-patterns — do not do these

- **Don't invent ADR template sections** (Status / Context / Decision / Consequences). This repo's ADRs are prose, and adding template scaffolding is drift.
- **Don't silently switch languages mid-ADR.** Edits and amendments stay in the file's existing language; supersedes can switch but only if the user confirms in Step 5.
- **Don't delete old ADRs.** Superseded files stay; that's the whole point of an append-only decision log.
- **Don't edit `CONTEXT.md` as part of this skill.** Flag the stale section and stop. Keeping the ADR change scoped lets the user (or `/grill-with-docs`) handle the doc sync deliberately.
- **Don't skip Step 5.** ADRs are read by future agents to decide how to write code — a silent edit can cascade into wrong-direction implementations.
