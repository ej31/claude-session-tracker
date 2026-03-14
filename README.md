# claude-session-tracker
[![npm downloads](https://img.shields.io/npm/dm/claude-session-tracker)](https://www.npmjs.com/package/claude-session-tracker)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/ej31/claude-session-tracker/pulls)

[![npm version](https://img.shields.io/npm/v/claude-session-tracker)](https://www.npmjs.com/package/claude-session-tracker)
[![Node.js](https://img.shields.io/badge/Node.js-%3E%3D18-green?logo=node.js)](https://nodejs.org)
[![OS Compatibility](https://github.com/ej31/claude-session-tracker/actions/workflows/compat.yml/badge.svg)](https://github.com/ej31/claude-session-tracker/actions/workflows/compat.yml)
[![Claude Code](https://img.shields.io/badge/Claude_Code-compatible-blueviolet)](https://claude.ai/code)



**Never lose a Claude Code conversation again.**

_Every prompt, every response, every decision — automatically saved to GitHub Projects._

[Quick Start](#quick-start) • [What It Does](#what-it-does) • [How It Works](#how-it-works) • [Use Cases](#use-cases) • [Configuration](#configuration)

---
## What it does

One command, done. A private GitHub repo gets created, a Project board gets wired up, and every Claude Code session you run from now on is automatically logged as a GitHub Issue — prompts, responses, timestamps, everything.

No config files to write. No tokens to paste. Just run the installer and forget about it.

## Demo

<p>

  <img alt="GitHub Issue with session comments" width="3540" height="2061" src="https://github.com/user-attachments/assets/2728a791-b1cc-40d8-9158-f18c6bd39f6b" />

</p>

## Quick Start

```bash
npx claude-session-tracker
```

> [!NOTE]
> Nightly builds are published to npm before the next stable release.
> Use `npx claude-session-tracker@nightly` to try the latest Context Operator changes early.
> For details, see the [latest nightly release notes](https://github.com/ej31/claude-session-tracker/releases).

### What the installer does

Everything is automatic. You just pick a language and hit enter.

- Creates a private repo for your session issues
- Spins up a GitHub Project with all statuses pre-configured
- Adds date fields (`Session Created`, `Last Active`)
- Tags each issue with the project name as a label
- Installs Claude Code hooks globally
- Recovers gracefully if something fails mid-setup
- Marks the project board `ON_TRACK` on completion

Already installed? Re-running the installer just reuses your existing setup. No duplicates, no mess.

---

### Key features

- One issue per session — your whole conversation in one place
- Prompts and responses saved automatically
- GitHub Projects board for at-a-glance status
- Searchable, shareable, permanent

## Custom date fields

The installer adds two date fields — `Created` (when a session starts) and `Last Active` (updated on every prompt).

GitHub's API can't modify project views, so you'll need to add these fields to your board manually. Go to your project, open any view, click `+` to add a field.

<img width="80%" alt="image" src="https://github.com/user-attachments/assets/c5cc4d4e-6f1f-4847-a901-9098af1db852" />

---

## What It Does

Every time you chat with Claude Code, the tracker kicks in —

- Creates a GitHub Issue for the session
- Logs every prompt and every response
- Updates the issue title with your latest prompt (so you can scan history fast)
- Stores the active project as an issue label (e.g. `ej31/my-app`)
- Tracks status — Registered, Responding, Waiting, Closed
- Auto-assigns issues to you
- Timestamps everything
- Auto-closes idle sessions (default 30 min)
- Health checks with `status` and `doctor` commands
- Pause/resume tracking whenever you want

Install once, then just use Claude Code like normal. That's literally it.

---

## Why?

Claude Code sessions vanish when they end.

Juggling multiple projects? Good luck remembering what you decided, what Claude suggested, or where you left off.

**claude-session-tracker** dumps your entire conversation history into GitHub Projects. Search it, share it, never lose it.

---

## How It Works

| Claude Code Event | GitHub Status | What Happens |
|---|---|---|
| Session starts | Registered | Issue created, added to Project |
| You submit prompt | Responding | Prompt saved, title updated |
| Claude responds | Waiting | Response saved, idle timer starts |
| Timer expires | Closed | Session auto-closed |

All hooks run async — zero slowdown.

### Features

**Session URL notification**

Session starts, you get a link —
```
This session is being tracked at https://github.com/you/repo/issues/42
```

**Smart title updates**

Issue title auto-updates with your latest prompt —
```
Fix session resume bug
```
The project name shows up as a label (like `ej31/claude-session-tracker`), keeping the title clean.

**Resume without duplicates**

`claude --resume`? The tracker reuses the same issue. No duplicates.

**Git remote auto-detection**

Got a GitHub remote? The tracker picks up `owner/repo` automatically. No remote? Falls back to your storage repo.

**Zero blocking**

Hooks are async. Claude never waits on the tracker.

**Built-in recovery and health checks**

Setup died halfway? Just re-run it — the installer picks up where it left off. After install, check things with —

```bash
claude-session-tracker status
claude-session-tracker doctor
```

`status` for a quick local check, `doctor` for a deep GitHub-backed diagnosis.

**Pause / resume tracking**

Working on something sensitive? Kill the logging —

```bash
claude-session-tracker pause
claude-session-tracker resume
```

`pause` stops all logging and marks the project `INACTIVE`. `resume` flips it back to `ON_TRACK`.

Every state change gets recorded as a project status update — session ID, workspace path, issue URL, timestamp, IP address. Full audit trail.

If someone marks the card `INACTIVE` from the GitHub web UI, the hooks detect it and stop logging until it's switched back.

---

## Use Cases

**Personal context log**
Never forget what you discussed. Search GitHub Projects, find any session in seconds.

**Team collaboration**
Share a Project with your team. Everyone sees what Claude is doing across all repos. Great for reviews and onboarding.

**Session handoff**
Left off yesterday? Resume tomorrow with full context. No duplicates.

**Audit trail**
Track everything Claude did, when, and why. Useful for reviews and figuring out what actually worked.

---

## Prerequisites

- Node.js 18+
- Python 3
- **GitHub CLI (`gh`)** — grab it from https://cli.github.com

  ```bash
  # macOS
  brew install gh

  # Login with required scopes
  gh auth login --scopes "project,repo"
  ```

  Needs `project` (read/write Projects) and `repo` (create issues, comments) scopes.

---

## Configuration

Re-run the installer or edit `~/.claude/hooks/config.env` directly —

```bash
npx claude-session-tracker
```

Config file format
```env
GITHUB_PROJECT_OWNER=your-username
GITHUB_PROJECT_NUMBER=1
GITHUB_PROJECT_ID=PVT_...
GITHUB_STATUS_FIELD_ID=PVTSSF_...
GITHUB_STATUS_REGISTERED=...
GITHUB_STATUS_RESPONDING=...
GITHUB_STATUS_WAITING=...
GITHUB_STATUS_CLOSED=...
NOTES_REPO=your-username/claude-session-storage
DONE_TIMEOUT_SECS=1800
CST_PROJECT_NAME_MODE=label
```

---

## Under the Hood

Files installed to `~/.claude/hooks/`
```
~/.claude/hooks/
├── cst_github_utils.py              # Shared utilities
├── cst_session_start.py             # SessionStart hook
├── cst_prompt_to_github_projects.py # UserPromptSubmit hook
├── cst_post_tool_use.py             # PostToolUse hook
├── cst_session_stop.py              # Stop / SessionEnd hook
├── cst_mark_done.py                 # Idle timeout handler
├── config.env                       # Your configuration
├── hooks.log                        # Execution logs
└── state/                           # Per-session state (JSON)
```

---

## Team Collaboration

Everyone installs under their own GitHub account. Want to share session visibility? Just invite teammates to your GitHub Project.

**How to invite**
1. Go to your GitHub Project page
2. Click **...** (menu) → **Settings**
3. Select **Manage access**
4. Click **Invite collaborators** and add people

Now the whole team can see session activity across all projects in one board.

---

## Uninstall

```bash
npx claude-session-tracker uninstall
```

Removes everything — hooks, config, state files, logs. Doesn't touch your other hooks.

---

## Known Issues

**Async hook completion messages showing up in Claude Code**

You might see this after every prompt/response —
```
⎿  Async hook UserPromptSubmit completed
⎿  Async hook Stop completed
```

That's Claude Code, not us. All hooks run with `async: true` so they don't block you, and Claude Code just doesn't have an option to hide these yet.

Tracked upstream — [anthropics/claude-code#32551](https://github.com/anthropics/claude-code/issues/32551)

---

## Contributing

Found a bug? Got an idea? PRs and issues are always welcome.

---

## License

MIT
