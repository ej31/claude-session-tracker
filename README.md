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

## Quick Start

```bash
npx claude-session-tracker
```

### Auto Setup (Recommended)

Pick this if you don't already have a GitHub Project set up.

The installer creates **everything** for you automatically —
- A private repository for storing session issues
- A GitHub Project with all status options configured
- Custom date fields (`Session Created`, `Last Active`)
- Claude Code hooks, globally installed

All you do is pick a language and confirm. That's it.

### Manual Setup

Pick this if you already have a GitHub Project you want to use.

The wizard asks you ~6 questions —
1. **GitHub Project Owner** — your username or org
2. **GitHub Project Number** — grab it from your project URL
3. **Status mapping** — connect your Project's Status field to our lifecycle stages
4. **Default repo** — fallback when there's no git remote
5. **Idle timeout** — how long before we auto-close (default: 30 mins)
6. **Scope** — this project only, or go global

Then use Claude Code like normal. Everything flows to GitHub Projects automatically.

---

## Demo

<p>
  <img alt="GitHub Projects board view" src="https://github.com/user-attachments/assets/0705f26d-9517-4186-9533-217a166bd177" />
</p>

<p>
  <img alt="GitHub Issue with session comments" src="https://github.com/user-attachments/assets/0f8ca148-a5bc-4c08-ac45-93aafc407962" />
</p>

### Custom date fields

Auto setup creates two custom date fields in your project — `Created` (set when a session starts) and `Last Active` (updated on every prompt).

The GitHub API doesn't support modifying project views programmatically, so you'll need to manually add these fields to your board view. Go to your project → open any view → click `+` to add a field.

<img width="80%" alt="image" src="https://github.com/user-attachments/assets/c5cc4d4e-6f1f-4847-a901-9098af1db852" />

---

## What It Does

When you chat with Claude Code, the tracker automatically

- Creates a GitHub Issue for your session
- Records every prompt you type
- Records every response Claude gives
- Updates the issue title with your latest prompt (so you can scan history at a glance)
- Tracks session status: Registered → Responding → Waiting → Closed
- Auto-assigns issues to you
- Saves timestamps for everything
- Auto-closes idle sessions (configurable)

No setup after install. Just use Claude Code like normal.

---

## Why?

Claude Code sessions disappear when they end.

If you're juggling multiple tasks across projects, it's chaos. What did you decide? What was the discussion? Where did you leave off?

**claude-session-tracker** fixes this by making your entire conversation history searchable and shareable on GitHub Projects.

---

## How It Works

| Claude Code Event | GitHub Status | What Happens |
|---|---|---|
| Session starts | Registered | Issue created, added to Project |
| You submit prompt | Responding | Prompt saved, title updated |
| Claude responds | Waiting | Response saved, idle timer starts |
| Timer expires | Closed | Session auto-closed |

All hooks run async — tracking never slows you down.

### Features

**Session URL notification**

When a session starts, Claude tells you where it's tracked —
```
This session is being tracked at https://github.com/you/repo/issues/42
```

**Smart title updates**

Issue title auto-updates with your latest prompt —
```
[project-name] your latest prompt here...
```
Long project names get truncated at 20 chars.

**Resume without duplicates**

Resume a session with `claude --resume`? Tracker reuses the same GitHub Issue instead of creating a new one.

**Git remote auto-detection**

Has a GitHub remote? We use it for the issue title prefix. Issues always land in your configured storage repo.

**Zero blocking**

All hooks run async. Tracking never slows down Claude.

---

## Use Cases

**Personal context log**
Never forget what you discussed. Search your GitHub Projects and find any session in seconds.

**Team collaboration**
Share a GitHub Project with your team. Everyone sees what Claude is working on across all your repos. Perfect for code reviews and onboarding.

**Session handoff**
Paused work? Come back tomorrow and resume with full context. The tracker picks up where you left off, no duplicates.

**Audit trail**
Track everything Claude did, when it did it, and why. Useful for reviews and learning what worked.

---

## Prerequisites

- Node.js 18+
- Python 3
- **GitHub CLI (`gh`) — REQUIRED.** Install it from https://cli.github.com.

  ```bash
  # Install (macOS)
  brew install gh

  # Login and set required scopes
  gh auth login --scopes "project,repo"
  ```

  Needs `project` (read/write GitHub Projects) and `repo` (create issues, comments) scopes.

- A GitHub Project (v2) with a Status field (auto setup creates one for you)

---

## Configuration

Edit `~/.claude/hooks/config.env` directly, or just re-run the installer

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
NOTES_REPO=your-username/dev-notes
DONE_TIMEOUT_SECS=1800
```

---

## Under the Hood

Files we install to `~/.claude/hooks/`
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

## Uninstall

```bash
npx claude-session-tracker uninstall
```

Removes everything we installed — hook scripts, configuration, state files and logs. Doesn't touch your other hooks.

---

## Known Issues

**Async hook completion messages appearing in Claude Code**

You may see messages like this after every prompt/response —
```
⎿  Async hook UserPromptSubmit completed
⎿  Async hook Stop completed
```

This is a Claude Code behavior, not a bug in this tool. All hooks run with `async: true` to avoid blocking your workflow, and Claude Code currently has no option to suppress these completion messages.

A feature request has been filed upstream: [anthropics/claude-code#32551](https://github.com/anthropics/claude-code/issues/32551)

---

## Contributing

Found a bug? Have an idea? Contributions are always welcome.

Open an issue, submit a PR, or drop a feature request.

---

## License

MIT
