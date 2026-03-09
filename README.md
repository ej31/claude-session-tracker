# claude-session-tracker

Never lose a Claude Code conversation again. Every prompt, every response, every decision automatically saved to GitHub Projects. It's like having a time machine for your coding sessions.

## Results

<p>
  <img width="49%" alt="GitHub Projects board view" src="https://github.com/user-attachments/assets/0705f26d-9517-4186-9533-217a166bd177" />
  <img width="49%" alt="GitHub Issue with session comments" src="https://github.com/user-attachments/assets/0f8ca148-a5bc-4c08-ac45-93aafc407962" />
</p>

## Install

```bash
npx claude-session-tracker
```

Enter. Enter. Enter. Done. Seriously... for real.

The wizard asks you like 6 questions:
1. **GitHub Project Owner** — your username or org
2. **GitHub Project Number** — grab it from your project URL
3. **Status mapping** — connect your Project's Status field to our lifecycle stages
4. **Default repo** — fallback when there's no git remote
5. **Idle timeout** — how long before we auto-close (default: 30 mins)
6. **Scope** — this project only, or go global

That's it. You're done.

## What It Does

When you chat with Claude Code, the tracker automatically:

- Creates a GitHub Issue for your session
- Records every prompt you type
- Records every response Claude gives
- Updates the issue title with your latest prompt (so you can scan history at a glance)
- Tracks session status: Registered → Responding → Waiting → Closed
- Auto-assigns issues to you
- Saves timestamps for everything
- Auto-closes idle sessions (configurable)

No setup after install. Just use Claude Code like normal. Everything flows to GitHub Projects automatically.

## Why?

Claude Code sessions disappear when they end. If you're juggling multiple tasks across projects, it's chaos. What did you decide? What was the discussion? Where did you leave off?

**claude-session-tracker** fixes this by making your entire conversation history searchable and shareable on GitHub Projects.

## Use Cases

**Personal context log**
Never forget what you discussed. Search your GitHub Projects and find any session in seconds.

**Team collaboration**
Share a GitHub Project with your team. Everyone sees what Claude is working on across all your repos. Perfect for code reviews and onboarding.

**Session handoff**
Paused work? Come back tomorrow and resume with full context. The tracker picks up where you left off, no duplicates.

**Audit trail**
Track everything Claude did, when it did it, and why. Useful for reviews and learning what worked.

## How It Works

Simple flow:

1. **SessionStart** — Tracker creates a GitHub Issue and adds it to your Project (status: Registered)
2. **You type a prompt** — Gets saved as a comment, issue title updates, status changes to Responding
3. **Claude responds** — Response gets saved as a comment, status changes to Waiting, idle timer starts
4. **Idle for 30 mins?** — Issue auto-closes
5. **You send another prompt?** — Timer resets, status goes back to Responding

All hooks run async, so tracking never slows you down.

**Status tracking:**
| Claude Code Event | GitHub Status | What Happens |
|---|---|---|
| Session starts | Registered | Issue created, added to Project |
| You submit prompt | Responding | Prompt saved, title updated |
| Claude responds | Waiting | Response saved, idle timer starts |
| Timer expires | Closed | Session auto-closed |

## Prerequisites

- Node.js 18+
- Python 3
- **GitHub CLI (`gh`) — REQUIRED.** Install it from https://cli.github.com. The tracker won't work without it.
  ```bash
  # Install (macOS)
  brew install gh

  # Login and set required scopes
  gh auth login --scopes "project,repo"
  ```
  Needs `project` (read/write GitHub Projects) and `repo` (create issues, comments) scopes.
- A GitHub Project (v2) with a Status field (auto setup creates one for you)

## Uninstall

```bash
npx claude-session-tracker uninstall
```

Removes everything we installed:
- Hook scripts
- Configuration
- State files and logs
- Doesn't touch your other hooks

## Under the Hood

Files we install to `~/.claude/hooks/`:
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

## Features

**Session URL notification**
When a session starts, Claude tells you where it's tracked:
```
This session is being tracked at https://github.com/you/repo/issues/42
```

**Smart title updates**
Issue title auto-updates with your latest prompt:
```
[project-name] your latest prompt here...
```
Long project names get truncated at 20 chars.

**Resume without duplicates**
Resume a session with `claude --resume`? Tracker reuses the same GitHub Issue instead of creating a new one.

**Git remote auto-detection**
Has a GitHub remote? We create issues there. No remote? Falls back to your configured default repo.

**Zero blocking**
All hooks run async. Tracking never slows down Claude.

## Configuration

Edit `~/.claude/hooks/config.env` directly, or just re-run the installer:

```bash
npx claude-session-tracker
```

Config file format:
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

## Contributing

Found a bug? Have an idea? Contributions are always welcome!

Open an issue, submit a PR, or drop a feature request. We're here to make tracking sessions less painful.

## License

MIT
