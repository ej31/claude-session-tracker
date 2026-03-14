#!/usr/bin/env node
import * as p from '@clack/prompts'
import { spawnSync, spawn } from 'node:child_process'
import {
  mkdirSync,
  writeFileSync,
  readFileSync,
  existsSync,
  copyFileSync,
  chmodSync,
  unlinkSync,
  rmSync,
  readdirSync,
  realpathSync,
  statSync,
} from 'node:fs'
import { join, dirname, basename } from 'node:path'
import { homedir, networkInterfaces } from 'node:os'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const HOME = homedir()
const HOOKS_DIR = join(HOME, '.claude', 'hooks')
const STATE_DIR = join(HOOKS_DIR, 'state')
const CONFIG_FILE = join(HOOKS_DIR, 'config.env')
const AUTO_SETUP_RECOVERY_FILE = join(HOOKS_DIR, 'auto_setup_recovery.json')
const PROJECT_STATUS_CACHE_FILE = join(HOOKS_DIR, 'project_status_update.json')
const RUNTIME_STATUS_FILE = join(HOOKS_DIR, 'runtime_status.json')
const HOOKS_SRC = join(__dirname, '..', 'hooks')
const PROJECT_STATUS_MARKER = '<!-- claude-session-tracker:project-status -->'
const AUTO_SETUP_STEPS = [
  'repo_created',
  'project_created',
  'status_configured',
  'date_fields_attempted',
  'hooks_installed',
]
const PY_FILES = [
  'cst_github_utils.py',
  'cst_session_start.py',
  'cst_prompt_to_github_projects.py',
  'cst_session_stop.py',
  'cst_mark_done.py',
  'cst_post_tool_use.py',
]
const LEGACY_PY_FILES = [
  'github_utils.py',
  'session_start.py',
  'prompt_to_github_projects.py',
  'session_stop.py',
  'mark_done.py',
  'post_tool_use.py',
]
const ALL_KNOWN_FILES = [...PY_FILES, ...LEGACY_PY_FILES]
const OUR_HOOK_KEYS = ['SessionStart', 'UserPromptSubmit', 'PostToolUse', 'Stop', 'SessionEnd']
const STATUS_LABELS = {
  en: { registered: 'Registered', responding: 'Responding', waiting: 'Waiting', closed: 'Closed' },
  ko: { registered: '세션 등록', responding: '답변 중', waiting: '입력 대기', closed: '세션 종료' },
  ja: { registered: 'セッション登録', responding: '応答中', waiting: '入力待ち', closed: 'セッション終了' },
  zh: { registered: '会话注册', responding: '响应中', waiting: '等待输入', closed: '会话关闭' },
}
const STATUS_COLORS = ['BLUE', 'GREEN', 'YELLOW', 'GRAY']
const STATUS_DESCRIPTIONS = ['Session started', 'Claude is responding', 'Waiting for user input', 'Session ended']
const STATUS_RECOMMENDATION_SYNONYMS = {
  registered: ['registered', 'new', 'open', 'todo', 'to do', 'queued', 'backlog', 'planned', 'ready'],
  responding: ['responding', 'in progress', 'in-progress', 'working', 'active', 'ongoing', 'doing'],
  waiting: ['waiting', 'pending', 'needs input', 'blocked', 'review', 'on hold', 'hold'],
  closed: ['closed', 'done', 'complete', 'completed', 'resolved', 'finished'],
}
const STATUS_ACTIONS = {
  install: {
    trackerState: 'installed',
    boardStatus: 'ON_TRACK',
    message:
      'Tracking is installed and active. Local hook-driven prompt/response capture is ready for the next Claude session.',
  },
  pause: {
    trackerState: 'paused',
    boardStatus: 'OFF_TRACK',
    message:
      'Local tracking is paused. Prompt/response comments, issue title updates, project item status transitions, and idle auto-close are suspended until resume.',
  },
  resume: {
    trackerState: 'resumed',
    boardStatus: 'ON_TRACK',
    message:
      'Local tracking is active again. Normal prompt/response capture and project item status transitions will continue from the next hook event.',
  },
}

// -- Utilities ----------------------------------------------------------------

function onCancel() {
  p.cancel('Setup cancelled.')
  process.exit(0)
}

function hasCmd(cmd) {
  const isWin = process.platform === 'win32'
  const finder = isWin ? 'where' : 'which'
  const result = spawnSync(finder, [cmd], { stdio: 'ignore', shell: isWin })
  return result.status === 0
}

function readJson(path, fallback = {}) {
  if (!existsSync(path)) return fallback
  try {
    return JSON.parse(readFileSync(path, 'utf-8'))
  } catch {
    return fallback
  }
}

function readJsonStrict(path) {
  return JSON.parse(readFileSync(path, 'utf-8'))
}

function writeJson(path, data) {
  mkdirSync(dirname(path), { recursive: true })
  writeFileSync(path, JSON.stringify(data, null, 2) + '\n')
}

function removeFileIfExists(path) {
  if (!existsSync(path)) return
  unlinkSync(path)
}

function readEnvFile(path) {
  if (!existsSync(path)) return null
  const env = {}
  for (const rawLine of readFileSync(path, 'utf-8').split('\n')) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#') || !line.includes('=')) continue
    const [rawKey, ...rawValue] = line.split('=')
    const key = rawKey.trim()
    let value = rawValue.join('=').trim()
    if (!(value.startsWith('"') || value.startsWith("'"))) {
      value = value.split('#')[0].trim()
    }
    env[key] = value.replace(/^['"]|['"]$/g, '')
  }
  return env
}

function formatYesNo(value) {
  return value ? 'yes' : 'no'
}

function getStatePath(sessionId) {
  return join(STATE_DIR, `${sessionId}.json`)
}

function saveState(sessionId, state) {
  writeJson(getStatePath(sessionId), state)
}

function loadRuntimeStatus() {
  return readJson(RUNTIME_STATUS_FILE, null)
}

function clearRuntimeStatus() {
  removeFileIfExists(RUNTIME_STATUS_FILE)
}

function loadProjectStatusCache() {
  return readJson(PROJECT_STATUS_CACHE_FILE, null)
}

function saveProjectStatusCache(cache) {
  writeJson(PROJECT_STATUS_CACHE_FILE, cache)
}

function loadAutoSetupRecovery() {
  return readJson(AUTO_SETUP_RECOVERY_FILE, null)
}

function saveAutoSetupRecovery(recovery) {
  writeJson(AUTO_SETUP_RECOVERY_FILE, recovery)
}

function clearAutoSetupRecovery() {
  removeFileIfExists(AUTO_SETUP_RECOVERY_FILE)
}

function hasRecoveryStep(recovery, step) {
  return Boolean(recovery?.completedSteps?.includes(step))
}

function markAutoSetupStep(recovery, step, patch = {}) {
  const completedSteps = Array.from(new Set([...(recovery.completedSteps ?? []), step]))
  const next = { ...recovery, ...patch, completedSteps, updatedAt: new Date().toISOString() }
  saveAutoSetupRecovery(next)
  return next
}

function issueUrlFromState(state) {
  if (!state?.repo || !state?.issue_number) return null
  return `https://github.com/${state.repo}/issues/${state.issue_number}`
}

function getLocalIp() {
  const interfaces = networkInterfaces()
  for (const addresses of Object.values(interfaces)) {
    for (const address of addresses ?? []) {
      if (address && address.family === 'IPv4' && !address.internal) {
        return address.address
      }
    }
  }
  return 'unknown'
}

function normalizeCwd(path) {
  try {
    return realpathSync.native(path)
  } catch {
    return path
  }
}

function cancelTimerPid(pid) {
  if (!pid) return
  try {
    process.kill(pid, 'SIGTERM')
  } catch {
    // noop
  }
}

function listSessionStates() {
  if (!existsSync(STATE_DIR)) return []
  return readdirSync(STATE_DIR)
    .filter(name => name.endsWith('.json'))
    .map((name) => {
      const path = join(STATE_DIR, name)
      try {
        const state = readJsonStrict(path)
        return {
          ok: true,
          path,
          sessionId: name.replace(/\.json$/, ''),
          state,
          mtimeMs: statSync(path).mtimeMs,
        }
      } catch (error) {
        return {
          ok: false,
          path,
          sessionId: name.replace(/\.json$/, ''),
          error,
          mtimeMs: statSync(path).mtimeMs,
        }
      }
    })
}

function findSessionByCwd(cwd, { pausedOnly = false, activeOnly = true } = {}) {
  const normalizedCwd = normalizeCwd(cwd)
  const candidates = listSessionStates()
    .filter(entry => entry.ok)
    .filter((entry) => {
      const state = entry.state
      if (normalizeCwd(state.cwd) !== normalizedCwd) return false
      if (activeOnly && state.status === 'closed') return false
      if (pausedOnly && !state.tracking_paused) return false
      return true
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
  return candidates[0] ?? null
}

function getSettingsPaths(cwd = process.cwd()) {
  return [
    { scope: 'global', path: join(HOME, '.claude', 'settings.json') },
    { scope: 'project', path: join(cwd, '.claude', 'settings.json') },
  ]
}

function hasTrackerHooks(settings) {
  if (!settings?.hooks) return false
  return OUR_HOOK_KEYS.some((key) => {
    const entries = settings.hooks[key]
    return Array.isArray(entries) && entries.some(entry => {
      const hooks = entry.hooks ?? []
      return hooks.some(hook => ALL_KNOWN_FILES.some(file => hook.command?.includes(file)))
    })
  })
}

function collectHookRegistrations(cwd = process.cwd()) {
  return getSettingsPaths(cwd).map(({ scope, path }) => {
    if (!existsSync(path)) return { scope, path, exists: false, installed: false }
    try {
      const settings = readJsonStrict(path)
      return {
        scope,
        path,
        exists: true,
        installed: hasTrackerHooks(settings),
        invalid: false,
      }
    } catch (error) {
      return {
        scope,
        path,
        exists: true,
        installed: false,
        invalid: true,
        error,
      }
    }
  })
}

function getInstallState(cwd = process.cwd()) {
  const config = readEnvFile(CONFIG_FILE)
  const hookFilesPresent = PY_FILES.every(file => existsSync(join(HOOKS_DIR, file)))
  const hookRegistrations = collectHookRegistrations(cwd)
  const installedTargets = hookRegistrations.filter(target => target.installed)
  const anyInstalledSignal = Boolean(config) || hookFilesPresent || hookRegistrations.some(target => target.exists)

  if (config && hookFilesPresent && installedTargets.length > 0) {
    return {
      state: 'installed',
      config,
      hookFilesPresent,
      hookRegistrations,
    }
  }

  return {
    state: anyInstalledSignal ? 'partial' : 'not-installed',
    config,
    hookFilesPresent,
    hookRegistrations,
  }
}

function normalizeStatusName(value) {
  return value.toLowerCase().replace(/[^a-z0-9\u3131-\u318e\uac00-\ud7a3\u3040-\u30ff\u4e00-\u9fff]+/g, ' ').trim()
}

function buildStatusCandidates(lang) {
  const candidates = {}
  for (const [key, synonyms] of Object.entries(STATUS_RECOMMENDATION_SYNONYMS)) {
    candidates[key] = new Set(synonyms.map(normalizeStatusName))
    for (const localized of Object.values(STATUS_LABELS)) {
      candidates[key].add(normalizeStatusName(localized[key]))
    }
    if (STATUS_LABELS[lang]?.[key]) {
      candidates[key].add(normalizeStatusName(STATUS_LABELS[lang][key]))
    }
  }
  return candidates
}

function scoreStatusOption(optionName, candidateNames) {
  const normalizedOption = normalizeStatusName(optionName)
  let bestScore = 0
  for (const candidate of candidateNames) {
    if (!candidate) continue
    if (normalizedOption === candidate) return 300
    if (normalizedOption.includes(candidate)) bestScore = Math.max(bestScore, 200 - (normalizedOption.length - candidate.length))
    if (candidate.includes(normalizedOption)) bestScore = Math.max(bestScore, 120 - (candidate.length - normalizedOption.length))
  }
  return bestScore
}

function recommendStatusMapping(options, lang) {
  const candidates = buildStatusCandidates(lang)
  const map = {}
  const unresolved = []
  const usedIds = new Set()

  for (const key of ['registered', 'responding', 'waiting', 'closed']) {
    const ranked = options
      .map(option => ({
        option,
        score: scoreStatusOption(option.name, candidates[key]),
      }))
      .filter(entry => entry.score > 0)
      .sort((left, right) => right.score - left.score)

    const winner = ranked.find(entry => !usedIds.has(entry.option.id))
    if (!winner) {
      unresolved.push(key)
      continue
    }

    const nextDistinctScore = ranked.find(entry => entry.option.id !== winner.option.id && !usedIds.has(entry.option.id))?.score ?? 0
    if (winner.score <= nextDistinctScore) {
      unresolved.push(key)
      continue
    }

    map[key] = winner.option.id
    usedIds.add(winner.option.id)
  }

  return { map, unresolved }
}

function ghGraphql(query, variables = {}) {
  const result = spawnSync(
    'gh',
    ['api', 'graphql', '--input', '-'],
    { input: JSON.stringify({ query, variables }), encoding: 'utf-8' },
  )
  if (!result.stdout?.trim()) {
    throw new Error(result.stderr || 'No response from gh api')
  }
  return JSON.parse(result.stdout)
}

function ghCommand(args) {
  const result = spawnSync('gh', args, { encoding: 'utf-8' })
  if (result.status !== 0) {
    throw new Error(result.stderr?.trim() || `gh command failed: gh ${args.join(' ')}`)
  }
  return result.stdout?.trim() ?? ''
}

function ghRepoIsPrivate(repo) {
  const value = ghCommand(['api', `repos/${repo}`, '--jq', '.private']).toLowerCase()
  if (value === 'true') return true
  if (value === 'false') return false
  throw new Error(`Unexpected repo visibility response for ${repo}: ${value}`)
}

const SESSION_STORAGE_REPO_NAME = 'claude-session-storage'
const META_JSON_PATH = '.claude-session-tracker/meta.json'

function sessionStorageRepoExists(username) {
  const result = spawnSync('gh', ['api', `repos/${username}/${SESSION_STORAGE_REPO_NAME}`, '--jq', '.full_name'], { encoding: 'utf-8' })
  return result.status === 0 && result.stdout?.trim() === `${username}/${SESSION_STORAGE_REPO_NAME}`
}

function fetchMetaJsonFromRepo(repoFullName) {
  const result = spawnSync(
    'gh',
    ['api', `repos/${repoFullName}/contents/${META_JSON_PATH}`, '--jq', '.content'],
    { encoding: 'utf-8' },
  )
  if (result.status !== 0 || !result.stdout?.trim()) return null
  try {
    const decoded = Buffer.from(result.stdout.trim(), 'base64').toString('utf-8')
    return JSON.parse(decoded)
  } catch {
    return null
  }
}

function pushFileToRepo(repoFullName, filePath, content, message) {
  const encoded = Buffer.from(content).toString('base64')
  const existingResult = spawnSync(
    'gh',
    ['api', `repos/${repoFullName}/contents/${filePath}`, '--jq', '.sha'],
    { encoding: 'utf-8' },
  )
  const body = { message, content: encoded }
  if (existingResult.status === 0 && existingResult.stdout?.trim()) {
    body.sha = existingResult.stdout.trim()
  }
  const result = spawnSync(
    'gh',
    ['api', `repos/${repoFullName}/contents/${filePath}`, '--method', 'PUT', '--input', '-'],
    { input: JSON.stringify(body), encoding: 'utf-8' },
  )
  if (result.status !== 0) {
    throw new Error(result.stderr?.trim() || `Failed to push ${filePath} to ${repoFullName}`)
  }
}

function buildRepoReadme() {
  return [
    '# Claude Session Storage',
    '',
    '> [!CAUTION]',
    '> This repository has a GitHub Projects board where all Claude Code sessions are stored.',
    '> This GitHub Repository and GitHub Projects installed by claude-session-tracker **MUST remain Private**.',
    '> Claude Code sessions may contain highly sensitive Secrets, Keys, and Tokens.',
    '',
    'This repository is auto-created by [`claude-session-tracker`](https://github.com/ej31/claude-session-tracker) for tracking Claude Code sessions via GitHub Projects.',
    '',
    '## How it works',
    '',
    '- Each Claude Code session is recorded as an issue in this repository.',
    '- Session status (registered, responding, waiting, closed) is tracked in the linked GitHub Projects board.',
    '- The `.claude-session-tracker/meta.json` file stores the GitHub Projects ID for consistent access across installations.',
  ].join('\n')
}

function hasRequiredScopes() {
  const result = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
  const output = result.stdout + result.stderr
  return output.includes('project') && output.includes('repo')
}

function openBrowser(url) {
  if (process.platform === 'darwin') {
    spawnSync('open', [url])
  } else if (process.platform === 'win32') {
    spawnSync('cmd', ['/c', 'start', url], { shell: true })
  } else {
    spawnSync('xdg-open', [url])
  }
}

// -- gh 자동 설치 -------------------------------------------------------------

function detectLinuxDistro() {
  try {
    const content = readFileSync('/etc/os-release', 'utf-8')
    const lines = Object.fromEntries(
      content.split('\n')
        .filter(line => line.includes('='))
        .map((line) => {
          const [key, ...value] = line.split('=')
          return [key.trim(), value.join('=').replace(/"/g, '').trim()]
        }),
    )
    const combined = `${lines.ID ?? ''} ${lines.ID_LIKE ?? ''}`.toLowerCase()
    if (combined.includes('debian') || combined.includes('ubuntu')) return 'debian'
    if (combined.includes('fedora') || combined.includes('rhel') || combined.includes('centos')) return 'fedora'
    if (combined.includes('arch') || combined.includes('manjaro')) return 'arch'
    if (combined.includes('opensuse') || combined.includes('suse')) return 'opensuse'
    return lines.ID?.toLowerCase() || 'unknown'
  } catch {
    return 'unknown'
  }
}

function runCmd(cmd, args) {
  const result = spawnSync(cmd, args, { stdio: 'inherit', shell: process.platform === 'win32' })
  return result.status === 0
}

async function tryInstallGh() {
  const os = process.platform

  if (os === 'darwin') {
    if (!hasCmd('brew')) {
      p.log.warn('Homebrew is not installed.')
      p.log.info('Install Homebrew first from https://brew.sh, then run: brew install gh')
      return false
    }
    p.log.info('Running: brew install gh')
    return runCmd('brew', ['install', 'gh'])
  }

  if (os === 'linux') {
    const distro = detectLinuxDistro()
    if (distro === 'debian') {
      p.log.info('Running: sudo apt update && sudo apt install gh -y')
      if (!runCmd('sudo', ['apt', 'update'])) return false
      return runCmd('sudo', ['apt', 'install', 'gh', '-y'])
    }
    if (distro === 'fedora') {
      p.log.info('Running: sudo dnf install gh -y')
      return runCmd('sudo', ['dnf', 'install', 'gh', '-y'])
    }
    if (distro === 'arch') {
      p.log.info('Running: sudo pacman -S github-cli --noconfirm')
      return runCmd('sudo', ['pacman', '-S', 'github-cli', '--noconfirm'])
    }
    if (distro === 'opensuse') {
      p.log.info('Running: sudo zypper install -y github-cli')
      return runCmd('sudo', ['zypper', 'install', '-y', 'github-cli'])
    }
    p.log.warn(`Unknown Linux distribution (${distro}): automatic installation is not supported.`)
    p.log.info('Manual install: https://cli.github.com/manual/installation')
    return false
  }

  if (os === 'win32') {
    if (hasCmd('winget')) {
      p.log.info('Running: winget install --id GitHub.cli -e --accept-source-agreements')
      return runCmd('winget', ['install', '--id', 'GitHub.cli', '-e', '--accept-source-agreements'])
    }
    if (hasCmd('choco')) {
      p.log.info('Running: choco install gh -y')
      return runCmd('choco', ['install', 'gh', '-y'])
    }
    if (hasCmd('scoop')) {
      p.log.info('Running: scoop install gh')
      return runCmd('scoop', ['install', 'gh'])
    }
    p.log.warn('winget, Chocolatey, or Scoop is required to install gh automatically.')
    p.log.info('Manual install: https://cli.github.com/manual/installation')
    return false
  }

  p.log.warn('Unsupported OS. Please install gh manually.')
  p.log.info('https://cli.github.com/manual/installation')
  return false
}

async function fallbackAuthGuide(mode = 'login') {
  const cmd = mode === 'login'
    ? 'gh auth login --web --scopes project,repo'
    : 'gh auth refresh --scopes project,repo'

  p.log.step('Run the command below in your terminal, then press Enter when done.\n')
  p.log.message(`  ${cmd}\n`)

  await p.text({
    message: 'Press Enter when done.',
    placeholder: '',
  })

  const recheck = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
  if (recheck.status !== 0) {
    p.log.error('Authentication was not completed.')
    p.outro('Setup aborted.')
    process.exit(1)
  }
}

async function runGhAuthWithStream(args, mode = 'login') {
  return new Promise((resolve, reject) => {
    const ghBrowser = process.platform === 'win32' ? 'cmd /c exit 0' : '/usr/bin/true'
    const child = spawn('gh', args, {
      env: { ...process.env, GH_BROWSER: ghBrowser },
      stdio: ['pipe', 'pipe', 'pipe'],
    })

    let codeShown = false
    let resolved = false
    const timeout = setTimeout(async () => {
      if (!codeShown && !resolved) {
        child.kill()
        try {
          await fallbackAuthGuide(mode)
          resolved = true
          resolve()
        } catch (error) {
          reject(error)
        }
      }
    }, 15000)

    const handleOutput = (data) => {
      const text = data.toString()
      if (codeShown) return
      const match = text.match(/([A-Z0-9]{4}-[A-Z0-9]{4})/)
      if (!match) return

      codeShown = true
      const code = match[1]
      p.note(
        'Why this is required:\n' +
        '  claude-session-tracker needs to create and manage GitHub Projects on your behalf.\n' +
        '  This requires read/write access via OAuth — your credentials are never seen\n' +
        '  or stored by claude-session-tracker. Login is handled entirely by GitHub.',
        'Why is GitHub login required?',
      )
      p.log.step('A browser has been opened.')
      p.log.info('  - Enter the code below in your browser.')
      p.log.info('  - claude-session-tracker does not collect any information during this process.')
      p.log.message('')
      p.log.message(`  Your GitHub authentication code:  ${code}`)

      openBrowser('https://github.com/login/device')
      child.stdin.write('\n')
    }

    child.stdout.on('data', handleOutput)
    child.stderr.on('data', handleOutput)

    child.on('close', (code) => {
      clearTimeout(timeout)
      if (resolved) return
      resolved = true
      if (code === 0) resolve()
      else reject(new Error(`gh auth failed (exit code: ${code})`))
    })

    child.on('error', (error) => {
      clearTimeout(timeout)
      if (!resolved) {
        resolved = true
        reject(error)
      }
    })
  })
}

async function runGhAuthLogin() {
  return runGhAuthWithStream(['auth', 'login', '--web', '--scopes', 'project,repo'], 'login')
}

async function runGhAuthRefresh() {
  return runGhAuthWithStream(['auth', 'refresh', '--scopes', 'project,repo'], 'refresh')
}

function getAuthenticatedUser() {
  const result = spawnSync('gh', ['api', 'user', '--jq', '.login'], { encoding: 'utf-8' })
  if (result.status !== 0 || !result.stdout?.trim()) return null
  return result.stdout.trim()
}

function fetchProjectMetadata(owner, number) {
  const query = `
    query($login: String!, $number: Int!) {
      user(login: $login) {
        projectV2(number: $number) {
          id
          title
          url
          fields(first: 30) {
            nodes {
              ... on ProjectV2SingleSelectField {
                id
                name
                options { id name }
              }
            }
          }
        }
      }
      organization(login: $login) {
        projectV2(number: $number) {
          id
          title
          url
          fields(first: 30) {
            nodes {
              ... on ProjectV2SingleSelectField {
                id
                name
                options { id name }
              }
            }
          }
        }
      }
    }`
  const response = ghGraphql(query, { login: owner, number })
  const project = response.data?.user?.projectV2 ?? response.data?.organization?.projectV2
  if (!project) throw new Error('Could not find the project. Please check the owner and project number.')
  const statusField = project.fields.nodes.find(field => field?.name === 'Status')
  if (!statusField) throw new Error("Could not find a 'Status' field in this project.")
  return { projectId: project.id, projectTitle: project.title, projectUrl: project.url, statusField }
}

function deleteProjectV2(projectId) {
  const mutation = `
    mutation($projectId: ID!) {
      deleteProjectV2(input: { projectId: $projectId }) {
        projectV2 { id }
      }
    }`
  ghGraphql(mutation, { projectId })
}

function mergeHooks(existing, hooksDir) {
  return {
    ...existing,
    hooks: {
      ...(existing.hooks ?? {}),
      SessionStart: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_session_start.py')}`, timeout: 15, async: true }],
      }],
      UserPromptSubmit: [{
        matcher: '',
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_prompt_to_github_projects.py')}`, timeout: 15, async: true }],
      }],
      PostToolUse: [{
        matcher: 'AskUserQuestion',
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_post_tool_use.py')}`, timeout: 15, async: true }],
      }],
      Stop: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_session_stop.py')}`, timeout: 10, async: true }],
      }],
      SessionEnd: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_session_stop.py')}`, timeout: 10, async: true }],
      }],
    },
  }
}

function removeOurHooks(settings) {
  if (!settings.hooks) return settings
  const cleaned = { ...settings, hooks: { ...settings.hooks } }
  for (const key of OUR_HOOK_KEYS) {
    const entries = cleaned.hooks[key]
    if (!Array.isArray(entries)) continue
    cleaned.hooks[key] = entries.filter((entry) => {
      const hooks = entry.hooks ?? []
      return !hooks.some(hook => ALL_KNOWN_FILES.some(file => hook.command?.includes(file)))
    })
    if (cleaned.hooks[key].length === 0) delete cleaned.hooks[key]
  }
  if (Object.keys(cleaned.hooks).length === 0) delete cleaned.hooks
  return cleaned
}

function installHooksAndConfig({
  owner,
  projectNumber,
  projectId,
  statusFieldId,
  statusMap,
  notesRepo,
  timeoutMinutes,
  scope,
  createdFieldId,
  lastActiveFieldId,
  lang,
}) {
  mkdirSync(HOOKS_DIR, { recursive: true })
  mkdirSync(STATE_DIR, { recursive: true })

  for (const file of PY_FILES) {
    copyFileSync(join(HOOKS_SRC, file), join(HOOKS_DIR, file))
    chmodSync(join(HOOKS_DIR, file), 0o755)
  }

  const configLines = [
    `GITHUB_PROJECT_OWNER=${owner}`,
    `GITHUB_PROJECT_NUMBER=${projectNumber}`,
    `GITHUB_PROJECT_ID=${projectId}`,
    `GITHUB_STATUS_FIELD_ID=${statusFieldId}`,
    `GITHUB_STATUS_REGISTERED=${statusMap.registered}`,
    `GITHUB_STATUS_RESPONDING=${statusMap.responding}`,
    `GITHUB_STATUS_WAITING=${statusMap.waiting}`,
    `GITHUB_STATUS_CLOSED=${statusMap.closed}`,
    `NOTES_REPO=${notesRepo}`,
    `DONE_TIMEOUT_SECS=${Number(timeoutMinutes) * 60}`,
  ]
  if (createdFieldId) configLines.push(`GITHUB_CREATED_FIELD_ID=${createdFieldId}`)
  if (lastActiveFieldId) configLines.push(`GITHUB_LAST_ACTIVE_FIELD_ID=${lastActiveFieldId}`)
  if (lang) configLines.push(`CST_LANG=${lang}`)
  writeFileSync(CONFIG_FILE, configLines.join('\n') + '\n')

  const settingsPath = scope === 'global'
    ? join(HOME, '.claude', 'settings.json')
    : (() => {
        mkdirSync(join(process.cwd(), '.claude'), { recursive: true })
        return join(process.cwd(), '.claude', 'settings.json')
      })()

  writeFileSync(
    settingsPath,
    JSON.stringify(mergeHooks(readJson(settingsPath), HOOKS_DIR), null, 2) + '\n',
  )
}

function buildProjectReadme() {
  return [
    '# Claude Session Tracker',
    '',
    'This project is managed by `claude-session-tracker`.',
    '',
    '## Managed status flag',
    '',
    '- `ON_TRACK` means local tracking is enabled.',
    '- `OFF_TRACK` means local tracking is paused.',
    '',
    '> Warning: do not manually change the tracker `ON_TRACK` / `OFF_TRACK` status values. Use `claude-session-tracker pause` and `claude-session-tracker resume` instead.',
    '',
    '## History',
    '',
    'Each local install / pause / resume action writes a project status update that includes session metadata such as workspace, timestamp, issue URL, and local IP.',
  ].join('\n')
}

function updateProjectReadme(projectId, readme) {
  const mutation = `
    mutation($projectId: ID!, $readme: String!) {
      updateProjectV2(input: {
        projectId: $projectId
        readme: $readme
      }) {
        projectV2 {
          id
          readme
        }
      }
    }`
  const response = ghGraphql(mutation, { projectId, readme })
  return response.data?.updateProjectV2?.projectV2
}

function ensureProjectReadmeAfterInstall(projectId) {
  const spin = p.spinner()
  spin.start('Configuring project README...')
  try {
    updateProjectReadme(projectId, buildProjectReadme())
    spin.stop('Project README configured')
    return true
  } catch (error) {
    spin.stop('Could not configure project README')
    p.log.warn(`The install completed, but setting the project README failed: ${error.message}`)
    return false
  }
}

function ensureProjectOnTrackAfterInstall(projectId, cwd = process.cwd()) {
  const spin = p.spinner()
  spin.start('Marking project board ON_TRACK...')
  const result = syncProjectStatusCard({ GITHUB_PROJECT_ID: projectId }, 'install', { cwd })
  if (result.ok) {
    spin.stop('Project board marked ON_TRACK')
    return true
  }

  spin.stop('Could not mark project board ON_TRACK')
  p.log.warn(`The install completed, but syncing ON_TRACK failed: ${result.error}`)
  p.log.info('You can retry later with: claude-session-tracker resume')
  return false
}

function buildProjectStatusBody(action, state) {
  const config = STATUS_ACTIONS[action]
  const issueUrl = issueUrlFromState(state) ?? '_Unavailable_'
  const workspace = state?.cwd || process.cwd()
  const sessionId = state?.session_id ?? '_Unavailable_'
  return [
    PROJECT_STATUS_MARKER,
    `**Tracker state:** ${config.trackerState}`,
    `**Session ID:** ${sessionId}`,
    `**Issue:** ${issueUrl}`,
    `**Workspace:** ${workspace}`,
    `**Updated at:** ${new Date().toISOString()}`,
    `**Local IP:** ${getLocalIp()}`,
    '',
    config.message,
  ].join('\n')
}

function createProjectStatusUpdate(projectId, status, body) {
  const mutation = `
    mutation($projectId: ID!, $status: ProjectV2StatusUpdateStatus!, $body: String!) {
      createProjectV2StatusUpdate(input: {
        projectId: $projectId
        status: $status
        body: $body
      }) {
        statusUpdate {
          id
          status
          updatedAt
          body
        }
      }
    }`
  const response = ghGraphql(mutation, { projectId, status, body })
  return response.data?.createProjectV2StatusUpdate?.statusUpdate
}

function syncProjectStatusCard(config, action, state) {
  const desired = STATUS_ACTIONS[action]
  const cache = loadProjectStatusCache()
  const body = buildProjectStatusBody(action, state)
  const projectId = config.GITHUB_PROJECT_ID

  try {
    const statusUpdate = createProjectStatusUpdate(projectId, desired.boardStatus, body)

    if (!statusUpdate?.id) {
      throw new Error('Failed to create the project status update card.')
    }

    const nextCache = {
      project_id: projectId,
      status_update_id: statusUpdate.id,
      last_status: desired.boardStatus,
      last_synced_at: new Date().toISOString(),
      last_issue_url: issueUrlFromState(state),
      last_cwd_basename: basename(state?.cwd || process.cwd()),
    }
    saveProjectStatusCache(nextCache)
    return { ok: true, cache: nextCache }
  } catch (error) {
    const failedCache = {
      project_id: projectId,
      status_update_id: cache?.project_id === projectId ? cache.status_update_id ?? null : null,
      last_status: desired.boardStatus,
      last_synced_at: cache?.last_synced_at ?? null,
      last_issue_url: issueUrlFromState(state),
      last_cwd_basename: basename(state?.cwd || process.cwd()),
      last_error: error.message,
      last_attempted_at: new Date().toISOString(),
    }
    saveProjectStatusCache(failedCache)
    return { ok: false, error: error.message, cache: failedCache }
  }
}

function describeBoardSync(sync) {
  if (!sync) return 'never synced'
  if (sync.success) return `${sync.status} at ${sync.synced_at}`
  if (sync.error) return `failed (${sync.error})`
  return 'unknown'
}

function printStatus() {
  const install = getInstallState(process.cwd())
  const config = install.config
  const activeSession = findSessionByCwd(process.cwd())
  const projectStatusCache = loadProjectStatusCache()
  const runtimeStatus = loadRuntimeStatus()

  console.log('Claude Session Tracker Status')
  console.log(`Install: ${install.state}`)
  console.log(`Hook files present: ${formatYesNo(install.hookFilesPresent)}`)

  for (const target of install.hookRegistrations) {
    const detail = target.invalid
      ? 'invalid settings.json'
      : target.installed
        ? 'installed'
        : target.exists
          ? 'present but tracker hooks missing'
          : 'not present'
    console.log(`Hook scope (${target.scope}): ${detail} [${target.path}]`)
  }

  if (config) {
    const scopes = install.hookRegistrations.filter(target => target.installed).map(target => target.scope).join(', ') || 'none'
    console.log(`Config: ${CONFIG_FILE}`)
    console.log(`Configured scope(s): ${scopes}`)
    console.log(`Notes repo: ${config.NOTES_REPO ?? '(missing)'}`)
    if (config.GITHUB_PROJECT_OWNER && config.GITHUB_PROJECT_NUMBER) {
      console.log(`Project URL: https://github.com/users/${config.GITHUB_PROJECT_OWNER}/projects/${config.GITHUB_PROJECT_NUMBER}`)
    }
    if (config.DONE_TIMEOUT_SECS) {
      console.log(`Idle timeout: ${Math.floor(Number(config.DONE_TIMEOUT_SECS) / 60)} min`)
    }
  }

  if (activeSession) {
    const { sessionId, state } = activeSession
    console.log(`Current session: ${sessionId}`)
    console.log(`Current status: ${state.status}`)
    console.log(`Tracking paused: ${formatYesNo(Boolean(state.tracking_paused))}`)
    console.log(`Issue: ${issueUrlFromState(state) ?? '(unavailable)'}`)
    console.log(`Project status sync: ${describeBoardSync(state.project_status_sync)}`)
  } else {
    console.log('Current session: none')
  }

  if (projectStatusCache) {
    const cacheDetail = projectStatusCache.last_error
      ? `${projectStatusCache.last_status} (last error: ${projectStatusCache.last_error})`
      : `${projectStatusCache.last_status} at ${projectStatusCache.last_synced_at}`
    console.log(`Project status cache: ${cacheDetail}`)
  }

  if (runtimeStatus) {
    const detail = runtimeStatus.reason === 'notes_repo_public'
      ? `tracking blocked because ${runtimeStatus.repo} is public`
      : runtimeStatus.reason === 'project_off_track'
        ? `tracking blocked because the project board is OFF_TRACK`
        : `tracking blocked: ${runtimeStatus.error ?? runtimeStatus.reason}`
    console.log(`Runtime status: ${detail}`)
  }
}

function runDoctor() {
  const checks = []
  const addCheck = (status, label, detail, help = null) => checks.push({ status, label, detail, help })
  const install = getInstallState(process.cwd())
  const config = install.config
  const installedTargets = install.hookRegistrations.filter(target => target.installed)
  const hasPython = hasCmd('python3')
  const hasGh = hasCmd('gh')

  addCheck(hasPython ? 'PASS' : 'FAIL', 'python3', hasPython ? 'python3 is available' : 'python3 is missing', 'Install Python 3 from https://python.org')
  addCheck(hasGh ? 'PASS' : 'FAIL', 'gh', hasGh ? 'GitHub CLI is available' : 'GitHub CLI is missing', 'Install gh from https://cli.github.com/manual/installation')

  if (hasGh) {
    const authStatus = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
    addCheck(authStatus.status === 0 ? 'PASS' : 'FAIL', 'GitHub auth', authStatus.status === 0 ? 'authenticated' : 'not authenticated', 'Run `gh auth login --scopes "project,repo"`')
    if (authStatus.status === 0) {
      addCheck(hasRequiredScopes() ? 'PASS' : 'FAIL', 'GitHub scopes', hasRequiredScopes() ? 'project and repo scopes present' : 'required scopes are missing', 'Run `gh auth refresh --scopes "project,repo"`')
    }
  }

  addCheck(config ? 'PASS' : 'FAIL', 'config.env', config ? `found at ${CONFIG_FILE}` : 'config.env not found', 'Run `npx claude-session-tracker` to install or reinstall')
  addCheck(install.hookFilesPresent ? 'PASS' : 'FAIL', 'hook files', install.hookFilesPresent ? 'all hook files are present' : 'one or more installed hook files are missing', 'Re-run `npx claude-session-tracker`')

  for (const target of install.hookRegistrations) {
    if (target.invalid) {
      addCheck('FAIL', `${target.scope} settings`, `${target.path} is not valid JSON`, `Fix or recreate ${target.path}`)
    } else if (!target.exists) {
      addCheck('WARN', `${target.scope} settings`, `${target.path} does not exist`, 'Only needed if you want tracker hooks in this scope')
    } else if (target.installed) {
      addCheck('PASS', `${target.scope} settings`, `tracker hooks registered in ${target.path}`)
    } else {
      addCheck('WARN', `${target.scope} settings`, `tracker hooks are not registered in ${target.path}`, 'Re-run setup only if you want tracker hooks in this scope')
    }
  }

  if (installedTargets.length === 0) {
    addCheck('FAIL', 'hook registration summary', 'tracker hooks are not registered in any settings.json', 'Run `npx claude-session-tracker` and choose reinstall if needed')
  }

  if (config && hasGh) {
    try {
      const isPrivate = ghRepoIsPrivate(config.NOTES_REPO)
      addCheck(isPrivate ? 'PASS' : 'FAIL', 'NOTES_REPO visibility', isPrivate ? `${config.NOTES_REPO} is private` : `${config.NOTES_REPO} is public`, isPrivate ? null : 'Use a private repository for NOTES_REPO')
    } catch (error) {
      addCheck('FAIL', 'NOTES_REPO visibility', error.message, 'Verify the repository exists and that your gh token can access it')
    }

    try {
      fetchProjectMetadata(config.GITHUB_PROJECT_OWNER, Number(config.GITHUB_PROJECT_NUMBER))
      addCheck('PASS', 'project metadata', 'project metadata can be queried')
    } catch (error) {
      addCheck('FAIL', 'project metadata', error.message, 'Verify GITHUB_PROJECT_OWNER/GITHUB_PROJECT_NUMBER in config.env')
    }
  }

  const invalidStates = listSessionStates().filter(entry => !entry.ok)
  addCheck(invalidStates.length === 0 ? 'PASS' : 'FAIL', 'session state files', invalidStates.length === 0 ? 'all session state files are valid JSON' : `${invalidStates.length} invalid session state file(s) found`, invalidStates.length === 0 ? null : `Inspect or remove the invalid files under ${STATE_DIR}`)

  for (const check of checks) {
    console.log(`[${check.status}] ${check.label}: ${check.detail}`)
    if (check.status !== 'PASS' && check.help) console.log(`  fix: ${check.help}`)
  }

  const hasFailures = checks.some(check => check.status === 'FAIL')
  console.log(hasFailures ? 'Doctor summary: action needed' : 'Doctor summary: healthy')
  process.exit(hasFailures ? 1 : 0)
}

function loadConfigOrExit() {
  const config = readEnvFile(CONFIG_FILE)
  if (!config) {
    console.error('No installation found. Run `npx claude-session-tracker` first.')
    process.exit(1)
  }
  return config
}

function updateSessionBoardSyncState(sessionId, state, result, status) {
  const syncState = {
    status,
    attempted_at: new Date().toISOString(),
    success: result.ok,
  }
  if (result.ok) {
    syncState.synced_at = result.cache.last_synced_at
    syncState.status_update_id = result.cache.status_update_id
  } else {
    syncState.error = result.error
  }
  state.project_status_sync = syncState
  saveState(sessionId, state)
}

function runPause() {
  const config = loadConfigOrExit()
  const entry = findSessionByCwd(process.cwd())
  if (!entry) {
    console.log('No active tracked session found for this workspace.')
    return
  }

  const { sessionId, state } = entry
  state.tracking_paused = true
  state.paused_at = new Date().toISOString()
  state.pause_scope = 'session'
  cancelTimerPid(state.timer_pid)
  delete state.timer_pid
  saveState(sessionId, state)

  const result = syncProjectStatusCard(config, 'pause', state)
  updateSessionBoardSyncState(sessionId, state, result, STATUS_ACTIONS.pause.boardStatus)

  console.log('Local pause succeeded.')
  if (!result.ok) {
    console.log(`Board sync failed: ${result.error}`)
    process.exit(1)
  }
  console.log('Project board marked OFF_TRACK.')
}

function runResume() {
  const config = loadConfigOrExit()
  const entry = findSessionByCwd(process.cwd(), { pausedOnly: true })
  if (!entry) {
    console.log('No paused tracked session found for this workspace.')
    return
  }

  const { sessionId, state } = entry
  const result = syncProjectStatusCard(config, 'resume', state)
  updateSessionBoardSyncState(sessionId, state, result, STATUS_ACTIONS.resume.boardStatus)

  if (!result.ok) {
    console.log(`Board sync failed: ${result.error}`)
    process.exit(1)
  }

  delete state.tracking_paused
  delete state.paused_at
  delete state.pause_scope
  saveState(sessionId, state)
  console.log('Project board marked ON_TRACK.')
  console.log('Local tracking resumed.')
}

function normalizeChoiceValue(result) {
  return typeof result === 'string' ? result : result?.toString?.() ?? ''
}

async function chooseRecommendedMapping(statusField, lang) {
  const choices = statusField.options.map(option => ({ value: option.id, label: option.name }))
  const recommendation = recommendStatusMapping(statusField.options, lang)
  const initialMap = { ...recommendation.map }

  if (Object.keys(recommendation.map).length > 0) {
    const lines = ['Recommended status mappings:']
    for (const key of ['registered', 'responding', 'waiting', 'closed']) {
      const optionId = recommendation.map[key]
      if (!optionId) continue
      const optionName = statusField.options.find(option => option.id === optionId)?.name ?? optionId
      lines.push(`  ${key} -> ${optionName}`)
    }
    p.note(lines.join('\n'), 'Recommended mapping')
    const useRecommended = await p.confirm({ message: 'Use the recommended mappings where available?' })
    if (p.isCancel(useRecommended)) onCancel()
    if (!useRecommended) {
      recommendation.unresolved = ['registered', 'responding', 'waiting', 'closed']
      for (const key of Object.keys(initialMap)) delete initialMap[key]
    }
  }

  const mapping = { ...initialMap }
  const used = new Set(Object.values(mapping))

  for (const key of ['registered', 'responding', 'waiting', 'closed']) {
    if (mapping[key]) continue
    const available = choices.filter(choice => !used.has(choice.value))
    const selected = await p.select({
      message: `${key.padEnd(17)} ->`,
      options: available.length > 0 ? available : choices,
    })
    if (p.isCancel(selected)) onCancel()
    mapping[key] = normalizeChoiceValue(selected)
    used.add(mapping[key])
  }

  return mapping
}

function validateNotesRepoPrivate(notesRepo) {
  const spin = p.spinner()
  spin.start(`Checking repository visibility for ${notesRepo}...`)
  try {
    const isPrivate = ghRepoIsPrivate(notesRepo)
    if (!isPrivate) {
      spin.stop('Repository visibility check failed')
      p.log.error(`Tracking repositories must be private. ${notesRepo} is public.`)
      process.exit(1)
    }
    spin.stop('Repository visibility confirmed (private)')
  } catch (error) {
    spin.stop('Repository visibility check failed')
    p.log.error(error.message)
    process.exit(1)
  }
}

function cleanupAutoSetupArtifacts(recovery) {
  if (!recovery) return

  if (recovery.projectId || recovery.projectNumber) {
    try {
      const projectId = recovery.projectId
        ?? fetchProjectMetadata(recovery.owner, Number(recovery.projectNumber)).projectId
      deleteProjectV2(projectId)
    } catch {
      // noop
    }
  }

  if (recovery.repoFullName) {
    try {
      ghCommand(['repo', 'delete', recovery.repoFullName, '--yes'])
    } catch {
      // noop
    }
  }

  clearAutoSetupRecovery()
}

// -- Star 요청 ----------------------------------------------------------------

async function askForStar() {
  const alreadyStarred = spawnSync(
    'gh',
    ['api', '/user/starred/ej31/claude-session-tracker'],
    { stdio: 'ignore' },
  ).status === 0

  if (alreadyStarred) {
    p.log.success('You already starred this repo — thank you! ⭐')
    return
  }

  p.note([
    '  If this tool has been useful to you,',
    '  a GitHub star would mean a lot — just one click!',
    '',
    '  https://github.com/ej31/claude-session-tracker',
  ].join('\n'), '⭐ One small favour')

  const wantStar = await p.confirm({
    message: 'Star the repo right now? (just press Enter!)',
  })
  if (p.isCancel(wantStar) || !wantStar) return

  const result = spawnSync(
    'gh',
    ['api', '-X', 'PUT', '/user/starred/ej31/claude-session-tracker'],
    { stdio: 'ignore' },
  )
  if (result.status === 0) {
    p.log.success('Thank you so much! ⭐ It really helps.')
  } else {
    p.log.warn('Could not star automatically. Feel free to do it manually!')
    p.log.info('https://github.com/ej31/claude-session-tracker')
  }
}

// -- Uninstall ----------------------------------------------------------------

async function uninstall() {
  console.clear()
  p.intro(' Claude Session Tracker — Uninstall ')

  const confirmed = await p.confirm({ message: 'Remove all installed hooks and configuration?' })
  if (p.isCancel(confirmed) || !confirmed) {
    p.cancel('Uninstall cancelled.')
    process.exit(0)
  }

  const spin = p.spinner()
  spin.start('Removing...')

  let removed = 0
  for (const file of ALL_KNOWN_FILES) {
    const target = join(HOOKS_DIR, file)
    if (existsSync(target)) {
      unlinkSync(target)
      removed++
    }
  }

  for (const path of [CONFIG_FILE, AUTO_SETUP_RECOVERY_FILE, PROJECT_STATUS_CACHE_FILE, RUNTIME_STATUS_FILE]) {
    if (existsSync(path)) {
      unlinkSync(path)
      removed++
    }
  }

  if (existsSync(STATE_DIR)) {
    rmSync(STATE_DIR, { recursive: true })
    removed++
  }

  for (const { path } of getSettingsPaths(process.cwd())) {
    if (!existsSync(path)) continue
    const original = readJson(path)
    if (!original.hooks) continue
    const cleaned = removeOurHooks(original)
    writeFileSync(path, JSON.stringify(cleaned, null, 2) + '\n')
    removed++
  }

  spin.stop(`Removal complete (${removed} items)`)
  p.note([
    'Python scripts, config.env, state, recovery data, and status caches have been deleted.',
    'Hook entries have been removed from settings.json.',
    '',
    'Restart Claude Code to apply changes.',
  ].join('\n'), 'Uninstall complete')

  p.outro('Session tracking has been deactivated.')
}

// -- Auto Setup ---------------------------------------------------------------

async function autoSetup(username) {
  let recovery = loadAutoSetupRecovery()
  if (recovery && !hasRecoveryStep(recovery, 'hooks_installed')) {
    p.note([
      `  Owner      : ${recovery.owner ?? username}`,
      `  Repository : ${recovery.repoFullName ?? '(not created yet)'}`,
      `  Project #  : ${recovery.projectNumber ?? '(not created yet)'}`,
      `  Steps      : ${(recovery.completedSteps ?? []).join(', ') || 'none'}`,
    ].join('\n'), 'Incomplete auto setup detected')

    const action = await p.select({
      message: 'How would you like to continue?',
      options: [
        { value: 'resume', label: 'Resume setup' },
        { value: 'cleanup', label: 'Cleanup partial setup' },
        { value: 'cancel', label: 'Cancel' },
      ],
    })
    if (p.isCancel(action) || action === 'cancel') onCancel()
    if (action === 'cleanup') {
      cleanupAutoSetupArtifacts(recovery)
      p.log.success('Partial auto setup has been cleaned up.')
      recovery = null
    }
  } else {
    recovery = null
  }

  let lang = recovery?.lang
  if (!lang) {
    lang = await p.select({
      message: 'Which language for status labels?',
      options: [
        { value: 'en', label: 'English', hint: 'Registered, Responding, Waiting, Closed' },
        { value: 'ko', label: 'Korean', hint: '세션 등록, 답변 중, 입력 대기, 세션 종료' },
        { value: 'ja', label: 'Japanese', hint: 'セッション登録, 応答中, 入力待ち, セッション終了' },
        { value: 'zh', label: 'Chinese', hint: '会话注册, 响应中, 等待输入, 会话关闭' },
      ],
    })
    if (p.isCancel(lang)) onCancel()
  }

  const repoFullName = `${username}/${SESSION_STORAGE_REPO_NAME}`
  const projectTitle = `${username}'s Claude Session Storage`

  if (!recovery) {
    // 기존 세션 저장소 리포지토리 존재 여부 확인
    const checkSpin = p.spinner()
    checkSpin.start('Checking for existing session storage...')
    const repoExists = sessionStorageRepoExists(username)

    if (repoExists) {
      checkSpin.stop('Existing session storage found')

      // 기존 리포지토리가 private 인지 검증
      if (!ghRepoIsPrivate(repoFullName)) {
        p.log.error(`Repository ${repoFullName} is PUBLIC. Session data may contain sensitive secrets.`)
        p.log.error('Please make the repository private before continuing, or delete it and re-run setup.')
        process.exit(1)
      }

      const meta = fetchMetaJsonFromRepo(repoFullName)
      const META_REQUIRED_FIELDS = ['projectId', 'projectNumber', 'statusFieldId', 'statusMap']
      const hasAllRequiredFields = meta != null && META_REQUIRED_FIELDS.every(f => meta[f] != null)

      if (hasAllRequiredFields) {
        p.note([
          `An existing session storage (https://github.com/${repoFullName}) was found.`,
          'The existing repository and GitHub Projects will be reused.',
          '',
          'If you do not want this, choose one of the following options.',
          '  Option 1 - Proceed with Manual Setup instead.',
          `  Option 2 - Rename or delete the existing GitHub Repository and GitHub Projects board.`,
        ].join('\n'), 'Existing session storage detected')

        const reuseConfirmed = await p.confirm({ message: 'Continue with the existing session storage?' })
        if (p.isCancel(reuseConfirmed) || !reuseConfirmed) onCancel()

        // meta.json 에서 프로젝트 정보를 읽어서 recovery 상태 복원
        recovery = {
          owner: username,
          lang,
          repoFullName,
          projectTitle,
          projectNumber: meta.projectNumber,
          projectId: meta.projectId,
          projectUrl: meta.projectUrl,
          statusFieldId: meta.statusFieldId,
          statusMap: meta.statusMap,
          createdFieldId: meta.createdFieldId,
          lastActiveFieldId: meta.lastActiveFieldId,
          completedSteps: ['repo_created', 'project_created', 'status_configured', 'date_fields_attempted'],
          updatedAt: new Date().toISOString(),
        }
        saveAutoSetupRecovery(recovery)
      } else {
        // 리포지토리는 있지만 meta.json 이 없거나 불완전한 경우 - 프로젝트 재설정 필요
        if (meta != null) {
          p.log.warn('Existing metadata is incomplete. Project configuration will be re-created.')
        }
        recovery = {
          owner: username,
          lang,
          repoFullName,
          projectTitle,
          completedSteps: ['repo_created'],
          updatedAt: new Date().toISOString(),
        }
        saveAutoSetupRecovery(recovery)
      }
    } else {
      checkSpin.stop('No existing session storage found')
      recovery = {
        owner: username,
        lang,
        repoFullName,
        projectTitle,
        completedSteps: [],
        updatedAt: new Date().toISOString(),
      }
      saveAutoSetupRecovery(recovery)
    }
  }

  const labels = STATUS_LABELS[lang]
  p.note([
    'A private repository will be created for storing session issues.',
    '',
    `  Repository : ${recovery.repoFullName} (private)`,
    `  Project    : ${recovery.projectTitle}`,
    `  Statuses   : ${labels.registered}, ${labels.responding}, ${labels.waiting}, ${labels.closed}`,
    `  Date fields: Session Created, Last Active`,
    '  Scope      : Global',
    '  Timeout    : 30 min',
  ].join('\n'), 'Setup plan')

  if (!hasRecoveryStep(recovery, 'repo_created')) {
    const confirmed = await p.confirm({ message: 'Looks good? Ready to create everything?' })
    if (p.isCancel(confirmed) || !confirmed) onCancel()

    const repoSpin = p.spinner()
    repoSpin.start('Creating private repository...')
    try {
      ghCommand([
        'repo',
        'create',
        recovery.repoFullName,
        '--private',
        '--description',
        'Claude Code session tracking storage (auto-created)',
      ])
      repoSpin.stop('Repository created')
      recovery = markAutoSetupStep(recovery, 'repo_created')
    } catch (error) {
      repoSpin.stop('Failed to create repository')
      p.log.error(error.message)
      process.exit(1)
    }

    // 새 리포지토리에 README.md 푸시
    const readmeSpin = p.spinner()
    readmeSpin.start('Pushing README.md to repository...')
    try {
      pushFileToRepo(recovery.repoFullName, 'README.md', buildRepoReadme(), 'docs: add session storage README with security warning')
      readmeSpin.stop('README.md pushed')
    } catch (error) {
      readmeSpin.stop('Could not push README.md (non-critical)')
      p.log.warn(`README push failed: ${error.message}`)
    }
  }

  if (!hasRecoveryStep(recovery, 'project_created')) {
    const projectSpin = p.spinner()
    projectSpin.start('Creating GitHub Project...')
    try {
      ghCommand(['project', 'create', '--title', recovery.projectTitle, '--owner', username])
      const listOutput = ghCommand(['project', 'list', '--owner', username, '--format', 'json', '--limit', '20'])
      const projects = JSON.parse(listOutput).projects ?? []
      const created = projects.find(project => project.title === recovery.projectTitle)
      if (!created) throw new Error('Project was created but could not be found in project list.')
      projectSpin.stop(`Project created (#${created.number})`)
      recovery = markAutoSetupStep(recovery, 'project_created', { projectNumber: created.number })
    } catch (error) {
      projectSpin.stop('Failed to create project')
      p.log.error(error.message)
      process.exit(1)
    }
  }

  if (!recovery.projectId) {
    const fetchSpin = p.spinner()
    fetchSpin.start('Fetching project metadata...')
    let projectMeta
    try {
      projectMeta = fetchProjectMetadata(username, recovery.projectNumber)
      fetchSpin.stop('Project metadata fetched')
      recovery = {
        ...recovery,
        projectId: projectMeta.projectId,
        projectUrl: projectMeta.projectUrl,
        statusFieldId: projectMeta.statusField.id,
      }
      saveAutoSetupRecovery(recovery)
    } catch (error) {
      fetchSpin.stop('Failed to fetch project metadata')
      p.log.error(error.message)
      process.exit(1)
    }
  }

  if (!hasRecoveryStep(recovery, 'status_configured')) {
    const statusSpin = p.spinner()
    statusSpin.start('Configuring status options...')
    try {
      const labelKeys = ['registered', 'responding', 'waiting', 'closed']
      const options = labelKeys.map((key, index) => ({
        name: labels[key],
        color: STATUS_COLORS[index],
        description: STATUS_DESCRIPTIONS[index],
      }))
      const mutation = `
        mutation($fieldId: ID!, $options: [ProjectV2SingleSelectFieldOptionInput!]!) {
          updateProjectV2Field(input: {
            fieldId: $fieldId
            singleSelectOptions: $options
          }) {
            projectV2Field {
              ... on ProjectV2SingleSelectField {
                options { id name }
              }
            }
          }
        }`
      const response = ghGraphql(mutation, { fieldId: recovery.statusFieldId, options })
      const updatedOptions = response.data?.updateProjectV2Field?.projectV2Field?.options
      if (!updatedOptions) throw new Error('Failed to configure status options.')
      const statusMap = {}
      for (const key of labelKeys) {
        const match = updatedOptions.find(option => option.name === labels[key])
        if (!match) throw new Error(`Could not find option ID for status: ${labels[key]}`)
        statusMap[key] = match.id
      }
      statusSpin.stop('Status options configured')
      recovery = markAutoSetupStep(recovery, 'status_configured', { statusMap })
    } catch (error) {
      statusSpin.stop('Failed to configure status options')
      p.log.error(error.message)
      process.exit(1)
    }
  }

  if (!hasRecoveryStep(recovery, 'date_fields_attempted')) {
    const dateFieldSpin = p.spinner()
    dateFieldSpin.start('Creating custom date fields...')
    let createdFieldId = recovery.createdFieldId
    let lastActiveFieldId = recovery.lastActiveFieldId
    try {
      const mutation = `
        mutation($projectId: ID!, $name: String!) {
          createProjectV2Field(input: {
            projectId: $projectId
            name: $name
            dataType: DATE
          }) {
            projectV2Field {
              ... on ProjectV2Field {
                id
                name
              }
            }
          }
        }`
      const createdRes = ghGraphql(mutation, { projectId: recovery.projectId, name: 'Session Created' })
      createdFieldId = createdRes.data?.createProjectV2Field?.projectV2Field?.id
      const lastActiveRes = ghGraphql(mutation, { projectId: recovery.projectId, name: 'Last Active' })
      lastActiveFieldId = lastActiveRes.data?.createProjectV2Field?.projectV2Field?.id
      dateFieldSpin.stop('Custom date fields created')
    } catch (error) {
      dateFieldSpin.stop('Skipped custom date fields (non-critical)')
      p.log.warn(`Date fields could not be created: ${error.message}\n  This is optional — setup will continue without them.`)
    }
    recovery = markAutoSetupStep(recovery, 'date_fields_attempted', { createdFieldId, lastActiveFieldId })
  }

  if (!hasRecoveryStep(recovery, 'hooks_installed')) {
    const installSpin = p.spinner()
    installSpin.start('Installing hooks...')
    try {
      installHooksAndConfig({
        owner: username,
        projectNumber: recovery.projectNumber,
        projectId: recovery.projectId,
        statusFieldId: recovery.statusFieldId,
        statusMap: recovery.statusMap,
        notesRepo: recovery.repoFullName,
        timeoutMinutes: 30,
        scope: 'global',
        createdFieldId: recovery.createdFieldId,
        lastActiveFieldId: recovery.lastActiveFieldId,
        lang,
      })
      installSpin.stop('Hooks installed')
      recovery = markAutoSetupStep(recovery, 'hooks_installed')
    } catch (error) {
      installSpin.stop('Failed to install hooks')
      p.log.error(error.message)
      process.exit(1)
    }
  }

  // meta.json 을 리포지토리에 푸시 (기존 저장소 재사용 경로에서는 이미 존재하므로 건너뜀)
  const restoredFromMeta = recovery.completedSteps.includes('repo_created')
    && recovery.completedSteps.includes('project_created')
    && recovery.completedSteps.includes('status_configured')
    && recovery.completedSteps.includes('date_fields_attempted')
  if (!restoredFromMeta) {
    const metaSpin = p.spinner()
    metaSpin.start('Saving project metadata to repository...')
    try {
      const metaContent = JSON.stringify({
        projectId: recovery.projectId,
        projectNumber: recovery.projectNumber,
        projectUrl: recovery.projectUrl,
        statusFieldId: recovery.statusFieldId,
        statusMap: recovery.statusMap,
        createdFieldId: recovery.createdFieldId ?? null,
        lastActiveFieldId: recovery.lastActiveFieldId ?? null,
        updatedAt: new Date().toISOString(),
      }, null, 2)
      pushFileToRepo(recovery.repoFullName, META_JSON_PATH, metaContent, 'chore: update session tracker metadata')
      metaSpin.stop('Project metadata saved to repository')
    } catch (error) {
      metaSpin.stop('Could not save project metadata (non-critical)')
      p.log.warn(`Metadata push failed: ${error.message}`)
    }
  }

  ensureProjectReadmeAfterInstall(recovery.projectId)
  ensureProjectOnTrackAfterInstall(recovery.projectId, process.cwd())

  clearAutoSetupRecovery()
  p.note([
    'Everything is all set! Here\'s what to do next:',
    '',
    '  1. Start Claude Code and have any conversation',
    `  2. Check your project board at: ${recovery.projectUrl}`,
    '',
    '  Session issues are stored in:',
    `     https://github.com/${recovery.repoFullName}`,
  ].join('\n'), 'You\'re ready to go!')

  p.outro(`Run Claude Code and start a conversation — then check ${recovery.projectUrl}`)
}

// -- Manual Setup -------------------------------------------------------------

async function manualSetup(username) {
  const owner = await p.text({
    message: 'GitHub Project Owner (username or org)',
    initialValue: username,
    validate: value => !value?.trim() ? 'This field is required.' : undefined,
  })
  if (p.isCancel(owner)) onCancel()
  const ownerVal = owner.trim()

  p.log.info(`If you don't have a project yet, no worries! Create one at: https://github.com/${ownerVal}?tab=projects`)

  const number = await p.text({
    message: 'Project number',
    placeholder: '1',
    validate: value => !value || Number.isNaN(Number(value)) ? 'Please enter a number.' : undefined,
  })
  if (p.isCancel(number)) onCancel()
  const projectNumber = Number(number)

  const fetchSpin = p.spinner()
  fetchSpin.start('Fetching project metadata...')
  let projectMeta
  try {
    projectMeta = fetchProjectMetadata(ownerVal, projectNumber)
    fetchSpin.stop(`Found project: ${projectMeta.projectTitle}`)
  } catch (error) {
    fetchSpin.stop('Failed to fetch project')
    p.log.error(error.message)
    process.exit(1)
  }

  const statusOptions = projectMeta.statusField.options.map(option => option.name).join(', ')
  p.note([
    `  Name    : ${projectMeta.projectTitle}`,
    `  URL     : ${projectMeta.projectUrl}`,
    `  ID      : ${projectMeta.projectId}`,
    `  Statuses: ${statusOptions}`,
  ].join('\n'), 'Project details')

  const rightProject = await p.confirm({ message: 'Is this the right project?' })
  if (p.isCancel(rightProject) || !rightProject) onCancel()

  const notesRepo = await p.text({
    message: 'Repository for session issues (when no git remote is available)',
    placeholder: `${ownerVal}/dev-notes`,
    validate: value => !value?.includes('/') ? 'Please use owner/repo format.' : undefined,
  })
  if (p.isCancel(notesRepo)) onCancel()
  validateNotesRepoPrivate(notesRepo.trim())

  const timeout = await p.text({
    message: 'Session close timer (minutes)',
    initialValue: '30',
    validate: value => !value || Number.isNaN(Number(value)) ? 'Please enter a number.' : undefined,
  })
  if (p.isCancel(timeout)) onCancel()

  const scope = await p.select({
    message: 'Hook scope',
    options: [
      { value: 'global', label: 'Global              (~/.claude/settings.json)' },
      { value: 'project', label: 'Current project     (.claude/settings.json)' },
    ],
  })
  if (p.isCancel(scope)) onCancel()

  const langManual = await p.select({
    message: 'Which language for issue comments?',
    options: [
      { value: 'en', label: 'English', hint: 'Prompt, Response' },
      { value: 'ko', label: 'Korean', hint: '프롬프트, 답변' },
      { value: 'ja', label: 'Japanese', hint: 'プロンプト, 回答' },
      { value: 'zh', label: 'Chinese', hint: '提示词, 回答' },
    ],
  })
  if (p.isCancel(langManual)) onCancel()

  p.log.info('Using recommended mappings where possible. You can still adjust anything below.')
  const statusMap = await chooseRecommendedMapping(projectMeta.statusField, normalizeChoiceValue(langManual))

  const scopeLabel = scope === 'global' ? 'Global' : 'Current project'
  p.note([
    `  Project    : ${projectMeta.projectTitle} (#${projectNumber})`,
    `  Notes Repo : ${notesRepo.trim()}`,
    `  Timeout    : ${timeout} min`,
    `  Scope      : ${scopeLabel}`,
  ].join('\n'), 'Setup summary')

  const confirmed = await p.confirm({ message: 'Ready to install?' })
  if (p.isCancel(confirmed) || !confirmed) onCancel()

  const installSpin = p.spinner()
  installSpin.start('Installing hooks...')
  try {
    installHooksAndConfig({
      owner: ownerVal,
      projectNumber,
      projectId: projectMeta.projectId,
      statusFieldId: projectMeta.statusField.id,
      statusMap,
      notesRepo: notesRepo.trim(),
      timeoutMinutes: Number(timeout),
      scope: normalizeChoiceValue(scope),
      lang: normalizeChoiceValue(langManual),
    })
    installSpin.stop('Hooks installed')
  } catch (error) {
    installSpin.stop('Failed to install hooks')
    p.log.error(error.message)
    process.exit(1)
  }

  ensureProjectReadmeAfterInstall(projectMeta.projectId)
  ensureProjectOnTrackAfterInstall(projectMeta.projectId, process.cwd())

  p.note([
    'Everything is all set! Here\'s what to do next:',
    '',
    '  1. Start Claude Code and have any conversation',
    `  2. Check your project board at: ${projectMeta.projectUrl}`,
    '',
    '  Session issues are stored in:',
    `     https://github.com/${notesRepo.trim()}`,
  ].join('\n'), 'You\'re ready to go!')

  p.outro(`Run Claude Code and start a conversation — then check ${projectMeta.projectUrl}`)
}

// -- Main ---------------------------------------------------------------------

async function runSetup() {
  console.clear()
  p.intro(' Claude Session Tracker — Setup ')

  const envSpin = p.spinner()
  envSpin.start('Checking environment...')

  if (!hasCmd('python3')) {
    envSpin.stop('Environment check failed')
    p.log.error('Missing required tool: python3')
    p.log.info('Install Python 3 from https://python.org')
    p.outro('Setup aborted.')
    process.exit(1)
  }

  if (!hasCmd('gh')) {
    envSpin.stop('GitHub CLI (gh) not found')
    const shouldInstall = await p.confirm({
      message: 'GitHub CLI (gh) is required but not installed. Install it now?',
    })
    if (p.isCancel(shouldInstall) || !shouldInstall) {
      p.log.info('Manual install: https://cli.github.com')
      p.outro('Setup aborted.')
      process.exit(1)
    }
    const installed = await tryInstallGh()
    if (!installed || !hasCmd('gh')) {
      p.log.error('Failed to install gh. Please install it manually and re-run setup.')
      p.log.info('https://cli.github.com/manual/installation')
      p.outro('Setup aborted.')
      process.exit(1)
    }
    p.log.success('GitHub CLI installed successfully!')
  }

  const authCheck = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
  if (authCheck.status !== 0) {
    envSpin.stop('GitHub authentication required')
    p.log.warn('GitHub authentication is required. Starting login...')
    try {
      await runGhAuthLogin()
      p.log.success('GitHub login successful')
    } catch (error) {
      p.log.error(error.message)
      p.outro('Setup aborted.')
      process.exit(1)
    }
  } else if (!hasRequiredScopes()) {
    envSpin.stop('Missing required GitHub scopes')
    p.log.warn('The scopes project and repo are required. Adding them now.')
    try {
      await runGhAuthRefresh()
      p.log.success('Scopes added successfully')
    } catch (error) {
      p.log.error(error.message)
      p.outro('Setup aborted.')
      process.exit(1)
    }
  }
  envSpin.stop('Environment looks good')

  const username = getAuthenticatedUser()
  if (!username) {
    p.log.error('Could not detect your GitHub username. Please make sure `gh auth login` is completed.')
    p.outro('Setup aborted.')
    process.exit(1)
  }

  p.log.message(`Hey ${username}! Let's set up session tracking for Claude Code.`)

  if (existsSync(CONFIG_FILE)) {
    p.log.warn('An existing installation was detected.')
    p.note([
      '  Config : ~/.claude/hooks/config.env',
      '  Hooks  : ~/.claude/hooks/cst_*.py',
      '',
      '  Continuing will overwrite your current settings.',
      '  To remove the existing installation first, run:',
      '    npx claude-session-tracker uninstall',
    ].join('\n'), 'Already installed')

    const action = await p.select({
      message: 'What would you like to do?',
      options: [
        { value: 'reinstall', label: 'Reinstall (overwrite current settings)' },
        { value: 'cancel', label: 'Cancel' },
      ],
    })
    if (p.isCancel(action) || action === 'cancel') {
      p.outro('Setup cancelled. Your existing installation is unchanged.')
      process.exit(0)
    }
  }

  const mode = await p.select({
    message: 'How would you like to set up?',
    options: [
      { value: 'auto', label: 'Auto setup (recommended)', hint: 'Creates a private repo and project for you' },
      { value: 'manual', label: 'Manual setup', hint: 'Use your own existing project' },
    ],
  })
  if (p.isCancel(mode)) onCancel()

  if (mode === 'auto') {
    await autoSetup(username)
  } else {
    await manualSetup(username)
  }

  await askForStar()
}

async function main() {
  const command = process.argv[2]

  if (command === 'status') {
    printStatus()
    return
  }

  if (command === 'doctor') {
    runDoctor()
    return
  }

  if (command === 'pause') {
    runPause()
    return
  }

  if (command === 'resume') {
    runResume()
    return
  }

  if (command === 'uninstall') {
    await uninstall()
    return
  }

  await runSetup()
}

main().catch((error) => {
  console.error(error.message)
  process.exit(1)
})
