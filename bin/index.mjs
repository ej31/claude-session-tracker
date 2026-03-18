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
  lstatSync,
} from 'node:fs'
import { join, dirname, basename, resolve } from 'node:path'
import { homedir, networkInterfaces } from 'node:os'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'
import { pathToFileURL } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const { printStatus: printStatusUI } = await import(pathToFileURL(join(__dirname, 'status.mjs')).href)
const PKG_VERSION = (() => {
  try {
    const pkg = JSON.parse(readFileSync(join(__dirname, '..', 'package.json'), 'utf-8'))
    return pkg.version || '0.0.0'
  } catch {
    return '0.0.0'
  }
})()
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
  'repo_linked',
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
  'cst_session_end.py',
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
const STATUS_ACTIONS = {
  install: {
    trackerState: 'installed',
    boardStatus: 'ON_TRACK',
    message:
      'Tracking is installed and active. Local hook-driven prompt/response capture is ready for the next Claude session.',
  },
  pause: {
    trackerState: 'paused',
    boardStatus: 'INACTIVE',
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

const EXIT_CODES = {
  SUCCESS: 0,
  GENERAL_ERROR: 1,
  INVALID_USAGE: 2,
  AUTH_FAILURE: 3,
}

const VALID_LANGUAGES = Object.keys(STATUS_LABELS)

// -- Non-interactive mode -----------------------------------------------------

function isNonInteractive(flags) {
  return !!(
    flags.yes ||
    flags.ci ||
    process.env.CI === 'true' ||
    process.env.CI === '1' ||
    process.env.GITHUB_ACTIONS === 'true' ||
    process.env.GITLAB_CI ||
    process.env.CIRCLECI ||
    process.env.JENKINS_URL ||
    process.env.CODEBUILD_BUILD_ID ||
    process.env.TF_BUILD ||
    !process.stdin.isTTY
  )
}

function resolveToken(flags) {
  // 1순위: --token-stdin (stdin 파이프, 가장 안전)
  if (flags.tokenStdin) {
    try {
      const data = readFileSync(0, 'utf-8').trim()
      if (data) return data
    } catch {
      // stdin 읽기 실패
    }
  }

  // 2순위: GITHUB_TOKEN 환경변수
  if (process.env.GITHUB_TOKEN) return process.env.GITHUB_TOKEN

  // 3순위: GH_TOKEN 환경변수
  if (process.env.GH_TOKEN) return process.env.GH_TOKEN

  // 4순위: --token 플래그 (프로세스 목록에 노출될 수 있음)
  if (flags.token) {
    if (process.stdin.isTTY) {
      console.warn('[WARN] Passing tokens via CLI flags exposes them in process listings. Consider using GITHUB_TOKEN environment variable instead.')
    }
    return flags.token
  }

  // 5순위: 기존 gh auth 상태
  if (hasCmd('gh')) {
    const authCheck = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
    if (authCheck.status === 0) return null // gh가 이미 인증됨, 별도 토큰 불필요
  }

  return undefined // 인증 수단 없음
}

function validateResolvedToken(token) {
  const env = token ? { ...process.env, GH_TOKEN: token } : { ...process.env }

  // 토큰 인증 확인 + username 획득
  const result = spawnSync('gh', ['api', 'user', '--jq', '.login'], { encoding: 'utf-8', env })
  if (result.status !== 0 || !result.stdout?.trim()) {
    return { valid: false, username: null, error: 'Token authentication failed. Verify your PAT is valid.' }
  }

  // scope 검증 (project, repo 필요)
  const scopeCheck = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8', env })
  const scopeOutput = (scopeCheck.stdout ?? '') + (scopeCheck.stderr ?? '')
  if (!scopeOutput.includes('project') || !scopeOutput.includes('repo')) {
    return {
      valid: false,
      username: result.stdout.trim(),
      error: 'Token missing required scopes: project, repo. Create a PAT with these scopes.',
    }
  }

  return { valid: true, username: result.stdout.trim(), error: null }
}

function ciSpinner(nonInteractive) {
  if (!nonInteractive) return p.spinner()
  return {
    start: (msg) => console.log(`  > ${msg}`),
    stop: (msg) => console.log(`  OK ${msg}`),
  }
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

function parseGitHubRepoFromRemoteUrl(remoteUrl) {
  const trimmed = remoteUrl.trim().replace(/\.git$/, '')

  if (trimmed.startsWith('http://') || trimmed.startsWith('https://') || trimmed.startsWith('ssh://')) {
    try {
      const parsed = new URL(trimmed)
      const allowedHosts = parsed.protocol === 'ssh:'
        ? new Set(['github.com', 'ssh.github.com'])
        : new Set(['github.com'])
      if (!allowedHosts.has(parsed.hostname)) return null
      const pathParts = parsed.pathname.replace(/^\/+/, '').split('/')
      if (pathParts.length < 2 || !pathParts[0] || !pathParts[1]) return null
      return `${pathParts[0]}/${pathParts[1]}`
    } catch {
      return null
    }
  }

  const scpLikeMatch = trimmed.match(/^(?:[^@]+@)?github\.com:(.+)$/)
  if (!scpLikeMatch) return null

  const pathParts = scpLikeMatch[1].split('/')
  if (pathParts.length < 2 || !pathParts[0] || !pathParts[1]) return null
  return `${pathParts[0]}/${pathParts[1]}`
}

function getGitHubRepoFromCwd(cwd = process.cwd()) {
  const result = spawnSync('git', ['-C', cwd, 'remote', 'get-url', 'origin'], {
    encoding: 'utf-8',
    timeout: 5000,
  })
  if (result.status !== 0) return null

  return parseGitHubRepoFromRemoteUrl(result.stdout)
}

function getContextRepoExample(fallbackRepo) {
  return getGitHubRepoFromCwd() ?? fallbackRepo
}

function getProjectNameDisplayExamples(contextRepo, samplePrompt = 'Fix session resume bug') {
  return {
    prefixTitle: `[${contextRepo}] ${samplePrompt}`,
    labelTitle: samplePrompt,
    labelName: contextRepo,
  }
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
        let mtime = 0
        try { mtime = statSync(path).mtimeMs } catch { /* 파일이 삭제된 경우 무시 */ }
        return {
          ok: false,
          path,
          sessionId: name.replace(/\.json$/, ''),
          error,
          mtimeMs: mtime,
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

function findAvailableRepoName(username) {
  // 기존 repo가 public으로 변경된 경우, 충돌 없는 새 이름을 찾는다.
  const MAX_SUFFIX = 10
  for (let i = 2; i <= MAX_SUFFIX; i++) {
    const candidate = `${SESSION_STORAGE_REPO_NAME}-${i}`
    const result = spawnSync('gh', ['api', `repos/${username}/${candidate}`], { stdio: 'pipe' })
    if (result.status !== 0) return `${username}/${candidate}`
  }
  throw new Error(`Could not find an available repository name (tried up to ${SESSION_STORAGE_REPO_NAME}-${MAX_SUFFIX})`)
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
    '',
    '> [!WARNING]',
    '> Do NOT rename this repository or the linked GitHub Project board.',
    '> The tracker identifies resources by their exact names.',
    '> Renaming will cause new installations to create duplicate repositories or projects.',
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
          closed
          public
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
          closed
          public
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
  if (project.closed) throw new Error('This project is closed. Please use an open project.')
  if (project.public) throw new Error('This project is public. Session data may contain sensitive information. Please use a private project.')
  const statusField = project.fields.nodes.find(field => field?.name === 'Status')
  if (!statusField) throw new Error("Could not find a 'Status' field in this project.")
  return { projectId: project.id, projectTitle: project.title, projectUrl: project.url, statusField }
}

function getProjectStatus(projectId) {
  const query = `
    query($projectId: ID!) {
      node(id: $projectId) {
        ... on ProjectV2 { closed, public }
      }
    }`
  const response = ghGraphql(query, { projectId })
  const node = response.data?.node ?? {}
  return { closed: node.closed === true, public: node.public === true }
}

function closeProjectV2(projectId) {
  const mutation = `
    mutation($projectId: ID!) {
      updateProjectV2(input: { projectId: $projectId, closed: true }) {
        projectV2 { id }
      }
    }`
  ghGraphql(mutation, { projectId })
}

function archiveRepo(repoFullName) {
  // 먼저 private으로 전환한 뒤 archive 처리한다.
  // archived 상태에서는 visibility 변경이 불가하므로 순서가 중요하다.
  try {
    ghCommand(['repo', 'edit', repoFullName, '--visibility', 'private'])
  } catch {
    // plan 제한 등으로 실패할 수 있다.
    // private 전환 실패 시 public 상태로 archive하면 세션 데이터가 노출되므로 재검증한다.
    if (!ghRepoIsPrivate(repoFullName)) {
      throw new Error(`Cannot make ${repoFullName} private before archiving. Session data may be publicly visible.`)
    }
  }
  ghCommand(['repo', 'archive', repoFullName, '--yes'])
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
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_session_end.py')}`, timeout: 10, async: true }],
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
  projectNameMode,
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
    `CST_PROJECT_NAME_MODE=${projectNameMode}`,
  ]
  if (createdFieldId) configLines.push(`GITHUB_CREATED_FIELD_ID=${createdFieldId}`)
  if (lastActiveFieldId) configLines.push(`GITHUB_LAST_ACTIVE_FIELD_ID=${lastActiveFieldId}`)
  if (lang) configLines.push(`CST_LANG=${lang}`)
  configLines.push(`CST_VERSION=${PKG_VERSION}`)
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
    'This project board automatically records and organizes every Claude Code session you run.',
    'Each time you start a Claude Code conversation, a GitHub Issue is created and added to this board as a project item.',
    'The issue captures the full session lifecycle — from the initial prompt, through tool calls and responses, to the final summary.',
    '',
    'You can use this board to review past sessions, search conversation history, and keep track of what Claude has done across all your projects.',
    '',
    '## How it works',
    '',
    '1. When a Claude Code session starts, a new GitHub Issue is created with session metadata (workspace, timestamp, session ID).',
    '2. As the session progresses, prompts and responses are appended to the issue as comments.',
    '3. When the session ends or goes idle, the issue is automatically closed and the project item status is updated.',
    '4. All items are organized on this board with status columns (Registered, Responding, Waiting, Closed).',
    '',
    '## Project board status (ON_TRACK / INACTIVE)',
    '',
    '- **`ON_TRACK`** — Session tracking is active. Every Claude Code session will be recorded to this board.',
    '- **`INACTIVE`** — Session tracking is paused. No new sessions will be recorded until tracking is resumed.',
    '',
    '### How to change the tracking status',
    '',
    'You can switch between `ON_TRACK` and `INACTIVE` in two ways.',
    '',
    '**Option 1 — From the GitHub web UI**',
    '',
    'Open this project board on GitHub, click the status badge, and change it directly to `ON_TRACK` or `INACTIVE`.',
    '',
    '**Option 2 — From the command line**',
    '',
    '```bash',
    '# Resume tracking (set to ON_TRACK)',
    'claude-session-tracker resume',
    '',
    '# Pause tracking (set to INACTIVE)',
    'claude-session-tracker pause',
    '```',
    '',
    '## History',
    '',
    'Each install, pause, and resume action writes a project status update with session metadata (workspace, timestamp, issue URL, and local IP).',
    'You can review these updates in the project board\'s status update history.',
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

function updateProjectDescription(projectId, shortDescription) {
  const mutation = `
    mutation($projectId: ID!, $shortDescription: String!) {
      updateProjectV2(input: {
        projectId: $projectId
        shortDescription: $shortDescription
      }) {
        projectV2 {
          id
          shortDescription
        }
      }
    }`
  const response = ghGraphql(mutation, { projectId, shortDescription })
  return response.data?.updateProjectV2?.projectV2
}

function ensureProjectReadmeAfterInstall(projectId) {
  const spin = p.spinner()
  spin.start('Configuring project README and description...')
  try {
    updateProjectReadme(projectId, buildProjectReadme())
    updateProjectDescription(
      projectId,
      'Automatically tracks all Claude Code sessions. Set status to ON_TRACK to enable tracking, INACTIVE to pause.',
    )
    spin.stop('Project README and description configured')
    return true
  } catch (error) {
    spin.stop('Could not configure project README/description')
    p.log.warn(`The install completed, but setting the project README/description failed: ${error.message}`)
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
  const activeSession = findSessionByCwd(process.cwd())
  if (activeSession) {
    activeSession.issueUrl = issueUrlFromState(activeSession.state)
  }
  printStatusUI({
    install: getInstallState(process.cwd()),
    activeSession,
    projectStatusCache: loadProjectStatusCache(),
    runtimeStatus: loadRuntimeStatus(),
    version: PKG_VERSION,
  })
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

function findAnyActiveSession({ pausedOnly = false } = {}) {
  return listSessionStates()
    .filter(entry => entry.ok)
    .filter((entry) => {
      const state = entry.state
      if (state.status === 'closed') return false
      if (pausedOnly && !state.tracking_paused) return false
      return true
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs)[0] ?? null
}

function runPause() {
  const config = loadConfigOrExit()
  const entry = findAnyActiveSession()

  if (entry) {
    const { sessionId, state } = entry
    state.tracking_paused = true
    state.paused_at = new Date().toISOString()
    state.pause_scope = 'global'
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
  } else {
    // 활성 세션이 없어도 보드 상태는 변경
    const globalState = { cwd: '(global)', session_id: '(no active session)' }
    const result = syncProjectStatusCard(config, 'pause', globalState)
    if (!result.ok) {
      console.log(`Board sync failed: ${result.error}`)
      process.exit(1)
    }
  }
  console.log('Project board marked INACTIVE.')
}

function runResume() {
  const config = loadConfigOrExit()
  const entry = findAnyActiveSession({ pausedOnly: true })

  if (entry) {
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
  } else {
    // 일시정지된 세션이 없어도 보드 상태는 변경
    const globalState = { cwd: '(global)', session_id: '(no active session)' }
    const result = syncProjectStatusCard(config, 'resume', globalState)
    if (!result.ok) {
      console.log(`Board sync failed: ${result.error}`)
      process.exit(1)
    }
  }
  console.log('Project board marked ON_TRACK.')
  console.log('Local tracking resumed.')
}


async function runUpdate() {
  const config = readEnvFile(CONFIG_FILE)
  if (!config) {
    console.error('No installation found. Run `npx claude-session-tracker` first.')
    process.exit(1)
  }

  const currentVersion = config.CST_VERSION || 'unknown'
  const spin = p.spinner()
  spin.start('Checking for updates...')

  let latestVersion
  try {
    const res = spawnSync('npm', ['view', 'claude-session-tracker', 'version'], {
      encoding: 'utf-8',
      timeout: 15000,
    })
    if (res.status !== 0) {
      spin.stop('Failed to check the latest version.')
      console.error(res.stderr?.trim() || 'npm view failed')
      process.exit(1)
    }
    latestVersion = res.stdout.trim()
  } catch (error) {
    spin.stop('Failed to check the latest version.')
    console.error(error.message)
    process.exit(1)
  }

  if (!latestVersion || !/^\d+\.\d+\.\d+$/.test(latestVersion)) {
    spin.stop('Could not determine a valid latest version.')
    process.exit(1)
  }

  const compareSemver = (a, b) => {
    const pa = a.split('.').map(Number)
    const pb = b.split('.').map(Number)
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
      if ((pa[i] || 0) !== (pb[i] || 0)) return (pa[i] || 0) - (pb[i] || 0)
    }
    return 0
  }

  // 현재 버전을 알 수 없으면 (레거시 설치) 항상 업데이트 제안
  const isNewer = !/^\d+\.\d+\.\d+$/.test(currentVersion) || compareSemver(latestVersion, currentVersion) > 0

  if (!isNewer) {
    spin.stop(`Already up to date (v${currentVersion}).`)
    return
  }

  spin.stop(`Update available: ${currentVersion} → ${latestVersion}`)

  const confirm = await p.confirm({
    message: `Update to v${latestVersion}?`,
  })
  if (p.isCancel(confirm) || !confirm) {
    p.log.info('Update cancelled.')
    return
  }

  const updateSpin = p.spinner()
  updateSpin.start('Downloading latest version...')

  // npm pack으로 최신 패키지를 임시 디렉토리에 다운로드
  const tmpDir = join(HOME, '.claude', 'hooks', '.update-tmp')
  try {
    const stat = lstatSync(tmpDir)
    if (stat.isSymbolicLink()) {
      throw new Error('Symlink detected at update temp directory')
    }
    rmSync(tmpDir, { recursive: true, force: true })
  } catch (e) {
    if (e.code !== 'ENOENT') throw e
  }
  mkdirSync(tmpDir, { recursive: true, mode: 0o700 })

  try {
    const packResult = spawnSync('npm', ['pack', `claude-session-tracker@${latestVersion}`, '--pack-destination', tmpDir], {
      encoding: 'utf-8',
      timeout: 30000,
    })
    if (packResult.status !== 0) {
      throw new Error(packResult.stderr?.trim() || 'npm pack failed')
    }

    const tarball = packResult.stdout.trim().split('\n').pop()
    if (!tarball || !tarball.endsWith('.tgz') || tarball.includes('/') || tarball.includes('..')) {
      throw new Error(`Unexpected npm pack output: ${tarball}`)
    }
    const tarPath = resolve(join(tmpDir, tarball))
    if (!tarPath.startsWith(resolve(tmpDir))) {
      throw new Error('Path traversal detected in tarball filename')
    }

    // tarball 풀기
    const extractDir = join(tmpDir, 'extracted')
    mkdirSync(extractDir, { recursive: true })
    const tarResult = spawnSync('tar', ['xzf', tarPath, '-C', extractDir], {
      encoding: 'utf-8',
      timeout: 15000,
    })
    if (tarResult.status !== 0) {
      throw new Error(tarResult.stderr?.trim() || 'tar extraction failed')
    }

    const packageDir = join(extractDir, 'package')
    if (!existsSync(join(packageDir, 'package.json'))) {
      throw new Error('Unexpected package structure after extraction')
    }

    // hook 파일을 스테이징 디렉토리에 먼저 복사 (원자적 업데이트)
    const stagingDir = join(tmpDir, 'staging')
    mkdirSync(stagingDir, { recursive: true })
    const filesToUpdate = []
    for (const file of PY_FILES) {
      const src = join(packageDir, 'hooks', file)
      if (existsSync(src)) {
        const stagingPath = join(stagingDir, file)
        copyFileSync(src, stagingPath)
        chmodSync(stagingPath, 0o755)
        filesToUpdate.push(file)
      }
    }

    // 모든 파일이 준비되면 한꺼번에 이동
    for (const file of filesToUpdate) {
      copyFileSync(join(stagingDir, file), join(HOOKS_DIR, file))
    }

    // config.env의 CST_VERSION 업데이트
    if (existsSync(CONFIG_FILE)) {
      let configContent = readFileSync(CONFIG_FILE, 'utf-8')
      if (configContent.includes('CST_VERSION=')) {
        configContent = configContent.replace(/^CST_VERSION=.*/m, `CST_VERSION=${latestVersion}`)
      } else {
        configContent = configContent.trimEnd() + `\nCST_VERSION=${latestVersion}\n`
      }
      writeFileSync(CONFIG_FILE, configContent)
    }

    // 업데이트 체크 캐시 초기화
    const updateCheckCache = join(HOOKS_DIR, 'update_check.json')
    removeFileIfExists(updateCheckCache)

    updateSpin.stop(`Updated ${filesToUpdate.length} hook files to v${latestVersion}.`)
    p.log.success('Restart Claude Code to apply changes.')
  } catch (error) {
    updateSpin.stop('Update failed.')
    console.error(error.message)
    process.exit(1)
  } finally {
    // 임시 디렉토리 정리
    try {
      rmSync(tmpDir, { recursive: true, force: true })
    } catch {
      // noop
    }
  }
}

function cleanupAutoSetupArtifacts(recovery) {
  if (!recovery) return

  // 생성된 프로젝트가 있으면 close 처리한다.
  if (recovery.projectId || recovery.projectNumber) {
    try {
      const projectId = recovery.projectId
        ?? fetchProjectMetadata(recovery.owner, Number(recovery.projectNumber)).projectId
      closeProjectV2(projectId)
    } catch (e) {
      p.log.warn(`Failed to close project during cleanup: ${e.message ?? e}`)
    }
  }

  // 생성된 리포지토리가 있으면 private 전환 후 archive 처리한다.
  if (recovery.repoFullName) {
    try {
      archiveRepo(recovery.repoFullName)
    } catch (e) {
      p.log.warn(`Failed to archive repository during cleanup: ${e.message ?? e}`)
    }
  }

  clearAutoSetupRecovery()
}

// -- 글로벌 설치 ---------------------------------------------------------------

function isInstalledGlobally() {
  const result = spawnSync('claude-session-tracker', ['--version'], {
    encoding: 'utf-8',
    timeout: 5000,
  })
  return result.status === 0
}

async function promptGlobalInstall() {
  if (isInstalledGlobally()) {
    return
  }

  const shouldInstall = await p.confirm({
    message: 'Install globally for easier access? (allows running "claude-session-tracker pause" without npx)',
    initialValue: true,
  })

  if (p.isCancel(shouldInstall) || !shouldInstall) {
    p.log.info('Skipped global install. You can always use: npx claude-session-tracker <command>')
    return
  }

  const spin = p.spinner()
  spin.start('Installing claude-session-tracker globally...')

  const result = spawnSync('npm', ['install', '-g', `claude-session-tracker@${PKG_VERSION}`], {
    encoding: 'utf-8',
    timeout: 30000,
  })

  if (result.status === 0) {
    spin.stop('claude-session-tracker is now available as a global command')
  } else {
    spin.stop('Global install failed')
    const errMsg = result.stderr?.trim() || 'unknown error'
    if (errMsg.includes('EACCES')) {
      p.log.warn('Permission denied. Try one of these options')
      p.log.info('  • Using nvm/Volta: npm install -g claude-session-tracker')
      p.log.info('  • System Node.js:  sudo npm install -g claude-session-tracker')
    } else {
      p.log.warn(`Could not install globally: ${errMsg}`)
      p.log.info('  You can install manually: npm install -g claude-session-tracker')
    }
    p.log.info('  Or just use: npx claude-session-tracker <command>')
  }
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

async function autoSetup(username, flags = {}) {
  const nonInteractive = isNonInteractive(flags)

  let recovery = loadAutoSetupRecovery()
  if (recovery && !hasRecoveryStep(recovery, 'hooks_installed')) {
    if (nonInteractive) {
      // 비대화형 모드에서는 자동으로 resume
      console.log('[INFO] Incomplete setup detected. Resuming automatically.')
    } else {
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
    }
  } else {
    recovery = null
  }

  let lang = recovery?.lang
  if (!lang) {
    if (nonInteractive) {
      lang = flags.language ?? 'en'
      if (!VALID_LANGUAGES.includes(lang)) {
        console.error(`[ERROR] Invalid language: ${lang}. Valid options: ${VALID_LANGUAGES.join(', ')}`)
        process.exit(EXIT_CODES.INVALID_USAGE)
      }
      console.log(`[INFO] Using language: ${lang}`)
    } else {
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
  }

  const repoFullName = `${username}/${SESSION_STORAGE_REPO_NAME}`
  const projectTitle = `${username}'s Claude Session Storage`

  if (!recovery) {
    // 기존 세션 저장소 리포지토리 존재 여부 확인
    const checkSpin = ciSpinner(nonInteractive)
    checkSpin.start('Checking for existing session storage...')
    const repoExists = sessionStorageRepoExists(username)

    if (repoExists) {
      checkSpin.stop('Existing session storage found')

      // 기존 리포지토리가 private 인지 검증
      if (!ghRepoIsPrivate(repoFullName)) {
        p.log.warn(`Repository ${repoFullName} is no longer private.`)
        p.log.warn('Session data must always be stored in a private repository to protect sensitive information.')
        try {
          archiveRepo(repoFullName)
          p.log.info(`Existing repository has been made private and archived.`)
        } catch (e) {
          p.log.warn(`Failed to archive existing repository: ${e.message ?? e}`)
        }
        const newRepoFullName = findAvailableRepoName(username)
        p.log.warn(`A new private repository will be created: ${newRepoFullName}`)
        recovery = {
          owner: username,
          lang,
          repoFullName: newRepoFullName,
          projectTitle,
          completedSteps: [],
          updatedAt: new Date().toISOString(),
        }
        saveAutoSetupRecovery(recovery)
        // 새 repo/project를 처음부터 생성하므로 기존 검사를 건너뛴다
      }

      if (!recovery) {
      const meta = fetchMetaJsonFromRepo(repoFullName)
      const META_REQUIRED_FIELDS = ['projectId', 'projectNumber', 'statusFieldId', 'statusMap']
      const hasAllRequiredFields = meta != null && META_REQUIRED_FIELDS.every(f => meta[f] != null)

      if (hasAllRequiredFields) {
        // 기존 프로젝트가 closed 또는 public 상태인지 확인 (단일 GraphQL 호출)
        const projectStatus = getProjectStatus(meta.projectId)

        if (projectStatus.closed || projectStatus.public) {
          if (projectStatus.closed) {
            p.log.warn(`Existing project #${meta.projectNumber} is closed. A new project will be created.`)
          } else {
            p.log.warn(`Existing project #${meta.projectNumber} is no longer private.`)
            p.log.warn('Session data must always be stored in a private project to protect sensitive information.')
            try {
              closeProjectV2(meta.projectId)
              p.log.info(`Existing project #${meta.projectNumber} has been closed.`)
            } catch (e) {
              p.log.warn(`Failed to close existing project: ${e.message ?? e}`)
            }
            p.log.warn('A new private project will be created.')
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
        } else {
          p.log.info(`Reusing existing session storage (https://github.com/${repoFullName})`)

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
            completedSteps: ['repo_created', 'project_created', 'repo_linked', 'status_configured', 'date_fields_attempted'],
            restoredFromExisting: true,
            updatedAt: new Date().toISOString(),
          }
          saveAutoSetupRecovery(recovery)
        }
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
      } // if (!recovery) - repo가 public이면 이 블록 전체를 건너뛴다
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
  const projectNameMode = 'label'
  const contextRepoExample = getContextRepoExample(recovery.repoFullName)
  const displayExamples = getProjectNameDisplayExamples(contextRepoExample)

  if (nonInteractive) {
    console.log('[INFO] Setup plan:')
    console.log(`  Repository : ${recovery.repoFullName} (private)`)
    console.log(`  Project    : ${recovery.projectTitle}`)
    console.log(`  Statuses   : ${labels.registered}, ${labels.responding}, ${labels.waiting}, ${labels.closed}`)
    console.log(`  Language   : ${lang}`)
  } else {
    p.note([
      'A private repository will be created for storing session issues.',
      '',
      `  Repository : ${recovery.repoFullName} (private)`,
      `  Project    : ${recovery.projectTitle}`,
      `  Statuses   : ${labels.registered}, ${labels.responding}, ${labels.waiting}, ${labels.closed}`,
      `  Date fields: Session Created, Last Active`,
      `  Display    : Label mode`,
      `  Example    : Issue title "${displayExamples.labelTitle}"`,
      `  Labels     : claude-code, ${displayExamples.labelName}`,
      `  Repo source: Current workspace repo if available, otherwise ${recovery.repoFullName}`,
      '  Scope      : Global',
      '  Timeout    : 30 min',
    ].join('\n'), 'Setup plan')
  }

  if (!hasRecoveryStep(recovery, 'repo_created')) {
    if (!nonInteractive) {
      const confirmed = await p.confirm({ message: 'Looks good? Ready to create everything?' })
      if (p.isCancel(confirmed) || !confirmed) onCancel()
    }

    const repoSpin = ciSpinner(nonInteractive)
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
      // 동시 설치 시 다른 서버가 이미 생성한 경우 - 기존 repo 사용
      if (error.message.includes('already exists')) {
        repoSpin.stop('Repository already exists (concurrent installation detected)')
        recovery = markAutoSetupStep(recovery, 'repo_created')
      } else {
        repoSpin.stop('Failed to create repository')
        p.log.error(error.message)
        process.exit(EXIT_CODES.GENERAL_ERROR)
      }
    }

    // 새 리포지토리에 README.md 푸시
    const readmeSpin = ciSpinner(nonInteractive)
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
    const projectSpin = ciSpinner(nonInteractive)
    projectSpin.start('Creating GitHub Project...')
    try {
      // 동시 설치 대응: 생성 전에 동일 이름의 기존 프로젝트 확인
      const listOutput = ghCommand(['project', 'list', '--owner', username, '--format', 'json', '--limit', '20'])
      const projects = JSON.parse(listOutput).projects ?? []
      const existing = projects.find(project => project.title === recovery.projectTitle)

      if (existing) {
        projectSpin.stop(`Reusing existing project (#${existing.number})`)
        recovery = markAutoSetupStep(recovery, 'project_created', { projectNumber: existing.number })
      } else {
        ghCommand(['project', 'create', '--title', recovery.projectTitle, '--owner', username])
        // GitHub Project 생성 후 목록 반영까지 3~10초 소요될 수 있으므로 재시도
        const MAX_RETRIES = 5
        const RETRY_DELAY_MS = 2000
        let created = null
        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
          const refreshOutput = ghCommand(['project', 'list', '--owner', username, '--format', 'json', '--limit', '20'])
          const refreshed = JSON.parse(refreshOutput).projects ?? []
          created = refreshed.find(project => project.title === recovery.projectTitle)
          if (created) break
          if (attempt < MAX_RETRIES) {
            projectSpin.message(`Waiting for project to appear (attempt ${attempt}/${MAX_RETRIES})...`)
            spawnSync('sleep', [String(RETRY_DELAY_MS / 1000)])
          }
        }
        if (!created) throw new Error('Project was created but could not be found in project list after multiple retries.')
        projectSpin.stop(`Project created (#${created.number})`)
        recovery = markAutoSetupStep(recovery, 'project_created', { projectNumber: created.number })
      }
    } catch (error) {
      projectSpin.stop('Failed to create project')
      p.log.error(error.message)
      process.exit(EXIT_CODES.GENERAL_ERROR)
    }
  }

  if (!recovery.projectId) {
    const fetchSpin = ciSpinner(nonInteractive)
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

  if (!hasRecoveryStep(recovery, 'repo_linked')) {
    const linkSpin = ciSpinner(nonInteractive)
    linkSpin.start('Linking repository to project...')
    try {
      // Repository node ID 조회
      const repoQuery = `
        query($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) { id }
        }`
      const [repoOwner, repoName] = recovery.repoFullName.split('/')
      const repoResponse = ghGraphql(repoQuery, { owner: repoOwner, name: repoName })
      const repositoryId = repoResponse.data?.repository?.id
      if (!repositoryId) throw new Error(`Could not resolve repository node ID for ${recovery.repoFullName}`)

      // Project에 Repository를 default repository로 연결
      const linkMutation = `
        mutation($projectId: ID!, $repositoryId: ID!) {
          linkProjectV2ToRepository(input: {
            projectId: $projectId
            repositoryId: $repositoryId
          }) {
            repository { id }
          }
        }`
      ghGraphql(linkMutation, { projectId: recovery.projectId, repositoryId })
      linkSpin.stop('Repository linked to project')
      recovery = markAutoSetupStep(recovery, 'repo_linked')
    } catch (error) {
      // 이미 연결되어 있는 경우 등 비치명적 오류 허용
      linkSpin.stop('Repository linking skipped (non-critical)')
      p.log.warn(`Could not link repository to project: ${error.message}`)
      recovery = markAutoSetupStep(recovery, 'repo_linked')
    }
  }

  if (!hasRecoveryStep(recovery, 'status_configured')) {
    const statusSpin = ciSpinner(nonInteractive)
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
    const dateFieldSpin = ciSpinner(nonInteractive)
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
    const installSpin = ciSpinner(nonInteractive)
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
        projectNameMode,
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
  if (!recovery.restoredFromExisting) {
    const metaSpin = ciSpinner(nonInteractive)
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
  await promptGlobalInstall()

  clearAutoSetupRecovery()

  if (nonInteractive) {
    console.log('[OK] Setup complete!')
    console.log(`  Project board: ${recovery.projectUrl}`)
    console.log(`  Session repo : https://github.com/${recovery.repoFullName}`)
  } else {
    p.note([
      'Everything is all set! Here\'s what to do next:',
      '',
      '  1. Run "claude-session-tracker status" to verify your setup',
      '  2. Start Claude Code and have any conversation',
      `  3. Check your project board at: ${recovery.projectUrl}`,
      '',
      '  Session issues are stored in:',
      `     https://github.com/${recovery.repoFullName}`,
    ].join('\n'), 'You\'re ready to go!')

    p.outro(`Try "claude-session-tracker status" now — then start a Claude Code conversation`)
  }
}


// -- Main ---------------------------------------------------------------------

async function runSetup(flags = {}) {
  const nonInteractive = isNonInteractive(flags)

  // 비대화형 모드: 토큰 해석 및 검증
  if (nonInteractive) {
    console.log('[INFO] Non-interactive mode detected')

    if (!hasCmd('python3')) {
      console.error('[ERROR] Missing required tool: python3')
      console.error('  Install Python 3 from https://python.org')
      process.exit(EXIT_CODES.INVALID_USAGE)
    }

    if (!hasCmd('gh')) {
      console.error('[ERROR] Missing required tool: gh (GitHub CLI)')
      console.error('  Install from https://cli.github.com')
      process.exit(EXIT_CODES.INVALID_USAGE)
    }

    const token = resolveToken(flags)
    if (token === undefined) {
      console.error('[ERROR] No GitHub authentication found.')
      console.error('  Provide a token using one of the following methods (in priority order):')
      console.error('    1. echo $TOKEN | npx claude-session-tracker --yes --token-stdin')
      console.error('    2. GITHUB_TOKEN=ghp_xxx npx claude-session-tracker --yes')
      console.error('    3. npx claude-session-tracker --yes --token ghp_xxx')
      console.error('    4. Pre-authenticate with: gh auth login')
      process.exit(EXIT_CODES.INVALID_USAGE)
    }

    // 토큰을 gh CLI에 주입 (null이면 기존 gh auth 사용)
    if (token) {
      process.env.GH_TOKEN = token
    }

    // 토큰 유효성 검증
    const validation = validateResolvedToken(token)
    if (!validation.valid) {
      console.error(`[ERROR] ${validation.error}`)
      process.exit(EXIT_CODES.AUTH_FAILURE)
    }

    const username = validation.username
    console.log(`[INFO] Authenticated as ${username}`)

    if (existsSync(CONFIG_FILE)) {
      console.log('[INFO] Existing installation detected. Reinstalling.')
    }

    await autoSetup(username, flags)
    return
  }

  // 대화형 모드: 기존 동작 유지
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
      '    claude-session-tracker uninstall',
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

  await autoSetup(username, flags)

  await promptGlobalInstall()

  await askForStar()
}

async function main() {
  const { values: flags, positionals } = parseArgs({
    args: process.argv.slice(2),
    options: {
      yes:          { type: 'boolean', short: 'y', default: false },
      ci:           { type: 'boolean', default: false },
      token:        { type: 'string',  short: 't' },
      'token-stdin': { type: 'boolean', default: false },
      language:     { type: 'string',  short: 'l' },
      version:      { type: 'boolean', short: 'v', default: false },
    },
    allowPositionals: true,
    strict: false,
  })

  if (flags.version) {
    console.log(PKG_VERSION)
    return
  }

  const command = positionals[0]

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

  if (command === 'update') {
    await runUpdate()
    return
  }

  if (command === 'uninstall') {
    await uninstall()
    return
  }

  await runSetup(flags)
}

main().catch((error) => {
  console.error(error.message)
  process.exit(1)
})
