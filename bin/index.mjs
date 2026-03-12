#!/usr/bin/env node
import * as p from '@clack/prompts'
import { spawnSync, spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import {
  mkdirSync, writeFileSync, readFileSync,
  existsSync, copyFileSync, chmodSync, unlinkSync, rmSync,
} from 'node:fs'
import { join, dirname } from 'node:path'
import { homedir } from 'node:os'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const HOME = homedir()
const HOOKS_DIR = join(HOME, '.claude', 'hooks')
const STATE_DIR = join(HOOKS_DIR, 'state')
const CONFIG_FILE = join(HOOKS_DIR, 'config.env')
const HOOKS_SRC = join(__dirname, '..', 'hooks')
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

// -- Utilities ----------------------------------------------------------------

function onCancel() {
  p.cancel('Setup cancelled.')
  process.exit(0)
}

function hasCmd(cmd) {
  const isWin = process.platform === 'win32'
  const finder = isWin ? 'where' : 'which'
  // Windows에서 choco, scoop 등 .cmd 파일도 찾으려면 shell: true 필요
  const result = spawnSync(finder, [cmd], { stdio: 'ignore', shell: isWin })
  return result.status === 0
}

function getGitHubRepoFromCwd(cwd = process.cwd()) {
  const result = spawnSync('git', ['-C', cwd, 'remote', 'get-url', 'origin'], {
    encoding: 'utf-8',
    timeout: 5000,
  })
  if (result.status !== 0) return null

  const url = result.stdout.trim().replace(/\.git$/, '')
  if (!url.includes('github.com')) return null

  if (url.startsWith('https://')) {
    const parts = url.split('/')
    return `${parts.at(-2)}/${parts.at(-1)}`
  }

  if (url.includes(':')) {
    return url.split(':').at(-1) ?? null
  }

  return null
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

// -- gh 자동 설치 -------------------------------------------------------------

function detectLinuxDistro() {
  try {
    const content = readFileSync('/etc/os-release', 'utf-8')
    const lines = Object.fromEntries(
      content.split('\n')
        .filter(l => l.includes('='))
        .map(l => { const [k, ...v] = l.split('='); return [k.trim(), v.join('=').replace(/"/g, '').trim()] }),
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
  // Windows에서 winget, choco, scoop 등 .cmd/.bat 파일 실행을 위해 shell: true 필요
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
      const updated = runCmd('sudo', ['apt', 'update'])
      if (!updated) return false
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

function ghGraphql(query, variables = {}) {
  const result = spawnSync(
    'gh', ['api', 'graphql', '--input', '-'],
    { input: JSON.stringify({ query, variables }), encoding: 'utf-8' },
  )
  if (!result.stdout?.trim()) throw new Error(result.stderr || 'No response from gh api')
  return JSON.parse(result.stdout)
}

function ghCommand(args) {
  const result = spawnSync('gh', args, { encoding: 'utf-8' })
  if (result.status !== 0) {
    throw new Error(result.stderr?.trim() || `gh command failed: gh ${args.join(' ')}`)
  }
  return result.stdout?.trim() ?? ''
}

function readJson(path) {
  if (!existsSync(path)) return {}
  try { return JSON.parse(readFileSync(path, 'utf-8')) } catch { return {} }
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
    cleaned.hooks[key] = entries.filter(entry => {
      const cmds = entry.hooks ?? []
      return !cmds.some(h => ALL_KNOWN_FILES.some(f => h.command?.includes(f)))
    })
    if (cleaned.hooks[key].length === 0) delete cleaned.hooks[key]
  }
  if (Object.keys(cleaned.hooks).length === 0) delete cleaned.hooks
  return cleaned
}

function getAuthenticatedUser() {
  const result = spawnSync('gh', ['api', 'user', '--jq', '.login'], { encoding: 'utf-8' })
  if (result.status !== 0 || !result.stdout?.trim()) return null
  return result.stdout.trim()
}

function hasRequiredScopes() {
  const result = spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' })
  const output = result.stdout + result.stderr
  return output.includes('project') && output.includes('repo')
}

function openBrowser(url) {
  const platform = process.platform
  if (platform === 'darwin') {
    spawnSync('open', [url])
  } else if (platform === 'win32') {
    spawnSync('cmd', ['/c', 'start', url], { shell: true })
  } else {
    spawnSync('xdg-open', [url])
  }
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
    // Windows에서는 GH_BROWSER 값을 다르게 설정
    const ghBrowser = process.platform === 'win32' ? 'cmd /c exit 0' : '/usr/bin/true'

    const child = spawn('gh', args, {
      env: { ...process.env, GH_BROWSER: ghBrowser },
      stdio: ['pipe', 'pipe', 'pipe'],
    })

    let codeShown = false
    let resolved = false

    // 15초 내 코드 미감지 시 폴백
    const timeout = setTimeout(async () => {
      if (!codeShown && !resolved) {
        child.kill()
        try {
          await fallbackAuthGuide(mode)
          resolved = true
          resolve()
        } catch (err) {
          reject(err)
        }
      }
    }, 15000)

    const handleOutput = (data) => {
      const text = data.toString()

      if (!codeShown) {
        const match = text.match(/([A-Z0-9]{4}-[A-Z0-9]{4})/)
        if (match) {
          codeShown = true
          const code = match[1]
          p.note(
            'Why this is required:\n' +
            '  claude-session-tracker needs to create and manage GitHub Projects on your behalf.\n' +
            '  This requires read/write access via OAuth — your credentials are never seen\n' +
            '  or stored by claude-session-tracker. Login is handled entirely by GitHub.',
            'Why is GitHub login required?'
          )
          p.log.step('A browser has been opened.')
          p.log.info('  - Enter the code below in your browser.')
          p.log.info('  - claude-session-tracker does not collect any information during this process.')
          p.log.message('')
          p.log.message(`  Your GitHub authentication code:  ${code}`)

          openBrowser('https://github.com/login/device')

          // gh가 "Press Enter" 대기 중이면 Enter 전송
          child.stdin.write('\n')
        }
      }
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

    child.on('error', (err) => {
      clearTimeout(timeout)
      if (!resolved) {
        resolved = true
        reject(err)
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

function fetchProjectMetadata(owner, number) {
  const query = `
    query($login: String!, $number: Int!) {
      user(login: $login) {
        projectV2(number: $number) {
          id title url
          fields(first: 30) {
            nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
          }
        }
      }
      organization(login: $login) {
        projectV2(number: $number) {
          id title url
          fields(first: 30) {
            nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
          }
        }
      }
    }`
  const res = ghGraphql(query, { login: owner, number })
  const pv2 = res.data?.user?.projectV2 ?? res.data?.organization?.projectV2
  if (!pv2) throw new Error('Could not find the project. Please check the owner and project number.')
  const statusField = pv2.fields.nodes.find(n => n?.name === 'Status')
  if (!statusField) throw new Error("Could not find a 'Status' field in this project.")
  return { projectId: pv2.id, projectTitle: pv2.title, projectUrl: pv2.url, statusField }
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

  for (const f of PY_FILES) {
    copyFileSync(join(HOOKS_SRC, f), join(HOOKS_DIR, f))
    chmodSync(join(HOOKS_DIR, f), 0o755)
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

// -- Star 요청 ----------------------------------------------------------------

async function askForStar() {
  const alreadyStarred = spawnSync(
    'gh', ['api', '/user/starred/ej31/claude-session-tracker'],
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
    'gh', ['api', '-X', 'PUT', '/user/starred/ej31/claude-session-tracker'],
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
  if (p.isCancel(confirmed) || !confirmed) { p.cancel('Uninstall cancelled.'); process.exit(0) }

  const spin = p.spinner()
  spin.start('Removing...')

  let removed = 0

  for (const f of ALL_KNOWN_FILES) {
    const target = join(HOOKS_DIR, f)
    if (existsSync(target)) { unlinkSync(target); removed++ }
  }

  if (existsSync(CONFIG_FILE)) { unlinkSync(CONFIG_FILE); removed++ }

  const logFile = join(HOOKS_DIR, 'hooks.log')
  if (existsSync(logFile)) { unlinkSync(logFile); removed++ }

  if (existsSync(STATE_DIR)) { rmSync(STATE_DIR, { recursive: true }); removed++ }

  const settingsPaths = [
    join(HOME, '.claude', 'settings.json'),
    join(process.cwd(), '.claude', 'settings.json'),
  ]
  for (const sp of settingsPaths) {
    if (!existsSync(sp)) continue
    const original = readJson(sp)
    if (!original.hooks) continue
    const cleaned = removeOurHooks(original)
    writeFileSync(sp, JSON.stringify(cleaned, null, 2) + '\n')
    removed++
  }

  spin.stop(`Removal complete (${removed} items)`)

  p.note([
    'Python scripts, config.env, state, and logs have been deleted.',
    'Hook entries have been removed from settings.json.',
    '',
    'Restart Claude Code to apply changes.',
  ].join('\n'), 'Uninstall complete')

  p.outro('Session tracking has been deactivated.')
}

// -- Auto Setup ---------------------------------------------------------------

async function autoSetup(username) {
  // Language selection
  const lang = await p.select({
    message: 'Which language for status labels?',
    options: [
      { value: 'en', label: 'English', hint: 'Registered, Responding, Waiting, Closed' },
      { value: 'ko', label: 'Korean', hint: '세션 등록, 답변 중, 입력 대기, 세션 종료' },
      { value: 'ja', label: 'Japanese', hint: 'セッション登録, 応答中, 入力待ち, セッション終了' },
      { value: 'zh', label: 'Chinese', hint: '会话注册, 响应中, 等待输入, 会话关闭' },
    ],
  })
  if (p.isCancel(lang)) onCancel()

  const labels = STATUS_LABELS[lang]
  const hash = randomBytes(3).toString('hex')
  const repoName = `claude-session-storage-${hash}`
  const repoFullName = `${username}/${repoName}`
  const projectTitle = 'Claude Session Tracker'
  const projectNameMode = 'label'
  const contextRepoExample = getContextRepoExample(repoFullName)
  const displayExamples = getProjectNameDisplayExamples(contextRepoExample)

  p.note([
    'A private repository will be created for storing session issues.',
    '',
    `  Repository : ${repoFullName} (private)`,
    `  Project    : ${projectTitle}`,
    `  Statuses   : ${labels.registered}, ${labels.responding}, ${labels.waiting}, ${labels.closed}`,
    `  Date fields: Session Created, Last Active`,
    `  Display    : Label mode`,
    `  Example    : Issue title "${displayExamples.labelTitle}"`,
    `  Labels     : claude-code, ${displayExamples.labelName}`,
    `  Repo source: Current workspace repo if available, otherwise ${repoFullName}`,
    `  Scope      : Global`,
    `  Timeout    : 30 min`,
  ].join('\n'), 'Setup plan')

  const confirmed = await p.confirm({ message: 'Looks good? Ready to create everything?' })
  if (p.isCancel(confirmed) || !confirmed) onCancel()

  // Step 1: Create private repo
  const repoSpin = p.spinner()
  repoSpin.start('Creating private repository...')
  try {
    ghCommand([
      'repo', 'create', repoFullName,
      '--private',
      '--description', 'Claude Code session tracking storage (auto-created)',
    ])
    repoSpin.stop('Repository created')
  } catch (e) {
    repoSpin.stop('Failed to create repository')
    p.log.error(e.message)
    process.exit(1)
  }

  // Step 2: Create project, then look up its number via project list
  const projSpin = p.spinner()
  projSpin.start('Creating GitHub Project...')
  let projectNumber
  try {
    ghCommand(['project', 'create', '--title', projectTitle, '--owner', username])
    // gh project create produces no output — look up the project we just created
    const listOutput = ghCommand(['project', 'list', '--owner', username, '--format', 'json', '--limit', '10'])
    const projects = JSON.parse(listOutput).projects ?? []
    const created = projects.find(proj => proj.title === projectTitle)
    if (!created) throw new Error('Project was created but could not be found in project list.')
    projectNumber = created.number
    projSpin.stop(`Project created (#${projectNumber})`)
  } catch (e) {
    projSpin.stop('Failed to create project')
    p.log.error(e.message)
    process.exit(1)
  }

  // Step 3: Fetch project metadata
  const fetchSpin = p.spinner()
  fetchSpin.start('Fetching project metadata...')
  let projectId, statusField
  try {
    const meta = fetchProjectMetadata(username, projectNumber)
    projectId = meta.projectId
    statusField = meta.statusField
    fetchSpin.stop('Project metadata fetched')
  } catch (e) {
    fetchSpin.stop('Failed to fetch project metadata')
    p.log.error(e.message)
    process.exit(1)
  }

  // Step 4: Update Status field with custom options
  const statusSpin = p.spinner()
  statusSpin.start('Configuring status options...')
  let statusMap
  try {
    const labelKeys = ['registered', 'responding', 'waiting', 'closed']
    const options = labelKeys.map((key, i) => ({
      name: labels[key],
      color: STATUS_COLORS[i],
      description: STATUS_DESCRIPTIONS[i],
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

    const res = ghGraphql(mutation, { fieldId: statusField.id, options })
    const updatedOptions = res.data?.updateProjectV2Field?.projectV2Field?.options
    if (!updatedOptions) throw new Error('Failed to update status options. Unexpected response.')

    // Map returned option IDs to config keys
    statusMap = {}
    for (const key of labelKeys) {
      const match = updatedOptions.find(o => o.name === labels[key])
      if (!match) throw new Error(`Could not find option ID for status: ${labels[key]}`)
      statusMap[key] = match.id
    }

    statusSpin.stop('Status options configured')
  } catch (e) {
    statusSpin.stop('Failed to configure status options')
    p.log.error(e.message)
    process.exit(1)
  }

  // Step 4.5: Create custom date fields
  const dateFieldSpin = p.spinner()
  dateFieldSpin.start('Creating custom date fields...')
  let createdFieldId, lastActiveFieldId
  try {
    const dateFieldMutation = `
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

    const createdRes = ghGraphql(dateFieldMutation, { projectId, name: 'Session Created' })
    createdFieldId = createdRes.data?.createProjectV2Field?.projectV2Field?.id
    if (!createdFieldId) {
      const errMsg = createdRes.errors?.map(e => e.message).join(', ') || JSON.stringify(createdRes)
      throw new Error(`Failed to create "Created" date field: ${errMsg}`)
    }

    const lastActiveRes = ghGraphql(dateFieldMutation, { projectId, name: 'Last Active' })
    lastActiveFieldId = lastActiveRes.data?.createProjectV2Field?.projectV2Field?.id
    if (!lastActiveFieldId) {
      const errMsg = lastActiveRes.errors?.map(e => e.message).join(', ') || JSON.stringify(lastActiveRes)
      throw new Error(`Failed to create "Last Active" date field: ${errMsg}`)
    }

    dateFieldSpin.stop('Custom date fields created')
  } catch (e) {
    dateFieldSpin.stop('Skipped custom date fields (non-critical)')
    p.log.warn(`Date fields could not be created: ${e.message}\n  This is optional — setup will continue without them.`)
  }

  // Step 5: Install hooks
  const installSpin = p.spinner()
  installSpin.start('Installing hooks...')
  try {
    installHooksAndConfig({
      owner: username,
      projectNumber,
      projectId,
      statusFieldId: statusField.id,
      statusMap,
      notesRepo: repoFullName,
      timeoutMinutes: 30,
      scope: 'global',
      createdFieldId,
      lastActiveFieldId,
      lang,
      projectNameMode,
    })
    installSpin.stop('Hooks installed')
  } catch (e) {
    installSpin.stop('Failed to install hooks')
    p.log.error(e.message)
    process.exit(1)
  }

  const projectUrl = `https://github.com/users/${username}/projects/${projectNumber}`

  p.note([
    'Everything is all set! Here\'s what to do next:',
    '',
    '  1. Start Claude Code and have any conversation',
    `  2. Check your project board at: ${projectUrl}`,
    '',
    '  Session issues are stored in:',
    `     https://github.com/${repoFullName}`,
  ].join('\n'), 'You\'re ready to go!')

  p.outro(`Run Claude Code and start a conversation — then check ${projectUrl}`)
}

// -- Manual Setup -------------------------------------------------------------

async function manualSetup(username) {
  const owner = await p.text({
    message: 'GitHub Project Owner (username or org)',
    initialValue: username,
    validate: v => !v?.trim() ? 'This field is required.' : undefined,
  })
  if (p.isCancel(owner)) onCancel()
  const ownerVal = owner.trim()

  p.log.info(`If you don't have a project yet, no worries! Create one at: https://github.com/${ownerVal}?tab=projects`)

  const number = await p.text({
    message: 'Project number',
    placeholder: '1',
    validate: v => !v || isNaN(Number(v)) ? 'Please enter a number.' : undefined,
  })
  if (p.isCancel(number)) onCancel()
  const projectNumber = Number(number)

  // Fetch project metadata
  const fetchSpin = p.spinner()
  fetchSpin.start('Fetching project metadata...')
  let projectId, statusField, projectTitle, projectUrl
  try {
    const meta = fetchProjectMetadata(ownerVal, projectNumber)
    projectId = meta.projectId
    statusField = meta.statusField
    projectTitle = meta.projectTitle
    projectUrl = meta.projectUrl
    fetchSpin.stop(`Found project: ${projectTitle}`)
  } catch (e) {
    fetchSpin.stop('Failed to fetch project')
    p.log.error(e.message)
    process.exit(1)
  }

  const statusOptions = statusField.options.map(o => `${o.name}`).join(', ')
  p.note([
    `  Name    : ${projectTitle}`,
    `  URL     : ${projectUrl}`,
    `  ID      : ${projectId}`,
    `  Statuses: ${statusOptions}`,
  ].join('\n'), 'Project details')

  const rightProject = await p.confirm({ message: 'Is this the right project?' })
  if (p.isCancel(rightProject) || !rightProject) onCancel()

  // Map each lifecycle stage to a status option
  p.log.info('Map each Claude Code lifecycle stage to a Status option below. You can always change these later in ~/.claude/hooks/config.env')

  const choices = statusField.options.map(o => ({ value: o.id, label: o.name }))

  const registered = await p.select({ message: 'Session started       ->', options: choices })
  if (p.isCancel(registered)) onCancel()

  const responding = await p.select({ message: 'Claude is responding  ->', options: choices })
  if (p.isCancel(responding)) onCancel()

  const waiting = await p.select({ message: 'Waiting for user      ->', options: choices })
  if (p.isCancel(waiting)) onCancel()

  const closed = await p.select({ message: 'Session ended         ->', options: choices })
  if (p.isCancel(closed)) onCancel()

  const statusMap = { registered, responding, waiting, closed }

  const notesRepo = await p.text({
    message: 'Repository for session issues (when no git remote is available)',
    placeholder: `${ownerVal}/dev-notes`,
    validate: v => !v?.includes('/') ? 'Please use owner/repo format.' : undefined,
  })
  if (p.isCancel(notesRepo)) onCancel()
  const notesRepoValue = notesRepo.trim()

  const contextRepoExample = getContextRepoExample(notesRepoValue)
  const displayExamples = getProjectNameDisplayExamples(contextRepoExample)
  p.note([
    'Choose how the active project name should appear on each issue.',
    `  Context source: Current workspace repo if available, otherwise ${notesRepoValue}`,
    '',
    '  Prefix in issue title',
    `  Issue title: ${displayExamples.prefixTitle}`,
    '  Labels     : claude-code',
    '',
    '  Label in GitHub Projects',
    `  Issue title: ${displayExamples.labelTitle}`,
    `  Labels     : claude-code, ${displayExamples.labelName}`,
  ].join('\n'), 'Project name display')

  const projectNameMode = await p.select({
    message: 'How should the project name be shown?',
    options: [
      { value: 'prefix', label: 'Prefix in issue title', hint: 'Shows [owner/repo] before the latest prompt' },
      { value: 'label', label: 'Label in GitHub Projects', hint: 'Keeps the title clean and stores owner/repo as a label' },
    ],
  })
  if (p.isCancel(projectNameMode)) onCancel()

  const timeout = await p.text({
    message: 'Session close timer (minutes)',
    initialValue: '30',
    validate: v => !v || isNaN(Number(v)) ? 'Please enter a number.' : undefined,
  })
  if (p.isCancel(timeout)) onCancel()

  const scope = await p.select({
    message: 'Hook scope',
    options: [
      { value: 'global',  label: 'Global              (~/.claude/settings.json)' },
      { value: 'project', label: 'Current project     (.claude/settings.json)' },
    ],
  })
  if (p.isCancel(scope)) onCancel()

  const langManual = await p.select({
    message: 'Which language for issue comments?',
    options: [
      { value: 'en', label: 'English', hint: 'Prompt, Response' },
      { value: 'ko', label: 'Korean',  hint: '프롬프트, 답변' },
      { value: 'ja', label: 'Japanese', hint: 'プロンプト, 回答' },
      { value: 'zh', label: 'Chinese', hint: '提示词, 回答' },
    ],
  })
  if (p.isCancel(langManual)) onCancel()

  // Confirm summary
  const scopeLabel = scope === 'global' ? 'Global' : 'Current project'
  p.note([
    `  Project    : ${projectTitle} (#${projectNumber})`,
    `  Notes Repo : ${notesRepoValue}`,
    `  Display    : ${projectNameMode === 'prefix' ? 'Prefix in issue title' : 'Label in GitHub Projects'}`,
    `  Timeout    : ${timeout} min`,
    `  Scope      : ${scopeLabel}`,
  ].join('\n'), 'Setup summary')

  const confirmed = await p.confirm({ message: 'Ready to install?' })
  if (p.isCancel(confirmed) || !confirmed) onCancel()

  // Install
  const installSpin = p.spinner()
  installSpin.start('Installing hooks...')
  try {
    installHooksAndConfig({
      owner: ownerVal,
      projectNumber,
      projectId,
      statusFieldId: statusField.id,
      statusMap,
      notesRepo: notesRepoValue,
      timeoutMinutes: Number(timeout),
      scope,
      lang: langManual,
      projectNameMode,
    })
    installSpin.stop('Hooks installed')
  } catch (e) {
    installSpin.stop('Failed to install hooks')
    p.log.error(e.message)
    process.exit(1)
  }

  p.note([
    'Everything is all set! Here\'s what to do next:',
    '',
    '  1. Start Claude Code and have any conversation',
    `  2. Check your project board at: ${projectUrl}`,
    '',
    '  Session issues are stored in:',
    `     https://github.com/${notesRepoValue}`,
  ].join('\n'), 'You\'re ready to go!')

  p.outro(`Run Claude Code and start a conversation — then check ${projectUrl}`)
}

// -- Main ---------------------------------------------------------------------

async function main() {
  if (process.argv.includes('uninstall')) return uninstall()

  console.clear()
  p.intro(' Claude Session Tracker — Setup ')

  // Environment check
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
    // 미인증 → 설치 플로우 안에서 로그인 유도
    envSpin.stop('GitHub authentication required')
    p.log.warn('GitHub authentication is required. Starting login...')

    try {
      await runGhAuthLogin()
      p.log.success('GitHub login successful')
    } catch (err) {
      p.log.error(err.message)
      p.outro('Setup aborted.')
      process.exit(1)
    }
  } else if (!hasRequiredScopes()) {
    // 인증됨 + 스코프 부족 → 스코프 보충
    envSpin.stop('Missing required GitHub scopes')
    p.log.warn('The scopes project and repo are required. Adding them now.')

    try {
      await runGhAuthRefresh()
      p.log.success('Scopes added successfully')
    } catch (err) {
      p.log.error(err.message)
      p.outro('Setup aborted.')
      process.exit(1)
    }
  }
  envSpin.stop('Environment looks good')

  // Detect authenticated user
  const username = getAuthenticatedUser()
  if (!username) {
    p.log.error('Could not detect your GitHub username. Please make sure `gh auth login` is completed.')
    p.outro('Setup aborted.')
    process.exit(1)
  }

  p.log.message(`Hey ${username}! Let's set up session tracking for Claude Code.`)

  // Detect existing installation
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

  // Choose setup mode
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

main().catch(e => { console.error(e.message); process.exit(1) })
