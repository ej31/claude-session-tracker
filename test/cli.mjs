#!/usr/bin/env node
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, unlinkSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const repoRoot = join(__dirname, '..')
const cliPath = join(repoRoot, 'bin', 'index.mjs')
const sessionStartPath = join(repoRoot, 'hooks', 'cst_session_start.py')

let pass = 0
let fail = 0

function assertOk(name, condition) {
  if (condition) {
    console.log(`  ✓ ${name}`)
    pass++
  } else {
    console.error(`  ✗ ${name}`)
    fail++
  }
}

function makeExecutable(path, content) {
  writeFileSync(path, content)
  chmodSync(path, 0o755)
}

function createGhStub(binDir) {
  const ghPath = join(binDir, 'gh')
  makeExecutable(ghPath, `#!/usr/bin/env node
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
const args = process.argv.slice(2)
const stdin = readFileSync(0, 'utf8')
const statePath = process.env.GH_STUB_STATE
const readState = () => existsSync(statePath) ? JSON.parse(readFileSync(statePath, 'utf8')) : {}
const writeState = (value) => writeFileSync(statePath, JSON.stringify(value, null, 2))
const respond = (value = '') => process.stdout.write(value)

if (args[0] === 'auth' && args[1] === 'status') {
  respond("github.com\\n  ✓ Logged in to github.com account stubuser\\n  - Token scopes: 'project', 'repo'\\n")
  process.exit(0)
}

if (args[0] === 'api' && args[1] === 'user') {
  respond('stubuser\\n')
  process.exit(0)
}

if (args[0] === 'api' && args[1] && args[1].startsWith('repos/')) {
  if (args.includes('POST') && args[1].endsWith('/issues')) {
    const state = readState()
    state.createdIssues = state.createdIssues || []
    state.createdIssues.push({ path: args[1] })
    writeState(state)
    respond('"I_node123"\\n42\\n')
    process.exit(0)
  }
  if (args.includes('PATCH')) {
    const state = readState()
    state.titleUpdates = state.titleUpdates || []
    state.titleUpdates.push({ path: args[1] })
    writeState(state)
    process.exit(0)
  }
  respond((process.env.GH_STUB_REPO_PRIVATE ?? 'true') + '\\n')
  process.exit(0)
}

if (args[0] === 'repo' && args[1] === 'create') {
  process.exit(0)
}

if (args[0] === 'project' && args[1] === 'create') {
  process.exit(0)
}

if (args[0] === 'project' && args[1] === 'list') {
  respond(JSON.stringify({ projects: [{ title: "stubuser's Claude Session Storage", number: 1 }] }))
  process.exit(0)
}

if (args[0] === 'repo' && args[1] === 'delete') {
  process.exit(0)
}

if (args[0] === 'issue' && args[1] === 'close') {
  const state = readState()
  state.closedIssues = state.closedIssues || []
  state.closedIssues.push({ number: args[2], repo: args[args.indexOf('--repo') + 1] || '' })
  writeState(state)
  process.exit(0)
}

if (args[0] === 'issue' && args[1] === 'comment') {
  const state = readState()
  state.comments = state.comments || []
  state.comments.push({ number: args[2] })
  writeState(state)
  process.exit(0)
}

if (args[0] === 'label' && args[1] === 'list') {
  respond('[]\\n')
  process.exit(0)
}

if (args[0] === 'label' && args[1] === 'create') {
  process.exit(0)
}

if (args[0] === 'api' && args[1] === 'graphql') {
  const payload = JSON.parse(stdin || '{}')
  const query = payload.query || ''
  const state = readState()

  if (query.includes('projectV2(number: $number)')) {
    respond(JSON.stringify({
      data: {
        user: {
          projectV2: {
            id: 'PVT_project',
            title: 'Test Project',
            url: 'https://github.com/users/tester/projects/1',
            fields: {
              nodes: [{
                id: 'PVTSSF_status',
                name: 'Status',
                options: [
                  { id: 'opt_reg', name: 'Registered' },
                  { id: 'opt_resp', name: 'Responding' },
                  { id: 'opt_wait', name: 'Waiting' },
                  { id: 'opt_closed', name: 'Closed' },
                ],
              }],
            },
          },
        },
        organization: null,
      },
    }))
    process.exit(0)
  }

  if (query.includes('updateProjectV2(input:')) {
    if (payload.variables.readme !== undefined) {
      state.projectReadme = payload.variables.readme
    }
    if (payload.variables.shortDescription !== undefined) {
      state.projectDescription = payload.variables.shortDescription
    }
    writeState(state)
    respond(JSON.stringify({
      data: {
        updateProjectV2: {
          projectV2: {
            id: payload.variables.projectId,
            readme: payload.variables.readme,
            shortDescription: payload.variables.shortDescription,
          },
        },
      },
    }))
    process.exit(0)
  }

  if (query.includes('createProjectV2StatusUpdate')) {
    state.statusUpdates = state.statusUpdates || []
    const nextId = 'PSU_' + (state.statusUpdates.length + 1)
    state.statusUpdateId = nextId
    state.boardStatus = payload.variables.status
    state.statusUpdates.push({
      id: nextId,
      status: payload.variables.status,
      updatedAt: '2026-03-12T00:00:00Z',
      body: payload.variables.body,
    })
    writeState(state)
    respond(JSON.stringify({
      data: {
        createProjectV2StatusUpdate: {
          statusUpdate: {
            id: nextId,
            status: payload.variables.status,
            updatedAt: '2026-03-12T00:00:00Z',
            body: payload.variables.body,
          },
        },
      },
    }))
    process.exit(0)
  }

  if (query.includes('updateProjectV2StatusUpdate')) {
    state.statusUpdates = state.statusUpdates || []
    state.statusUpdateId = payload.variables.statusUpdateId || state.statusUpdateId || 'PSU_1'
    state.boardStatus = payload.variables.status
    state.statusUpdates = state.statusUpdates.map((entry) => {
      if (entry.id !== state.statusUpdateId) return entry
      return {
        ...entry,
        status: payload.variables.status,
        body: payload.variables.body,
        updatedAt: '2026-03-12T00:00:00Z',
      }
    })
    writeState(state)
    respond(JSON.stringify({
      data: {
        updateProjectV2StatusUpdate: {
          statusUpdate: {
            id: state.statusUpdateId,
            status: payload.variables.status,
            updatedAt: '2026-03-12T00:00:00Z',
            body: payload.variables.body,
          },
        },
      },
    }))
    process.exit(0)
  }

  if (query.includes('statusUpdates(first: 20')) {
    const boardStatus = process.env.GH_STUB_BOARD_STATUS || state.boardStatus || ''
    const nodes = state.statusUpdates?.length
      ? [...state.statusUpdates].reverse()
      : boardStatus
        ? [{
            id: 'PSU_REMOTE',
            body: '<!-- claude-session-tracker:project-status -->\\n**Tracker state:** paused',
            status: boardStatus || 'INACTIVE',
            updatedAt: '2026-03-12T00:00:00Z',
          }]
        : []
    respond(JSON.stringify({ data: { node: { statusUpdates: { nodes } } } }))
    process.exit(0)
  }

  if (query.includes('updateProjectV2Field(input:')) {
    respond(JSON.stringify({
      data: {
        updateProjectV2Field: {
          projectV2Field: {
            options: [
              { id: 'opt_reg', name: 'Registered' },
              { id: 'opt_resp', name: 'Responding' },
              { id: 'opt_wait', name: 'Waiting' },
              { id: 'opt_closed', name: 'Closed' },
            ],
          },
        },
      },
    }))
    process.exit(0)
  }

  if (query.includes('createProjectV2Field(input:')) {
    respond(JSON.stringify({
      data: {
        createProjectV2Field: {
          projectV2Field: {
            id: 'PF_' + (payload.variables.name || '').replace(/\\s/g, '_'),
            name: payload.variables.name,
          },
        },
      },
    }))
    process.exit(0)
  }

  if (query.includes('repository(owner:') && query.includes('{ id }')) {
    respond(JSON.stringify({ data: { repository: { id: 'R_repo123' } } }))
    process.exit(0)
  }

  if (query.includes('linkProjectV2ToRepository')) {
    state.repoLinked = {
      projectId: payload.variables.projectId,
      repositoryId: payload.variables.repositoryId,
    }
    writeState(state)
    respond(JSON.stringify({ data: { linkProjectV2ToRepository: { repository: { id: payload.variables.repositoryId } } } }))
    process.exit(0)
  }

  if (query.includes('addProjectV2ItemById')) {
    respond(JSON.stringify({ data: { addProjectV2ItemById: { item: { id: 'PVTI_new' } } } }))
    process.exit(0)
  }

  if (query.includes('updateProjectV2ItemFieldValue')) {
    respond(JSON.stringify({ data: { updateProjectV2ItemFieldValue: { projectV2Item: { id: payload.variables.itemId } } } }))
    process.exit(0)
  }

  if (query.includes('updateProjectV2') && query.includes('closed')) {
    respond(JSON.stringify({ data: { updateProjectV2: { projectV2: { id: payload.variables.projectId } } } }))
    process.exit(0)
  }

  respond(JSON.stringify({ data: {} }))
  process.exit(0)
}

if (args[0] === 'repo' && args[1] === 'edit') {
  process.exit(0)
}

if (args[0] === 'repo' && args[1] === 'archive') {
  process.exit(0)
}

process.stderr.write(\`Unhandled gh args: \${args.join(' ')}\\n\`)
process.exit(1)
`)
  return ghPath
}

function createTestEnv() {
  const root = mkdtempSync(join(tmpdir(), 'cst-cli-'))
  const home = join(root, 'home')
  const binDir = join(root, 'bin')
  const workspace = join(root, 'workspace')
  const hooksDir = join(home, '.claude', 'hooks')
  const stateDir = join(hooksDir, 'state')
  const ghStatePath = join(root, 'gh-state.json')

  mkdirSync(binDir, { recursive: true })
  mkdirSync(workspace, { recursive: true })
  mkdirSync(stateDir, { recursive: true })

  createGhStub(binDir)
  writeFileSync(ghStatePath, '{}\n')

  return { root, home, binDir, workspace, hooksDir, stateDir, ghStatePath }
}

function writeTrackerInstall({ home, hooksDir, workspace, notesRepo = 'tester/private-notes' }) {
  mkdirSync(join(home, '.claude'), { recursive: true })
  const hookCommands = {
    hooks: {
      SessionStart: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'cst_session_start.py')}` }],
      }],
    },
  }
  writeFileSync(join(home, '.claude', 'settings.json'), JSON.stringify(hookCommands, null, 2))
  mkdirSync(join(workspace, '.claude'), { recursive: true })
  writeFileSync(join(workspace, '.claude', 'settings.json'), JSON.stringify({ hooks: {} }, null, 2))

  for (const file of [
    'cst_github_utils.py',
    'cst_hash_chain.py',
    'cst_session_start.py',
    'cst_prompt_to_github_projects.py',
    'cst_session_stop.py',
    'cst_mark_done.py',
    'cst_post_tool_use.py',
    'cst_session_end.py',
  ]) {
    writeFileSync(join(hooksDir, file), '# stub\n')
  }

  writeFileSync(join(hooksDir, 'config.env'), [
    'GITHUB_PROJECT_OWNER=tester',
    'GITHUB_PROJECT_NUMBER=1',
    'GITHUB_PROJECT_ID=PVT_project',
    'GITHUB_STATUS_FIELD_ID=PVTSSF_status',
    'GITHUB_STATUS_REGISTERED=opt_reg',
    'GITHUB_STATUS_RESPONDING=opt_resp',
    'GITHUB_STATUS_WAITING=opt_wait',
    'GITHUB_STATUS_CLOSED=opt_closed',
    `NOTES_REPO=${notesRepo}`,
    'DONE_TIMEOUT_SECS=1800',
    'CST_LANG=en',
  ].join('\n') + '\n')
}

function runNode(args, { cwd, home, binDir, ghStatePath, extraEnv = {} }) {
  return spawnSync('node', [cliPath, ...args], {
    cwd,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: home,
      PATH: `${binDir}:${process.env.PATH}`,
      GH_STUB_STATE: ghStatePath,
      ...extraEnv,
    },
  })
}

function runPythonHook({ cwd, home, binDir, ghStatePath, stdin, extraEnv = {}, scriptPath = sessionStartPath }) {
  return spawnSync('python3', [scriptPath], {
    cwd,
    encoding: 'utf-8',
    input: stdin,
    env: {
      ...process.env,
      HOME: home,
      PATH: `${binDir}:${process.env.PATH}`,
      GH_STUB_STATE: ghStatePath,
      ...extraEnv,
    },
  })
}

function testStatusOutput() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  writeFileSync(join(env.stateDir, 'session-1.json'), JSON.stringify({
    session_id: 'session-1',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 42,
    item_id: 'ITEM_1',
    status: 'waiting',
    tracking_paused: true,
    project_status_sync: {
      status: 'INACTIVE',
      success: false,
      error: 'simulated sync error',
    },
  }, null, 2))

  writeFileSync(join(env.hooksDir, 'project_status_update.json'), JSON.stringify({
    project_id: 'PVT_project',
    status_update_id: 'PSU_1',
    last_status: 'INACTIVE',
    last_synced_at: '2026-03-12T00:00:00Z',
  }, null, 2))

  writeFileSync(join(env.hooksDir, 'runtime_status.json'), JSON.stringify({
    status: 'blocked',
    reason: 'notes_repo_public',
    repo: 'tester/private-notes',
  }, null, 2))

  const result = runNode(['status'], { ...env, cwd: env.workspace })
  assert.equal(result.status, 0)
  assertOk('status shows installed state', result.stdout.includes('Installed and active'))
  assertOk('status shows paused session', result.stdout.includes('paused'))
  assertOk('status shows board sync error', result.stdout.includes('simulated sync error'))
  assertOk('status shows runtime block detail', result.stdout.includes('tester/private-notes is public'))
}

function testDoctorPublicRepoFailure() {
  const env = createTestEnv()
  writeTrackerInstall({ ...env, notesRepo: 'tester/public-notes' })

  const result = runNode(['doctor'], {
    ...env,
    extraEnv: { GH_STUB_REPO_PRIVATE: 'false' },
  })

  assert.equal(result.status, 1)
  assertOk('doctor fails on public notes repo', result.stdout.includes('[FAIL] NOTES_REPO visibility: tester/public-notes is public'))
  assertOk('doctor summary reports action needed', result.stdout.includes('Doctor summary: action needed'))
}

function testPauseResumeLifecycle() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  const statePath = join(env.stateDir, 'session-2.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-2',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 99,
    item_id: 'ITEM_99',
    status: 'waiting',
  }, null, 2))

  const pauseResult = runNode(['pause'], { ...env, cwd: env.workspace })
  assert.equal(pauseResult.status, 0)
  const pausedState = JSON.parse(readFileSync(statePath, 'utf-8'))
  const pauseCache = JSON.parse(readFileSync(join(env.hooksDir, 'project_status_update.json'), 'utf-8'))
  const ghStateAfterPause = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('pause sets tracking_paused', pausedState.tracking_paused === true)
  assertOk('pause stores INACTIVE cache', pauseCache.last_status === 'INACTIVE')
  assertOk('pause writes one status update history entry', ghStateAfterPause.statusUpdates.length === 1)
  assertOk('pause history body includes session id', ghStateAfterPause.statusUpdates[0].body.includes('**Session ID:** session-2'))
  assertOk('pause history body includes workspace path', ghStateAfterPause.statusUpdates[0].body.includes(`**Workspace:** ${env.workspace}`))

  const resumeResult = runNode(['resume'], { ...env, cwd: env.workspace })
  assert.equal(resumeResult.status, 0)
  const resumedState = JSON.parse(readFileSync(statePath, 'utf-8'))
  const resumeCache = JSON.parse(readFileSync(join(env.hooksDir, 'project_status_update.json'), 'utf-8'))
  const ghStateAfterResume = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('resume clears tracking_paused', !('tracking_paused' in resumedState))
  assertOk('resume stores ON_TRACK cache', resumeCache.last_status === 'ON_TRACK')
  assertOk('resume appends another history entry', ghStateAfterResume.statusUpdates.length === 2)
  assertOk('pause and resume use different status update ids', ghStateAfterResume.statusUpdates[0].id !== ghStateAfterResume.statusUpdates[1].id)
  assertOk('latest history entry is ON_TRACK', ghStateAfterResume.statusUpdates.at(-1).status === 'ON_TRACK')
}

function testInstallHelperConfiguresReadmeAndOnTrack() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const tempModule = join(repoRoot, 'bin', `.tmp-install-recheck-${Date.now()}-${Math.random().toString(16).slice(2)}.mjs`)
  const source = readFileSync(cliPath, 'utf-8').replace(
    /\nmain\(\)\.catch\(\(error\) => \{\n  console\.error\(error\.message\)\n  process\.exit\(1\)\n\}\)\s*$/,
    '',
  ) + '\nif (process.env.CST_TEST_INSTALL_HELPERS === "1") {\n  ensureProjectReadmeAfterInstall(process.env.CST_PROJECT_ID)\n  ensureProjectOnTrackAfterInstall(process.env.CST_PROJECT_ID, process.cwd())\n}\n'
  writeFileSync(tempModule, source)

  const result = spawnSync('node', [tempModule], {
    cwd: env.workspace,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${env.binDir}:${process.env.PATH}`,
      GH_STUB_STATE: env.ghStatePath,
      CST_TEST_INSTALL_HELPERS: '1',
      CST_PROJECT_ID: 'PVT_project',
    },
  })
  unlinkSync(tempModule)

  if (result.status !== 0) {
    console.error('  [DEBUG] install helper stderr:', result.stderr)
    console.error('  [DEBUG] install helper stdout:', result.stdout)
  }
  assert.equal(result.status, 0)
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('install helper writes project readme', ghState.projectReadme?.includes('Claude Session Tracker'))
  assertOk('install helper creates ON_TRACK entry', ghState.statusUpdates.length === 1 && ghState.statusUpdates[0].status === 'ON_TRACK')
}

function testSessionStartBlocksPublicRepo() {
  const env = createTestEnv()
  writeTrackerInstall({ ...env, notesRepo: 'tester/public-notes' })
  const transcriptPath = join(env.root, 'transcript.jsonl')
  writeFileSync(transcriptPath, '')

  const result = runPythonHook({
    ...env,
    stdin: JSON.stringify({
      session_id: 'session-public',
      cwd: env.workspace,
      transcript_path: transcriptPath,
    }),
    extraEnv: { GH_STUB_REPO_PRIVATE: 'false' },
  })

  assert.equal(result.status, 0)
  assertOk('session_start prints public repo block', result.stdout.includes('Tracking is disabled because tester/public-notes is public'))
  assertOk('session_start does not create session state', !existsSync(join(env.stateDir, 'session-public.json')))
  const runtimeStatus = JSON.parse(readFileSync(join(env.hooksDir, 'runtime_status.json'), 'utf-8'))
  assertOk('session_start records runtime block reason', runtimeStatus.reason === 'notes_repo_public')
}

function testSessionStartBlocksOffTrackBoard() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const transcriptPath = join(env.root, 'transcript-off-track.jsonl')
  writeFileSync(transcriptPath, '')

  const result = runPythonHook({
    ...env,
    stdin: JSON.stringify({
      session_id: 'session-off-track',
      cwd: env.workspace,
      transcript_path: transcriptPath,
    }),
    extraEnv: { GH_STUB_BOARD_STATUS: 'INACTIVE' },
  })

  assert.equal(result.status, 0)
  assertOk('session_start prints INACTIVE block', result.stdout.includes('project board is currently INACTIVE'))
  assertOk('session_start skips state creation when board is INACTIVE', !existsSync(join(env.stateDir, 'session-off-track.json')))
  const runtimeStatus = JSON.parse(readFileSync(join(env.hooksDir, 'runtime_status.json'), 'utf-8'))
  assertOk('session_start records project_inactive reason', runtimeStatus.reason === 'project_inactive')
}

function testPromptSkipsWhenBoardOffTrack() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const statePath = join(env.stateDir, 'session-board-off-track.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-board-off-track',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 7,
    item_id: 'ITEM_7',
    status: 'waiting',
  }, null, 2))

  const promptHookPath = join(repoRoot, 'hooks', 'cst_prompt_to_github_projects.py')
  const result = spawnSync('python3', [promptHookPath], {
    cwd: env.workspace,
    encoding: 'utf-8',
    input: JSON.stringify({
      session_id: 'session-board-off-track',
      prompt: 'do not persist me',
    }),
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${env.binDir}:${process.env.PATH}`,
      GH_STUB_STATE: env.ghStatePath,
      GH_STUB_BOARD_STATUS: 'INACTIVE',
    },
  })

  assert.equal(result.status, 0)
  const state = JSON.parse(readFileSync(statePath, 'utf-8'))
  assertOk('prompt keeps prior status when board is INACTIVE', state.status === 'waiting')
  const runtimeStatus = JSON.parse(readFileSync(join(env.hooksDir, 'runtime_status.json'), 'utf-8'))
  assertOk('prompt records project_inactive runtime status', runtimeStatus.reason === 'project_inactive')
}

function testSessionEndClosesIssue() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const sessionEndPath = join(repoRoot, 'hooks', 'cst_session_end.py')

  // waiting 상태의 세션 생성
  const statePath = join(env.stateDir, 'session-end-test.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-end-test',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 99,
    item_id: 'ITEM_END',
    status: 'waiting',
  }, null, 2))

  const result = spawnSync('python3', [sessionEndPath], {
    cwd: env.workspace,
    encoding: 'utf-8',
    input: JSON.stringify({ session_id: 'session-end-test' }),
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${env.binDir}:${process.env.PATH}`,
      GH_STUB_STATE: env.ghStatePath,
    },
  })

  assert.equal(result.status, 0)
  const state = JSON.parse(readFileSync(statePath, 'utf-8'))
  assertOk('session_end sets status to closed', state.status === 'closed')
  assertOk('session_end clears timer_pid', !state.timer_pid)

  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('session_end closes GitHub issue', ghState.closedIssues?.some(i => i.number === '99'))
}

function testSessionEndSkipsAlreadyClosed() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const sessionEndPath = join(repoRoot, 'hooks', 'cst_session_end.py')

  const statePath = join(env.stateDir, 'session-already-closed.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-already-closed',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 100,
    item_id: 'ITEM_CLOSED',
    status: 'closed',
  }, null, 2))

  const result = spawnSync('python3', [sessionEndPath], {
    cwd: env.workspace,
    encoding: 'utf-8',
    input: JSON.stringify({ session_id: 'session-already-closed' }),
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${env.binDir}:${process.env.PATH}`,
      GH_STUB_STATE: env.ghStatePath,
    },
  })

  assert.equal(result.status, 0)
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('session_end skips close for already-closed session', !ghState.closedIssues?.length)
}

function testMarkDoneClosesIssue() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const markDonePath = join(repoRoot, 'hooks', 'cst_mark_done.py')

  const statePath = join(env.stateDir, 'session-mark-done.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-mark-done',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 77,
    item_id: 'ITEM_DONE',
    status: 'waiting',
  }, null, 2))

  // DONE_TIMEOUT_SECS=0으로 즉시 만료
  const result = spawnSync('python3', [markDonePath, 'session-mark-done'], {
    cwd: env.workspace,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${env.binDir}:${process.env.PATH}`,
      GH_STUB_STATE: env.ghStatePath,
      DONE_TIMEOUT_SECS: '0',
    },
  })

  assert.equal(result.status, 0)
  const state = JSON.parse(readFileSync(statePath, 'utf-8'))
  assertOk('mark_done sets status to closed', state.status === 'closed')

  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('mark_done closes GitHub issue', ghState.closedIssues?.some(i => i.number === '77'))
}

function testCleanupStaleSessions() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  // stale 세션 생성 (파일 mtime을 과거로 설정)
  const stalePath = join(env.stateDir, 'session-stale.json')
  writeFileSync(stalePath, JSON.stringify({
    session_id: 'session-stale',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 55,
    item_id: 'ITEM_STALE',
    status: 'waiting',
  }, null, 2))
  // mtime을 2시간 전으로 설정
  const past = new Date(Date.now() - 7200 * 1000)
  utimesSync(stalePath, past, past)

  // 이미 closed인 세션 (정리 대상 아님)
  const closedPath = join(env.stateDir, 'session-already-done.json')
  writeFileSync(closedPath, JSON.stringify({
    session_id: 'session-already-done',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 56,
    item_id: 'ITEM_DONE2',
    status: 'closed',
  }, null, 2))
  utimesSync(closedPath, past, past)

  // 새 세션 시작으로 cleanup 트리거
  const transcriptPath = join(env.root, 'transcript-cleanup.jsonl')
  writeFileSync(transcriptPath, '')

  const result = runPythonHook({
    ...env,
    stdin: JSON.stringify({
      session_id: 'session-new-after-cleanup',
      cwd: env.workspace,
      transcript_path: transcriptPath,
    }),
  })

  assert.equal(result.status, 0)

  // stale 세션이 closed로 변경되었는지 확인
  const staleState = JSON.parse(readFileSync(stalePath, 'utf-8'))
  assertOk('cleanup marks stale session as closed', staleState.status === 'closed')

  // stale 세션의 issue가 close되었는지 확인
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('cleanup closes stale session issue', ghState.closedIssues?.some(i => i.number === '55'))

  // 이미 closed인 세션은 건드리지 않음
  assertOk('cleanup skips already-closed session', !ghState.closedIssues?.some(i => i.number === '56'))
}

function testVersionFlag() {
  const env = createTestEnv()
  const result = runNode(['--version'], { ...env, cwd: env.workspace })
  assert.equal(result.status, 0)
  const version = result.stdout.trim()
  assertOk('--version prints valid semver', /^\d+\.\d+\.\d+/.test(version))

  const resultV = runNode(['-v'], { ...env, cwd: env.workspace })
  assert.equal(resultV.status, 0)
  assertOk('-v prints same version', resultV.stdout.trim() === version)
}

function testPauseResumeWithoutSession() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  // 세션 파일 없이 pause 실행 (다른 cwd에서)
  const pauseResult = runNode(['pause'], { ...env, cwd: env.root })
  assert.equal(pauseResult.status, 0)
  assertOk('pause without session syncs board', pauseResult.stdout.includes('Project board marked INACTIVE'))

  const pauseCache = JSON.parse(readFileSync(join(env.hooksDir, 'project_status_update.json'), 'utf-8'))
  assertOk('pause without session stores INACTIVE cache', pauseCache.last_status === 'INACTIVE')

  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('pause without session creates status update', ghState.statusUpdates.length === 1)
  assertOk('pause without session body shows no active session', ghState.statusUpdates[0].body.includes('(no active session)'))

  // 세션 파일 없이 resume 실행
  const resumeResult = runNode(['resume'], { ...env, cwd: env.root })
  assert.equal(resumeResult.status, 0)
  assertOk('resume without session syncs board', resumeResult.stdout.includes('Project board marked ON_TRACK'))
}

// -- Non-interactive mode tests -----------------------------------------------

function createNoAuthGhStub(binDir) {
  makeExecutable(join(binDir, 'gh'), `#!/usr/bin/env node
const args = process.argv.slice(2)
if (args[0] === 'auth' && args[1] === 'status') {
  process.stderr.write('not logged in\\n')
  process.exit(1)
}
process.exit(1)
`)
}

function createBadTokenGhStub(binDir) {
  makeExecutable(join(binDir, 'gh'), `#!/usr/bin/env node
const args = process.argv.slice(2)
if (args[0] === 'auth' && args[1] === 'status') {
  process.stderr.write('no scopes\\n')
  process.exit(0)
}
if (args[0] === 'api' && args[1] === 'user') {
  process.stderr.write('Bad credentials\\n')
  process.exit(1)
}
process.exit(1)
`)
}

function testNonInteractiveFailsWithoutToken() {
  const env = createTestEnv()
  const stubDir = join(env.root, 'noauth-bin')
  mkdirSync(stubDir, { recursive: true })
  createNoAuthGhStub(stubDir)

  const result = spawnSync('node', [cliPath, '--yes'], {
    cwd: env.workspace,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${stubDir}:${process.env.PATH}`,
      GH_TOKEN: '',
      GITHUB_TOKEN: '',
    },
  })

  assert.equal(result.status, 2)
  assertOk('non-interactive fails without token (exit 2)', result.stderr.includes('No GitHub authentication found'))
}

function testNonInteractiveFailsWithInvalidToken() {
  const env = createTestEnv()
  const stubDir = join(env.root, 'badtoken-bin')
  mkdirSync(stubDir, { recursive: true })
  createBadTokenGhStub(stubDir)

  const result = spawnSync('node', [cliPath, '--yes', '--token', 'ghp_invalid'], {
    cwd: env.workspace,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${stubDir}:${process.env.PATH}`,
      GH_TOKEN: '',
      GITHUB_TOKEN: '',
    },
  })

  assert.equal(result.status, 3)
  assertOk('non-interactive fails with invalid token (exit 3)', result.stderr.includes('Token authentication failed'))
}

function testNonInteractiveInvalidLanguage() {
  const env = createTestEnv()
  const result = runNode(['--yes', '--language', 'fr'], {
    ...env,
    cwd: env.workspace,
    extraEnv: {
      GITHUB_TOKEN: 'ghp_test',
    },
  })

  assertOk('invalid language produces error output',
    result.stderr.includes('Invalid language') || result.stdout.includes('Invalid language'))
  assertOk('invalid language exits with code 2', result.status === 2)
}

function testNonInteractiveDetectsCI() {
  const env = createTestEnv()
  const stubDir = join(env.root, 'noci-bin')
  mkdirSync(stubDir, { recursive: true })
  createNoAuthGhStub(stubDir)

  const result = spawnSync('node', [cliPath], {
    cwd: env.workspace,
    encoding: 'utf-8',
    env: {
      ...process.env,
      HOME: env.home,
      PATH: `${stubDir}:${process.env.PATH}`,
      CI: 'true',
      GH_TOKEN: '',
      GITHUB_TOKEN: '',
    },
  })

  assert.equal(result.status, 2)
  assertOk('CI env triggers non-interactive mode', result.stderr.includes('Non-interactive mode detected') || result.stdout.includes('Non-interactive mode detected'))
}

function testNonInteractiveTokenFromEnv() {
  const env = createTestEnv()
  // GITHUB_TOKEN 환경변수로 토큰이 전달되는지 확인
  // gh stub에서 auth status가 scope를 보여주도록 설정
  const result = runNode(['--yes', '--language', 'en'], {
    ...env,
    cwd: env.workspace,
    extraEnv: {
      GITHUB_TOKEN: 'ghp_test_token_env',
    },
  })

  // stub gh가 인증을 처리하므로 setup이 진행됨
  assertOk('GITHUB_TOKEN env is accepted in non-interactive mode',
    result.stdout.includes('Authenticated as stubuser') || result.stdout.includes('Setup complete'))

  // linkProjectV2ToRepository 호출 검증
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('setup links repository to project',
    ghState.repoLinked != null && ghState.repoLinked.projectId === 'PVT_project' && ghState.repoLinked.repositoryId === 'R_repo123')
  assertOk('setup output confirms repo linked',
    result.stdout.includes('Repository linked to project'))
}

function testNonInteractiveReinstallAutoApproves() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  // 기존 설치가 있는 상태에서 --yes로 실행하면 자동 reinstall
  const result = runNode(['--yes', '--language', 'en'], {
    ...env,
    cwd: env.workspace,
    extraEnv: {
      GITHUB_TOKEN: 'ghp_test_token',
    },
  })

  assertOk('non-interactive auto-reinstalls over existing installation',
    result.stdout.includes('Existing installation detected. Reinstalling') || result.stdout.includes('Setup complete'))
}

function testExistingCommandsStillWork() {
  const env = createTestEnv()
  writeTrackerInstall(env)

  // 기존 명령어가 positional arg로 계속 동작하는지 확인
  const statusResult = runNode(['status'], { ...env, cwd: env.workspace })
  assertOk('status command still works with parseArgs', statusResult.status === 0)

  const doctorResult = runNode(['doctor'], { ...env, cwd: env.workspace })
  // doctor는 exit 0 또는 1 (환경에 따라 다름) - 크래시하지 않으면 OK
  assertOk('doctor command still works with parseArgs', doctorResult.status === 0 || doctorResult.status === 1)
}

function testInstalledHooksImportCleanly() {
  const env = createTestEnv()
  // 실제 설치 플로우로 temp HOME에 설치한 뒤 "설치된 디렉터리"에서 import를 검증한다.
  // repo 체크아웃에서 실행하면 누락된 파일도 옆에 있어 패키징 누락을 잡지 못한다.
  const install = runNode(['--yes', '--language', 'en'], {
    ...env,
    cwd: env.workspace,
    extraEnv: { GITHUB_TOKEN: 'ghp_test_token' },
  })
  assertOk('setup completes for install-path test', install.status === 0)
  assertOk('installer ships cst_hash_chain.py', existsSync(join(env.hooksDir, 'cst_hash_chain.py')))

  for (const file of [
    'cst_github_utils.py',
    'cst_hash_chain.py',
    'cst_session_start.py',
    'cst_prompt_to_github_projects.py',
    'cst_session_stop.py',
    'cst_mark_done.py',
    'cst_post_tool_use.py',
    'cst_session_end.py',
  ]) {
    const mod = file.replace(/\.py$/, '')
    const result = spawnSync('python3', [
      '-c',
      `import sys; sys.path.insert(0, ${JSON.stringify(env.hooksDir)}); import ${mod}`,
    ], { encoding: 'utf-8', env: { ...process.env, HOME: env.home } })
    assertOk(`installed ${file} imports cleanly`, result.status === 0 && !result.stderr.includes('ModuleNotFoundError'))
  }

  const run = runPythonHook({
    ...env,
    cwd: env.workspace,
    stdin: '{}',
    scriptPath: join(env.hooksDir, 'cst_session_start.py'),
  })
  assertOk('installed session_start runs from installed dir', run.status === 0 && !(run.stderr || '').includes('Traceback'))
}

function testResumeReusesItemViaSourceField() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  writeFileSync(join(env.stateDir, 'old-session.json'), JSON.stringify({
    session_id: 'old-session',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 42,
    item_id: 'ITEM_OLD',
    status: 'waiting',
  }, null, 2))
  // 현행 transcript 첫 줄은 type: mode — 레거시 스니핑으로는 resume을 감지할 수 없는 상황
  const transcriptPath = join(env.root, 'transcript-resume.jsonl')
  writeFileSync(transcriptPath, JSON.stringify({ type: 'mode' }) + '\n')

  const result = runPythonHook({
    ...env,
    cwd: env.workspace,
    stdin: JSON.stringify({
      session_id: 'resumed-session',
      cwd: env.workspace,
      transcript_path: transcriptPath,
      source: 'resume',
    }),
  })

  assert.equal(result.status, 0)
  assertOk('resume reuses existing item via source field', result.stdout.includes('(resumed)'))
  const newState = JSON.parse(readFileSync(join(env.stateDir, 'resumed-session.json'), 'utf-8'))
  assertOk('resume keeps existing item id', newState.item_id === 'ITEM_OLD')
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('resume does not create a new issue', !(ghState.createdIssues?.length))
}

function testStartupSourceIgnoresLegacyMarker() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  // 레거시 마커가 있어도 source=startup이면 신규 세션으로 처리해야 한다 (source 우선)
  const transcriptPath = join(env.root, 'transcript-startup.jsonl')
  writeFileSync(transcriptPath, JSON.stringify({ type: 'file-history-snapshot' }) + '\n')

  const result = runPythonHook({
    ...env,
    cwd: env.workspace,
    stdin: JSON.stringify({
      session_id: 'fresh-session',
      cwd: env.workspace,
      transcript_path: transcriptPath,
      source: 'startup',
    }),
  })

  assert.equal(result.status, 0)
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('startup source creates a new issue despite legacy marker', (ghState.createdIssues?.length ?? 0) === 1)
}

function testTaskNotificationPromptSkipped() {
  const env = createTestEnv()
  writeTrackerInstall(env)
  const statePath = join(env.stateDir, 'session-sys-turn.json')
  writeFileSync(statePath, JSON.stringify({
    session_id: 'session-sys-turn',
    cwd: env.workspace,
    repo: 'tester/private-notes',
    issue_number: 7,
    item_id: 'ITEM_7',
    status: 'waiting',
    first_prompt_notified: true,
  }, null, 2))

  const result = runPythonHook({
    ...env,
    cwd: env.workspace,
    scriptPath: join(repoRoot, 'hooks', 'cst_prompt_to_github_projects.py'),
    stdin: JSON.stringify({
      session_id: 'session-sys-turn',
      prompt: '<task-notification>\n<task-id>abc</task-id>\n</task-notification>',
    }),
  })

  assert.equal(result.status, 0)
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('system turn does not update issue title', !(ghState.titleUpdates?.length))
  assertOk('system turn does not add prompt comment', !(ghState.comments?.length))
  const state = JSON.parse(readFileSync(statePath, 'utf-8'))
  assertOk('system turn still flips status to responding', state.status === 'responding')
}

function testMergePreservesThirdPartyHooks() {
  const env = createTestEnv()
  mkdirSync(join(env.home, '.claude'), { recursive: true })
  writeFileSync(join(env.home, '.claude', 'settings.json'), JSON.stringify({
    hooks: {
      SessionStart: [{
        hooks: [{ type: 'command', command: 'python3 /opt/other-tool/hook.py' }],
      }],
    },
  }, null, 2))

  // 두 번 설치해 서드파티 hook 보존과 멱등성을 함께 검증
  for (let i = 0; i < 2; i++) {
    runNode(['--yes', '--language', 'en'], {
      ...env,
      cwd: env.workspace,
      extraEnv: { GITHUB_TOKEN: 'ghp_test_token' },
    })
  }

  const settings = JSON.parse(readFileSync(join(env.home, '.claude', 'settings.json'), 'utf-8'))
  const sessionStart = settings.hooks.SessionStart ?? []
  const thirdParty = sessionStart.filter(e => (e.hooks ?? []).some(h => h.command?.includes('other-tool')))
  const ours = sessionStart.filter(e => (e.hooks ?? []).some(h => h.command?.includes('cst_session_start.py')))
  assertOk('third-party SessionStart hook survives reinstall', thirdParty.length === 1)
  assertOk('exactly one CST SessionStart entry after two installs', ours.length === 1)
  for (const key of ['UserPromptSubmit', 'PostToolUse', 'Stop', 'SessionEnd']) {
    const entries = settings.hooks[key] ?? []
    const cst = entries.filter(e => (e.hooks ?? []).some(h => (h.command || '').includes('cst_')))
    assertOk(`exactly one CST ${key} entry after two installs`, cst.length === 1)
  }
}

console.log('\n[cli]')
testStatusOutput()
testDoctorPublicRepoFailure()
testPauseResumeLifecycle()
testPauseResumeWithoutSession()
testInstallHelperConfiguresReadmeAndOnTrack()
testSessionStartBlocksPublicRepo()
testSessionStartBlocksOffTrackBoard()
testPromptSkipsWhenBoardOffTrack()
testSessionEndClosesIssue()
testSessionEndSkipsAlreadyClosed()
testMarkDoneClosesIssue()
testCleanupStaleSessions()
testVersionFlag()

console.log('\n[non-interactive]')
testNonInteractiveFailsWithoutToken()
testNonInteractiveFailsWithInvalidToken()
testNonInteractiveInvalidLanguage()
testNonInteractiveDetectsCI()
testNonInteractiveTokenFromEnv()
testNonInteractiveReinstallAutoApproves()
testExistingCommandsStillWork()

console.log('\n[regression]')
testInstalledHooksImportCleanly()
testResumeReusesItemViaSourceField()
testStartupSourceIgnoresLegacyMarker()
testTaskNotificationPromptSkipped()
testMergePreservesThirdPartyHooks()

console.log(`\n${pass} passed, ${fail} failed\n`)
if (fail > 0) process.exit(1)
