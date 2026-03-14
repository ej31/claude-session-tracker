#!/usr/bin/env node
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs'
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
  respond((process.env.GH_STUB_REPO_PRIVATE ?? 'true') + '\\n')
  process.exit(0)
}

if (args[0] === 'project' && args[1] === 'create') {
  process.exit(0)
}

if (args[0] === 'project' && args[1] === 'list') {
  respond(JSON.stringify({ projects: [{ title: 'Claude Session Tracker', number: 1 }] }))
  process.exit(0)
}

if (args[0] === 'repo' && args[1] === 'delete') {
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
    state.projectReadme = payload.variables.readme
    writeState(state)
    respond(JSON.stringify({
      data: {
        updateProjectV2: {
          projectV2: {
            id: payload.variables.projectId,
            readme: payload.variables.readme,
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
            status: boardStatus || 'OFF_TRACK',
            updatedAt: '2026-03-12T00:00:00Z',
          }]
        : []
    respond(JSON.stringify({ data: { node: { statusUpdates: { nodes } } } }))
    process.exit(0)
  }

  if (query.includes('deleteProjectV2')) {
    respond(JSON.stringify({ data: { deleteProjectV2: { projectV2: { id: 'PVT_project' } } } }))
    process.exit(0)
  }

  respond(JSON.stringify({ data: {} }))
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
    'cst_session_start.py',
    'cst_prompt_to_github_projects.py',
    'cst_session_stop.py',
    'cst_mark_done.py',
    'cst_post_tool_use.py',
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

function runPythonHook({ cwd, home, binDir, ghStatePath, stdin, extraEnv = {} }) {
  return spawnSync('python3', [sessionStartPath], {
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
      status: 'OFF_TRACK',
      success: false,
      error: 'simulated sync error',
    },
  }, null, 2))

  writeFileSync(join(env.hooksDir, 'project_status_update.json'), JSON.stringify({
    project_id: 'PVT_project',
    status_update_id: 'PSU_1',
    last_status: 'OFF_TRACK',
    last_synced_at: '2026-03-12T00:00:00Z',
  }, null, 2))

  writeFileSync(join(env.hooksDir, 'runtime_status.json'), JSON.stringify({
    status: 'blocked',
    reason: 'notes_repo_public',
    repo: 'tester/private-notes',
  }, null, 2))

  const result = runNode(['status'], { ...env, cwd: env.workspace })
  assert.equal(result.status, 0)
  assertOk('status shows installed state', result.stdout.includes('Install: installed'))
  assertOk('status shows paused session', result.stdout.includes('Tracking paused: yes'))
  assertOk('status shows board sync error', result.stdout.includes('simulated sync error'))
  assertOk('status shows runtime block detail', result.stdout.includes('tracking blocked because tester/private-notes is public'))
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
  assertOk('pause stores OFF_TRACK cache', pauseCache.last_status === 'OFF_TRACK')
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
  const tempModule = join(repoRoot, `.tmp-install-recheck-${Date.now()}-${Math.random().toString(16).slice(2)}.mjs`)
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

  assert.equal(result.status, 0)
  const ghState = JSON.parse(readFileSync(env.ghStatePath, 'utf-8'))
  assertOk('install helper writes project readme', ghState.projectReadme.includes('do not manually change'))
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
    extraEnv: { GH_STUB_BOARD_STATUS: 'OFF_TRACK' },
  })

  assert.equal(result.status, 0)
  assertOk('session_start prints OFF_TRACK block', result.stdout.includes('project board is currently OFF_TRACK'))
  assertOk('session_start skips state creation when board is OFF_TRACK', !existsSync(join(env.stateDir, 'session-off-track.json')))
  const runtimeStatus = JSON.parse(readFileSync(join(env.hooksDir, 'runtime_status.json'), 'utf-8'))
  assertOk('session_start records project_off_track reason', runtimeStatus.reason === 'project_off_track')
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
      GH_STUB_BOARD_STATUS: 'OFF_TRACK',
    },
  })

  assert.equal(result.status, 0)
  const state = JSON.parse(readFileSync(statePath, 'utf-8'))
  assertOk('prompt keeps prior status when board is OFF_TRACK', state.status === 'waiting')
  const runtimeStatus = JSON.parse(readFileSync(join(env.hooksDir, 'runtime_status.json'), 'utf-8'))
  assertOk('prompt records project_off_track runtime status', runtimeStatus.reason === 'project_off_track')
}

console.log('\n[cli]')
testStatusOutput()
testDoctorPublicRepoFailure()
testPauseResumeLifecycle()
testInstallHelperConfiguresReadmeAndOnTrack()
testSessionStartBlocksPublicRepo()
testSessionStartBlocksOffTrackBoard()
testPromptSkipsWhenBoardOffTrack()

console.log(`\n${pass} passed, ${fail} failed\n`)
if (fail > 0) process.exit(1)
