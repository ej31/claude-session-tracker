#!/usr/bin/env node
import * as p from '@clack/prompts'
import { execSync, spawnSync } from 'node:child_process'
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
  'github_utils.py',
  'session_start.py',
  'prompt_to_github_projects.py',
  'session_stop.py',
  'mark_done.py',
  'post_tool_use.py',
]

// ── 유틸 ──────────────────────────────────────────────────────────────────────

function hasCmd(cmd) {
  try { execSync(`which ${cmd}`, { stdio: 'ignore' }); return true }
  catch { return false }
}

function ghGraphql(query, variables = {}) {
  const result = spawnSync(
    'gh', ['api', 'graphql', '--input', '-'],
    { input: JSON.stringify({ query, variables }), encoding: 'utf-8' },
  )
  if (!result.stdout?.trim()) throw new Error(result.stderr || 'gh api 응답 없음')
  return JSON.parse(result.stdout)
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
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'session_start.py')}`, timeout: 15, async: true }],
      }],
      UserPromptSubmit: [{
        matcher: '',
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'prompt_to_github_projects.py')}`, timeout: 15, async: true }],
      }],
      PostToolUse: [{
        matcher: 'AskUserQuestion',
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'post_tool_use.py')}`, timeout: 15, async: true }],
      }],
      Stop: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'session_stop.py')}`, timeout: 10, async: true }],
      }],
      SessionEnd: [{
        hooks: [{ type: 'command', command: `python3 ${join(hooksDir, 'session_stop.py')}`, timeout: 10, async: true }],
      }],
    },
  }
}

// ── 우리가 등록하는 훅 키 목록 ───────────────────────────────────────────────

const OUR_HOOK_KEYS = ['SessionStart', 'UserPromptSubmit', 'PostToolUse', 'Stop', 'SessionEnd']

function removeOurHooks(settings) {
  if (!settings.hooks) return settings
  const cleaned = { ...settings, hooks: { ...settings.hooks } }
  for (const key of OUR_HOOK_KEYS) {
    const entries = cleaned.hooks[key]
    if (!Array.isArray(entries)) continue
    // 우리 스크립트 경로가 포함된 항목만 제거
    cleaned.hooks[key] = entries.filter(entry => {
      const cmds = entry.hooks ?? []
      return !cmds.some(h => PY_FILES.some(f => h.command?.includes(f)))
    })
    if (cleaned.hooks[key].length === 0) delete cleaned.hooks[key]
  }
  if (Object.keys(cleaned.hooks).length === 0) delete cleaned.hooks
  return cleaned
}

// ── uninstall ────────────────────────────────────────────────────────────────

async function uninstall() {
  console.clear()
  p.intro(' Claude Session Tracker 제거 ')

  const confirmed = await p.confirm({ message: '설치된 훅과 설정을 제거할까요?' })
  if (!confirmed) { p.cancel('제거 취소'); process.exit(0) }

  const spin = p.spinner()
  spin.start('제거 중…')

  let removed = 0

  // 1. Python 스크립트 제거
  for (const f of PY_FILES) {
    const target = join(HOOKS_DIR, f)
    if (existsSync(target)) { unlinkSync(target); removed++ }
  }

  // 2. config.env 제거
  if (existsSync(CONFIG_FILE)) { unlinkSync(CONFIG_FILE); removed++ }

  // 3. hooks.log 제거
  const logFile = join(HOOKS_DIR, 'hooks.log')
  if (existsSync(logFile)) { unlinkSync(logFile); removed++ }

  // 4. state 디렉토리 제거
  if (existsSync(STATE_DIR)) { rmSync(STATE_DIR, { recursive: true }); removed++ }

  // 5. settings.json에서 훅 항목 제거 (전역 + 프로젝트)
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

  spin.stop(`제거 완료 (${removed}개 항목)`)

  p.note([
    'Python 스크립트, config.env, state, 로그가 삭제되었습니다.',
    'settings.json에서 관련 훅 항목이 제거되었습니다.',
    '',
    'Claude Code를 재시작하면 적용됩니다.',
  ].join('\n'), '제거 완료')

  p.outro('세션 트래킹이 비활성화되었습니다.')
}

// ── 메인 ──────────────────────────────────────────────────────────────────────

async function main() {
  if (process.argv.includes('uninstall')) return uninstall()

  console.clear()
  p.intro(' Claude Session Tracker 설치 ')

  // 1. 환경 확인
  const envSpin = p.spinner()
  envSpin.start('환경 확인 중…')

  const missing = []
  if (!hasCmd('python3')) missing.push('python3')
  if (!hasCmd('gh')) missing.push('gh  →  https://cli.github.com')
  if (missing.length) {
    envSpin.stop('환경 확인 실패', 1)
    p.log.error(`다음이 설치되어 있지 않습니다:\n${missing.map(m => `  • ${m}`).join('\n')}`)
    p.outro('설치를 중단합니다.')
    process.exit(1)
  }

  if (spawnSync('gh', ['auth', 'status'], { encoding: 'utf-8' }).status !== 0) {
    envSpin.stop('gh 인증 필요', 1)
    p.log.warn('먼저 아래 명령어를 실행해주세요:\n\n  gh auth login\n')
    p.outro('설치를 중단합니다.')
    process.exit(1)
  }
  envSpin.stop('환경 확인 완료')

  // 2. GitHub Project 정보 입력
  const project = await p.group({
    owner: () => p.text({
      message: 'GitHub Project Owner  (username 또는 org)',
      placeholder: 'your-username',
      validate: v => !v?.trim() ? '필수 입력입니다.' : undefined,
    }),
    number: () => p.text({
      message: 'GitHub Project Number',
      placeholder: '1',
      validate: v => !v || isNaN(Number(v)) ? '숫자를 입력해주세요.' : undefined,
    }),
  }, { onCancel: () => { p.cancel('설치 취소'); process.exit(0) } })

  // 3. Project / Status field 자동 조회
  const fetchSpin = p.spinner()
  fetchSpin.start('GitHub Project 조회 중…')

  let projectId, statusField
  try {
    const query = `
      query($login: String!, $number: Int!) {
        user(login: $login) {
          projectV2(number: $number) {
            id
            fields(first: 30) {
              nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
            }
          }
        }
        organization(login: $login) {
          projectV2(number: $number) {
            id
            fields(first: 30) {
              nodes { ... on ProjectV2SingleSelectField { id name options { id name } } }
            }
          }
        }
      }`
    const res = ghGraphql(query, {
      login: project.owner.trim(),
      number: Number(project.number),
    })
    const pv2 = res.data?.user?.projectV2 ?? res.data?.organization?.projectV2
    if (!pv2) throw new Error('Project를 찾을 수 없습니다. Owner와 Number를 확인해주세요.')

    projectId = pv2.id
    statusField = pv2.fields.nodes.find(n => n?.name === 'Status')
    if (!statusField) throw new Error("'Status' 필드를 찾을 수 없습니다.")

    fetchSpin.stop(`조회 완료  (Status 옵션 ${statusField.options.length}개)`)
  } catch (e) {
    fetchSpin.stop('조회 실패', 1)
    p.log.error(e.message)
    process.exit(1)
  }

  // 4. 각 lifecycle에 Status 옵션 매핑
  p.log.info('각 Claude Code lifecycle 단계에 맞는 Status를 선택해주세요.')
  const choices = statusField.options.map(o => ({ value: o.id, label: o.name }))

  const statusMap = await p.group({
    registered: () => p.select({ message: '세션 시작 시     →', options: choices }),
    responding: () => p.select({ message: '답변 중          →', options: choices }),
    waiting:    () => p.select({ message: '응답 완료 (대기) →', options: choices }),
    closed:     () => p.select({ message: '세션 종료        →', options: choices }),
  }, { onCancel: () => { p.cancel('설치 취소'); process.exit(0) } })

  // 5. 기타 설정
  const extras = await p.group({
    notesRepo: () => p.text({
      message: 'git remote 없을 때 Issue 생성할 기본 repo',
      placeholder: `${project.owner.trim()}/dev-notes`,
      validate: v => !v?.includes('/') ? 'owner/repo 형식으로 입력해주세요.' : undefined,
    }),
    timeout: () => p.text({
      message: '세션 종료 타이머  (분)',
      initialValue: '30',
      validate: v => !v || isNaN(Number(v)) ? '숫자를 입력해주세요.' : undefined,
    }),
    scope: () => p.select({
      message: 'Hook 적용 범위',
      options: [
        { value: 'project', label: '현재 프로젝트만   (.claude/settings.json)' },
        { value: 'global',  label: '전역              (~/.claude/settings.json)' },
      ],
    }),
  }, { onCancel: () => { p.cancel('설치 취소'); process.exit(0) } })

  // 6. 설치 확인
  const confirmed = await p.confirm({
    message: [
      '설치를 진행할까요?',
      `  Project ID  : ${projectId}`,
      `  Notes Repo  : ${extras.notesRepo}`,
      `  타이머      : ${extras.timeout}분`,
      `  적용 범위   : ${extras.scope === 'global' ? '전역' : '현재 프로젝트'}`,
    ].join('\n'),
  })
  if (!confirmed) { p.cancel('설치 취소'); process.exit(0) }

  // 7. 설치 실행
  const installSpin = p.spinner()
  installSpin.start('설치 중…')

  try {
    // 디렉토리 생성
    mkdirSync(HOOKS_DIR, { recursive: true })
    mkdirSync(STATE_DIR, { recursive: true })

    // Python 스크립트 복사 + 실행 권한
    for (const f of PY_FILES) {
      copyFileSync(join(HOOKS_SRC, f), join(HOOKS_DIR, f))
      chmodSync(join(HOOKS_DIR, f), 0o755)
    }

    // config.env 저장
    writeFileSync(CONFIG_FILE, [
      `GITHUB_PROJECT_OWNER=${project.owner.trim()}`,
      `GITHUB_PROJECT_NUMBER=${project.number}`,
      `GITHUB_PROJECT_ID=${projectId}`,
      `GITHUB_STATUS_FIELD_ID=${statusField.id}`,
      `GITHUB_STATUS_REGISTERED=${statusMap.registered}`,
      `GITHUB_STATUS_RESPONDING=${statusMap.responding}`,
      `GITHUB_STATUS_WAITING=${statusMap.waiting}`,
      `GITHUB_STATUS_CLOSED=${statusMap.closed}`,
      `NOTES_REPO=${extras.notesRepo.trim()}`,
      `DONE_TIMEOUT_SECS=${Number(extras.timeout) * 60}`,
    ].join('\n') + '\n')

    // settings.json 업데이트
    const settingsPath = extras.scope === 'global'
      ? join(HOME, '.claude', 'settings.json')
      : (() => {
          mkdirSync(join(process.cwd(), '.claude'), { recursive: true })
          return join(process.cwd(), '.claude', 'settings.json')
        })()

    writeFileSync(
      settingsPath,
      JSON.stringify(mergeHooks(readJson(settingsPath), HOOKS_DIR), null, 2) + '\n',
    )

    installSpin.stop('설치 완료')
  } catch (e) {
    installSpin.stop('설치 실패', 1)
    p.log.error(e.message)
    process.exit(1)
  }

  p.note([
    `Hook 위치  : ${HOOKS_DIR}`,
    `설정 파일  : ${CONFIG_FILE}`,
    `로그 파일  : ${join(HOOKS_DIR, 'hooks.log')}`,
    '',
    'Claude Code를 재시작하면 자동으로 적용됩니다.',
  ].join('\n'), '설치 완료')

  p.outro('세션 트래킹이 활성화되었습니다.')
}

main().catch(e => { console.error(e.message); process.exit(1) })
