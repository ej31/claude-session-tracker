import * as p from '@clack/prompts'

// 상태 아이콘
const ICON = {
  ok: '✓',
  warn: '⚠',
  fail: '✗',
  dot: '·',
  arrow: '→',
}

// 라벨 정렬을 위한 패딩
function pad(label, width = 16) {
  return label.padEnd(width)
}

// 설치 상태에 따른 요약 텍스트
function describeInstallState(state) {
  if (state === 'installed') return 'Installed and active'
  if (state === 'partial') return 'Partially installed'
  return 'Not installed'
}

// 훅 스코프 상태 텍스트
function describeHookTarget(target) {
  if (target.invalid) return 'invalid settings.json'
  if (target.installed) return 'registered'
  if (target.exists) return 'present but hooks missing'
  return 'not configured'
}

// 섹션 내용을 포맷팅된 문자열로 구성
function buildInstallSection(install, config, version) {
  const lines = []

  // 설치 상태
  const stateIcon = install.state === 'installed' ? ICON.ok : ICON.warn
  lines.push(`  ${stateIcon}  ${describeInstallState(install.state)}`)

  // 훅 파일 상태
  const hookIcon = install.hookFilesPresent ? ICON.ok : ICON.fail
  lines.push(`  ${hookIcon}  Hook files ${install.hookFilesPresent ? 'present' : 'missing'}`)

  // 훅 스코프
  for (const target of install.hookRegistrations) {
    const icon = target.installed ? ICON.ok : target.invalid ? ICON.fail : ICON.warn
    lines.push(`  ${icon}  ${target.scope} scope: ${describeHookTarget(target)}`)
  }

  // 구성 정보
  if (config) {
    lines.push('')
    lines.push(`  ${pad('Notes repo')}${config.NOTES_REPO ?? '(not set)'}`)

    if (config.GITHUB_PROJECT_OWNER && config.GITHUB_PROJECT_NUMBER) {
      const url = `https://github.com/users/${config.GITHUB_PROJECT_OWNER}/projects/${config.GITHUB_PROJECT_NUMBER}`
      lines.push(`  ${pad('Project board')}${url}`)
    }

    if (config.DONE_TIMEOUT_SECS) {
      lines.push(`  ${pad('Idle timeout')}${Math.floor(Number(config.DONE_TIMEOUT_SECS) / 60)} min`)
    }

    lines.push(`  ${pad('Version')}v${version}`)
  }

  return lines.join('\n')
}

function buildSessionSection(activeSession) {
  if (!activeSession) return null

  const { sessionId, state } = activeSession
  const lines = []

  lines.push(`  ${pad('Session ID')}${sessionId}`)
  lines.push(`  ${pad('Status')}${state.status}`)

  const isPaused = Boolean(state.tracking_paused)
  const trackingIcon = isPaused ? ICON.warn : ICON.ok
  const trackingLabel = isPaused ? 'paused' : 'active'
  lines.push(`  ${pad('Tracking')}${trackingIcon}  ${trackingLabel}`)

  lines.push(`  ${pad('Issue')}${activeSession.issueUrl ?? '(unavailable)'}`)

  const syncDetail = describeSyncStatus(state.project_status_sync)
  lines.push(`  ${pad('Board sync')}${syncDetail}`)

  return lines.join('\n')
}

function describeSyncStatus(sync) {
  if (!sync) return 'never synced'
  if (sync.success) return `${sync.status} at ${sync.synced_at}`
  if (sync.error) return `failed (${sync.error})`
  return 'unknown'
}

function buildWarnings(runtimeStatus, projectStatusCache, activeSession) {
  const lines = []

  if (runtimeStatus) {
    let detail
    if (runtimeStatus.reason === 'notes_repo_public') {
      detail = `Tracking blocked: ${runtimeStatus.repo} is public`
    } else if (runtimeStatus.reason === 'project_inactive') {
      detail = 'Tracking blocked: project board is INACTIVE'
    } else {
      detail = `Tracking blocked: ${runtimeStatus.error ?? runtimeStatus.reason}`
    }
    lines.push(`  ${ICON.warn}  ${detail}`)
  }

  if (activeSession?.state?.project_status_sync && !activeSession.state.project_status_sync.success && activeSession.state.project_status_sync.error) {
    lines.push(`  ${ICON.warn}  Board sync failed: ${activeSession.state.project_status_sync.error}`)
  }

  if (projectStatusCache?.last_error) {
    lines.push(`  ${ICON.warn}  Status cache error: ${projectStatusCache.last_error}`)
  }

  return lines.length > 0 ? lines.join('\n') : null
}

function buildQuickReference() {
  const cmds = [
    ['pause', 'Pause tracking'],
    ['resume', 'Resume tracking'],
    ['doctor', 'Deep health check'],
    ['update', 'Update hook scripts'],
    ['uninstall', 'Remove all hooks and config'],
  ]

  const lines = cmds.map(([cmd, desc]) =>
    `  ${pad(cmd)}${desc}`
  )

  return lines.join('\n')
}

/**
 * 새로운 status UI
 * @param {object} ctx - 상태 데이터 컨텍스트
 * @param {object} ctx.install - getInstallState 결과
 * @param {object|null} ctx.activeSession - findSessionByCwd 결과
 * @param {object|null} ctx.projectStatusCache - loadProjectStatusCache 결과
 * @param {object|null} ctx.runtimeStatus - loadRuntimeStatus 결과
 * @param {string} ctx.version - 패키지 버전
 */
export function printStatus(ctx) {
  const { install, activeSession, projectStatusCache, runtimeStatus, version } = ctx
  const config = install.config

  // 헤더
  p.intro(`Claude Session Tracker v${version}`)

  // 섹션 1: Installation
  p.note(buildInstallSection(install, config, version), 'Installation')

  // 섹션 2: Active Session
  const sessionContent = buildSessionSection(activeSession)
  if (sessionContent) {
    p.note(sessionContent, 'Active Session')
  } else {
    p.log.info('No active session. Start Claude Code to begin tracking.')
  }

  // 섹션 3: Warnings (문제가 있을 때만)
  const warnings = buildWarnings(runtimeStatus, projectStatusCache, activeSession)
  if (warnings) {
    p.note(warnings, 'Warnings')
  }

  // 섹션 4: Quick Reference
  p.note(buildQuickReference(), 'Quick Reference')

  // 마무리
  const hasIssues = Boolean(runtimeStatus) || install.state !== 'installed'
  if (hasIssues) {
    p.outro('Some issues detected. Run "claude-session-tracker doctor" for details.')
  } else {
    p.outro('Everything looks good.')
  }
}
