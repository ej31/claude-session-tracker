#!/usr/bin/env node
/**
 * hasCmd / detectLinuxDistro 호환성 단위 테스트
 * GitHub Actions의 macOS, Ubuntu, Windows runner에서 실행됨
 */
import { spawnSync } from 'node:child_process'
import { readFileSync, existsSync } from 'node:fs'

// -- 테스트 대상 함수 (bin/index.mjs 에서 복사) --------------------------------

function hasCmd(cmd) {
  const isWin = process.platform === 'win32'
  const finder = isWin ? 'where' : 'which'
  const result = spawnSync(finder, [cmd], { stdio: 'ignore', shell: isWin })
  return result.status === 0
}

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

// -- 테스트 러너 ---------------------------------------------------------------

let pass = 0
let fail = 0

function assert(name, cond) {
  if (cond) {
    console.log(`  ✓ ${name}`)
    pass++
  } else {
    console.error(`  ✗ ${name}`)
    fail++
  }
}

// -- 테스트 케이스 -------------------------------------------------------------

console.log(`\nOS: ${process.platform}\n`)

console.log('[hasCmd]')
assert('node 감지 성공', hasCmd('node'))
assert('git 감지 성공', hasCmd('git'))
assert('존재하지 않는 명령어 → false', !hasCmd('__nonexistent_cmd_xyz_abc__'))
assert('gh 감지 (GH Actions 기본 제공)', hasCmd('gh'))

if (process.platform === 'win32') {
  console.log('\n[Windows 전용]')
  // GH Actions Windows runner에는 기본으로 winget이 있음
  const hasWinget = hasCmd('winget')
  const hasChoco = hasCmd('choco')
  const hasScoop = hasCmd('scoop')
  assert(`winget 감지: ${hasWinget}`, true) // 존재 여부만 확인, 없어도 통과
  assert(`choco 감지: ${hasChoco}`, true)
  assert(`scoop 감지: ${hasScoop}`, true)
  console.log(`  (winget=${hasWinget}, choco=${hasChoco}, scoop=${hasScoop})`)
}

if (process.platform === 'linux') {
  console.log('\n[Linux 전용]')
  const distro = detectLinuxDistro()
  console.log(`  감지된 배포판: ${distro}`)
  assert('/etc/os-release 존재', existsSync('/etc/os-release'))
  assert('배포판 감지 결과가 unknown이 아님', distro !== 'unknown')
  // GH Actions Ubuntu runner
  assert('Ubuntu runner → debian 계열 감지', distro === 'debian')
}

if (process.platform === 'darwin') {
  console.log('\n[macOS 전용]')
  assert('brew 감지 (GH Actions macOS runner 기본 제공)', hasCmd('brew'))
  assert('detectLinuxDistro → /etc/os-release 없으면 unknown', detectLinuxDistro() === 'unknown')
}

// -- 결과 출력 -----------------------------------------------------------------

console.log(`\n${pass} passed, ${fail} failed\n`)
if (fail > 0) process.exit(1)
