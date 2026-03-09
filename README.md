# claude-session-tracker

Claude Code 세션을 GitHub Projects에 자동으로 트래킹하는 hook 설치 도구.

Claude Code의 lifecycle hook에 맞춰 GitHub Projects item 상태를 자동으로 업데이트하고, 프롬프트·답변을 GitHub Issue 댓글로 기록합니다.

## 요구 사항

- Node.js 18 이상
- Python 3
- [GitHub CLI (`gh`)](https://cli.github.com) — 설치 후 `gh auth login` 필요

## 설치

```bash
npx claude-session-tracker
```

대화형 설치 마법사가 실행됩니다. 아래 항목을 순서대로 입력하면 자동 설치됩니다.

1. GitHub Project Owner (username 또는 org)
2. GitHub Project Number
3. 각 lifecycle 단계별 Status 옵션 매핑
4. 기본 repo (git remote 없을 때 Issue 생성 대상)
5. 세션 종료 타이머 (분)
6. Hook 적용 범위 (현재 프로젝트 / 전역)

## 동작 방식

| Claude Code 이벤트 | GitHub Projects 상태 | 동작 |
|---|---|---|
| `SessionStart` | 세션 등록 됨 | GitHub Issue 생성 + Project에 연결 |
| `UserPromptSubmit` | 답변 중 | 프롬프트를 Issue 댓글로 기록 |
| `Stop` | 프롬프트 입력 대기 | 답변을 Issue 댓글로 기록 + 종료 타이머 시작 |
| 타이머 만료 | 세션 종료 | 설정한 시간(기본 30분) 동안 입력 없으면 종료 처리 |

### Resume 감지

`claude --resume`으로 세션을 재개하면 기존 GitHub Issue를 재활용합니다. 새 Issue가 중복 생성되지 않습니다.

### git remote 자동 감지

작업 디렉토리에 GitHub remote가 있으면 해당 repository에 Issue를 생성합니다. 없으면 설치 시 지정한 기본 repo에 생성합니다.

## 설치 후 생성되는 파일

```
~/.claude/hooks/
├── github_utils.py          # 공통 유틸리티
├── session_start.py         # SessionStart hook
├── prompt_to_github_projects.py  # UserPromptSubmit hook
├── session_stop.py          # Stop hook
├── mark_done.py             # 세션 종료 타이머
├── config.env               # 설정값 (Project ID, Status ID 등)
├── hooks.log                # 실행 로그
└── state/                   # 세션별 상태 파일 (JSON)
```

## 설정 변경

`~/.claude/hooks/config.env`를 직접 수정하거나, `npx claude-session-tracker`를 다시 실행합니다.

## 라이선스

MIT
