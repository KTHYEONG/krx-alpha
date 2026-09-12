# Core Architecture Components

본 문서는 `krx-alpha` 저장소의 핵심 컴포넌트별 책임(Responsibility), 입출력 계약, 의존 관계 및 구현 클래스/함수 매핑을 정의합니다.

---

## 1. Core Subsystem (Layer 0)

### Config & Paths Single Source
* **Responsibility**: 데이터 루트 기반 전 파일 경로(`DataPaths`), Pydantic 기반 설정(`CollectorSettings`, `ExecutionSettings`) 및 자격증명(`KrxCredentials`, `LsCredentials`, `TossCredentials`, `KisCredentials`) 단일 진입점 관리. 슬롯 예산 기술상한 초과 검증, Live 실행 무장 검증.
* **Input**: 환경 변수 (`KRX_ALPHA_*`)
* **Output**: 불변 설정 인스턴스, 정규화된 `Path` 객체
* **Dependencies**: None
* **Key Implementation**:
  * [`DataPaths`](file:///home/kth/krx-alpha/src/core/config.py#L28-L87): 파일시스템 트리 유도 불변 데이터클래스
  * [`CollectorSettings`](file:///home/kth/krx-alpha/src/core/config.py#L88-L127): `universe_slot_budget <= subscription_symbol_budget` 모델 검증기
  * [`ExecutionSettings`](file:///home/kth/krx-alpha/src/core/config.py#L174-L203): `check_live_armed` 모델 검증기

### Session Calendar & State Transition
* **Responsibility**: KST 시간대 기준 정규장 스케줄(`SessionSchedule`) 단일 소스 정의 및 현재 시각에 따른 결정론적 세션 상태(`SessionState`) 평가.
* **Input**: `datetime.datetime` (KST 기준 현재 시각)
* **Output**: `SessionState` (`WEEKEND_SLEEP`, `PRE_MARKET_SLEEP`, `STREAMER_ACTIVE`, `FULL_ACTIVE`, `POST_MARKET_EOD`, `NIGHT_SLEEP`)
* **Dependencies**: [`src.core.errors`](file:///home/kth/krx-alpha/src/core/errors.py)
* **Key Implementation**:
  * [`SessionSchedule`](file:///home/kth/krx-alpha/src/core/calendar.py#L27-L43): 단조 시각 검증
  * [`get_target_state`](file:///home/kth/krx-alpha/src/core/calendar.py#L44-L59): 상태 전이 함수
  * [`calc_sleep_seconds`](file:///home/kth/krx-alpha/src/core/calendar.py#L61-L73): 목표 시각 대기 초 계산

---

## 2. Market Data Subsystem (Layer 0, 1, 4)

### KRX Bars Ingestor & Store
* **Responsibility**: KRX 정보데이터시스템 공식 Open API를 호출하여 KOSPI/KOSDAQ 당일 매매정보를 수집하고, 결측 및 절단 응답을 검증한 뒤 `(date, symbol)` 멱등 upsert로 Parquet에 저장.
* **Input**: 거래 일자 (`dt.date`), API `AUTH_KEY`
* **Output**: `data/bars/daily.parquet` 증분 적재, `data/market_map.json` 종목별 시장 매핑
* **Dependencies**: `requests`, `polars`, `tenacity`
* **Key Implementation**:
  * [`fetch_daily_bars`](file:///home/kth/krx-alpha/src/marketdata/krx_bars.py#L54-L83): KOSPI/KOSDAQ 시장 완결성 검사
  * [`append_daily_bars`](file:///home/kth/krx-alpha/src/marketdata/krx_bars.py#L103-L141): 직전일 대비 90% 이상 행수 검증(`ImplausibleRowCountError`) 및 원자적 `.tmp` 치환 쓰기
  * [`refresh_bars`](file:///home/kth/krx-alpha/src/marketdata/service.py#L40-L67): 일봉 갱신 유스케이스 서비스 진입점

### Toss Calendar Gate
* **Responsibility**: 토스증권 OpenAPI를 호출하여 한국 시장 영업일 여부 및 전후 영업일 메타데이터를 취득. 휴장일 오기동 및 주말/공휴일 과거 유니버스 오수집 차단.
* **Input**: 기준 일자 (`dt.date`), 토스 API 키/시크릿
* **Output**: `TradingDay` (is_business_day, previous_business_day, next_business_day)
* **Dependencies**: `requests`, `tenacity`
* **Key Implementation**:
  * [`fetch_trading_day`](file:///home/kth/krx-alpha/src/marketdata/toss_calendar.py#L66-L94): 토스 마켓 캘린더 파싱

### KIS Daily Bars Fallback
* **Responsibility**: KRX 공식 API 장애 시 직전 `market_map.json` 종목군에 대해 한국투자증권 `FHKST03010100` REST API로 1회성 폴백 일봉 수집을 수행하여 당일 오케스트레이션 영구 중단 방지.
* **Input**: 직전 `market_map.json`, KIS REST Client
* **Output**: 당일 일봉 DataFrame 생성 및 Parquet upsert
* **Dependencies**: [`src.execution.kis_client`](file:///home/kth/krx-alpha/src/execution/kis_client.py)
* **Key Implementation**:
  * [`refresh_bars_via_kis_fallback`](file:///home/kth/krx-alpha/src/marketdata/service.py#L69-L113)

---

## 3. Universe Subsystem (Layer 1, 2, 4)

### Universe Selection Policy
* **Responsibility**: 일봉 시계열에 대해 롤링 20일 거래대금 중앙값 및 60일 최고종가를 벡터화 계산하고, 상한가·급등·거래대금급증·신고가 4대 모멘텀 조건과 50억원 유동성 하한을 적용하여 최대 90개 종목을 선정. Look-Ahead 시점 오염 감지 시 Fail-Closed.
* **Input**: `daily.parquet` Polars DataFrame, `decision_date`
* **Output**: `data/universe/YYYY-MM-DD.parquet`
* **Dependencies**: `polars`
* **Key Implementation**:
  * [`compute_selection_features`](file:///home/kth/krx-alpha/src/universe/policy.py#L29-L59): 롤링 피처 생성
  * [`select_universe`](file:///home/kth/krx-alpha/src/universe/policy.py#L61-L102): 조건 필터 및 `SlotBudgetExceededError` 강제

### Universe IPC Bridge
* **Responsibility**: 선정된 유니버스 종목 및 태그 목록을 스캐너/스트리머 간 안전한 프로세스 간 통신을 위해 원자적 파일 치환(Atomic File Replace)으로 발행.
* **Input**: 선정된 유니버스 레코드 목록, 버전 번호(`rev`)
* **Output**: `data/candidates.json`
* **Dependencies**: None
* **Key Implementation**:
  * [`write_candidates`](file:///home/kth/krx-alpha/src/universe/ipc.py#L17-L23): `.tmp` 생성 후 `os.replace` 원자적 교체
  * [`read_candidates`](file:///home/kth/krx-alpha/src/universe/ipc.py#L25-L36): JSON 디코드 오류 시 `CandidateFileError` 발생

---

## 4. Realtime Subsystem (Layer 1, 2, 3, 4)

### NTP Clock Gate
* **Responsibility**: 세션 시작 시 외부 타임서버(`kr.pool.ntp.org`)와 로컬 시계 간의 오프셋을 실측하여 호가 시계열 역전 위험을 사전에 방어.
* **Input**: NTP 호스트명, 샘플 수 (기본 5회)
* **Output**: 클럭 오프셋 나노초 (`int`)
* **Dependencies**: `ntplib`
* **Key Implementation**:
  * [`measure_ntp_offset_ns`](file:///home/kth/krx-alpha/src/realtime/clock.py#L17-L29): 오차 2초 초과 또는 불통 시 `ClockUnsyncedError` 발생

### Subscription Planner & Registry
* **Responsibility**: 선정 종목 수와 브로커 기술용량(LS: 200쌍)을 매핑하여 실제 구독할 `(symbol, stream)` 쌍을 유도하고, 중복 구독 방지 및 재접속 시 재생 목록 제공.
* **Input**: 종목 리스트, 벤더 용량 객체
* **Output**: 벤더별 구독 요청 쌍 매핑
* **Dependencies**: None
* **Key Implementation**:
  * [`SubscriptionPlanner.plan`](file:///home/kth/krx-alpha/src/realtime/contracts.py#L67-L93): 기술용량 초과 시 `SlotBudgetExceededError` 차단

### LS Realtime WebSocket Adapter
* **Responsibility**: LS증권 WebSocket 서버와 연결하여 OAuth2 토큰 발급, 180쌍 실시간 체결/호가 등록, PINGPONG 처리, 비동기 프레임 분류기 기반 역직렬화를 수행하여 표준 `L0Frame` 생성.
* **Input**: LS API Key/Secret, 구독 종목 쌍
* **Output**: 표준 `L0Frame` 비동기 제너레이션
* **Dependencies**: `aiohttp`
* **Key Implementation**:
  * [`LsRealtimeAdapter`](file:///home/kth/krx-alpha/src/realtime/adapters/ls.py#L27-L150): `_pending` 큐를 통한 끼어든 데이터 프레임 보존, 고유 `conn_id` 및 `conn_seq` 부여

### Realtime Streamer Loop
* **Responsibility**: 비동기 네트워크 루프 내에서 소켓 수신과 중단 신호(`asyncio.Event`) 간의 경합을 제어하고, 주기적(200건) 및 절단 시 저널 flush 보장.
* **Input**: `VendorAdapter`, `FrameSink`, `replay_pairs`
* **Output**: 저널 파일 디스크 플러시
* **Dependencies**: `asyncio`
* **Key Implementation**:
  * [`RealtimeStreamer.pump`](file:///home/kth/krx-alpha/src/realtime/streamer.py#L63-L96): `asyncio.FIRST_COMPLETED` 기반 안전 종료

---

## 5. Storage Subsystem (Layer 2)

### L0 Journal Writer
* **Responsibility**: 실시간 수신된 원문을 시간대별(Hourly) 파티션 디렉터리에 zstd 레벨 3 압축 JSONL 형태로 append-only 저장.
* **Input**: `L0Frame` 레코드
* **Output**: `data/l0/{vendor}/{stream}/dt=YYYY-MM-DD/{HH}.jsonl.zst`
* **Dependencies**: `zstandard`
* **Key Implementation**:
  * [`L0JournalWriter`](file:///home/kth/krx-alpha/src/storage/journal.py#L25-L70): 버퍼링 후 시간대별 자동 분기 쓰기

### Data Quality Barrier
* **Responsibility**: EOD 정규화 배치 시 체결 틱과 10단계 호가 데이터의 수학적·금융적 정합성을 검증하고 이상치 통계를 산출.
* **Input**: L0 파티션 역압축 Polars DataFrame
* **Output**: `TickQualitySummary`, `QuoteQualitySummary`
* **Dependencies**: `polars`
* **Key Implementation**:
  * [`decode_and_flag_ticks`](file:///home/kth/krx-alpha/src/storage/quality.py#L73-L204): 체결량 보존법칙, 가격제한폭(±30%), 거래량 역행, 틱 손실 검증
  * [`decode_and_flag_quotes`](file:///home/kth/krx-alpha/src/storage/quality.py#L238-L289): 20,000행 청크 단위 호가 사다리 단조성, 음수 잔량, 비동시호가 크로스북 검증

### Storage Retention & Quarantine Guard
* **Responsibility**: 3GB 여유 디스크 워터마크 강제, L0 원본의 L1 Parquet 정규화 성공 시에만 원본 저널 삭제(Offload-before-delete), 정규화 실패 파티션의 비파괴 격리(`quarantine/`).
* **Input**: 로컬 파티션 디렉터리 경로, 기준 일자
* **Output**: L1 Parquet 생성 및 로컬 파티션 안전 회수
* **Dependencies**: `shutil`, `polars`, `zstandard`
* **Key Implementation**:
  * [`check_disk_watermark`](file:///home/kth/krx-alpha/src/storage/retention.py#L34-L37): 3GB 미만 시 `StorageExhaustedError`
  * [`normalize_l0_partition`](file:///home/kth/krx-alpha/src/storage/retention.py#L39-L122): `conn_seq` 충돌 검증 및 L1 Parquet 원자적 생성
  * [`prune_old_journals`](file:///home/kth/krx-alpha/src/storage/retention.py#L124-L169): 검증 성공 시 삭제, 실패 시 `quarantine/` 이동

### Remote Archiver
* **Responsibility**: 정규화된 L1 Parquet 파일을 rclone CLI를 통해 Google Drive로 업로드하고, 원격 `lsjson` 크기 일치 확인 후 로컬 파일 정리.
* **Input**: 로컬 L1 Parquet 디렉터리
* **Output**: 원격 Google Drive 동기화 통계 (`uploaded`, `skipped`, `failed`, `purged`)
* **Dependencies**: `subprocess`, `rclone`
* **Key Implementation**:
  * [`RcloneArchiver.sync_l1_tree`](file:///home/kth/krx-alpha/src/storage/remote.py#L83-L104): L1 트리 동기화
  * [`RcloneArchiver.upload_and_verify`](file:///home/kth/krx-alpha/src/storage/remote.py#L56-L65): 바이트 크기 완전 일치 검증

---

## 6. Orchestration Subsystem (Layer 5, 6)

### Process Supervisor & Circuit Breaker
* **Responsibility**: 실시간 수집 서브프로세스(`collect-stream`)의 생존 상태를 감시하고, 비정상 종료 시 재시작 서킷브레이커(1800초 내 최대 5회)를 적용하여 무한 크래시 루프 방어. 정상 종료 시 SIGTERM(15초) 후 SIGKILL 에스컬레이션.
* **Input**: 실행 커맨드 리스트 (`cmd`)
* **Output**: 프로세스 상태 (`started`, `restarted`, `circuit_open`, `graceful`, `killed`)
* **Dependencies**: `subprocess`
* **Key Implementation**:
  * [`RestartCircuitBreaker`](file:///home/kth/krx-alpha/src/orchestration/supervisor.py#L10-L29): 슬라이딩 윈도우 기반 재시작 제한
  * [`ProcessSupervisor`](file:///home/kth/krx-alpha/src/orchestration/supervisor.py#L31-L69): 프로세스 생존 및 안전 종료 감독

### 24/7 Cloud Daemon
* **Responsibility**: 컨테이너 PID 1로 구동되어 KST 정규장 일정에 맞춰 장전 오케스트레이션, 장중 스트리머 서브프로세스 감독, 장마감 후 EOD 유지보수 및 당일 세션 갭(Reconciliation) 감사를 자율 수행.
* **Input**: `CollectorSettings`
* **Output**: 24/7 무인 자동 수집 및 감사 로그
* **Dependencies**: 모든 하위 서비스 레이어
* **Key Implementation**:
  * [`run_collector_daemon`](file:///home/kth/krx-alpha/src/orchestration/daemon.py#L130-L255): 24/7 상주 메인 루프
  * [`run_session_orchestration`](file:///home/kth/krx-alpha/src/orchestration/daemon.py#L66-L123): 일봉 갱신 + 유니버스 선정 유스케이스 결선
  * [`check_session_reconciliation`](file:///home/kth/krx-alpha/src/orchestration/eod.py#L48-L59): 당일 일봉 존재 시 매니페스트 부재(09-10형 사고) 감사

---

## 7. Execution Subsystem (Layer 1, 2, 3, 4, 5, 6)

### Pre-Trade Risk Gate
* **Responsibility**: 주문 의도(`OrderIntent`)가 거래소 호가단위(Tick Ladder)에 부합하는지, 킬스위치 파일이 존재하는지, 미체결 및 포지션 한도를 초과하는지, 일일 누적 손실 한도를 넘지 않는지 사전 검증하여 부적격 주문을 Fail-Closed 거부.
* **Input**: `OrderIntent`, `Quote`, `Ledger`, `ExecutionSettings`
* **Output**: `RejectCode` 검증 통과 여부
* **Dependencies**: [`src.execution.ticks`](file:///home/kth/krx-alpha/src/execution/ticks.py)
* **Key Implementation**:
  * [`evaluate_pre_trade_risk`](file:///home/kth/krx-alpha/src/execution/risk.py): 사전 리스크 평가 함수
  * [`krx_tick_size`](file:///home/kth/krx-alpha/src/execution/ticks.py#L14-L29): KRX 주식 가격대별 호가단위 계산

### Dual Order Gateways (Paper vs Live)
* **Responsibility**: 동일한 OMS 인터페이스 하에서 주문 전송 경로를 분기. Paper 모드에서는 실전과 동일한 KIS 주문 바디를 직렬화하여 저널에 기록하고 실시간 10단계 호가 잔량으로 모의체결. Live 모드에서는 `KRX_ALPHA_EXEC_LIVE_ARMED=true` 환경변수가 명시된 경우에만 KIS OpenAPI로 실제 발송.
* **Input**: `Order`, `Quote`
* **Output**: `BrokerOutcome`
* **Dependencies**: [`src.execution.kis_client`](file:///home/kth/krx-alpha/src/execution/kis_client.py), [`src.execution.journal`](file:///home/kth/krx-alpha/src/execution/journal.py)
* **Key Implementation**:
  * [`PaperGateway`](file:///home/kth/krx-alpha/src/execution/gateways.py#L70-L153): 실계좌 조회 + 모의체결(`paper_l10_sweep_v1`) + `paper_would_send` 저널링
  * [`LiveGateway`](file:///home/kth/krx-alpha/src/execution/gateways.py#L154-L199): 실전 KIS REST 전송기

### Order Management System (OMS) & Ledger
* **Responsibility**: 주문 상태 전이 규칙(`ALLOWED_TRANSITIONS`)을 엄격히 강제하고, 브로커 누적체결 대사(`derive_status`)를 통해 주문 상태를 결정론적으로 동기화하며 계좌 원장(현금, 보유종목, 실현손익)을 갱신.
* **Input**: `Order`, 브로커 체결 내역
* **Output**: 원장 갱신 및 주문 이벤트 저널
* **Dependencies**: [`src.execution.contracts`](file:///home/kth/krx-alpha/src/execution/contracts.py), [`src.execution.ledger`](file:///home/kth/krx-alpha/src/execution/ledger.py)
* **Key Implementation**:
  * [`transition`](file:///home/kth/krx-alpha/src/execution/contracts.py#L205-L212): 비정상 전이 시 `IllegalTransitionError` 발생
  * [`derive_status`](file:///home/kth/krx-alpha/src/execution/contracts.py#L214-L228): 브로커 누적 수량 기반 상태 도출
