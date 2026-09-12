# Architectural Decision Records (ADRs)

본 문서는 `krx-alpha` 시스템의 핵심 기술적 결정사항(Architecture Decision Records)을 기록합니다. 과거 단순 수정 내역을 배제하고, 아키텍처 설계 배경, 대안 평가, 채택 이유 및 트레이드오프를 엄밀히 다룹니다.

---

## ADR-001: 2단계 유니버스 수집 정책 (일봉 배치 필터 + 장중 WS 스트리밍)

### Decision
국내 주식 전 종목(2,500+)을 실시간 스트리밍하지 않고, 장전(08:20) 공식 일봉 데이터를 바탕으로 단타 유니버스(최대 90종목)를 선별한 뒤, 해당 유니버스에 대해서만 실시간 체결(`H0STCNT0`) 및 10단계 호가(`H0STASP0`)를 웹소켓으로 수집한다.

### Context
* 한국 주식시장 전 종목의 L1 틱/호가 데이터를 상시 수집할 경우 일 수십 GB의 데이터가 발생하여 저사양 클라우드 VPS(50GB~200GB) 디스크를 수일 내 고갈시킴.
* 국내 증권사 리테일 OpenAPI는 커넥션당 웹소켓 등록 종목 수에 하드캡(LS증권: 약 100종목/200스트림 쌍, KIS: 41스트림 쌍)을 적용하고 있어 물리적으로 전종목 스트리밍이 불가.

### Alternatives
1. **전종목 실시간 틱 수집**: 다중 계정 및 수십 개 프로세스를 병렬 기동하여 전종목 분산 수집.
2. **상한가 종목만 단독 수집**: 당일 상한가 진입 종목만 타겟팅.
3. **일봉 배치 필터링 후 최대 90종목 웹소켓 수집**: 장전 모멘텀/유동성 지표 기반 선별.

### Selected Approach
대안 3 채택. 최근 60영업일 일봉을 바탕으로 상한가(`limit_up`), 10% 급등(`surge10`), 거래대금 급증(`volsurge`), 60일 신고가(`newhigh60`)의 4대 조건을 만족하고 거래대금 50억원 이상의 유동성을 확보한 종목군(일 평균 3~20종목, 최대 90종목)을 확정하여 LS 단일 커넥션(용량 200쌍)으로 전량 수용.

### Rationale
* LS증권의 단일 커넥션 용량(200쌍 = 체결+호가 100종목) 내에 100% 수용 가능하여 웹소켓 다중화에 따른 연결 관리 복잡도 제거.
* 연간 스토리지 발생량을 ~10GB 수준으로 통제하여 장기 보존 및 백테스트 비용 최소화.
* 유니버스 선정 시 미래 일봉 데이터 유입 시 즉시 중단되는 Look-Ahead 검증기 포함.

### Trade-offs
* 장 시작 후 장중에 갑작스럽게 테마/호재로 급등하는 종목(장전 기준 비적격 종목)의 틱 데이터는 수집 대상에서 제외됨.

---

## ADR-002: L0/L1 3계층 무결성 배리어 및 Offload-before-Delete 원칙

### Decision
실시간 틱 데이터는 수집 시 원문 그대로 L0 저널(JSONL.zst)에 append-only로 기록하고, 장마감 후(EOD) 배치 단계에서 틱 보존법칙/호가 정합성 검증 후 L1 Parquet로 변환하며, 원격 Google Drive로 업로드되어 바이트 크기가 일치 확인된 파티션만 로컬에서 순환 삭제한다.

### Context
* 실시간 수집 루프에서 고부하 JSON 파싱, 사다리 검증, Parquet 압축을 동시에 수행할 경우 이벤트 루프 블로킹으로 인한 웹소켓 버퍼 오버플로우 및 틱 드랍 위험 존재.
* 단순 기간 경과(`retain_days=3`) 기반의 무조건적 삭제를 적용할 경우, L1 변환 실패나 원격 백업 실패 시에도 원본 데이터가 영구 유실될 위험이 있음.

### Alternatives
1. **실시간 인메모리 정규화 및 즉시 Parquet 쓰기**: 수집과 동시에 L1 스키마로 변환.
2. **단순 파일 보존 후 수동 백업**: 로컬에 계속 쌓아두고 수동으로 외장 스토리지 이전.
3. **L0 원본 보존 → EOD 검증 및 L1 변환 → 원격 바이트 확인 후 로컬 삭제 (3계층)**.

### Selected Approach
대안 3 채택.
1. **1계층 (장중 수집)**: 무파싱 raw 문자열 + 나노초 시각 + 세션 시퀀스를 zstd 압축 저널에 append.
2. **2계층 (EOD 무결성 배리어)**: Polars 기반 벡터화 연산으로 누적체결량 보존법칙, 가격제한폭($\pm 30\%$), 호가 사다리 단조성 검증. 실패 파티션은 `quarantine/`으로 비파괴 격리.
3. **3계층 (스토리지 순환)**: rclone CLI를 통해 Google Drive로 업로드하고, `rclone lsjson`으로 원격 파일 크기가 로컬과 일치함을 확인한 경우에만 로컬 L0/L1을 순환 삭제.

### Rationale
* 장중 수집 루프의 CPU/메모리 부하를 최소화하여 틱 유실 0% 달성.
* L1 변환이나 네트워크 장애로 백업이 실패한 원본 저널은 절대 삭제되지 않으므로 데이터 영구 유실 위험 원천 차단.
* 오염된 데이터가 정상 L1 데이터셋에 섞이지 않고 격리 디렉터리에 보존되어 사후 분석 가능.

### Trade-offs
* EOD 시점에 L0 역압축, 데이터 검증, L1 Parquet 인코딩을 수행하기 위한 일시적 CPU 연산량이 요구됨.

---

## ADR-003: NTP 타임서버 오프셋 실측 기반 Fail-Closed 클럭 게이트

### Decision
실시간 스트리밍 세션 기동(`bootstrap_session`) 시 외부 표준 타임서버(`kr.pool.ntp.org`)와의 클럭 오프셋을 실측하고, 2초를 초과하거나 측정 실패 시 세션을 즉시 거부(`ClockUnsyncedError`)한다.

### Context
* WSL2 환경이나 클라우드 가상머신은 호스트 슬립/웨이크업 또는 하이퍼바이저 타임슬라이싱으로 인해 시스템 벽시계가 실제 시각과 수 초 이상 틀어지는 현상이 빈번히 발생 (실측 결과 WSL2에서 +1089ms 편차 관측).
* 고빈도 호가/체결 데이터에서 시스템 시간이 틀어질 경우, 이벤트 발생 시각 역전 및 타 데이터 소스와의 타임스탬프 결합 불일치가 발생하여 백테스트 신뢰성이 붕괴됨.

### Alternatives
1. **시스템 벽시계 무조건 신뢰**: 로컬 `time.time()`을 그대로 사용.
2. **클럭 오프셋 측정 후 소프트 경고 로그만 출력**: 오프셋이 커도 수집 계속 진행.
3. **클럭 오프셋 측정 후 2초 초과 시 Fail-Closed 거부**: 임계값 초과 시 기동 중단.

### Selected Approach
대안 3 채택. 세션 시작 시 ntplib을 통해 5개 샘플을 추출하여 중간값(median) 오프셋을 산출하고, 세션 매니페스트(`SessionManifest`)에 `clock_offset_ns`를 기록하며 허용 오차 초과 시 즉시 프로세스를 중단.

### Rationale
* 오염된 타임스탬프를 가진 틱 데이터가 수집되어 파이프라인 전체를 오염시키는 사태를 입구에서 원천 차단.
* 모든 틱 레코드에 단조 시계(`recv_mono_ns`)와 벽시계(`recv_wall_ns`)를 병기하여 정밀 시계열 분석 토대 마련.

### Trade-offs
* 외부 인터넷 단절 또는 NTP UDP 포트(123) 차단 환경에서는 수집 프로세스가 시작되지 못함.

---

## ADR-004: 단일 OMS 기반 Dual Gateway 분기 (Paper 섀도 vs Live 무장)

### Decision
주문 관리 시스템(OMS)과 리스크 게이트는 단일 인터페이스를 유지하고, 하위 전송 계층만 `PaperGateway`와 `LiveGateway`로 분기하며, Live 모드는 명시적 이중 무장 플래그(`KRX_ALPHA_EXEC_LIVE_ARMED=true`)를 필수로 요구한다.

### Context
* 실전 알고리즘 트레이딩 인프라에서 개발/검증 단계의 모의 로직과 실전 주문 로직이 별도 코드로 분리되어 있을 경우, 실전 배포 시 예상치 못한 구현 불일치로 금융 사고가 발생할 위험이 큼.
* 반대로 실전 코드가 실수로 실행되어 원치 않는 시장가 주문이 거래소로 전송되는 대참사를 방지해야 함.

### Alternatives
1. **환경별 완전 분리된 독립 모듈 개발**: paper_engine.py vs live_engine.py.
2. **동일 게이트웨이 내부에 `if is_paper:` 분기문 다수 배치**.
3. **단일 OMS 인터페이스 + Paper/Live 게이트웨이 전략 패턴 분기 + 엄격한 Pydantic 무장 가드**.

### Selected Approach
대안 3 채택.
* `OrderGateway` Protocol을 기반으로 `PaperGateway`와 `LiveGateway`를 구현.
* `PaperGateway`: KIS 실계좌 잔고 조회는 연동하되 실제 주문 REST 호출은 하지 않고, 실시간 10단계 호가 잔량을 소진하는 모의체결(`paper_l10_sweep_v1`)을 적용하며 실전 전송 바디를 `paper_would_send` 저널에 완벽히 기록.
* `LiveGateway`: KIS OpenAPI `TTTC0802U`(매수)/`TTTC0801U`(매도)를 실제로 호출. 단, `ExecutionSettings` 생성 시 `mode == LIVE and not live_armed`이면 `LiveNotArmedError`를 발생시켜 환경변수 없이는 인스턴스화 자체를 불가능하게 설계.

### Rationale
* 검증된 페이퍼 로직이 실전 전송기에서도 100% 동일한 주문 라이프사이클을 보장.
* 오작동으로 인한 실주문 발송 위험 0%.
* 주문 전 단계에서 호가단위(Tick Ladder), 킬스위치, 일일 손실 한도 등 동일한 `Pre-Trade Risk Gate` 통과 보장.

### Trade-offs
* 페이퍼 체결 모델은 호가창 내 내 주문의 대기열 우선순위(Queue Position) 및 시장 충격(Market Impact)을 완벽히 모사하지는 못함.

---

## ADR-005: AST 파싱 기반 아키텍처 불변식 테스트 및 설정 단일 소스

### Decision
모든 모듈 간 의존성 계층 랭크(Layer 0~7), 파일시스템 경로 리터럴 사용, 환경변수 접근을 `tests/architecture/` 내부의 pytest 단위 테스트에서 Python `ast` 모듈을 사용해 정적으로 파싱하여 강제한다.

### Context
* AI 페어 프로그래밍 및 대규모 리팩토링 과정에서 모듈 간 상위 레이어 역참조, 순환 의존(Circular Import), 하드코딩된 임의 경로 생성, 무분별한 `os.getenv` 분산이 발생하여 시스템 안정성을 저해하기 쉬움.

### Alternatives
1. **코드 리뷰 시 육안 확인**: 린터 규칙만 사용하고 구조는 수동 검토.
2. **런타임 임포트 검사**: 모듈 로드 시점에 동적 트리 확인.
3. **정적 AST 분석 기반 Pytest 아키텍처 가드**: 빌드/테스트 파이프라인에서 자동 차단.

### Selected Approach
대안 3 채택. `tests/architecture/test_layering.py`에 다음 불변식을 고정:
1. `test_no_upward_layer_dependency_in_src`: 함수 내부 지연 임포트를 포함하여 모든 `src.*` 임포트 간선 수집 후 하위->상위 참조 0건 강제.
2. `test_no_hardcoded_filesystem_paths_outside_config`: `src/core/config.py` 이외의 코드에서 `Path("...")` 문자열 리터럴 생성 시 테스트 실패.
3. `test_environment_access_confined_to_core_config`: `os.environ` / `os.getenv` 호출이 `src/core/config.py` 이외에 존재할 경우 테스트 실패.

### Rationale
* 개발자나 AI 어시스턴트의 실수로 인한 아키텍처 침식을 원천 차단.
* 모든 경로와 자격증명이 Pydantic 단일 클래스로 응집되어 배포 및 유지보수 용이.

### Trade-offs
* 새로운 모듈을 추가할 때 반드시 `tests/architecture/layers.py`의 `LAYER_RANK` 계약 테이블에 등재해야 함.

---

## ADR-006: 다중 벤더(토스·KRX·KIS) 결합 탄력적 오케스트레이션 및 폴백

### Decision
영업일 판정은 토스증권 메타데이터 API를 게이트로 활용하고, 일봉 수집은 KRX 공식 API를 1차로 하되 장애 시 KIS REST 일봉 조회로 1회성 폴백을 수행한다.

### Context
* KRX OpenData 시스템은 비정기 점검, 네트워크 절단, 공휴일 오응답 등이 발생할 수 있어 단일 벤더 의존 시 전체 무인 수집 파이프라인이 중단(fail-closed)됨.
* 주말이나 대체공휴일에 데몬이 오기동하여 과거 영업일 유니버스를 당일 실시간으로 오인 스트리밍하는 문제를 방지할 독립된 영업일 캘린더가 필요함.

### Alternatives
1. **KRX 단일 벤더 의존**: 실패 시 당일 전체 수집 포기.
2. **pykrx 등 비공식 스크래핑 라이브러리 사용**: 웹페이지 구조 변경 시 잦은 크래시 발생.
3. **토스 영업일 게이트 + KRX 공식 수집 + KIS 공식 일봉 폴백 결합**.

### Selected Approach
대안 3 채택.
* 토스 `/market-calendar/KR`을 통해 한국 거래소 통합 개장일 여부를 선제 검사하여 휴장일은 즉시 스킵. 토스 API 장애 시에는 소프트 열화(None)되어 파이프라인의 새로운 단일 장애점(SPOF)이 되지 않음.
* KRX 일봉 수집 실패 또는 Stale 감지 시 한국투자증권 `FHKST03010100` REST API로 직전 유니버스에 대한 일봉 데이터를 1회 폴백 수집.

### Rationale
* 공식 인가된 증권사 API만을 조합하여 안정성 확보 (스크래핑 배제).
* 특정 벤더의 일시적 장애가 전체 트레이딩 파이프라인의 전면 중단으로 이어지지 않도록 다중화.

### Trade-offs
* 여러 벤더의 API 키(`KRX_OPENAPI_KEY`, `TOSS_APP_KEY`, `KIS_APP_KEY`) 설정이 요구됨.
