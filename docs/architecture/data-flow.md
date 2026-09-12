# End-to-End Data Flow Specification

본 문서는 `krx-alpha` 시스템 내에서 외부 시장 데이터가 유입되어 저장, 정규화, 유니버스 선정, 실시간 스트리밍, 원격 백업 및 주문집행에 이르기까지의 전 데이터 흐름과 스키마 계약을 상세히 정의합니다.

---

## 1. End-to-End Data Pipeline Flow

```mermaid
flowchart TD
    subgraph S1 [Stage 1: Daily Bars Ingestion]
        KRX_RAW[KRX OpenData REST<br/>KOSPI/KOSDAQ 당일 매매정보] -->|OutBlock_1 JSON| BARS_PARSE[krx_bars.fetch_daily_bars<br/>시장 완결성 & 90% 행수 검증]
        BARS_PARSE -->|DataFrame| BARS_UPSERT[krx_bars.append_daily_bars<br/>(date, symbol) 멱등 upsert]
        BARS_UPSERT --> BARS_STORE[(data/bars/daily.parquet<br/>zstd 압축 시계열 일봉)]
    end

    subgraph S2 [Stage 2: Universe & Candidate Generation]
        BARS_STORE -->|최근 60영업일 슬라이스| FEAT_ENG[policy.compute_selection_features<br/>tv_median_20, close_max_60, tv_ratio]
        FEAT_ENG -->|피처 데이터프레임| UNIV_SEL[policy.select_universe<br/>상한가/급등/거래대금/신고가 필터<br/>+ 50억 유동성 하한<br/>+ lookahead 차단 가드]
        UNIV_SEL -->|선정 결과| UNIV_PARQUET[(data/universe/YYYY-MM-DD.parquet)]
        UNIV_SEL -->|원자적 IPC 기록| CAND_JSON[(data/candidates.json<br/>최대 90종목 슬롯 예산 강제)]
    end

    subgraph S3 [Stage 3: Realtime Tick Streaming]
        CAND_JSON -->|선정 종목 로드| REGISTRY[subscription.SubscriptionRegistry<br/>LS 180 스트림 쌍 유도]
        REGISTRY --> WS_STREAM[adapters.ls.LsRealtimeAdapter<br/>WSS 수신 + 프레임 분류기]
        WS_STREAM -->|L0Frame DTO| STREAM_PUMP[streamer.RealtimeStreamer<br/>200건 단위 또는 종료시 flush]
        STREAM_PUMP --> L0_STORE[(data/l0/ls/H0ST*/dt=YYYY-MM-DD/HH.jsonl.zst<br/>시간대별 원본 JSONL)]
    end

    subgraph S4 [Stage 4: Post-Market Normalization & DQ Barrier]
        L0_STORE -->|시간대별 청크 병합| NORM_DECOMP[retention.normalize_l0_partition<br/>zstd 해제 + conn_seq 충돌 검증]
        NORM_DECOMP --> DQ_BARRIER[quality.decode_and_flag_ticks / quotes<br/>체결 보존법칙, 틱 손실, 호가 사다리 검증]
        DQ_BARRIER -->|정상 검증| L1_STORE[(data/l1/ls/H0ST*/dt=YYYY-MM-DD.parquet<br/>정규화 L1 데이터셋)]
        DQ_BARRIER -.->|정규화 실패시| QUARANTINE[(data/quarantine/<br/>비파괴 원본 격리)]
    end

    subgraph S5 [Stage 5: Remote Offload & Storage Circulation]
        L1_STORE --> RCLONE_PUT[remote.RcloneArchiver<br/>rclone copyto gdrive]
        RCLONE_PUT --> RCLONE_VERIFY[rclone lsjson 바이트 크기 대조]
        RCLONE_VERIFY -->|검증 성공| LOCAL_PRUNE[retention.prune_local_l1<br/>30일 초과 로컬 L1 정리]
        L1_STORE --> L0_PRUNE[retention.prune_old_journals<br/>3일 초과 로컬 L0 정리]
    end

    subgraph S6 [Stage 6: Order Execution Flow]
        ORDER_INTENT[OrderIntent<br/>symbol, side, qty, type, price] --> RISK_CHECK[risk.evaluate_pre_trade_risk<br/>호가단위, 킬스위치, 잔고, 손실한도]
        RISK_CHECK --> GATEWAY_DISPATCH{실행 모드}
        GATEWAY_DISPATCH -->|paper| PAPER_SIM[gateways.PaperGateway<br/>10단계 호가 모의체결<br/>+ paper_would_send 저널]
        GATEWAY_DISPATCH -->|live| LIVE_REST[gateways.LiveGateway<br/>KIS OpenAPI REST POST]
        PAPER_SIM --> OMS_UPDATE[oms.OMS & ledger.Ledger<br/>체결/주문 상태 갱신]
        LIVE_REST --> OMS_UPDATE
    end
```

---

## 2. Pipeline Stages Specification

| Stage | Input | Processing | Output | Main Module |
| :--- | :--- | :--- | :--- | :--- |
| **0. Trading Day Gate** | `dt.date` (현재일) | 토스 OpenAPI 캘린더 조회 (`/market-calendar/KR`), 통합개장 여부 확인 | `TradingDay` (영업일, 이전/다음 영업일) | [`src.marketdata.toss_calendar`](file:///home/kth/krx-alpha/src/marketdata/toss_calendar.py) |
| **1. Daily Bars Refresh** | KRX API (`KOSPI`/`KOSDAQ`) 또는 KIS 일봉 폴백 | 시장별 빈 응답 검사, 직전일 대비 행수 90% 이상 검증, `(date, symbol)` 기준 중복 방지 멱등 병합 | `data/bars/daily.parquet`, `data/market_map.json` | [`src.marketdata.krx_bars`](file:///home/kth/krx-alpha/src/marketdata/krx_bars.py), [`src.marketdata.service`](file:///home/kth/krx-alpha/src/marketdata/service.py) |
| **2. Feature & Universe Plan** | `daily.parquet` | 20일 거래대금 중앙값(`tv_median_20`), 60일 최고종가(`close_max_60`), 당일 거래대금 비율(`tv_ratio`) 산출 후 4대 조건 선정 및 90슬롯 제한 강제 | `data/universe/YYYY-MM-DD.parquet`, `data/candidates.json` | [`src.universe.policy`](file:///home/kth/krx-alpha/src/universe/policy.py), [`src.universe.service`](file:///home/kth/krx-alpha/src/universe/service.py) |
| **3. Realtime Stream Bootstrap** | `candidates.json`, NTP 서버 | NTP 오프셋 실측 (`max_clock_offset_ns=2.0s`), LS증권 기술용량(200쌍) 기준 구독 계획 수립, 매니페스트 초기화 | `SessionManifest`, L0 저널 핸들러 준비 | [`src.realtime.clock`](file:///home/kth/krx-alpha/src/realtime/clock.py), [`src.realtime.session`](file:///home/kth/krx-alpha/src/realtime/session.py) |
| **4. Realtime Ingestion Loop** | LS증권 WebSocket 스트림 | PINGPONG 에코, 비동기 프레임 분류기(`_is_data_frame`) 및 지연 ACK 큐 처리, 200건 단위 flush | `data/l0/{vendor}/{stream}/dt=YYYY-MM-DD/{HH}.jsonl.zst` | [`src.realtime.adapters.ls`](file:///home/kth/krx-alpha/src/realtime/adapters/ls.py), [`src.realtime.streamer`](file:///home/kth/krx-alpha/src/realtime/streamer.py) |
| **5. Post-Market DQ Barrier** | L0 JSONL.zst 파티션 | zstd 역압축, `(conn_id, conn_seq)` 충돌 검증, Polars 체결 틱/호가 사다리 이상치 벡터 검증, 비정상 파티션 격리 | `data/l1/{vendor}/{stream}/dt=YYYY-MM-DD.parquet`, `data/quarantine/` | [`src.storage.quality`](file:///home/kth/krx-alpha/src/storage/quality.py), [`src.storage.retention`](file:///home/kth/krx-alpha/src/storage/retention.py) |
| **6. Offload & Retention** | L1 Parquet, L0 저널 | Google Drive rclone 업로드 후 원격 파일 바이트 크기 일치 확인, 3일 경과 L0 삭제, 30일 경과 L1 정리 | 원격 Google Drive 동기화, 로컬 디스크 공간 회수 | [`src.storage.remote`](file:///home/kth/krx-alpha/src/storage/remote.py), [`src.orchestration.eod`](file:///home/kth/krx-alpha/src/orchestration/eod.py) |
| **7. Order Execution & OMS** | `OrderIntent`, 최신 호가 | KRX 호가단위(Tick Ladder) 정합성, 일일 손실 한도, 킬스위치 검증 후 Paper 10단계 호가 모의체결 또는 Live KIS API 발송 | `OrderRecord`, 체결 저널 (`execution/journal/`) | [`src.execution.risk`](file:///home/kth/krx-alpha/src/execution/risk.py), [`src.execution.gateways`](file:///home/kth/krx-alpha/src/execution/gateways.py), [`src.execution.oms`](file:///home/kth/krx-alpha/src/execution/oms.py) |

---

## 3. Data Schema & Contracts

### 3.1 Daily Bars Store Schema (`data/bars/daily.parquet`)
일봉 데이터는 수집 시점에 엄격한 Polars 데이터 타입으로 강제 캐스팅됩니다.

| Column | Polars Type | Description | Invariant / Validation |
| :--- | :--- | :--- | :--- |
| `date` | `pl.Date` | 거래 일자 (KST 기준) | ISO 8601 포맷 |
| `symbol` | `pl.String` | 6자리 단축 종목코드 | 예: `"005930"` |
| `close` | `pl.Float64` | 당일 종가 (원) | `close > 0` |
| `volume` | `pl.Int64` | 누적 거래량 (주) | `volume >= 0` |
| `trade_value_100m` | `pl.Float64` | 거래대금 (억원) | KRX 원본(원)에서 $10^8$ 단위 변환 |
| `daily_change_pct` | `pl.Float64` | 전일대비 등락률 (%) | KRX 가격제한폭 내: $\|daily\_change\_pct\| \le 31.0\%$ |
| `market` | `pl.String` | 소속 시장 | `"KOSPI"` 또는 `"KOSDAQ"` |

### 3.2 Universe IPC Schema (`data/candidates.json`)
장전 유니버스 선정 결과는 스캐너 프로세스 간 원자적 교환(atomic rename)을 위해 JSON 형식으로 기록됩니다.

```json
{
  "rev": 20260912,
  "candidates": [
    {
      "symbol": "005930",
      "selection_reasons": ["volsurge", "newhigh60"]
    }
  ]
}
```

* `selection_reasons` 허용 태그:
  * `"limit_up"`: 당일 등락률 $\ge 29.0\%$
  * `"surge10"`: 당일 등락률 $\ge 10.0\%$
  * `"volsurge"`: $tv\_ratio \ge 5.0$ 이고 당일 등락률 $\ge 5.0\%$
  * `"newhigh60"`: 당일 종가가 최근 60일 최고가에 도달하고 당일 등락률 $\ge 5.0\%$
  * **공통 필수 조건**: `trade_value_100m >= 50.0` (유동성 하한 50억원)
  * **예산 상한**: 선정 종목 수 $\le 90$ (`SlotBudgetExceededError` Fail-Closed)

### 3.3 Raw Tick Frame DTO (`L0Frame`) & L0 Journal
실시간 웹소켓으로 수신된 원시 데이터는 파싱/가공 없이 메타데이터와 함께 zstd 압축 JSONL에 보존됩니다.

```python
@dataclass(frozen=True)
class L0Frame:
    vendor: str         # 벤더 식별자 ("ls")
    stream: str         # 표준 스트림명 ("H0STCNT0", "H0STASP0")
    symbol: str         # 6자리 종목코드
    raw: str            # 벤더 수신 원문 문자열 (JSON)
    recv_mono_ns: int   # 시스템 모노토닉 나노초 (지연시간 계측용)
    recv_wall_ns: int   # 시스템 벽시계 나노초 (UTC Epoch ns)
    conn_seq: int       # 해당 웹소켓 세션 내 1부터 단조 증가하는 시퀀스 번호
    conn_id: str        # 접속 고유 식별자 (예: "ls-1757667200000000000")
```

### 3.4 L1 Parquet Schema (`data/l1/{vendor}/{stream}/dt=YYYY-MM-DD.parquet`)
EOD 배치 단계에서 동일 `(conn_id, conn_seq)`로 중복을 제거하고 시간순 정렬한 L1 데이터셋입니다.

| Column | Polars Type | Description |
| :--- | :--- | :--- |
| `recv_mono_ns` | `pl.Int64` | 수신 모노토닉 타임스탬프 (단조 시계) |
| `recv_wall_ns` | `pl.Int64` | 수신 벽시계 타임스탬프 (UTC ns) |
| `conn_seq` | `pl.Int64` | 세션별 단조 증가 시퀀스 번호 |
| `raw` | `pl.String` | 원본 JSON 페이로드 문자열 |
| `conn_id` | `pl.String` | 웹소켓 연결 고유 식별자 |
| `vendor` | `pl.String` | 벤더명 (`"ls"`) |
| `tr_id` | `pl.String` | 스트림 식별자 (`"H0STCNT0"`, `"H0STASP0"`) |

---

## 4. Financial & Microstructure Correctness Invariants

### 4.1 Time Conventions & Clock Synchronization
1. **Timezone**: 모든 거래소 일정, 장 개폐 시각, 일별 파티셔닝은 `Asia/Seoul` (KST, UTC+9)을 기준으로 합니다.
2. **Dual Timestamps**: 각 틱 메시지는 레이턴시 측정을 위한 `recv_mono_ns`(`time.monotonic_ns()`)와 시계열 정렬을 위한 `recv_wall_ns`(`time.time_ns()`)를 동시에 기록합니다.
3. **NTP Clock Gate**: WSL2 가상머신 또는 컨테이너의 시간 편차(+1000ms 이상)가 호가 순서 왜곡을 유발하는 것을 방지하기 위해, 세션 기동 시 `kr.pool.ntp.org`의 중간값(Median) 오프셋을 실측하여 2초 초과 시 세션 진입을 즉시 거부합니다.

### 4.2 Look-Ahead Bias Prevention
* `select_universe` 함수는 인자로 전달된 `decision_date`를 초과하는 미래 일봉이 입력 데이터셋에 1행이라도 존재할 경우 즉시 `ValueError("lookahead: max date exceeds decision_date")`를 발생시키며 즉시 중단됩니다.
* 장전 08:20에 산출되는 롤링 피처(`tv_median_20`, `close_max_60`)는 strictly $T-1$ 거래일 종가까지의 데이터만을 사용합니다.

### 4.3 Tick Conservation Law & Ladder Validation
배치 L1 정규화 단계(`quality.py`)에서 벤더 스트림에 대한 수학적 불변식을 벡터 연산으로 검증합니다:

1. **체결량 보존법칙 (Tick Loss Check)**:
   * 벤더가 여러 체결을 1건으로 묶어 보낼 때의 오탐을 방지하기 위해, 누적 체결건수 델타($\Delta checnt \le 1$)인 구간에서 누적 거래량 증가량($\Delta volume$)이 당일 체결량($cvolume$)을 초과하는지 검사합니다.
   * 초과 시 `dq_tick_loss` 및 유실량(`lost_volume`)을 계측하여 통계로 기록합니다.
2. **가격제한폭 검증**:
   * 전일대비 부호(`sign`)와 전일대비 가격(`change`)으로 역산한 기준가 대비 틱 체결가가 $\pm 30\%$ 밴드 내에 위치하는지 검증합니다.
3. **호가 사다리 단조성 (Ladder Monotonicity)**:
   * 매도호가 10단계: $offerho_1 < offerho_2 < \dots < offerho_{10}$
   * 매수호가 10단계: $bidho_1 > bidho_2 > \dots > bidho_{10}$
   * 각 호가 잔량이 음수이거나($rem < 0$), 동시호가 시간대(08:30~09:00, 15:20~15:30) 외에서 최우선 매수호가가 최우선 매도호가 이상인 크로스북($bidho_1 \ge offerho_1$) 발생 시 품질 경고를 기록합니다.
