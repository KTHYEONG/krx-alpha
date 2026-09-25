# 📋 AI 프롬프트: 아키텍처 및 README 전면 개편 가이드라인

> 본 문서는 임의의 퀀트/백엔드/데이터 엔지니어링 프로젝트를 **"면접관 및 기술 리더 관점에서 1분 만에 시스템의 깊이와 엔지니어링 설계를 파악할 수 있는 고밀도 쇼케이스"**로 전면 개편할 때, AI 어시스턴트에게 제공하는 표준 명령 프롬프트(Prompt)와 가이드라인입니다.

---

## 🚀 [복사해서 AI에게 전달할 프롬프트 전문]

```markdown
당신은 최고 수준의 시니어 소프트웨어 아키텍트이자 테크니컬 라이터입니다.
현재 프로젝트의 코드베이스와 기존 문서를 심층 분석하여, **면접관(CTO, 시니어 엔지니어, 퀀트 리서처 등)이 1분 안에 시스템의 가치, 신뢰성, 엔지니어링 깊이를 한눈에 직관적으로 파악할 수 있도록** `README.md`와 아키텍처 문서(`docs/architecture/`)를 전면 개편해주세요.

아래의 5대 핵심 원칙과 출력 템플릿을 엄격히 준수하여 작업해야 합니다.

---

### [원칙 1] 고밀도 정량 쇼케이스 (수치와 불변식 중심)
* 단순한 기능 나열이나 장황한 줄글 설명을 전면 배제합니다.
* 모든 성과와 신뢰성은 **정량적 수치(볼드/백틱 표기)**와 **아키텍처 불변식(이를 강제하는 코드/알고리즘 메커니즘)**의 1:1 매핑으로 증명합니다.
* 추상적인 자랑 대신, 시스템이 보장하는 불변식(Invariant)과 장애 차단 원리를 전면에 내세웁니다.

### [원칙 2] README.md 표준 5단계 구조 (150~200줄 내외 완결)
README.md는 스크롤 압박 없이 한눈에 들어오는 고밀도 쇼케이스로 작성합니다:
1. **Header & Badges**: 1줄 프로젝트 정의 및 6개 핵심 기술 배지.
2. **1. System Highlights**: `[핵심 엔지니어링 지표 | 실측 성과/보장 기준 | 아키텍처 불변식 및 강제 장치]` 3열 지표 카드 테이블.
3. **2. Tech Stack**: `[분류 | 기술 | 채택 근거 및 트레이드오프]` 매트릭스 표 (단순 스택 나열 금지).
4. **3. Daily Workflow & Pipeline**: 4단계 일별 라이프사이클 요약 표 + 5색 테마 Mermaid 파이프라인 다이어그램.
5. **4. Top 5 Real-world Engineering Invariants (핵심 챌린지)**:
   - 각 항목을 반드시 `🚨 문제(Problem) ➔ 📐 원칙(Principle) ➔ 💡 해결(Solution)`의 3줄 구조로 작성.
   - 실제 겪을 수 있는 데이터 누락, 클럭 오차, 배포 공백, 과적합 등의 현실 난제 극복기를 기술.
6. **5. Verified Performance Matrix (또는 검증 매트릭스)**: 기준 모델 대비 개선 모델의 실측 정본 성과 표.
7. **6. Architecture Layer Contracts**: 엄격한 계층 구조 및 정적 검증 명령어 안내.

### [원칙 3] 아키텍처 문서의 '단 2개 핵심 정본' 통폐합
파편화되어 방치되기 쉬운 다수의 문서를 과감히 정리하고, 오직 **2개의 핵심 정본 문서**로 통폐합합니다 (각 문서 파일당 300줄 이하 엄수):
1. `docs/architecture/system-design.md`:
   - 시스템 목표 및 비목표(In/Out of Scope 명시적 경계 설정).
   - 컴포넌트 토폴로지 및 외부 연동 인터페이스.
   - 24/7 상태머신 및 오케스트레이션 라이프사이클.
   - 데이터 모델 / 도메인 금융 무결성 배리어.
   - 엄격한 계층 구조(Layer 0 ~ N) 및 정적 불변식 규칙.
2. `docs/architecture/engineering-decisions.md` (ADR):
   - 1열 요약 매트릭스: `[ADR 번호 | 의사결정 주제 | 기각된 대안 | 채택된 솔루션 | 핵심 엔지니어링 근거 및 트레이드오프]` 표.
   - 5~8대 핵심 기술 결정 상세 (Decision, Why, Trade-off 3단 구성).
* 프로젝트와 무관한 단순 배포 런북이나 레거시 파편 문서는 단호히 삭제합니다.

### [원칙 4] 가독성 및 용어 정제 (면접관 친화적 어휘)
* **설명 없는 도메인 약어 금지**: 아무리 업계 표준이라도(예: MTM, MMR, Book, Embargo 등), 비전공자나 다른 도메인 면접관이 오해하지 않도록 **친절한 한글 맥락을 괄호나 수식어로 병기**합니다.
  - 예: `단위북/성장북` ➔ `기준 포트폴리오(단위북 1.0x) / 레버리지 포트폴리오(성장북 2.5x)`
  - 예: `MMR/cum` ➔ `유지증거금율(MMR) 및 누적 공제액(cum)`
  - 예: `MTM 평가` ➔ `실시간 마크 가격 시가평가(MTM)`
  - 예: `168h 엠바고` ➔ `168시간 시계열 엠바고(정보 누출 방지 유예)`
  - 예: `Schmitt-Trigger` ➔ `Top-60 진입 / 120위 방출 이중 임계값(Schmitt-Trigger 히스테리시스)`
* **기계적 직역투 제거**:
  - `단조시계/벽시계` ➔ `경과시간 계측용 단조시각 / 타임스탬프용 절대시각(UTC)`
  - `호가단조성` ➔ `호가 순차정렬(단조성, Monotonicity)`
  - `보존법칙` ➔ `체결량 보존법칙 (틱 누락 검출, Tick Loss)`
  - `이중 무장` ➔ `실전 주문 이중 안전 확인(Dual-Arming)`
  - `flock 백업` ➔ `파일 잠금(flock) 기반 원격 백업`

### [원칙 5] 기술적 구문 안전성 (절대 준수 규칙)
1. **로컬 절대 경로(file:///home/...) 전면 금지**:
   - 마크다운 문서 내에 로컬 머신의 파일 경로나 URI를 직접 링크하지 말고, 표준 백틱 인라인 코드(`` `SimulatedInventoryLedger` ``) 또는 저장소 상대 경로(`src/...`)로 표기합니다.
2. **Mermaid 엣지 레이블 구문 에러 방지**:
   - Mermaid 화살표 레이블 `-->|...|` 내부에는 **소괄호 `()`를 절대로 사용하지 마십시오** (파싱 에러 원인).
   - 잘못된 예: `-->|정상 (검증 완료)|` ❌
   - 올바른 예: `-->|정상 검증 완료|` ⭕
3. **Mermaid 5색 테마 클래스 적용**:
   - `vendor(#f1f3f5)`, `premarket/data(#e7f5ff)`, `intraday/research(#ebfbee)`, `eod/remote(#f3f0ff)`, `exec/live(#fff4e6)` 등 역할별 배경색(`classDef`)을 지정하여 다이어그램 가독성을 극대화합니다.
4. **문서 길이 제한 준수**:
   - 아키텍처 문서는 파일당 300줄 이하를 엄격히 유지합니다.

---

### [작업 절차]
1. 저장소의 실제 소스 코드(`src/`), 설정, 테스트 코드를 탐색하여 **시스템의 진짜 불변식과 핵심 지표**를 발굴하십시오.
2. 위의 [원칙 1~5]에 맞추어 `README.md`, `docs/architecture/system-design.md`, `docs/architecture/engineering-decisions.md`를 작성하십시오.
3. 파편화된 레거시 문서를 삭제하거나 통폐합하고, 코드맵(`code_map.json` 등)이 있다면 참조를 갱신하십시오.
4. 작성 완료 후 로컬 절대 경로(`file:///`) 잔여 여부와 프로젝트 테스트 스위트를 검증하여 완결성을 입증하십시오.
```

---

## 📊 부록: 표준 README.md 구성 골격 (Skeleton Reference)

```markdown
# [Project Name]

> **[프로젝트의 핵심 역할과 기술적 가치를 정의하는 1줄 선언]**

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![Engine](https://img.shields.io/badge/Engine-Polars-cd792c.svg)
![Storage](https://img.shields.io/badge/Storage-Parquet_&_zstd-4c1.svg)
![Concurrency](https://img.shields.io/badge/Concurrency-asyncio-darkgreen.svg)
![Architecture](https://img.shields.io/badge/Architecture-Contract_Guarded-blueviolet.svg)
![Deployment](https://img.shields.io/badge/Deployment-Docker_Compose-2496ed.svg)

---

## 1. System Highlights

| 핵심 엔지니어링 지표 | 실측 성과 / 보장 기준 | 아키텍처 불변식 및 강제 장치 |
| :--- | :---: | :--- |
| 📈 **[지표 1: 도메인 성과]** | **`수치`** | [이를 보장하는 아키텍처 원장/알고리즘 장치] |
| 🛡️ **[지표 2: 안정성/생존]** | **`0회`** | [장애 차단/브래킷/한도 제어 장치] |
| ⚡ **[지표 3: 성능/처리속도]** | **`단축 수치` (전 $\to$ 후)** | [증분/벡터화/인프로세스 최적화 기법] |
| 🚀 **[지표 4: 운영 연속성]** | **`공백 최소화 수치`** | [지문 기반 선택적 기동/유예 가드] |
| ⏱️ **[지표 5: 시계열 정합성]** | **`오차 한도`** | [Fail-Closed 차단/NTP 실측 가드] |
| 🔒 **[지표 6: 품질/리스크]** | **`0.00%`** | [AST 파싱/정적 불변식 강제] |

---

## 2. Tech Stack

| 분류 | 기술 | 채택 근거 및 트레이드오프 |
| :--- | :--- | :--- |
| **Language & Tooling** | `Python 3.x`, `uv` | [채택 이유 및 성능상 이점] |
| **Data Engine & Storage** | `Polars`, `Parquet`, `zstd` | [메모리/디스크 효율 및 벡터 연산 근거] |
| **Concurrency & Network** | `asyncio`, `websockets` | [실시간 비동기 무차단 펌프 처리 근거] |
| **Domain Engine** | `[핵심 모듈명]` | [현실적 제약 반영 및 무결성 보장] |
| **Infra & Security** | `Docker`, `[보안도구]` | [24/7 무인 안정 운용 및 자율 순환] |
| **Verification & Quality**| `pytest`, `Python AST` | [계층 위계 및 정적 불변식 기계적 강제] |

---

## 3. Daily Workflow & Pipeline

| 시각 | 단계 | 핵심 처리 내용 |
| :---: | :--- | :--- |
| 🌅 **[시각 1]** | **[준비/배치]** | [영업일 확인 $\to$ 데이터 수집 $\to$ 당일 유니버스 확정] |
| ⚡ **[시각 2]** | **[실시간/스트리밍]** | [무차단 수신 $\to$ 원시 데이터 저널 append-only 적재] |
| 🌙 **[시각 3]** | **[마감/품질검증]** | [데이터 무결성 배리어 검증 $\to$ 정규화 Parquet $\to$ 원격 백업] |
| 🛡️ **[시각 4]** | **[집행/모의체결]** | [사전 리스크 검증 $\to$ 모의 호가소진(Paper) 또는 실거래(Live)] |

```mermaid
flowchart TD
    classDef vendor fill:#f1f3f5,stroke:#495057,stroke-width:1px,color:#212529;
    classDef stage1 fill:#e7f5ff,stroke:#1971c2,stroke-width:2px,color:#0c4a6e;
    classDef stage2 fill:#ebfbee,stroke:#2f9e44,stroke-width:2px,color:#14532d;
    classDef stage3 fill:#f3f0ff,stroke:#7950f2,stroke-width:2px,color:#3b0764;
    classDef stage4 fill:#fff4e6,stroke:#f76707,stroke-width:2px,color:#7c2d12;

    %% 5색 테마 노드 구성 (레이블 화살표에 괄호 사용 금지!)
```

---

## 4. Top 5 Real-world Engineering Invariants (핵심 챌린지)

### 1. [챌린지 1 제목]
* 🚨 **문제**: [발생하는 기술적 결함 또는 현실 괴리]
* 📐 **원칙**: [타협할 수 없는 엔지니어링/도메인 설계 원칙]
* 💡 **해결**: [이를 해결한 구체적 구현체 및 메커니즘, 정량적 개선 결과]

### 2. [챌린지 2 제목]
...

---

## 5. Verified Performance Matrix (실측 정본 성과)

> **출처**: `[검증 데이터 파일 경로]`  
> **조건**: [실제 반영된 수수료, 슬리피지, 체결 모델 조건]

| 모델 / 파이프라인 | 방식 | 핵심 지표 1 | 핵심 지표 2 | 핵심 지표 3 |
| :--- | :---: | :---: | :---: | :---: |
| **기준 모델** | Base | **수치** | **수치** | 검증 결과 |
| **개선 모델** | Advanced | **수치** | **수치** | **개선 폭 (볼드)** |

---

## 6. Architecture Layer Contracts

```text
Layer N: CLI 진입점
   ↓
Layer N-1: 오케스트레이션 및 감독 엔진
   ↓
Layer N-2: 도메인 서비스 및 게이트웨이
   ↓
Layer 0: 코어 스키마 및 설정 기반
```

---

## 7. Quick Start & Verification

```bash
# 1. 의존성 설치 및 불변식 테스트 실행
uv sync
uv run pytest
```
```
