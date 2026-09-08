FROM python:3.11-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul \
    UV_LINK_MODE=copy

# OS 패키지 설치 (시간 동기화 및 기본 도구)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    ca-certificates \
    curl \
    && ln -fs /usr/share/zoneinfo/Asia/Seoul /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

# 의존성 캐싱 레이어: pyproject/uv.lock 변경 시에만 설치
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .
RUN uv sync --frozen --no-dev

# 24/7 수집 데몬을 PID 1로 구동 (exec form)
CMD ["uv", "run", "python", "-m", "src.collector.daemon"]
