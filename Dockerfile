# syntax=docker/dockerfile:1.7
# Pin base and tool images: an upstream release must not break an unrelated deploy.
FROM python:3.11.16-slim-trixie AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.20 /uv /uvx /bin/

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Seoul \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/root/.cache/uv

# OS 패키지 설치 (시간 동기화 및 기본 도구)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    ca-certificates \
    curl \
    rclone \
    libgomp1 \
    && ln -fs /usr/share/zoneinfo/Asia/Seoul /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

# 의존성 캐싱 레이어: pyproject/uv.lock 변경 시에만 설치
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked uv sync --frozen --no-dev --no-install-project

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv,sharing=locked uv sync --frozen --no-dev

# 24/7 수집 데몬을 PID 1로 구동 (exec form, --no-dev 로 불필요한 개발 도구 제외)
CMD ["/app/.venv/bin/python", "-m", "src.orchestration.daemon"]
