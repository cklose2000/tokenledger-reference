FROM python:3.11.5-slim-bookworm@sha256:edaf703dce209d774af3ff768fc92b1e3b60261e7602126276f9ceb0e3a96874
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
ENV PYTHONUTF8=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /workspace
