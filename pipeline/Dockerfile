#Docker instruction
#uv를 통해서 가상환경도 만들고 package도 uv add로 설치하면 pyproject.toml에 자동으로 입력이 됨.
#미리 env path도 정의해두면 uv run을 entrypoint에 넣을 필요도 없음.
#uv.lock 파일에 dependencies 정의, python script를 copy해서 local과 완전히 다른 환경에서 실행가능


# base Docker image that we will build on
FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern) -> available to use uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
# set up the working directory inside the container
WORKDIR /app
# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"
# Copy dependency files first to current directory (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# copy the script to the container. 1st name is source file, 2nd is destination
COPY pipeline.py pipeline.py

# define what to do first when the container runs
# in this example, we will just run the script
ENTRYPOINT ["python", "pipeline.py"]