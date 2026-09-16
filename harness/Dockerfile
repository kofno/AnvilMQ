FROM oven/bun:1.2.14 AS runner
WORKDIR /app/client
COPY client/package.json client/bun.lock ./
RUN bun install --frozen-lockfile
COPY client/src ./src
COPY proto /app/proto
COPY harness /app/harness
ENTRYPOINT ["bun", "/app/harness/runner.ts"]
CMD ["smoke"]
