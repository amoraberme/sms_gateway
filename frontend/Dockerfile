FROM node:20-alpine AS builder
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY . .
ARG BACKEND_INTERNAL_URL=http://gateway-api:8000
ENV BACKEND_INTERNAL_URL=$BACKEND_INTERNAL_URL
RUN pnpm run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
RUN corepack enable
COPY --from=builder /app ./
EXPOSE 3000
CMD ["pnpm", "run", "start", "--", "-H", "0.0.0.0", "-p", "3000"]
