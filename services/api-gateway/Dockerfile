FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm ci

COPY src ./src
RUN npm run build

# Production Stage
FROM node:20-alpine

WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

ENV PORT=3000
ENV HOST=0.0.0.0
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

EXPOSE 3000

CMD ["node", "dist/server.js"]
