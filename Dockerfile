# Production-ready Node 24 image with native node:sqlite support
FROM node:24-alpine

WORKDIR /app

# Copy package files
COPY package.json ./

# Copy source code and public web dashboard
COPY src/ ./src/
COPY public/ ./public/

# Ensure directory for durable SQLite database volume exists
RUN mkdir -p /app/data

# Expose web dashboard & API port
EXPOSE 3000

ENV NODE_ENV=production
ENV PORT=3000

# Mountable volume for data persistence across container redeploys
VOLUME ["/app/data"]

CMD ["node", "src/server.js"]
