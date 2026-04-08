FROM node:20-bookworm-slim
WORKDIR /app
COPY . .
ENV NODE_ENV=production
ENV PORT=10000
EXPOSE 10000
CMD ["node", "server.js"]
