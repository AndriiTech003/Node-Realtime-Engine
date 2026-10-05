FROM node:22-alpine AS build
WORKDIR /repo
RUN corepack enable
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps
RUN pnpm install --frozen-lockfile --filter "@ashamrai/realtime-loadgen..."
RUN pnpm --filter "@ashamrai/realtime-loadgen..." run build
RUN pnpm deploy --filter "@ashamrai/realtime-loadgen" --prod --legacy /out

FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /out ./
USER node
CMD ["node", "dist/cli.js"]
