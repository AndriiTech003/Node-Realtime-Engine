FROM node:22-alpine AS build
WORKDIR /repo
RUN corepack enable
COPY pnpm-workspace.yaml package.json pnpm-lock.yaml tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps
RUN pnpm install --frozen-lockfile --filter "@ashamrai/pulse-rooms..."
RUN pnpm --filter "@ashamrai/realtime-protocol" --filter "@ashamrai/realtime-client" run build
RUN pnpm --filter "@ashamrai/pulse-rooms" run build

FROM nginx:1.27-alpine
COPY apps/demo/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /repo/apps/demo/dist /usr/share/nginx/html
EXPOSE 4321
