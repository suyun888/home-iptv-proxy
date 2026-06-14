#!/usr/bin/env bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()   { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERR ]${NC} $*"; exit 1; }

INSTALL_DIR="${INSTALL_DIR:-/opt/home-iptv-proxy}"
COMPOSE_FILE="${INSTALL_DIR}/docker-compose.yml"
CONFIG_DIR="${INSTALL_DIR}/config"
CONFIG_FILE="${CONFIG_DIR}/sources.yaml"
IMAGE_NAME="${IMAGE_NAME:-ghcr.io/suyun888/home-iptv-proxy:latest}"
WATCHTOWER_IMAGE="${WATCHTOWER_IMAGE:-containrrr/watchtower:latest}"
XHS_IMAGE="${XHS_IMAGE:-iptvtop/xhsuhd:latest}"
PORT="${PORT:-28788}"
XHS_ENV_FILE="${CONFIG_DIR}/xhsuhd.env"

SUDO=""
if [ "$(id -u)" != "0" ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
  else
    error "当前不是 root，且系统未安装 sudo"
  fi
fi

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || error "缺少命令: $1"
}

write_compose() {
  $SUDO mkdir -p "$CONFIG_DIR"
  $SUDO tee "$COMPOSE_FILE" >/dev/null <<EOF
services:
  xhsuhd:
    image: ${XHS_IMAGE}
    container_name: xhsuhd
    restart: always
    ports:
      - "34567:34567"
    env_file:
      - ${XHS_ENV_FILE}

  home-iptv-proxy:
    image: ${IMAGE_NAME}
    container_name: home-iptv-proxy
    restart: always
    ports:
      - "${PORT}:8787"
    volumes:
      - ${CONFIG_DIR}:/app/config
      - /var/run/docker.sock:/var/run/docker.sock
      - ${COMPOSE_FILE}:/app/deploy/docker-compose.yml:ro
    environment:
      IPTV_CONFIG: /app/config/sources.yaml
      IPTV_IMAGE_NAME: ${IMAGE_NAME}
      IPTV_AUTO_UPDATE_ENABLED: "true"
      IPTV_UPDATE_COMMAND: docker-compose -f /app/deploy/docker-compose.yml pull && docker-compose -f /app/deploy/docker-compose.yml up -d
      IPTV_XHS_APPLY_COMMAND: docker-compose -f /app/deploy/docker-compose.yml up -d xhsuhd

  watchtower:
    image: ${WATCHTOWER_IMAGE}
    container_name: home-iptv-proxy-watchtower
    restart: always
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      DOCKER_API_VERSION: "1.40"
    command: home-iptv-proxy --cleanup --interval 3600
EOF
}

write_config() {
  if [ -f "$CONFIG_FILE" ]; then
    log "保留现有配置: $CONFIG_FILE"
    return
  fi

  local secret
  secret="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32)"
  $SUDO tee "$CONFIG_FILE" >/dev/null <<EOF
bind: 0.0.0.0:8787
public_base_url: null
epg_source_url: null
epg_proxy_url: null
epg_cache_minutes: 720
epg_cache_dir: /app/config/cache
recordings_dir: /app/config/recordings
refresh_minutes: 30
user_agent: "home-iptv-proxy/0.1"
signing_secret: "${secret}"
sources:
  - name: "xhsuhd"
    url: "http://xhsuhd:34567/xhslist.m3u"
    proxy_url: null
    enabled: true
EOF
  log "已生成默认配置: $CONFIG_FILE"
}

write_xhs_env() {
  if [ -f "$XHS_ENV_FILE" ]; then
    log "保留现有 xhsuhd 凭证文件: $XHS_ENV_FILE"
    return
  fi

  $SUDO tee "$XHS_ENV_FILE" >/dev/null <<EOF
XHS_A1=${XHS_A1:-}
XHS_WEB_SESSION=${XHS_WEB_SESSION:-}
EOF
  log "已生成 xhsuhd 凭证文件: $XHS_ENV_FILE"
}

do_install() {
  need_cmd docker
  docker compose version >/dev/null 2>&1 || error "当前 Docker 不支持 docker compose"
  write_compose
  write_config
  write_xhs_env
  $SUDO docker compose -f "$COMPOSE_FILE" pull
  $SUDO docker compose -f "$COMPOSE_FILE" up -d
  echo
  echo -e "${GREEN}${BOLD}安装完成${NC}"
  echo -e "配置文件: ${CYAN}${CONFIG_FILE}${NC}"
  echo -e "播放列表: ${CYAN}http://<你的主机IP>:${PORT}/list.m3u${NC}"
  echo -e "健康检查: ${CYAN}http://<你的主机IP>:${PORT}/health${NC}"
}

do_update() {
  need_cmd docker
  $SUDO docker compose -f "$COMPOSE_FILE" pull
  $SUDO docker compose -f "$COMPOSE_FILE" up -d
}

do_uninstall() {
  need_cmd docker
  if [ -f "$COMPOSE_FILE" ]; then
    $SUDO docker compose -f "$COMPOSE_FILE" down || true
  fi
  warn "容器已停止。配置目录保留在: $CONFIG_DIR"
}

case "${1:-install}" in
  install) do_install ;;
  update) do_update ;;
  uninstall) do_uninstall ;;
  *)
    echo "用法: bash install.sh [install|update|uninstall]"
    exit 1
    ;;
esac
