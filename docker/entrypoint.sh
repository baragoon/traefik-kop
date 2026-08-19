#!/bin/sh
# entrypoint: select correct traefik-kop binary based on runtime arch
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64)
    BIN=/opt/traefik-kop/amd64/traefik-kop
    ;;
  aarch64|arm64)
    BIN=/opt/traefik-kop/arm64/traefik-kop
    ;;
  *)
    echo "Unsupported architecture: $ARCH" 1>&2
    exit 2
    ;;
esac
if [ ! -x "$BIN" ]; then
  echo "Executable not found for arch $ARCH: $BIN" 1>&2
  ls -la /opt/traefik-kop || true
  exit 3
fi
exec "$BIN" "$@"
