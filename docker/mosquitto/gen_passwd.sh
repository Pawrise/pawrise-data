#!/usr/bin/env bash
# Génère docker/mosquitto/passwd (mots de passe hachés) pour les comptes MQTT `bridge` et `iot`.
# Lit MQTT_BRIDGE_PASSWORD et MQTT_IOT_PASSWORD depuis l'environnement ou le fichier .env à la racine.
# Sans valeur : mots de passe de développement local (bridge-dev / iot-dev).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$DIR/../../.env"
if [ -f "$ENV_FILE" ]; then
  set -a; . "$ENV_FILE"; set +a
fi
export BRIDGE_PWD="${MQTT_BRIDGE_PASSWORD:-bridge-dev}"
export IOT_PWD="${MQTT_IOT_PASSWORD:-iot-dev}"

rm -f "$DIR/passwd"
docker run --rm -v "$DIR":/work -e BRIDGE_PWD -e IOT_PWD eclipse-mosquitto:2 sh -c '
  mosquitto_passwd -b -c /work/passwd bridge "$BRIDGE_PWD" &&
  mosquitto_passwd -b /work/passwd iot "$IOT_PWD" &&
  chmod 0644 /work/passwd'
echo "docker/mosquitto/passwd généré (comptes : bridge, iot)."
