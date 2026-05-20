#!/bin/sh
# Generates a self-signed CA per cluster plus broker and client mTLS material.
# Idempotent: skips work if the marker file is already present.
#
# Outputs (per cluster X = a|b) into ${CERTS_DIR}/cluster-X/:
#   ca.crt, ca.key                        — cluster CA
#   kafka-X-keystore.jks                  — broker keystore (server cert + key + CA)
#   kafka-X-truststore.jks                — broker truststore (cluster CA only)
#   client-X-keystore.jks                 — client keystore (client cert + key + CA)
#   client-X-truststore.jks               — client truststore (cluster CA only)
#   admin.properties                      — kafka-clients config for admin tools
set -eu

CERTS_DIR="${CERTS_DIR:-/certs}"
PASS="${KEYSTORE_PASS:-kafka123}"
DAYS="${VALIDITY_DAYS:-3650}"

mkdir -p "$CERTS_DIR"

if [ -f "$CERTS_DIR/.generated" ]; then
  echo "[certs] Material already exists in $CERTS_DIR — skipping."
  exit 0
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "[certs] Installing openssl..." >&2
  apk add --no-cache openssl >/dev/null
fi

generate_cluster() {
  c="$1"          # 'a' or 'b'
  broker="kafka-${c}"
  client="client-${c}"
  dir="$CERTS_DIR/cluster-${c}"
  mkdir -p "$dir"

  echo "[certs] === cluster-${c} ==="

  echo "[certs] CA"
  openssl req -new -x509 -nodes -days "$DAYS" \
    -keyout "$dir/ca.key" -out "$dir/ca.crt" \
    -subj "/CN=ca-cluster-${c}/O=kafka-sdk-demo"

  echo "[certs] broker keystore + signed cert (SAN: ${broker},localhost)"
  keytool -keystore "$dir/${broker}-keystore.jks" -alias "${broker}" \
    -genkey -keyalg RSA -keysize 2048 -validity "$DAYS" \
    -dname "CN=${broker},O=kafka-sdk-demo" \
    -storepass "$PASS" -keypass "$PASS" \
    -ext "SAN=DNS:${broker},DNS:localhost,IP:127.0.0.1"

  keytool -keystore "$dir/${broker}-keystore.jks" -alias "${broker}" \
    -certreq -file "$dir/${broker}.csr" \
    -storepass "$PASS" -keypass "$PASS"

  cat > "$dir/${broker}-ext.cnf" <<EOF
subjectAltName=DNS:${broker},DNS:localhost,IP:127.0.0.1
EOF

  openssl x509 -req -in "$dir/${broker}.csr" \
    -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/${broker}-signed.crt" -days "$DAYS" \
    -extfile "$dir/${broker}-ext.cnf"

  keytool -keystore "$dir/${broker}-keystore.jks" -alias CARoot \
    -import -file "$dir/ca.crt" -storepass "$PASS" -noprompt
  keytool -keystore "$dir/${broker}-keystore.jks" -alias "${broker}" \
    -import -file "$dir/${broker}-signed.crt" -storepass "$PASS" -noprompt

  echo "[certs] broker truststore"
  keytool -keystore "$dir/${broker}-truststore.jks" -alias CARoot \
    -import -file "$dir/ca.crt" -storepass "$PASS" -noprompt

  echo "[certs] client keystore + signed cert"
  keytool -keystore "$dir/${client}-keystore.jks" -alias "${client}" \
    -genkey -keyalg RSA -keysize 2048 -validity "$DAYS" \
    -dname "CN=${client},O=kafka-sdk-demo" \
    -storepass "$PASS" -keypass "$PASS"

  keytool -keystore "$dir/${client}-keystore.jks" -alias "${client}" \
    -certreq -file "$dir/${client}.csr" \
    -storepass "$PASS" -keypass "$PASS"

  openssl x509 -req -in "$dir/${client}.csr" \
    -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
    -out "$dir/${client}-signed.crt" -days "$DAYS"

  keytool -keystore "$dir/${client}-keystore.jks" -alias CARoot \
    -import -file "$dir/ca.crt" -storepass "$PASS" -noprompt
  keytool -keystore "$dir/${client}-keystore.jks" -alias "${client}" \
    -import -file "$dir/${client}-signed.crt" -storepass "$PASS" -noprompt

  echo "[certs] client truststore"
  keytool -keystore "$dir/${client}-truststore.jks" -alias CARoot \
    -import -file "$dir/ca.crt" -storepass "$PASS" -noprompt

  cat > "$dir/admin.properties" <<EOF
security.protocol=SSL
ssl.keystore.location=${dir}/${client}-keystore.jks
ssl.keystore.password=${PASS}
ssl.key.password=${PASS}
ssl.truststore.location=${dir}/${client}-truststore.jks
ssl.truststore.password=${PASS}
ssl.endpoint.identification.algorithm=
EOF

  rm -f "$dir/${broker}.csr" "$dir/${client}.csr" \
        "$dir/${broker}-ext.cnf" "$dir/ca.srl"

  chmod -R a+rX "$dir"
}

generate_cluster a
generate_cluster b

touch "$CERTS_DIR/.generated"
chmod -R a+rX "$CERTS_DIR"
echo "[certs] Done."
