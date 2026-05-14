#!/usr/bin/env bash
# Initializes a single-node MongoDB replica set. Change streams (used by the
# alerter) require a replica set, so even local development uses one.
set -euo pipefail

CONTAINER="${1:-logstream-mongo}"

echo "==> Waiting for mongod in container '$CONTAINER'..."
for i in $(seq 1 30); do
  if docker exec "$CONTAINER" mongosh --quiet --eval "db.adminCommand({ ping: 1 }).ok" 2>/dev/null | grep -q 1; then
    break
  fi
  sleep 1
done

echo "==> Initiating replica set..."
docker exec "$CONTAINER" mongosh --quiet --eval '
try {
  const status = rs.status();
  print("Replica set already initialized:", status.set);
} catch (e) {
  rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongodb:27017"}]});
  print("Replica set initialized.");
}
'
