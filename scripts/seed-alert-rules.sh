#!/usr/bin/env bash
# Seeds a few example alert rules into the alert_rules collection.
set -euo pipefail

MONGO_URI="${MONGODB_URI:-mongodb://localhost:27017}"
DB="${MONGODB_DATABASE:-logstream}"

mongosh "$MONGO_URI/$DB" --quiet <<'EOF'
db.alert_rules.deleteMany({});
db.alert_rules.insertMany([
  { _id: "fatal-any",     name: "Any fatal event",     level: "FATAL",  enabled: true,  channel: "slack", threshold: 1, window_secs: 60 },
  { _id: "auth-failures", name: "Auth failures",       service_name: "auth-service", pattern: "(?i)auth.*fail", enabled: true, channel: "slack", threshold: 5, window_secs: 120 },
  { _id: "pg-timeouts",   name: "Payment gateway 5xx", service_name: "payment-service", pattern: "gateway returned 5\\d{2}", enabled: true, channel: "slack", threshold: 3, window_secs: 60 }
]);
print("seeded", db.alert_rules.countDocuments(), "rules");
EOF
