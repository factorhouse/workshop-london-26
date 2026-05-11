#!/bin/bash

# Validate arguments
if [ -z "$1" ]; then
  echo "Usage: $0 <path-to-env-file>"
  echo "Example: $0 ./setup.remote.env"
  exit 1
fi

ENV_FILE=$1

if [ ! -f "$ENV_FILE" ]; then
  echo "Error: File '$ENV_FILE' not found!"
  exit 1
fi

# Determine tasks.max based on file name
ENV_BASENAME=$(basename "$ENV_FILE")
TASKS_MAX=3

if [ "$ENV_BASENAME" = "setup.local.env" ]; then
  TASKS_MAX=1
fi

# Extract variables safely using grep and cut (removes potential quotes too)
SR_URL=$(grep "^SCHEMA_REGISTRY_URL=" "$ENV_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'")
SR_USER=$(grep "^SCHEMA_REGISTRY_USER=" "$ENV_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'")
SR_PASS=$(grep "^SCHEMA_REGISTRY_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'")

# Verify we got the credentials
if [ -z "$SR_URL" ] || [ -z "$SR_USER" ] ||[ -z "$SR_PASS" ]; then
  echo "Error: Missing SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_USER, or SCHEMA_REGISTRY_PASSWORD in $ENV_FILE"
  exit 1
fi

# Define a function to generate the JSON template
generate_json() {
  local type=$1
  local topic_name="orders-${type}"
  local filename="${topic_name}.json"

  # Use a Here-Doc to create the JSON file
  cat <<EOF > "$filename"
{
  "name": "${topic_name}",
  "config": {
    "connector.class": "com.amazonaws.mskdatagen.GeneratorSourceConnector",
    "tasks.max": "${TASKS_MAX}",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": false,
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schemas.enable": true,
    "value.converter.schema.registry.url": "${SR_URL}",
    "value.converter.basic.auth.credentials.source": "USER_INFO",
    "value.converter.basic.auth.user.info": "${SR_USER}:${SR_PASS}",

    "genv.${topic_name}.order_id.with": "#{Internet.uuid}",
    "genv.${topic_name}.bid_time.with": "#{date.past '5','SECONDS'}",
    "genv.${topic_name}.price.with": "#{number.random_double '2','1','150'}",
    "genv.${topic_name}.item.with": "#{Commerce.productName}",
    "genv.${topic_name}.supplier.with": "#{regexify '(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)'}",

    "global.throttle.ms": "1000",
    "global.history.records.max": "1000"
  }
}
EOF
  
  echo "✅ Successfully created $filename"
}

# Generate the two files
generate_json "ui"
generate_json "api"