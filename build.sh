#!/bin/bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

if [ $# -ne 3 ]; then
    echo "Usage: $0 <target_dir> <flink_version> <connector_name>"
    echo "Example: $0 /tmp 1.20 mysql/flink-sql-connector-mysql-cdc/all"
    exit 1
fi
ALLOWED_VERSIONS=("1.18" "1.20")
FLINK_CDC_VERSION="3.4"
OCEANUS_VERSION="9.25"
TARGET_DIR="$1"
FLINK_VERSION="$2"
CONNECTOR_NAME=""

# Set CONNECTOR_NAME based on the third parameter
case "$3" in
    "all")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc"
        ;;
    "mysql"|"flink-sql-connector-mysql-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc"
        ;;
    "postgres"|"flink-sql-connector-postgres-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"
        ;;
    "oracle"|"flink-sql-connector-oracle-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc"
        ;;
    "postgres"|"flink-sql-connector-postgres-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"
        ;;
    "mongodb"|"flink-sql-connector-mongodb-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc"
        ;;
    "sqlserver"|"flink-sql-connector-sqlserver-cdc")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc"
        ;;
    *)
        echo "Error: Unsupported connector name '$3'"
        echo "Supported connector names: mysql, flink-sql-connector-mysql-cdc,
                  oracle, flink-sql-connector-oracle-cdc,
                  postgres, flink-sql-connector-postgres-cdc,
                  all"
        exit 1
        ;;
esac

if [[ ! " ${ALLOWED_VERSIONS[*]} " =~ " ${FLINK_VERSION} " ]]; then
  echo "Supported Flink versions: ${ALLOWED_VERSIONS[*]}"
  echo "Current input version: ${FLINK_VERSION}"
  exit 1
fi

mkdir -p "${TARGET_DIR}"

echo "=== Starting project build ==="
echo "=== Building connector: $connector ==="
mvn clean package -DskipTests -pl "$CONNECTOR_NAME" -am -Pflink-${FLINK_VERSION}
if [ $? -ne 0 ]; then
    echo "=== Error: $CONNECTOR_NAME Maven build failed ==="
    exit 1
fi
echo "=== $CONNECTOR_NAME build successful ==="

# Change CONNECTOR_NAME to array
IFS=',' read -ra CONNECTOR_ARRAY <<< "$CONNECTOR_NAME"
# Copy connector to target directory
for connector in "${CONNECTOR_ARRAY[@]}"; do
    CONNECTOR_TARGET_DIR="$connector/target"
    JAR_BASE_PATTERN=$(echo "$connector-*.jar" | awk -F'/' '{print $NF}')
    TEST_JAR_PATTERN="*-tests.jar"
    TARGET_JAR=$(find "${CONNECTOR_TARGET_DIR}" -maxdepth 1 -type f -name "${JAR_BASE_PATTERN}" \
     ! -name "${TEST_JAR_PATTERN}" | head -n 1)

    echo "Target artifact: ${TARGET_JAR}"

    if [ -z "${TARGET_JAR}" ] || [ ! -f "${TARGET_JAR}" ]; then
        echo "Error: $connector build artifact JAR not found, please check the packaging process"
        exit 1
    fi

    FINAL_JAR_FILENAME=$connector-${FLINK_CDC_VERSION}-${FLINK_VERSION}-${OCEANUS_VERSION}.jar
    TARGET_CONNECTOR_NAME=$(echo "$FINAL_JAR_FILENAME" | awk -F'/' '{print $NF}')
    cp -v "${TARGET_JAR}" "${TARGET_DIR}/${TARGET_CONNECTOR_NAME}"
    echo "=== $connector packaging completed ==="
    echo "Build artifact output to: ${TARGET_DIR}/${TARGET_CONNECTOR_NAME}"
done

echo "=== All connectors build completed ==="