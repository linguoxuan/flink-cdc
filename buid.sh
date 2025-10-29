#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <target_dir> <flink_version> <connector_name>"
    echo "Example: $0 /tmp 1.16 mysql/flink-sql-connector-mysql-cdc/all"
    exit 1
fi
ALLOWED_VERSIONS=("1.13" "1.14" "1.16" "1.18")
FLINK_CDC_VERSION="2.2"
OCEANUS_VERSION="9.24"
TARGET_DIR="$1"
FLINK_VERSION="$2"
CONNECTOR_NAME=""

# Set CONNECTOR_NAME based on the third parameter
case "$3" in
    "all")
        CONNECTOR_NAME="flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc,flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc"
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

git clone https://pacinogong:%mail135246@git.woa.com/InLong/SyncWithGitHub.git
cd SyncWithGitHub
git checkout origin/feature-r3
mvn clean install -DskipTests -Dfast -pl inlong-sort/sort-connectors/base,inlong-sort/sort-formats/format-json -am
cd ../
rm -rf SyncWithGitHub
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