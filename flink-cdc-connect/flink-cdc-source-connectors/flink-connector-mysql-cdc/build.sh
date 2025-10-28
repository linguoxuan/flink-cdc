#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <target_dir> <flink_version>"
    echo "Example: $0 /tmp 1.20"
    exit 1
fi
ALLOWED_VERSIONS=("1.20")
FLINK_CDC_VERSION="3.4"
OCEANUS_VERSION="9.24"
TARGET_DIR="$1"
FLINK_VERSION="$2"
CONNECTOR_NAME="flink-sql-connector-mysql-cdc"

if [[ ! " ${ALLOWED_VERSIONS[*]} " =~ " ${FLINK_VERSION} " ]]; then
  echo "支持的Flink 版本为： ${ALLOWED_VERSIONS[*]}"
  echo "当前输入版本：${FLINK_VERSION}"
  exit 1
fi

mkdir -p "${TARGET_DIR}"

echo "=== 开始构建项目 ==="

WORK_DIR="$(pwd)"
cd "../../../"
mvn clean package -pl flink-cdc-connect/flink-cdc-source-connectors/${CONNECTOR_NAME} -am -T 12
if [ $? -ne 0 ]; then
    echo "=== 错误: Maven构建失败 ==="
    exit 1
fi
cd "${WORK_DIR}"

echo "=== 构建成功 ==="

CONNECTOR_TARGET_DIR="../${CONNECTOR_NAME}/target"
JAR_BASE_PATTERN="${CONNECTOR_NAME}-*.jar"
TEST_JAR_PATTERN="*-tests.jar"
TARGET_JAR=$(find "${CONNECTOR_TARGET_DIR}" -maxdepth 1 -type f -name "${JAR_BASE_PATTERN}" \
 ! -name "${TEST_JAR_PATTERN}" | head -n 1)

echo "目标制品：${TARGET_JAR}"

if [ -z "${TARGET_JAR}" ] || [ ! -f "${TARGET_JAR}" ]; then
    echo "错误: 未找到构建产物JAR，请检查打包过程"
    exit 1
fi

FINAL_JAR_FILENAME=${CONNECTOR_NAME}-${FLINK_CDC_VERSION}-${FLINK_VERSION}-${OCEANUS_VERSION}.jar

cp -v "${TARGET_JAR}" "${TARGET_DIR}/${FINAL_JAR_FILENAME}"

echo "=== 打包完成 ==="
echo "构建制品已输出到: ${TARGET_DIR}/${FINAL_JAR_FILENAME}"
