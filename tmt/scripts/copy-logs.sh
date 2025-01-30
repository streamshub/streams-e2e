#!/bin/sh -eux


TEST_LOG_DIR="${TMT_PLAN_DATA}/../discover/default-0/tests/target/logs"
XUNIT_LOG_DIR="${TMT_PLAN_DATA}/../discover/default-0/tests/target/failsafe-reports"
REPORT_LOG_DIR="${TMT_PLAN_DATA}/../discover/default-0/tests/target/site"

TARGET_DIR="${TMT_PLAN_DATA}"
LOGS_DIR="${TARGET_DIR}/logs"
XUNIT_DIR="${TARGET_DIR}/xunit"
REPORT_DIR="${TARGET_DIR}/report"

mkdir -p "${LOGS_DIR}"
mkdir -p "${XUNIT_DIR}"
mkdir -p "${REPORT_DIR}"

cp -R "${TEST_LOG_DIR}" "${LOGS_DIR}" || true
cp -R "${XUNIT_LOG_DIR}" "${XUNIT_DIR}" || true
cp -R "${REPORT_LOG_DIR}" "${REPORT_DIR}" || true
