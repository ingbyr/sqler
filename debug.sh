#!/usr/bin/env bash
SQLER_CFG_AES_KEY=796debf8ad7b70607091ee08c8b27ff4 dlv debug --headless --listen=:2345 --api-version=2 --accept-multiclient . -- -i -c config-sqlite.yml
