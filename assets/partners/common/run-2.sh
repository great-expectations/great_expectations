set -e

docker compose up --build --exit-code-from integration_test
