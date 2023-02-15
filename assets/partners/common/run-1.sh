set -e

docker build -t integration_test -f ../common/int-1.Dockerfile .
docker compose -f ../common/docker-compose.int-1.yml up --exit-code-from integration_test
