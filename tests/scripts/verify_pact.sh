#!/bin/bash
VERSION=$1
if [ -x $VERSION ]; then
    echo "ERROR: You must specify a provider version"
    exit
fi

pipenv run pact-verifier --provider-base-url=https://api.dev.greatexpectations.io \
  --pact-url="http://localhost:9292/pacts/provider/mercury/consumer/great_expectations/latest" \
  --provider-states-setup-url=http://localhost:5001/_pact/provider_states \
  --provider-app-version $VERSION \
  --pact-broker-username pact_broker \
  --pact-broker-password \
  --publish-verification-results
