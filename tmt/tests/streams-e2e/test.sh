#!/bin/sh -eux

# Move to root folder of streams-e2e
cd ../../../

#run tests

./mvnw verify -P test -Dgroups=${GROUPS}
