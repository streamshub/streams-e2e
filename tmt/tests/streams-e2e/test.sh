#!/bin/sh -eux

# Move to root folder of streams-e2e
cd ../../../

#get install files
./mvnw install -q -P get-operator-files

#run tests
./mvnw verify -P test -Dgroups="${TEST_GROUPS}"
