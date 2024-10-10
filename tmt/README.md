# Testing farm

This document gives a detailed breakdown of the testing processes using testing farm service.

## Pre-requisites

* Python >=3.9
* TMT command line tool (optional) - for lint and check tmt formatted test plans and tests
  * `pip install tmt[all]`
* Testing farm command line tool - for trigger your test plan in testing-farm
  * `pip install tft-cli`

## Links

* [Test Management Tool (tmt)](https://tmt.readthedocs.io/en/latest/index.html)
* [Testing farm](https://docs.testing-farm.io/general/0.1/index.html)

## Current plans and tests
Plans are stored in [plans](./plans) folder, there is a file called `main.fmf` which contains test plan definition. 
This definition is composed of hw requirements, prepare steps for created VM executor and specific plans. Specific
plan defines selectors for [tests](./tests) which should be executed.

### List of plans
* flink-all
* smoke
* flink-sql-example

## Usage

### Pre-requisites
1. Get API token for testing farm [(how-to obtain token)](https://docs.testing-farm.io/general/0.1/onboarding.html)
2. Store token into env var ```export TESTING_FARM_API_TOKEN="your_token"```

### Run tests

Run all plans
```commandline
testing-farm request --compose CentOS-Stream-9 --git-url https://github.com/streamshub/streams-e2e.git
```

Select specific plan and git branch
```commandline
testing-farm request --compose CentOS-Stream-9 \
 --git-url https://github.com/streamshub/streams-e2e.git \
 --git-ref some-branch \
 --plan smoke
```

Run multi-arch build
```commandline
testing-farm request --compose CentOS-Stream-9 \
 --git-url https://github.com/streamshub/streams-e2e.git \
 --git-ref some-branch \
 --plan smoke \
 --arch aarch64,x86_64
```

## Packit-as-a-service for PR check

[Packit-as-a-service](https://github.com/marketplace/packit-as-a-service) is a github application
for running testing-farm jobs from PR requested by command. Definition of the jobs is stored in
[.packit.yaml](../.packit.yaml). Packit can be triggered from the PR by comment, but only members of strimzi
organization are able to run tests.

### Usage

Run all jobs for PR
```
/packit test
```

Run selected jobs by label
```
/packit test --labels flink-all
```
