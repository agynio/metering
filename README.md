# Metering

Usage metering service for ingesting and querying platform usage data.

Architecture: [Metering](https://github.com/agynio/architecture/blob/main/architecture/metering.md)

## Local Development

Full setup: [Local Development](https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md)

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

See [bootstrap](https://github.com/agynio/bootstrap) for details.

### Run from sources

Deploys the service from local source code. This patches the service pod — it does not affect other services or the test pod.

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```

### Run tests

Runs E2E tests in a separate test pod. This command only manages the test pod — it does not deploy or modify the service. Tests run against whatever is currently deployed: pinned release images by default, or source code if `devspace dev` was called first.

```bash
devspace run test-e2e
```

See [E2E Testing](https://github.com/agynio/architecture/blob/main/architecture/operations/e2e-testing.md).
