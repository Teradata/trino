# Teradata Connector for Trino

The Teradata Connector for Trino enables Trino to connect to Teradata Vantage.

This repository is maintained by Teradata for the development and distribution
of the Teradata Connector for Trino.

## Documentation

For installation, configuration, supported features, and usage information, see
the [Teradata Connector for Trino documentation](https://teradata.github.io/trino/connector/teradata.html).

## Releases

Tested versions of the Teradata Connector for Trino are published under
[GitHub Releases](https://github.com/Teradata/trino/releases). Each release is identified by a tag of the
form `teradata-trino-<trino-version>`. The release version always matches the
Trino version it is built for (for example, `teradata-trino-482` is built for
Trino 482).

Customers should use published releases rather than the `teradata-development`
branch.

## Supported Versions

| Release               | Trino version |
|-----------------------|---------------|
| `teradata-trino-482`  | 482           |

The `teradata-development` branch is currently based on Trino 485-SNAPSHOT (see
the root `pom.xml`).

## Installation

1. Download the plugin `.zip` from the [latest release](https://github.com/Teradata/trino/releases/latest).
2. Unpack it into the `plugin/` directory of every Trino coordinator and worker.
3. Create `etc/catalog/teradata.properties` and restart Trino.

See the [documentation](https://teradata.github.io/trino/connector/teradata.html)
for the full list of configuration properties and authentication options.

## Building from Source

```bash
./mvnw -pl plugin/trino-teradata -am install -DskipTests
```

The plugin archive is created in `plugin/trino-teradata/target/`.
Developer notes and instructions for running the integration tests against
Teradata ClearScape Analytics Experience are in
[plugin/trino-teradata/README.md](../plugin/trino-teradata/README.md).

## Repository Branches

- `teradata-development` - Latest Teradata Connector development.
- `master` - Synchronized with the upstream Trino repository.

The `master` branch does not contain Teradata Connector changes until those
changes are incorporated upstream.

## Changelog

Newest first. Each released version links to its GitHub Release for the full
list of changes.

### Unreleased (`teradata-development`)

- Rebased onto upstream Trino 485-SNAPSHOT.
- Trino-managed views: `CREATE VIEW`, `DROP VIEW`, and `SELECT` from views.
- JWT, secret, and bearer-token authentication.
- Pushdown for `CAST`, `LOWER`/`UPPER`, `SUBSTRING`, and `AVG` with `FLOAT`
  casting.
- `TIME` and `TIMESTAMP` types with precision.
- ClearScape-based integration test infrastructure and CI matrix.
- Removed the legacy product tests, which depended on a test framework that
  upstream Trino has dropped.

### [482](https://github.com/Teradata/trino/releases/tag/teradata-trino-482)

- Initial release of the Teradata Connector for Trino on Trino 482.
- See the release page for details.

## Contributing and Support

Report issues through this repository's issue tracker. Contributions to the
connector are made as pull requests against `teradata-development`.
Changes intended for upstream Trino should branch from `master`.

## Upstream Trino

This repository is a fork of [Trino](https://github.com/trinodb/trino) and is
licensed under the [Apache License 2.0](../LICENSE).
