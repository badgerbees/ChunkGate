# Releases

ChunkGate releases are produced by the `release` GitHub Actions workflow. The workflow builds both `chunkgate` and `chunkgate-delta` for Linux, macOS, and Windows on `amd64` and `arm64`.

## Create A Release

Tag from `main`:

```sh
git checkout main
git pull
git tag vX.Y.Z
git push origin vX.Y.Z
```

The release workflow creates archives named:

```text
chunkgate_vX.Y.Z_linux_amd64.tar.gz
chunkgate_vX.Y.Z_linux_arm64.tar.gz
chunkgate_vX.Y.Z_darwin_amd64.tar.gz
chunkgate_vX.Y.Z_darwin_arm64.tar.gz
chunkgate_vX.Y.Z_windows_amd64.zip
chunkgate_vX.Y.Z_windows_arm64.zip
```

Each archive contains:

- `chunkgate`
- `chunkgate-delta`
- `README.md`
- `delta-protocol.md`

The release also includes:

- `checksums.txt`
- `sbom.cdx.json`

Tagged releases publish the container image to:

```text
ghcr.io/badgerbees/chunkgate:vX.Y.Z
ghcr.io/badgerbees/chunkgate:latest
```

## Validate A Release

Download an archive and verify its checksum:

```sh
sha256sum -c checksums.txt
```

Run the binary:

```sh
./chunkgate --help
```

ChunkGate currently does not expose a version subcommand, so use the archive filename and GitHub release tag as the release identity.

## CI Security Gates

The CI workflow runs:

- `go test ./...`
- `go vet ./...`
- command builds for `chunkgate` and `chunkgate-delta`
- `govulncheck ./...`
- CycloneDX SBOM generation
- Trivy filesystem vulnerability scanning for high and critical findings

Treat failed vulnerability checks as release blockers unless the finding is proven unreachable and documented in the release notes.
