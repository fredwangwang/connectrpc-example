# syntax=docker/dockerfile:1
#
# Builds the Go server and the Rust client inside Docker so the host needs no
# Go/Rust/buf/protoc. Use `make build-docker`, which exports only the
# `artifacts` stage into ./build.

ARG GO_VERSION=1.27.2
ARG RUST_VERSION=1.99.1

FROM golang:${GO_VERSION}-bookworm AS go

FROM rust:${RUST_VERSION}-bookworm AS build
# Rust image + Go toolchain + protoc (needed by client-rs/build.rs)
COPY --from=go /usr/local/go /usr/local/go
ENV PATH=/usr/local/go/bin:/root/go/bin:$PATH
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*

# buf + Go protobuf plugins (cached unless the versions below change)
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOBIN=/root/go/bin \
    go install github.com/bufbuild/buf/cmd/buf@latest \
 && GOBIN=/root/go/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
 && GOBIN=/root/go/bin go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest

WORKDIR /src
COPY . .

# Regenerate Go code from the proto, then build the static server.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    buf lint && buf generate \
 && cd server \
 && CGO_ENABLED=0 go build -o /out/server .

# Build the Rust client (build.rs generates the Rust code with protoc).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/client-rs/target \
    cd client-rs \
 && cargo build --release \
 && cp target/release/client /out/client-rs

RUN cp client-curl/client.sh /out/client-curl.sh

# Only this stage is exported to the host.
FROM scratch AS artifacts
COPY --from=build /out/ /
