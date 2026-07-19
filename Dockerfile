# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG RUST_VERSION=1.89-bullseye
#ARG RUST_VERSION=latest
ARG APP_NAME=greeting-kafka-operator

################################################################################
# Create a stage for building the application.

FROM docker.io/rust:${RUST_VERSION} AS build
ARG APP_NAME
WORKDIR /app


# Install host build dependencies.
#RUN apk add --no-cache clang lld musl-dev git cmake g++ make
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
#ENV SQLX_OFFLINE true


# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies, a cache mount to /usr/local/cargo/git/db
# for git repository dependencies, and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
#RUN --mount=type=bind,source=src,target=src \
#    --mount=type=bind,source=.env,target=.env \
#    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
#    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
#    --mount=type=cache,target=target/ \
#    --mount=type=cache,target=/usr/local/cargo/git/db \
#    --mount=type=cache,target=/usr/local/cargo/registry/ \
COPY . .
RUN cargo build --locked --release
RUN cp ./target/release/$APP_NAME /usr/bin/server
RUN libdir="$(dpkg-architecture -qDEB_HOST_MULTIARCH)" && \
	mkdir -p "/runtime-libs/lib/${libdir}" && \
	cp -a "/lib/${libdir}/libz.so.1"* "/runtime-libs/lib/${libdir}/"


################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
#
# Using distroless/cc-debian12 for minimal size while keeping libc and openssl
# support for Kafka operator TLS/network operations. This is ~100x smaller than
# rust:slim-bullseye while maintaining runtime compatibility.
FROM gcr.io/distroless/cc-debian12:nonroot AS final

# Copy the zlib runtime required by rdkafka/libz-sys.
COPY --from=build /runtime-libs/ /

# Copy the executable from the "build" stage.
COPY --chown=nonroot:nonroot --from=build /usr/bin/server /usr/bin/server

USER nonroot:nonroot

# Set the entrypoint to run the server
ENTRYPOINT ["/usr/bin/server"]
