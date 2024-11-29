FROM rust:1-alpine AS builder
WORKDIR /usr/src/rhio
COPY . .
RUN apk add musl-dev libressl libressl-dev
RUN cargo install --path ./rhio

FROM rust:1-alpine
RUN apk add musl-dev libressl libressl-dev
COPY --from=builder /usr/local/cargo/bin/rhio /usr/local/bin/rhio
CMD ["/usr/local/bin/rhio", "-c", "/etc/rhio/config.yaml"]
