FROM rust:1-alpine AS builder
WORKDIR /usr/src/rhio
COPY . .
RUN apk add musl-dev libressl libressl-dev
RUN cargo install --path ./rhio
RUN cargo install --path ./rhio-operator

FROM rust:1-alpine
RUN apk add musl-dev libressl libressl-dev
COPY --from=builder /usr/local/cargo/bin/rhio /usr/local/bin/rhio
COPY --from=builder /usr/local/cargo/bin/rhio-operator /usr/local/bin/rhio-operator
CMD ["/usr/local/bin/rhio", "-c", "/etc/rhio/config.yaml"]
