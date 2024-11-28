FROM rust:latest AS builder
WORKDIR /usr/src/rhio
COPY . .
RUN cargo install --path ./rhio

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /usr/local/cargo/bin/rhio /usr/local/bin/rhio
CMD /usr/local/bin/rhio -c /etc/rhio/config.yaml
