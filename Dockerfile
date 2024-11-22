FROM rust:latest AS BUILDER

# Add source code
ADD . ./

# Build our application
RUN cargo build --release

# Now, we need to build our _real_ Docker container, copying in `rhio`
FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=BUILDER \
    /volume/target/x86_64-unknown-linux-musl/release/rhio \
    /usr/local/bin/

CMD /usr/local/bin/rhio