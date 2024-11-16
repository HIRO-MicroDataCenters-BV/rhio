# rhio Walkthrough

A small step-by-step guide to play with rhio.

## Requirements

* Rust 1.82.0
* [`rhio`](https://github.com/HIRO-MicroDataCenters-BV/rhio)
* [`nats-server`](https://docs.nats.io/running-a-nats-service/introduction/installation#getting-the-binary-from-the-command-line)
* [`nats`](https://docs.nats.io/running-a-nats-service/clients#installing-the-nats-cli-tool)
* Docker & DockerCompose

## Files

There's a bunch of files required to follow the steps, make sure you have the following ready:

<details>
<summary><code>cluster-stream-1.json</code></summary>

```json
{
  "name": "cluster-stream-1",
  "subjects": [
    "*.foo.*"
  ],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs_per_subject": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "memory",
  "discard": "old",
  "num_replicas": 1,
  "duplicate_window": 120000000000,
  "sealed": false,
  "deny_delete": false,
  "deny_purge": false,
  "allow_rollup_hdrs": false,
  "allow_direct": true,
  "mirror_direct": false,
  "consumer_limits": {}
}
```
</details>

<details>
<summary><code>cluster-stream-2.json</code></summary>

```json
{
  "name": "cluster-stream-2",
  "subjects": [
    "*.foo.*"
  ],
  "retention": "limits",
  "max_consumers": -1,
  "max_msgs_per_subject": -1,
  "max_msgs": -1,
  "max_bytes": -1,
  "max_age": 0,
  "max_msg_size": -1,
  "storage": "memory",
  "discard": "old",
  "num_replicas": 1,
  "duplicate_window": 120000000000,
  "sealed": false,
  "deny_delete": false,
  "deny_purge": false,
  "allow_rollup_hdrs": false,
  "allow_direct": true,
  "mirror_direct": false,
  "consumer_limits": {}
}
```
</details>

<details>
<summary><code>config-1.yaml</code></summary>

```yaml
# cluster-1

bind_port: 8022
network_id: "rhio-default-network-1"

# d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076
private_key_path: "private-key-1.txt"

s3:
  endpoint: "http://localhost:8000"
  region: "eu-central-1"
  credentials:
    access_key: "rhio"
    secret_key: "rhio_password"

nats:
  endpoint: "localhost:8009"

nodes:
  - public_key: "5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee"
    endpoints:
      - "192.168.178.100:9022"
      - "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:9023"

publish:
  s3_buckets:
    - "bucket-1"
    - "bucket-2"
  nats_subjects:
    - subject: "d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076.foo.*"
      stream: "cluster-stream-1"

subscribe:
  s3_buckets:
    - "bucket-1/5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee"
    - "bucket-2/5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee"
  nats_subjects:
    - subject: "5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee.foo.meta"
      stream: "cluster-stream-1"
```
</details>

<details>
<summary><code>config-2.yaml</code></summary>

```yaml
# cluster-2

bind_port: 9022
network_id: "rhio-default-network-1"

# 5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee
private_key_path: "private-key-2.txt"

s3:
  endpoint: "http://localhost:9000"
  region: "eu-central-1"
  credentials:
    access_key: "rhio"
    secret_key: "rhio_password"

nats:
  endpoint: "localhost:9009"

nodes:
  - public_key: "d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076"
    endpoints:
      - "192.168.178.100:8022"
      - "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:8023"

publish:
  s3_buckets:
    - "bucket-1"
    - "bucket-2"
  nats_subjects:
    - subject: "5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee.foo.*"
      stream: "cluster-stream-2"

subscribe:
  s3_buckets:
    - "bucket-1/d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076"
    - "bucket-2/d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076"
  nats_subjects:
    - subject: "d4e8b43fccc2d65c36f47cf999aee94c3480184b3c8fdf7a077aa6f0ee648076.foo.*"
      stream: "cluster-stream-2"
```
</details>

<details>
<summary><code>docker-compose.yaml</code></summary>

```yaml
services:
  minio_1:
    image: docker.io/minio/minio:latest
    command: [ "server", "/data", "--console-address", ":9001" ]
    ports:
      - '8000:9000'
      - '8001:9001'
    environment:
      - MINIO_ROOT_USER=rhio
      - MINIO_ROOT_PASSWORD=rhio_password
  minio_2:
    image: docker.io/minio/minio:latest
    command: [ "server", "/data", "--console-address", ":9001" ]
    ports:
      - '9000:9000'
      - '9001:9001'
    environment:
      - MINIO_ROOT_USER=rhio
      - MINIO_ROOT_PASSWORD=rhio_password
```
</details>

<details>
<summary><code>private-key-1.txt</code></summary>

```
c749c4c7bca73136001520041ac8a00e138dd075da798be33d4644a69cb0c5f8
```
</details>

<details>
<summary><code>private-key-2.txt</code></summary>

```
d00f23f44b598d0b789b7ff0f1d99a24dc11eda434ad485f692786b624ac83f4
```
</details>

## Steps

1. Run both NATS server (in separate terminals)
```bash
./nats-server -js -p 8009
./nats-server -js -p 9009
```
2. Launch both MinIO servers (see `docker-compose.yaml`)
```bash
docker-compose up -d
```
3. Make sure the "bucket-1" and "bucket-2" buckets exists and that they are both set to public (@TODO) in both MinIO databases, you can log in via a web interface at http://localhost:8001 and http://localhost:9001 to check that
4. Create streams on both NATS servers
```bash
./nats -s localhost:8009 str add --config cluster-stream-1.json
./nats -s localhost:9009 str add --config cluster-stream-2.json
```
5. Launch both rhio nodes (in separate terminals), make sure that `private-key-*.txt` and `config-*.yaml` files are in the same directory, or adjust the paths accordingly
```bash
cargo run -- -c config-1.yaml -l TRACE
cargo run -- -c config-2.yaml -l TRACE
```
6. Subscribe to a subject (example is for first NATS server)
```bash
./nats -s localhost:8009 sub "*.foo.*"
```
7. Start a NATS client and publish messages
```bash
./nats -s localhost:9009 pub "5ee70a7e7abdf7174178434eebd1d45a0c879086d19eebe175eb1d99e9f4feee.foo.meta" test
```
