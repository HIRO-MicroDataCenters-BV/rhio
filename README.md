# rhio

rhio is a peer-to-peer message stream and blob storage solution allowing processes to rapidly exchange messages and efficiently replicate large blobs without any centralised coordination.

rhio has been designed to be integrated into a Kubernetes cluster where _internal_ cluster messaging and persistence is handled centrally via [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) while _external_ cluster messaging is decentralised and handled via [p2panda](https://p2panda.org). Blobs of any size are replicated separately with efficient [bao encoding](https://github.com/oconnor663/bao/tree/master) and stored in a [MinIO](https://min.io/) database.

Similar to NATS JetStream, any number of streams can be subscribed to and filtered by "subjects".

## Usage

`TODO`

## Publish

### Messages

rhio does not create or publish any messages by itself and serves merely as an "router" coordinating streams inside and outside the cluster. To publish messages into the stream the regular NATS JetStream API is used: Other processes inside the cluster can independently publish messages to the NATS Server which will then be automatically picked up and processed by rhio.

## Blobs

`TODO`

## Stream

rhio does not offer any direct APIs to subscribe to message streams. To consume data the regular NATS JetStream API is used.

`TODO`
