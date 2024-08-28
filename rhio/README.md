# rhio

rhio is a peer-to-peer message stream and blob storage solution allowing processes to fastly exchange messages or efficiently replicate large blobs without any centralised coordination.

rhio has been designed to be integrated into a Kubernetes cluster where _internal_ cluster messaging and persistence is handled centrally via [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) while _external_ cluster messaging is decentralised and handled via [p2panda](https://p2panda.org). Blobs of any size are replicated separately with efficient [bao encoding](https://github.com/oconnor663/bao/tree/master) and stored in a [MinIO](https://min.io/) database.
