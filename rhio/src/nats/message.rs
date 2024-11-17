use async_nats::Message as NatsMessage;
use p2panda_core::Hash;

pub fn hash_nats_message(message: &NatsMessage) -> Hash {
    Hash::new(message.subject.as_bytes())
}
