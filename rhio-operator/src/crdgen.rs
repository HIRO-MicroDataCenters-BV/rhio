use rhio_operator::api::message_stream::ReplicatedMessageStream;
use rhio_operator::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
use rhio_operator::api::object_store::ReplicatedObjectStore;
use rhio_operator::api::object_store_subscription::ReplicatedObjectStoreSubscription;
use rhio_operator::api::service::RhioService;
use stackable_operator::kube::CustomResourceExt;

fn main() {
    print!(
        "{}",
        serde_yaml::to_string(&ReplicatedObjectStore::crd()).unwrap()
    );
    println!("---");
    print!(
        "{}",
        serde_yaml::to_string(&ReplicatedObjectStoreSubscription::crd()).unwrap()
    );
    println!("---");
    print!(
        "{}",
        serde_yaml::to_string(&ReplicatedMessageStream::crd()).unwrap()
    );
    println!("---");
    print!(
        "{}",
        serde_yaml::to_string(&ReplicatedMessageStreamSubscription::crd()).unwrap()
    );
    println!("---");
    print!("{}", serde_yaml::to_string(&RhioService::crd()).unwrap());
}
