pub mod statefulset {
    pub const RHIO: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/rhio.yaml"
    ));

    pub const STS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/statefulset.yaml"
    ));

    pub const SVC: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/service.yaml"
    ));
}

pub mod service {
    pub const RHIO: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/rhio.yaml"
    ));

    pub const SVC: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/service.yaml"
    ));
}
