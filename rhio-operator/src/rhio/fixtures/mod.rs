pub mod statefulset {
    pub const STS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/statefulset.yaml"
    ));

    pub const SVC: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rhio/fixtures/service.yaml"
    ));
}
