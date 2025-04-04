pub mod minimal {
    pub const RHIO: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/minimal/rhio.yaml"
    ));
}

pub mod full {
    pub const RHIO: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/rhio.yaml"
    ));
    pub const RHIO_NATS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/rhio_nats_credentials_secret.yaml"
    ));
    pub const RHIO_S3: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/rhio_s3_credentials_secret.yaml"
    ));

    pub const RMS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/rms.yaml"
    ));
    pub const RMSS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/rmss.yaml"
    ));
    pub const ROS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/ros.yaml"
    ));
    pub const ROSS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/configuration/fixtures/full/ross.yaml"
    ));
}
