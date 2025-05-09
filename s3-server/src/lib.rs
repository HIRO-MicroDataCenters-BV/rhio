use std::borrow::Borrow;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use once_cell::sync::Lazy;
use s3::Region;
use s3::creds::Credentials;
use s3s::auth::SimpleAuth;
use s3s::service::{S3ServiceBuilder, SharedS3Service};
use s3s_fs::FileSystem;
use std::io::Read;
use std::io::Write;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use url::Url;

static TEST_INSTANCE_S3_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(34000));

/// `FakeS3Server` is a fake implementation of an S3 server for testing purposes.
/// It provides methods to create buckets, check for file existence, read and write files,
/// and manage the server lifecycle.
///
/// # Fields
/// - `runtime`: An `Arc` to a Tokio runtime used to run the server.
/// - `handle`: A `JoinHandle` to the async task running the server.
/// - `cancel`: A `CancellationToken` to signal the server to stop.
/// - `root`: A temporary directory used as the root filesystem for the server.
///
/// # Methods
/// - `new`: Creates a new instance of `FakeS3Server`.
/// - `run_inner`: Internal method to run the server, accepting connections and serving requests.
/// - `create_bucket`: Creates a new bucket in the server's filesystem.
/// - `exists`: Checks if a file exists in a specified bucket.
/// - `get_bytes`: Reads the contents of a file in a specified bucket as bytes.
/// - `get`: Opens a file in a specified bucket.
/// - `put_bytes`: Writes bytes to a file in a specified bucket.
/// - `discard`: Stops the server and cleans up resources.
///
pub struct FakeS3Server {
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
    handle: JoinHandle<Result<()>>,
    cancel: CancellationToken,
    root: TempDir,
}

impl FakeS3Server {
    /// Creates a new instance of `FakeS3Server`.
    ///
    /// # Arguments
    ///
    /// * `host` - The host address for the server.
    /// * `port` - The port number for the server.
    /// * `maybe_auth` - Optional authentication for the server.
    /// * `runtime` - The runtime for the server.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `FakeS3Server` instance or an error.
    pub fn new(
        host: String,
        port: u16,
        maybe_auth: Option<SimpleAuth>,
        runtime: Arc<Runtime>,
    ) -> Result<FakeS3Server> {
        // Fake S3 server root filesystem
        let root = TempDir::new().context("Temporary directory")?;
        debug!(
            "FakeS3Server {}:{} has root fs {:?}",
            host,
            port,
            root.path()
        );

        // Setup S3 File system
        let fs = FileSystem::new(&root)
            .map_err(|err| anyhow!("{:?}", err))
            .context("FileSysten creation")?;

        // Setup S3 service
        let service = {
            let mut builder = S3ServiceBuilder::new(fs);
            if let Some(auth) = maybe_auth {
                builder.set_auth(auth);
            }
            builder.build()
        };
        // Running HTTP server
        let cancel = CancellationToken::new();
        let cancel_token = cancel.clone();
        let handle = runtime.spawn(async move {
            FakeS3Server::run_inner(service.into_shared(), cancel_token, host, port)
                .await
                .inspect_err(|e| error!("Fake3Server: connection loop failure {}", e))
        });
        Ok(FakeS3Server {
            runtime,
            handle,
            root,
            cancel,
        })
    }

    /// Runs the inner server logic.
    ///
    /// # Arguments
    ///
    /// * `service` - The shared S3 service.
    /// * `cancel_token` - The cancellation token for the server.
    /// * `host` - The host address for the server.
    /// * `port` - The port number for the server.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    async fn run_inner(
        service: SharedS3Service,
        cancel_token: CancellationToken,
        host: String,
        port: u16,
    ) -> Result<()> {
        let connection = ConnBuilder::new(TokioExecutor::new());

        let listener = TcpListener::bind((host, port))
            .await
            .context("FakeS3Server: tcp listener binding")?;

        tokio::select! {
            result = listener.accept() => {
                let (socket, _) = match result {
                    Ok(ok) => ok,
                    Err(err) => {
                        bail!("FakeS3Server: error accepting connection: {err}");
                    }
                };
                let service = service.clone();
                let conn = connection.clone();
                tokio::spawn(async move {
                    conn.serve_connection(TokioIo::new(socket), service).await
                        .inspect_err(|e| error!("Serve connection error: {}", e))
                        .ok();
                });
            }
            _ = cancel_token.cancelled() => bail!("FakeS3Server: cancelled")
        };
        Ok(())
    }

    /// Creates a new bucket in the fake S3 server.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket to create.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn create_bucket<P: Borrow<str>>(&self, bucket: P) -> Result<()> {
        let path = self.root.path().join(bucket.borrow());
        debug!(
            "FakeS3Server: creating bucket {}, fs path {}",
            bucket.borrow(),
            path.to_str().unwrap()
        );
        std::fs::create_dir(path).context("Create bucket path")?;
        Ok(())
    }
    /// Deletes a bucket from the fake S3 server.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket to delete.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    ///
    pub fn delete_bucket<P: Borrow<str>>(&self, bucket: P) -> Result<()> {
        let path = self.root.path().join(bucket.borrow());
        debug!(
            "FakeS3Server: deleting bucket {}, fs path {}",
            bucket.borrow(),
            path.to_str().unwrap()
        );
        std::fs::remove_dir(path).context("Delete bucket path")?;
        Ok(())
    }
    /// Checks if a file exists in the specified bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket.
    /// * `file_path` - The path of the file to check.
    ///
    /// # Returns
    ///
    /// A `Result` containing a boolean indicating whether the file exists.
    pub fn exists<P: Borrow<str>>(&self, bucket: P, file_path: P) -> Result<bool> {
        let path = self
            .root
            .path()
            .join(bucket.borrow())
            .join(file_path.borrow());
        Ok(std::fs::exists(path.as_path())?)
    }

    /// Retrieves the contents of a file as bytes from the specified bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket.
    /// * `file_path` - The path of the file to retrieve.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of bytes representing the file contents.
    pub fn get_bytes<P: Borrow<str>, F: Borrow<str>>(
        &self,
        bucket: P,
        file_path: F,
    ) -> Result<Vec<u8>> {
        let mut file = self.get(bucket, file_path)?;
        let mut target_bytes = Vec::new();
        file.read_to_end(&mut target_bytes)
            .context("File reading")?;
        Ok(target_bytes)
    }

    /// Retrieves a file from the specified bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket.
    /// * `file_path` - The path of the file to retrieve.
    ///
    /// # Returns
    ///
    /// A `Result` containing the file.
    pub fn get<P: Borrow<str>, F: Borrow<str>>(&self, bucket: P, file_path: F) -> Result<File> {
        let path = self
            .root
            .path()
            .join(bucket.borrow())
            .join(file_path.borrow());
        debug!(
            "FakeS3Server: reading from bucket {}, file_path {}, fs path {}",
            bucket.borrow(),
            file_path.borrow(),
            path.to_str().unwrap()
        );
        Ok(File::open(path)?)
    }

    /// Stores bytes into a file in the specified bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket.
    /// * `file_path` - The path of the file to store.
    /// * `contents` - The contents to store in the file.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub fn put_bytes<P: Borrow<str>, F: Borrow<str>>(
        &self,
        bucket: P,
        file_path: F,
        contents: &[u8],
    ) -> Result<()> {
        let path = self
            .root
            .path()
            .join(bucket.borrow())
            .join(file_path.borrow());
        debug!(
            "FakeS3Server: save bytes into bucket {}, file_path {}, fs path {}",
            bucket.borrow(),
            file_path.borrow(),
            path.to_str().unwrap()
        );
        std::fs::File::create(path)
            .context("Open file for write")?
            .write_all(contents)
            .context("Writing file contents")?;
        Ok(())
    }

    pub fn delete_bytes<P: Borrow<str>, F: Borrow<str>>(
        &self,
        bucket: P,
        file_path: F,
    ) -> Result<()> {
        let path = self
            .root
            .path()
            .join(bucket.borrow())
            .join(file_path.borrow());
        debug!(
            "FakeS3Server: delete bytes from bucket {}, file_path {}, fs path {}",
            bucket.borrow(),
            file_path.borrow(),
            path.to_str().unwrap()
        );

        std::fs::remove_file(path).context("Delete file")?;

        Ok(())
    }

    /// Discards the fake S3 server, cancelling any ongoing operations.
    pub fn discard(self) {
        self.cancel.cancel();
        self.handle.abort();
    }
}

pub fn new_s3_server(
    region: Region,
    credentials: Option<Credentials>,
    runtime: Arc<Runtime>,
) -> Result<FakeS3Server> {
    let maybe_auth = if let Some(creds) = credentials {
        match (creds.access_key, creds.secret_key) {
            (Some(access_key), Some(secret_key)) => {
                Some(SimpleAuth::from_single(access_key, secret_key))
            }
            _ => None,
        }
    } else {
        None
    };

    debug!("s3 server {} has auth {:?}", region.endpoint(), maybe_auth);

    let url: Url = region
        .endpoint()
        .parse()
        .context(format!("Invalid endpoint address {}", region.endpoint()))?;

    let host = url
        .host()
        .ok_or(anyhow!("s3 url does not have host"))?
        .to_string();
    let port = url
        .port()
        .ok_or(anyhow!("s3 url does not have port specified"))?;

    let s3 = FakeS3Server::new(host, port, maybe_auth, runtime.clone())
        .context(format!("Creating FakeS3Server {}", region.endpoint()))?;
    Ok(s3)
}

pub fn generate_s3_config() -> (Region, Credentials) {
    let port = TEST_INSTANCE_S3_PORT.fetch_add(1, Ordering::SeqCst);

    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: format!("http://127.0.0.1:{}", port),
    };

    let credentials = Credentials {
        access_key: Some("access-key".into()),
        secret_key: Some("secret-key".into()),
        security_token: None,
        session_token: None,
        expiration: None,
    };
    (region, credentials)
}
