pub const RHIO_PREFIX: &str = ".rhio/";

pub const META_SUFFIX: &str = ".rhio.json";

pub const OUTBOARD_SUFFIX: &str = ".rhio.bao4";

pub const NO_PREFIX: String = String::new(); // Empty string.

#[derive(Debug, Clone)]
pub struct Paths {
    path: String,
}

impl Paths {
    pub fn new(path: &str) -> Self {
        let path = path.to_string().replace(RHIO_PREFIX, "");
        Self { path }
    }

    pub fn from_meta(path: &str) -> Self {
        Self::new(&path[0..path.len() - META_SUFFIX.len()])
    }

    pub fn data(&self) -> String {
        self.path.clone()
    }

    pub fn meta(&self) -> String {
        format!("{RHIO_PREFIX}{}{META_SUFFIX}", self.path)
    }

    pub fn outboard(&self) -> String {
        format!("{RHIO_PREFIX}{}{OUTBOARD_SUFFIX}", self.path)
    }
}
