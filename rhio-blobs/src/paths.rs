pub const META_SUFFIX: &str = ".meta";
pub const OUTBOARD_SUFFIX: &str = ".bao4";

#[derive(Debug, Clone)]
pub struct Paths {
    path: String,
}

impl Paths {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    pub fn data(&self) -> String {
        self.path.clone()
    }

    pub fn meta(&self) -> String {
        format!("{}{META_SUFFIX}", self.path)
    }

    pub fn outboard(&self) -> String {
        format!("{}{OUTBOARD_SUFFIX}", self.path)
    }
}
