#[derive(Debug, Clone)]
pub struct Paths {
    path: String,
}

impl Paths {
    pub fn new(path: String) -> Self {
        Self { path }
    }
    pub fn data(&self) -> String {
        format!("{}", self.path)
    }
    pub fn temp_data(&self) -> String {
        format!("{}.parts", self.path)
    }
    pub fn outboard(&self) -> String {
        format!("{}.bao4", self.path)
    }
}
