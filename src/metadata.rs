/// current cargo package version
pub fn cargo_package_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
