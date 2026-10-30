fn main() {
    println!("cargo:rerun-if-env-changed=DUCKDB_LIB_DIR");
    if let Ok(path) = std::env::var("DUCKDB_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
    }
}
