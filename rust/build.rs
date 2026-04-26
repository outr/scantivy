fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the vendored protoc binary so the build doesn't depend on `protoc` being installed
    // in the host environment (CI runners, `cross` Docker images, fresh dev setups, etc.).
    // `prost-build` reads the `PROTOC` env var to locate the compiler.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts are single-threaded; setting an env var here is sound.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    let proto_files = &["proto/scantivy.proto"];
    let proto_include_dirs = &["proto"];
    prost_build::Config::new().compile_protos(proto_files, proto_include_dirs)?;
    Ok(())
}
