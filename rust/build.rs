fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = &["proto/scantivy.proto"];
    let proto_include_dirs = &["proto"];
    prost_build::Config::new().compile_protos(proto_files, proto_include_dirs)?;
    Ok(())
}
