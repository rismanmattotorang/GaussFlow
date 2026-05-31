fn main() {
    // On macOS, Python symbols are resolved from the interpreter at load time.
    // Tell the linker to allow undefined symbols and look them up dynamically.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-link-arg=-undefined");
        println!("cargo:rustc-link-arg=dynamic_lookup");
    }
}
