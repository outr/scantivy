#!/bin/bash
set -e

export CARGO_TARGET_DIR=$(pwd)/rust/target
export OUT_DIR=$(pwd)/rust/target/x86_64-pc-windows-gnu/release/build

echo "🔧 Building Rust library..."
cd rust/

cargo install cross

echo "🔧 Building for Linux..."
cargo build --release

#echo "🔧 Building for Windows..."
#cargo build --release --target=x86_64-pc-windows-gnu

echo "🔧 Building for macOS (Intel)..."
cargo zigbuild --release --target=x86_64-apple-darwin

echo "🔧 Building for macOS (Apple Silicon)..."
cargo zigbuild --release --target=aarch64-apple-darwin

cd ..

echo "📦 Copying shared library to Scala project..."
mkdir -p scala/lib
cp rust/target/release/libscantivy.so scala/lib/
cp rust/target/aarch64-apple-darwin/release/libscantivy.dylib scala/lib/libscantivy-aarch64.dylib
cp rust/target/x86_64-apple-darwin/release/libscantivy.dylib scala/lib/libscantivy-x86_64.dylib

echo "🚀 Building Scala project..."
cd scala/
sbt compile
cd ..

echo "✅ Build complete!"
