#!/bin/bash
set -e

echo "🔧 Building Rust library..."
cd rust/
cargo build --release
cd ..

echo "📦 Copying shared library to Scala project..."
mkdir -p scala/lib
cp rust/target/release/libscantivy.so scala/lib/

echo "🚀 Building Scala project..."
cd scala/
sbt compile
cd ..

echo "✅ Build complete!"
