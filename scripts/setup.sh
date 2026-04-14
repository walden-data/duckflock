#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# DuckFlock development setup
# ============================================================

echo "🦆 Setting up DuckFlock development environment..."

# --- Git hooks ---
echo "📎 Configuring git hooks..."
git config core.hooksPath .githooks
echo "   Hooks installed from .githooks/"

# --- Verify toolchain ---
echo "🔧 Checking Rust toolchain..."
rustc_version=$(rustc --version)
echo "   $rustc_version"

if ! command -v cargo-fmt &> /dev/null; then
    echo "   Installing rustfmt..."
    rustup component add rustfmt
fi

if ! command -v cargo-clippy &> /dev/null; then
    echo "   Installing clippy..."
    rustup component add clippy
fi

# --- Build check ---
echo "🏗️  Verifying build..."
cargo check --quiet
echo "   Build OK"

echo ""
echo "✅ Setup complete!"
echo ""
echo "Git hooks active:"
echo "  pre-commit: blocks commits to main, checks formatting"
echo "  pre-push:   blocks pushes to main, runs clippy + tests on changed crates"
echo ""
echo "To bypass hooks in emergencies: git commit --no-verify / git push --no-verify"
