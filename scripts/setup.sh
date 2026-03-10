#!/usr/bin/env bash
#
# Bootstrap script for TraceHouse development environment.
# Checks for required tools and installs what's missing.
#
# Usage:
#   ./setup.sh              # check & install everything
#   ./setup.sh --check      # dry-run: only report what's missing
#   ./setup.sh --security   # also install security tools (semgrep)
#
# This script must live OUTSIDE the justfile since just itself is a dependency.

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────
REQUIRED_NODE_MAJOR=20
REQUIRED_PYTHON_MINOR=10   # 3.10+
REQUIRED_JUST_VERSION="1.0"

# ── Colors ───────────────────────────────────────────────────────
R='\033[0;31m' G='\033[0;32m' Y='\033[1;33m' B='\033[0;34m' D='\033[0;90m' N='\033[0m'

ok()   { echo -e "  ${G}✓${N} $1 ${D}$2${N}"; }
warn() { echo -e "  ${Y}!${N} $1 ${D}$2${N}"; }
fail() { echo -e "  ${R}✗${N} $1 ${D}$2${N}"; }
info() { echo -e "  ${B}→${N} $1"; }

DRY_RUN=false
INSTALL_SECURITY=false
MISSING=0

for arg in "$@"; do
    case "$arg" in
        --check)    DRY_RUN=true ;;
        --security) INSTALL_SECURITY=true ;;
    esac
done

# ── OS Detection ─────────────────────────────────────────────────
detect_os() {
    case "$(uname -s)" in
        Darwin*) OS="macos" ;;
        Linux*)  OS="linux" ;;
        *)       OS="unknown" ;;
    esac

    if [[ "$OS" == "linux" ]]; then
        if command -v apt-get &>/dev/null; then
            PKG_MGR="apt"
        elif command -v dnf &>/dev/null; then
            PKG_MGR="dnf"
        elif command -v yum &>/dev/null; then
            PKG_MGR="yum"
        elif command -v pacman &>/dev/null; then
            PKG_MGR="pacman"
        else
            PKG_MGR="unknown"
        fi
    elif [[ "$OS" == "macos" ]]; then
        PKG_MGR="brew"
    else
        PKG_MGR="unknown"
    fi
}

# ── Helpers ──────────────────────────────────────────────────────
has() { command -v "$1" &>/dev/null; }

version_gte() {
    # Returns 0 if $1 >= $2 (dot-separated version comparison)
    printf '%s\n%s' "$2" "$1" | sort -t. -k1,1n -k2,2n -k3,3n -C
}

install_or_warn() {
    local name="$1" install_cmd="$2"
    if $DRY_RUN; then
        warn "$name is missing" "install with: $install_cmd"
    else
        info "Installing $name..."
        eval "$install_cmd"
    fi
}

# ── Checks ───────────────────────────────────────────────────────

check_homebrew() {
    if [[ "$OS" != "macos" ]]; then return; fi
    echo ""
    echo "Homebrew"
    if has brew; then
        ok "brew" "$(brew --version | head -1)"
    else
        MISSING=$((MISSING + 1))
        fail "brew" "required on macOS to install other dependencies"
        if ! $DRY_RUN; then
            info "Installing Homebrew..."
            /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        else
            warn "brew is missing" "see https://brew.sh"
        fi
    fi
}

check_just() {
    echo ""
    echo "just (command runner)"
    if has just; then
        local ver
        ver=$(just --version | awk '{print $2}')
        if version_gte "$ver" "$REQUIRED_JUST_VERSION"; then
            ok "just $ver" ""
        else
            MISSING=$((MISSING + 1))
            fail "just $ver" "need >= $REQUIRED_JUST_VERSION"
            case "$PKG_MGR" in
                brew)   install_or_warn "just" "brew install just" ;;
                apt)    install_or_warn "just" "sudo apt-get install -y just || cargo install just" ;;
                *)      install_or_warn "just" "cargo install just  # or see https://just.systems/man/en/installation.html" ;;
            esac
        fi
    else
        MISSING=$((MISSING + 1))
        fail "just" "not found"
        case "$PKG_MGR" in
            brew)    install_or_warn "just" "brew install just" ;;
            apt)     install_or_warn "just" "sudo snap install --edge --classic just || cargo install just" ;;
            pacman)  install_or_warn "just" "sudo pacman -S just" ;;
            *)       install_or_warn "just" "cargo install just  # or see https://just.systems/man/en/installation.html" ;;
        esac
    fi
}

check_python() {
    echo ""
    echo "Python (>= 3.${REQUIRED_PYTHON_MINOR})"
    local py_cmd=""
    for cmd in python3 python; do
        if has "$cmd"; then
            py_cmd="$cmd"
            break
        fi
    done

    if [[ -n "$py_cmd" ]]; then
        local ver minor
        ver=$($py_cmd --version 2>&1 | awk '{print $2}')
        minor=$(echo "$ver" | cut -d. -f2)
        if [[ "$minor" -ge "$REQUIRED_PYTHON_MINOR" ]]; then
            ok "$py_cmd $ver" ""
        else
            MISSING=$((MISSING + 1))
            fail "$py_cmd $ver" "need >= 3.${REQUIRED_PYTHON_MINOR}"
            case "$PKG_MGR" in
                brew)   install_or_warn "python3" "brew install python@3.12" ;;
                apt)    install_or_warn "python3" "sudo apt-get install -y python3" ;;
                dnf)    install_or_warn "python3" "sudo dnf install -y python3" ;;
                *)      warn "Please install Python >= 3.${REQUIRED_PYTHON_MINOR}" "https://www.python.org/downloads/" ;;
            esac
        fi
    else
        MISSING=$((MISSING + 1))
        fail "python3" "not found"
        case "$PKG_MGR" in
            brew)   install_or_warn "python3" "brew install python@3.12" ;;
            apt)    install_or_warn "python3" "sudo apt-get install -y python3" ;;
            dnf)    install_or_warn "python3" "sudo dnf install -y python3" ;;
            *)      warn "Please install Python >= 3.${REQUIRED_PYTHON_MINOR}" "https://www.python.org/downloads/" ;;
        esac
    fi
}

check_uv() {
    echo ""
    echo "uv (Python package runner)"
    if has uv; then
        local ver
        ver=$(uv --version | awk '{print $2}')
        ok "uv $ver" ""
    else
        MISSING=$((MISSING + 1))
        fail "uv" "not found"
        install_or_warn "uv" "curl -LsSf https://astral.sh/uv/install.sh | sh"
    fi
}

check_node() {
    echo ""
    echo "Node.js (>= ${REQUIRED_NODE_MAJOR}.x)"
    if has node; then
        local ver major
        ver=$(node --version | tr -d 'v')
        major=$(echo "$ver" | cut -d. -f1)
        if [[ "$major" -ge "$REQUIRED_NODE_MAJOR" ]]; then
            ok "node $ver" ""
        else
            MISSING=$((MISSING + 1))
            fail "node $ver" "need >= ${REQUIRED_NODE_MAJOR}.x"
            case "$PKG_MGR" in
                brew)   install_or_warn "node" "brew install node@${REQUIRED_NODE_MAJOR}" ;;
                apt)    install_or_warn "node" "curl -fsSL https://deb.nodesource.com/setup_${REQUIRED_NODE_MAJOR}.x | sudo -E bash - && sudo apt-get install -y nodejs" ;;
                *)      warn "Please install Node.js >= ${REQUIRED_NODE_MAJOR}" "https://nodejs.org/" ;;
            esac
        fi
    else
        MISSING=$((MISSING + 1))
        fail "node" "not found"
        case "$PKG_MGR" in
            brew)   install_or_warn "node" "brew install node@${REQUIRED_NODE_MAJOR}" ;;
            apt)    install_or_warn "node" "curl -fsSL https://deb.nodesource.com/setup_${REQUIRED_NODE_MAJOR}.x | sudo -E bash - && sudo apt-get install -y nodejs" ;;
            *)      warn "Please install Node.js >= ${REQUIRED_NODE_MAJOR}" "https://nodejs.org/" ;;
        esac
    fi
}

check_npm() {
    echo ""
    echo "npm"
    if has npm; then
        ok "npm $(npm --version)" ""
    else
        MISSING=$((MISSING + 1))
        fail "npm" "not found (should come with Node.js)"
        warn "npm is bundled with Node.js" "install Node.js first"
    fi
}

# ── Security tools (--security flag) ─────────────────────────────

check_semgrep() {
    echo ""
    echo "semgrep (security scanner — LGPL engine, proprietary rules license)"
    if has semgrep; then
        local ver
        ver=$(semgrep --version 2>/dev/null)
        ok "semgrep $ver" ""
    else
        MISSING=$((MISSING + 1))
        fail "semgrep" "not found"
        case "$PKG_MGR" in
            brew)   install_or_warn "semgrep" "brew install semgrep" ;;
            apt)    install_or_warn "semgrep" "python3 -m pip install semgrep" ;;
            dnf)    install_or_warn "semgrep" "python3 -m pip install semgrep" ;;
            pacman) install_or_warn "semgrep" "python3 -m pip install semgrep" ;;
            *)      install_or_warn "semgrep" "python3 -m pip install semgrep" ;;
        esac
    fi
}

# ── Optional tools ───────────────────────────────────────────────

check_optional() {
    echo ""
    echo "Optional tools"

    if has docker; then
        ok "docker" "$(docker --version | awk '{print $3}' | tr -d ',')"
    else
        warn "docker" "not installed — needed for 'just docker-start' / 'just start'"
    fi

    if has kind; then
        ok "kind" "$(kind --version | awk '{print $3}')"
    else
        warn "kind" "not installed — needed for 'just dev-k8s'"
    fi

    if has kubectl; then
        ok "kubectl" "$(kubectl version --client -o json 2>/dev/null | grep gitVersion | head -1 | awk -F'"' '{print $4}')"
    else
        warn "kubectl" "not installed — needed for K8s workflows"
    fi

    if has clickhouse; then
        ok "clickhouse" "$(clickhouse client --version 2>/dev/null | awk '{print $NF}' || echo 'installed')"
    else
        warn "clickhouse" "not installed — needed for 'just local-start' and 'just drop-data'"
    fi
}

# ── npm install ──────────────────────────────────────────────────

run_npm_install() {
    if $DRY_RUN; then return; fi
    if ! has npm; then return; fi
    echo ""
    read -p "Run 'npm install' for workspace dependencies? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        npm install
        ok "npm install" "workspace dependencies installed"
    else
        info "Skipped npm install. Run 'just install' later if needed."
    fi
}

# ── Main ─────────────────────────────────────────────────────────

main() {
    echo ""
    echo "╔══════════════════════════════════════════════╗"
    echo "║   TraceHouse — Environment Setup    ║"
    echo "╚══════════════════════════════════════════════╝"

    if $DRY_RUN; then
        echo ""
        echo "(dry run — nothing will be installed)"
    fi

    detect_os
    echo ""
    echo "System: $OS ($PKG_MGR)"

    check_homebrew
    check_just
    check_python
    check_uv
    check_node
    check_npm
    if $INSTALL_SECURITY; then
        check_semgrep
    fi
    check_optional

    if [[ "$MISSING" -eq 0 ]]; then
        run_npm_install
        echo ""
        echo -e "${G}All required tools are installed.${N}"
        echo ""
        echo "Next steps:"
        echo "  just install    # install workspace npm deps (if not done above)"
        echo "  just start      # start everything"
        echo "  just status     # check service status"
    else
        echo ""
        if $DRY_RUN; then
            echo -e "${Y}${MISSING} tool(s) missing.${N} Run ./setup.sh to install them."
        else
            echo -e "${Y}Some tools could not be auto-installed.${N} See warnings above."
        fi
    fi
    echo ""
}

main "$@"
