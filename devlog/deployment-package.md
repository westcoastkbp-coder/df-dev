# Deployment Package

- Added `deploy/install.sh` to copy `app/`, `runtime/`, and `scripts/` into `/opt/digital_foreman/`.
- Installer creates the target directory and applies executable permissions to scripts.
- Did not add systemd installation logic.
