# Vault Secrets Management & Rotation Guide

**Security Model:** Vault forbids blank or default credentials outside the explicit local `dev` Spring profile (`SPRING_PROFILES_ACTIVE=dev`). Startup checks in `ProductionSafetyCheck` validate `spring.datasource.password`, `vault.cluster.node-secret`, and `vault.security.api-keys` against known exposed defaults (`vault_secret`, `changeme`, `secret`, `secret123`, `dev-only-node-secret`, `vault-dev-key`, `test-key`, `vault-secret`).

Real secrets (`.env`, `secrets/db_password.txt`) are gitignored and must never be committed.

## Secret Rotation Procedure

> **Important:** Rotate credentials on all live/existing databases and hosts **before** purging any exposed commit from Git history, and coordinate any history rewrite with collaborators.

### Step 1: Rotate PostgreSQL Password & Cluster Credentials

1. Connect to any running PostgreSQL instance storing Vault metadata and rotate the role password:
   ```sql
   ALTER USER vault WITH PASSWORD 'REPLACE_WITH_NEW_RANDOM_PASSWORD';
   ```
2. Generate fresh local secret files and `.env` (never commit them):
   ```bash
   umask 077
   openssl rand -base64 32 | tr -d '\n' > secrets/db_password.txt
   cp .env.example .env
   ```
   Update `.env` with strong random values for:
   - `VAULT_NODE_SECRET` (`openssl rand -hex 32`)
   - `VAULT_API_KEYS` (`openssl rand -hex 24`)
   - `GF_SECURITY_ADMIN_PASSWORD` (`openssl rand -base64 24`)
3. Recreate containers so Compose picks up the rotated secret and `.env`:
   ```bash
   docker compose up -d --force-recreate
   ```
   *(If upgrading an existing `postgres-data` Docker volume, `POSTGRES_PASSWORD_FILE` only runs on first cluster initialization—run the `ALTER USER` SQL above against the running DB first.)*

### Step 2: Purge Exposed Files From Git History (If Ever Committed)

If a real credential file (such as an untracked `.env` or `secrets/db_password.txt`) is ever committed accidentally, rotate every exposed credential first and purge it across all commits with `git-filter-repo`:

```bash
# Install git-filter-repo if not present: pip install git-filter-repo
git filter-repo --invert-paths --path secrets/db_password.txt --force
git remote add origin https://github.com/Jayanand07/Vault.git
git push --force-with-lease origin main
git push --force --tags
```

All contributors must then re-clone (or run `git fetch && git reset --hard origin/main`) so the exposed blob is no longer referenced.
