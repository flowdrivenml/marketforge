# Live deployment configuration

The daemon and standalone management TUI locate the workspace using
`MARKETFORGE_PROJECT_ROOT` or the enclosing checkout. Set the root explicitly
when launching them from another directory. The daemon reads
`data/.config/live/general.toml` by default.

## Credential files

Create credential files for the account that runs the corresponding process.
Use regular UTF-8 files owned by that account with mode `0600`. The loaders
reject symlinks and group or other access. The existing Git ignore rule covers
the workspace `.secrets/` directory.

| Workspace file | Contents |
| --- | --- |
| `.secrets/postgres.env` | Literal dotenv assignments. Use `PRODUCTION_DATABASE_KEY` for Live's definition database and `TEST_DATABASE_KEY` for the separate testing database. Use PostgreSQL URLs for credentials shared with Live. |
| `.secrets/live-management.json` | JSON array of objects with `identity`, `role`, and `token`. Supported roles are `viewer`, `operator`, and `administrator`. Existing credential bounds and role checks apply. |
| `.secrets/live-management-token` | A single bearer token for the client identity the operator has authorized. |

For local PostgreSQL provisioning, use the installer to create the selected
database and write its URL privately. An existing valid credential is reused.
For an externally provisioned database, place its URL under the appropriate
key. Keep a single assignment per key and preserve the other environment's
credentials.

For management, create the directory with mode `0700` if it is absent. Use a
private editor or secret manager to write the server JSON with one independent
high-entropy token per identity. Choose the minimum role needed for each
client. Install only that identity's token in the client file on the client
account, or use an explicit private token file for that account. Create files
under `umask 077` and verify file ownership and mode `0600` before starting
Live. Keep existing files and identities when upgrading. Management credential
provisioning and rotation are explicit operator actions.

For Live's definition database, `MARKETFORGE_DATABASE_URL` overrides
`PRODUCTION_DATABASE_KEY` in the environment. The same production key then
falls back to the workspace file. The shared Rust resolver also accepts a
caller-supplied explicit URL. Testing uses its selected key and an explicitly
named database. Blank selected database-key environment variables fall back
to the file, while a blank legacy Live override is an error. The dotenv reader
accepts optional `export`, quoting, and comments. Both Rust and Forge reject
duplicate assignments for the selected key. Forge continues to accept its
existing libpq connection-string forms.

Server precedence is `control.management.credentials_file`, the configured
environment source, then the workspace JSON file. The standard environment
variable is `MARKETFORGE_MANAGEMENT_CREDENTIALS` and contains the JSON itself.
A custom `credentials_env` explicitly selects that variable. Relative
`credentials_file` overrides retain their invocation-directory interpretation.
Use the implicit workspace default to avoid dependence on the invocation directory.

Client precedence is `--token-file`, `MARKETFORGE_MANAGEMENT_TOKEN`, then
the workspace token file. A custom `--token-env` explicitly selects that variable.
Provision each client with its own authorized credential. The daemon does not
provision identities on startup.

## REST quotas and egress

Add an optional `[deployment]` table to `general.toml` with `egress_id` set to
the identity shared by Sources using the same egress. When omitted, the identity
is `local-egress`. For each weighted profile used by this deployment, add
`[deployment.rest_weight.PROFILE]` with positive integers for `capacity` and
`window_seconds`. In the bundled profiles these tables apply to
`binance_spot`, `binance_linear`, and `binance_inverse`, which use distinct
REST API families. Enter the verified deployment quota for each applicable
family. `--egress-id` and `--rest-weight PROFILE=CAPACITY/WINDOW_SECONDS`
override the corresponding values for one invocation.

Live compiles operational profiles at startup and shares the existing limiter
manager across Sources and transports. Profile preparation opens no exchange
connections. A profile missing a prerequisite is logged as unavailable. An
unavailable profile required by an enabled persisted Source prevents startup.
`--profiles` can explicitly restrict the set prepared by the daemon.

`--validate-only` validates the selected configuration offline. It checks
the configured quota and protocol bindings and reports declared automatic trade
recovery strategies without executable support. Credential resolution, database
connectivity, exchange connectivity, and persisted Source readiness are outside
this offline check.
