# How to address production security considerations

This guide lists the security controls you must set up around `eventsource`
in production. The library provides some of them. You provide the rest. Each
section states which is which.

## Before you start

- You use a PostgreSQL or SQLite event store, and a bus if you use one
  (Kafka, RabbitMQ, or Redis).
- You know which parts of your system read and write the event store, the read
  models, and the snapshot tables.

## Encrypt broker connections (TLS)

Traffic between your application and a broker can carry event payloads and
credentials. Use TLS on every connection that leaves a trusted network.

- **Kafka.** Set `security_protocol` to `SSL` or `SASL_SSL` on
  `KafkaEventBusConfig`. `PLAINTEXT` and `SASL_PLAINTEXT` send data without
  encryption. Set `ssl_cafile` to your certificate authority. Keep
  `ssl_check_hostname=True`, the default.
- **RabbitMQ.** Use an `amqps://` URL. On `RabbitMQEventBusConfig`, keep
  `verify_ssl=True`, the default. Pass `ca_file`, `cert_file`, or an
  `ssl_context` when your broker uses a private certificate authority.
- **Redis.** Use a `rediss://` URL.

The RabbitMQ and Redis adapters warn when a URL uses a plain scheme
(`amqp://` or `redis://`) and points at a remote host. Treat that warning as a
failed check. Do not ignore it.

## Keep secrets out of code

- Load broker passwords, SASL credentials, and database passwords from
  environment variables or a secret manager. Do not write them in source code,
  configuration files in version control, or test fixtures.
- Give each service its own database role and broker user. Grant each role only
  the rights it needs.
- Rotate credentials through your secret manager. The library reads the
  credentials you pass when it starts. Restart the process after you rotate
  them.

The library hides user names and passwords in connection URLs when the RabbitMQ
and Redis adapters build a `repr` of their configuration. Do not depend on this
for log output. Do not log a raw connection string or a config object yourself.

## Redact logs

- **Do not log event payloads.** Events often hold personal data. The library
  logs identifiers (aggregate ids, event ids, stream names) and error class
  names. It does not log payload contents. Keep it that way in your own
  handlers, projections, and middleware.
- Log the exception class and the identifiers you need to find the failing
  event. Do not log the full event or aggregate state.
- Set the `eventsource` loggers to a level and destination that your log policy
  allows. Review what your handlers write to those logs.
- The library writes an error summary (`ClassName: message`) to its logs,
  spans, health output, and DLQ entries. For validation errors and SQL
  statement errors, the summary keeps only the error structure, with no input
  values. The stored DLQ stack trace holds frames and exception type names
  only.
- Library exceptions such as `BulkCopyError` and `CutoverError` keep the
  original error as their cause (`raise ... from error`). If you log one of
  these exceptions with `exc_info=True` or `logger.exception()`, the log shows
  the cause text. That text can hold payload values. Log the exception class
  and the identifiers instead.

## Limit tenant isolation

Read the [multi-tenancy guide](../guides/multi-tenant.md) before you rely on
tenant boundaries. These limits apply:

- Loading an aggregate by its id does not filter by tenant. A
  `TenantAwareRepository` with `require_tenant_context=True` requires an active
  tenant scope, but the load query does not add a tenant condition. Any caller
  that knows an aggregate id can load that aggregate.
- Use UUID v4 aggregate ids that come from `uuid4()`. Do not use sequential
  ids, timestamps, or ids derived from user input. An unguessable id is the
  only protection against a caller who loads an aggregate by id.
- Check the tenant on every request path in your application. A command whose
  `tenant_id` differs from the active tenant scope raises `TenantMismatchError`.

## Validate read-model query input

The library validates read-model queries before it runs them. The check covers
the types and ranges of `limit` and `offset`, the sort direction, and whether
`order_by` names a field of the model.

Validation does not decide which fields a user may sort by. Keep an allow-list
in your application:

- Map user-facing sort options to fixed field names.
- Never pass raw user input as `order_by`.
- Set a maximum `limit` in your application.

## Protect operator operations

These operations change or replay data. They are for operators, not end users:

- Dead-letter queue inspection and reprocessing.
- Projection rebuilds.
- Replays of events through projections or subscriptions.

Put access control in front of each one. Expose them only through
authenticated tooling, such as an internal admin service or a command-line
tool that runs with operator credentials. Do not expose them on a public
endpoint. Log each operator action with the operator identity.

## Encrypt payloads at rest

The library has no built-in hook for encrypting event payloads or read-model
data. Choose one of these controls:

- **Storage encryption.** Enable encryption at rest for the database and its
  volumes. A managed PostgreSQL service with encryption at rest meets this
  need. For SQLite, encrypt the volume that holds the file.
- **Field encryption.** Encrypt specific fields in your application before you
  append an event. Use keys that the event store does not hold. Decrypt the
  fields in your projections and handlers. Note that event payloads are then
  ciphertext in the store. Plan for key rotation before you start.

Encryption at rest protects data on disk. It does not protect data that an
authorized database user reads.

## Restrict snapshot tables

Snapshot state is read back and used to rebuild aggregates. The library treats
the stored snapshot as trusted input. A changed snapshot changes the aggregate
that loads from it. Restrict write access to snapshot tables:

- Grant `INSERT`, `UPDATE`, and `DELETE` on snapshot tables only to the
  application role that writes snapshots.
- Grant read-only access to every other role.

## Set SQLite file permissions

A SQLite event store is one file. Anyone who can read the file can read every
event and snapshot in it. Set the file to be readable and writable only by the
service user. For example, `chmod 600` on the database file and `chmod 700` on
its directory. Keep the file out of shared or world-readable paths.

The library creates a new database file with mode 0600. It does not change the
mode of a file that exists. SQLite creates the `-wal` and `-shm` files with the
mode of the database file, so set the mode on the database file.

## Checklist

- [ ] Kafka uses `SSL` or `SASL_SSL`. RabbitMQ uses `amqps://`. Redis uses `rediss://`.
- [ ] No credential is in source code, configuration in version control, or test fixtures.
- [ ] Your handlers and middleware do not log event payloads.
- [ ] Aggregate ids are UUID v4 values from `uuid4()`.
- [ ] Read-model sort fields come from an allow-list.
- [ ] Dead-letter queue, rebuild, and replay tools require operator authentication.
- [ ] Storage encryption is on, or you encrypt the fields you need to protect.
- [ ] Only the application role can write snapshot tables.
- [ ] The SQLite database file is readable only by the service user.

## See also

- [Multi-tenancy](../guides/multi-tenant.md)
- [Choose an event store backend](choose-an-event-store-backend.md)
- [Event bus guide](../guides/event-bus.md)
