# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately, not as a public issue. Use
GitHub's private vulnerability reporting for this repository (the "Security" tab
-> "Report a vulnerability"). Include a description, the affected version, and
reproduction steps. We aim to acknowledge within a few days and will keep you
posted on remediation.

## Scope

persistent-queue is a small, safe storage primitive: no `unsafe` in our code, no
network, no credentials. It does local disk I/O through the configured backend.
The security-relevant promises it makes are that acked items are not lost and
that delivery is at-least-once. In scope, for example:

- A path where an acked item is redelivered, or an unacked item is silently
  dropped (the delivery guarantee does not hold).
- Store corruption or a failed reopen reachable from safe, documented use.
- A panic, deadlock, or overflow in the queue bookkeeping reachable from safe,
  documented use.

Documented behavior is not a vulnerability on its own: at-least-once redelivering
an item after a crash between the side effect and the ack, and `Durability::None`
losing recent items on a crash, are both by design and covered in the README and
rustdoc.

## Supported versions

Pre-1.0: fixes land on the latest release published to crates.io.
