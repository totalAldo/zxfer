# zxfer Examples

This directory holds small command templates for the most common zxfer
workflows. Review and edit dataset names, hostnames, and option flags before
running anything against a real system.

## Included Templates

- [local-recursive-replication.sh](./local-recursive-replication.sh): local recursive send/receive
- [remote-pull-origin.sh](./remote-pull-origin.sh): pull from a remote origin with `-O`
- [remote-push-target-compressed.sh](./remote-push-target-compressed.sh): push to a remote target with `-T -z`
- [property-backup-restore.sh](./property-backup-restore.sh): capture and later restore property metadata with `-k` and `-e`
- [error-log-email-notify.sh](./error-log-email-notify.sh): mirror structured failure reports into `ZXFER_ERROR_LOG` and email the current failure report captured from stderr with `mailx`, BSD `mail`, or `sendmail`

These are intentionally conservative examples. They show the command shape and
the main options involved without trying to automate environment-specific host
or pool setup.

Each script resolves the project root relative to its own location, so it can
be run from any current working directory while still targeting the local
checkout's `zxfer` entry point.

For [`error-log-email-notify.sh`](./error-log-email-notify.sh):

- Linux and illumos deployments commonly use `mailx`; FreeBSD and macOS often provide BSD `mail` in the base system. Leave `MAILER=auto` unless you need to force `mailx`, `mail`, or `sendmail`.
- Set `TARGET_HOST`, `ORIGIN_HOST`, and `RAW_SEND=1` when the wrapped zxfer command needs `-T`, `-O`, or `-w` in addition to the default `-v -R`.
- Set `MAIL_FROM` when your MTA requires a sender address. The default `MAIL_FROM_FLAG` is `-r` for `mailx`/`mail`, and the default `SENDMAIL_FROM_FLAG` is `-f` for `sendmail`; override either flag if your local mailer expects something different.
- Validate the wrapper logic without touching ZFS or a real mail server by running `sh ./examples/error-log-email-notify.sh --self-test`.
