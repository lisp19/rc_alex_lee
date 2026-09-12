#!/bin/sh
set -eu
# Fresh local data directories only. Production uses an independent DBA job.
for password in "$DB_BUSINESS_PASSWORD" "$DB_CONTROL_PASSWORD" "$DB_ADMIN_PASSWORD"; do
  case "$password" in ''|*[!a-f0-9]*) printf '%s\n' 'expected hexadecimal DB password' >&2; exit 1;; esac
done
MYSQL_PWD="$MARIADB_ROOT_PASSWORD" mariadb -uroot <<SQL
CREATE USER IF NOT EXISTS 'notify_runtime_business'@'%' IDENTIFIED BY '$DB_BUSINESS_PASSWORD';
CREATE USER IF NOT EXISTS 'notify_runtime_control'@'%' IDENTIFIED BY '$DB_CONTROL_PASSWORD';
CREATE USER IF NOT EXISTS 'notify_admin_control'@'%' IDENTIFIED BY '$DB_ADMIN_PASSWORD';
GRANT SELECT,INSERT,UPDATE,DELETE ON notify_business.* TO 'notify_runtime_business'@'%';
GRANT SELECT ON notify_control.* TO 'notify_runtime_control'@'%';
GRANT SELECT,INSERT,UPDATE,DELETE ON notify_control.* TO 'notify_admin_control'@'%';
SQL
