#!/bin/bash

function log {
    echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") - $0 - $*"
}

[[ -z $1 ]] && echo "Usage: $0 PGDATA" && exit 1

log "I was called as: $0 $*"

readonly PGDATA=$1
NUM_TO_RETAIN=${BACKUP_NUM_TO_RETAIN:-0}  # default to 0 if not set
DAYS_TO_RETAIN=${DAYS_TO_RETAIN:-0}       # default to 0 if not set

IN_RECOVERY=$(psql -tXqAc "select pg_catalog.pg_is_in_recovery()")
readonly IN_RECOVERY
if [[ $IN_RECOVERY == "f" ]]; then
    [[ "$WALG_BACKUP_FROM_REPLICA" == "true" ]] && log "Cluster is not in recovery, not running backup" && exit 0
elif [[ $IN_RECOVERY == "t" ]]; then
    [[ "$WALG_BACKUP_FROM_REPLICA" != "true" ]] && log "Cluster is in recovery, not running backup" && exit 0
else
    log "ERROR: Recovery state unknown: $IN_RECOVERY" && exit 1
fi

# leave at least 2 days base backups before creating a new one
[[ "$DAYS_TO_RETAIN" -lt 2 ]] && DAYS_TO_RETAIN=2

BACKUPS_TO_DELETE=()
NOW=$(date +%s -u)
readonly NOW
while read -r name last_modified rest; do
    last_modified=$(date +%s -ud "$last_modified")
    age_days=$(((NOW-last_modified)/86400))
    
    if [[ $age_days -ge $DAYS_TO_RETAIN ]] && [[ ${#BACKUPS_TO_DELETE[@]} -lt $((NUM_TO_RETAIN-1)) ]]; then
        BACKUPS_TO_DELETE+=("$name")
    fi
done < <($WAL_E backup-list 2> /dev/null | sed '0,/^name\s*\(last_\)\?modified\s*/d')

# Delete old backups, ensuring we retain the minimum number as per BACKUP_NUM_TO_RETAIN
for backup in "${BACKUPS_TO_DELETE[@]}"; do
    if [[ "$USE_WALG_BACKUP" == "true" ]]; then
        $WAL_E delete --confirm FIND_FULL "$backup"
    else
        $WAL_E delete --confirm "$backup"
    fi
done

# push a new base backup
log "producing a new backup"
# We reduce the priority of the backup for CPU consumption
exec nice -n 5 $WAL_E backup-push "$PGDATA" "${POOL_SIZE[@]}"
