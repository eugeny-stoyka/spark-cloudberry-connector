#!/usr/bin/env bash
# The command of the cloudberry service of the stand: the start script of the image (/start_cbdb.sh) with
# CLOUDBERRY_SEGMENTS primary segments. The single-node mode of the image has 2, written in that script: the
# directories of DATA_DIRECTORY and the setup_segments "00" / "01" calls — they become N here. The run fails if the
# image changes those lines (a new version of the image: check its script).
set -euo pipefail

n="${CLOUDBERRY_SEGMENTS:-4}"
[[ "$n" =~ ^[1-9][0-9]*$ ]] || { echo "CLOUDBERRY_SEGMENTS must be a positive integer, got: $n" >&2; exit 2; }
dirs=""
for ((i = 0; i < n; i++)); do
  dirs+="$(printf '%s/%02d/primary ' "$CLOUDBERRY_DATA_DIRECTORY" "$i")"
done

awk -v dirs="${dirs% }" -v n="$n" '
  /^declare -a DATA_DIRECTORY=\(/ { print "declare -a DATA_DIRECTORY=(" dirs ")"; patchedDirs++; next }
  /^ *setup_segments "00" "primary"$/ {
    indent = $0; sub(/setup_segments.*/, "", indent)
    for (i = 0; i < n; i++) printf "%ssetup_segments \"%02d\" \"primary\"\n", indent, i
    patchedSetup++; next
  }
  /^ *setup_segments "01" "primary"$/ { next }
  { print }
  END { if (patchedDirs != 1 || patchedSetup != 1) exit 3 }
' /start_cbdb.sh > /tmp/start_cbdb.sh ||
  { echo "the segments of /start_cbdb.sh not found: the script of the image has changed" >&2; exit 3; }

echo "INFO - Cloudberry with $n primary segments"
exec bash /tmp/start_cbdb.sh "$@"
