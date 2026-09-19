#!/usr/bin/env bash
set -euo pipefail

SRC="${1:-dump.txt}"
OUT="${2:-core-proxy}"

[[ -f "$SRC" ]] || { echo "ERROR: $SRC tidak ditemukan"; exit 1; }

rm -rf "$OUT" "${OUT}.zip"

awk -v out="$OUT" '
BEGIN { curfile = ""; count = 0 }
/^=== FILE: .* ===$/ {
    if (curfile != "") close(curfile)
    path = $0
    sub(/^=== FILE: /, "", path)
    sub(/ ===[[:space:]]*$/, "", path)
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", path)

    curfile = out "/" path

    n = split(curfile, parts, "/")
    dir = ""
    for (i = 1; i < n; i++) {
        dir = dir (i > 1 ? "/" : "") parts[i]
    }
    if (dir != "") system("mkdir -p \"" dir "\"")

    print "  + " path
    count++
    next
}
{
    if (curfile != "") print > curfile
}
END {
    if (curfile != "") close(curfile)
    print "\nTotal file diekstrak: " count
}
' "$SRC"

if command -v zip >/dev/null 2>&1; then
    zip -qr "${OUT}.zip" "$OUT"
    echo "Selesai: ${OUT}/ dan ${OUT}.zip"
else
    echo "Selesai: ${OUT}/  (zip tidak tersedia, dilewati)"
fi

