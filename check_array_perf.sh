#!/usr/bin/env bash
# Checks whether running simulation tasks make good progress.
#
# Usage: check_array_perf.sh [-w SECONDS] [-s SINCE] [JOBID...]
#   -w  sampling window per node (default 10)
#   -s  sacct start time for finished tasks (default now-1days)
#   JOBID  array job ids to look at (default: every job you are running)
#
# Part 1 ssh's to each node running your jobs and samples every process of yours
# that holds a .spec/.speck file open. Healthy: CPU% high, the same output files
# held open for the whole window (FDS equal), MB/s steady. Unhealthy: state D, CPU%
# low, FDS changing, which means the file is being reopened.
# A running job missing from the table either is not in a specsims/Katydid step or
# reopens its files between samples.
# Part 2 summarizes finished tasks from sacct.

set -uo pipefail
USER=${USER:-$(id -un)}

window=10
since="now-1days"
while getopts "w:s:" opt; do
  case $opt in
    w) window=$OPTARG ;;
    s) since=$OPTARG ;;
    *) exit 2 ;;
  esac
done
shift $((OPTIND - 1))

# Runs on a compute node. Prints: JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB_PER_S
probe() {
  local w=$1 tick pid fd t rest n bytes sz
  tick=$(getconf CLK_TCK)
  snap() {
    for p in /proc/[0-9]*; do
      [ -O "$p" ] || continue
      pid=${p#/proc/}
      n=0 bytes=0
      for fd in "$p"/fd/*; do
        t=$(readlink "$fd" 2>/dev/null) || continue
        case $t in
          *.spec | *.speck)
            n=$((n + 1))
            sz=$(stat -L -c %s "$fd" 2>/dev/null) && bytes=$((bytes + sz))
            ;;
        esac
      done
      [ "$n" -gt 0 ] || continue
      rest=$(sed 's/^.*) //' "$p/stat" 2>/dev/null) || continue
      echo "$pid $rest $n $bytes" | awk '{print $1, $2, $13 + $14, $(NF-1), $NF}'
    done
  }
  snap >/tmp/.perf0.$$
  sleep "$w"
  snap >/tmp/.perf1.$$
  awk -v w="$w" -v tick="$tick" '
    NR == FNR { t0[$1] = $3; n0[$1] = $4; b0[$1] = $5; next }
    ($1 in t0) {
      cmd = "grep -o \"job_[0-9]*\" /proc/" $1 "/cgroup 2>/dev/null | head -1 | cut -d_ -f2"
      job = ""; cmd | getline job; close(cmd)
      if (job == "") job = "-"
      printf "%s %s %s %.0f %d %d %.1f\n", job, $1, $2, ($3 - t0[$1]) / tick / w * 100, n0[$1], $4, ($5 - b0[$1]) / 1048576 / w
    }' /tmp/.perf0.$$ /tmp/.perf1.$$
  rm -f /tmp/.perf0.$$ /tmp/.perf1.$$
}

median() { sort -n | awk '{a[NR] = $1} END {if (NR) print a[int((NR + 1) / 2)]; else print "n/a"}'; }

if [ $# -gt 0 ]; then
  jobs=$(for j in "$@"; do squeue -j "$j" -t R -h -o "%A %N"; done)
else
  jobs=$(squeue -u "$USER" -t R -h -o "%A %N")
fi
nodes=$(echo "$jobs" | awk 'NF {print $2}' | sort -u)

echo "== Running processes (window ${window}s) =="
if [ -z "$nodes" ]; then
  echo "no running jobs"
else
  tmp=$(mktemp -d)
  for node in $nodes; do
    ssh -o BatchMode=yes -o ConnectTimeout=5 "$node" "$(declare -f probe); probe $window" >"$tmp/$node" 2>"$tmp/$node.err" &
  done
  wait
  {
    echo "NODE JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB/s VERDICT"
    for node in $nodes; do
      if [ -s "$tmp/$node.err" ]; then echo "$node ERROR $(head -1 "$tmp/$node.err")"; fi
      echo "$node has $(echo "$jobs" | awk -v n="$node" '$2 == n' | wc -l) running jobs, $(wc -l <"$tmp/$node") with .spec/.speck files open" >&2
      awk -v node="$node" '{v = ($3 != "D" && $4 >= 70 && $5 == $6) ? "OK" : "SLOW"; print node, $0, v}' "$tmp/$node"
    done
  } | awk '{for (i = 1; i <= NF; i++) printf "%-*s", (i == NF ? 0 : 11), $i; print ""}'
  rm -rf "$tmp"
fi

echo
echo "== Finished tasks (sacct since $since) =="
if [ $# -gt 0 ]; then
  acct=$(sacct -n -P -X -j "$(IFS=,; echo "$*")" -s CD,TO -o State,Elapsed,TotalCPU,AllocCPUS)
else
  acct=$(sacct -n -P -X -u "$USER" -S "$since" -s CD,TO -o State,Elapsed,TotalCPU,AllocCPUS)
fi
rows=$(echo "$acct" | awk -F'|' '
  function secs(t,   d, p, n, s) {
    d = 0
    if (index(t, "-")) { split(t, p, "-"); d = p[1]; t = p[2] }
    n = split(t, p, ":")
    s = (n == 3) ? p[1] * 3600 + p[2] * 60 + p[3] : p[1] * 60 + p[2]
    return d * 86400 + s
  }
  $1 == "COMPLETED" && secs($2) > 0 { printf "C %.3f %d\n", secs($3) / (secs($2) * $4), secs($2) }
  $1 == "TIMEOUT" { print "T" }')
echo "completed: $(echo "$rows" | grep -c '^C')   timed out: $(echo "$rows" | grep -c '^T')"
echo "median CPU efficiency of completed (TotalCPU / Elapsed / CPUs): $(echo "$rows" | awk '$1 == "C" {print $2}' | median)"
echo "median elapsed seconds of completed: $(echo "$rows" | awk '$1 == "C" {print $3}' | median)"
