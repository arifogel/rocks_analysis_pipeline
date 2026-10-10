#!/usr/bin/env bash
# Checks whether running simulation tasks make good progress.
#
# Usage: check_array_perf.sh [-w SECONDS] [-s SINCE] [JOBID...]
#   -w  sampling window per node (default 10)
#   -n  max nodes to ssh to, chosen at random; 0 means all (default 3)
#   -N  comma-separated nodes to probe instead of a random sample
#   -s  sacct start time for finished tasks (default now-1days)
#   JOBID  array job ids to look at (default: every job you are running)
#
# Part 1 ssh's to each node running your jobs and samples every process of yours
# that holds a .spec/.speck file open. Healthy: CPU% high, the same output files
# held open for the whole window (FDS equal), MB/min steady. Unhealthy: state D, CPU%
# low, FDS changing, which means the file is being reopened.
# A running job missing from the table either is not in a specsims/Katydid step or
# reopens its files between samples.
# Part 2 counts your jobs by Slurm state from sacct and summarizes the completed ones.

set -uo pipefail
USER=${USER:-$(id -un)}

window=10
max_nodes=3
pinned_nodes=
since="now-1days"
while getopts "w:n:N:s:" opt; do
  case $opt in
    w) window=$OPTARG ;;
    n) max_nodes=$OPTARG ;;
    N) pinned_nodes=$OPTARG ;;
    s) since=$OPTARG ;;
    *) exit 2 ;;
  esac
done
shift $((OPTIND - 1))

# Runs on a compute node. Prints: JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB_PER_MIN CMD WCHAN THREADS AGE_MIN
probe() {
  local w=$1 tick pid fd t rest n bytes sz comm wchan thr start el up
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
      comm=$(tr ' ' _ <"$p/comm" 2>/dev/null)
      wchan=$(cat "$p/wchan" 2>/dev/null)
      thr=$(cat "$p"/task/*/stat 2>/dev/null | sed 's/^.*) //' | cut -c1 | sort | uniq -c | awk '{printf "%s%s", $2, $1}')
      start=$(echo "$rest" | awk '{print $20}')
      up=$(cut -d' ' -f1 /proc/uptime)
      el=$(awk -v u="$up" -v s="$start" -v t="$tick" 'BEGIN {printf "%d", (u - s / t) / 60}')
      echo "$pid $rest $n $bytes ${comm:-?} ${wchan:-0} ${thr:-?} $el" | awk '{print $1, $2, $13 + $14, $(NF-5), $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
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
      printf "%s %s %s %.0f %d %d %.1f %s %s %s %s\n", job, $1, $2, ($3 - t0[$1]) / tick / w * 100, n0[$1], $4, ($5 - b0[$1]) / 1048576 / w * 60, $6, $7, $8, $9
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
total_nodes=$(echo "$nodes" | grep -c .)
if [ -n "$pinned_nodes" ]; then
  nodes=$(echo "$pinned_nodes" | tr ',' '\n')
elif [ "$max_nodes" -gt 0 ] && [ "$total_nodes" -gt "$max_nodes" ]; then
  nodes=$(echo "$nodes" | sort -R | head -n "$max_nodes")
fi

echo "== Running processes (window ${window}s, $(echo "$nodes" | grep -c .) of $total_nodes nodes) =="
if [ -z "$nodes" ]; then
  echo "no running jobs"
else
  tmp=$(mktemp -d)
  for node in $nodes; do
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$node" "$(declare -f probe); probe $window" >"$tmp/$node" 2>"$tmp/$node.err" &
  done
  wait
  {
    echo "NODE JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB/min CMD WCHAN THREADS AGE_MIN VERDICT"
    for node in $nodes; do
      if [ -s "$tmp/$node.err" ]; then echo "$node ERROR $(head -1 "$tmp/$node.err")"; fi
      echo "$node has $(echo "$jobs" | awk -v n="$node" '$2 == n' | wc -l) running jobs, $(wc -l <"$tmp/$node") with .spec/.speck files open" >&2
      awk -v node="$node" '{v = ($3 != "D" && $4 >= 70 && $5 == $6) ? "OK" : "SLOW"; print node, $0, v}' "$tmp/$node"
    done
  } | awk '{for (i = 1; i <= NF; i++) printf "%-*s ", (i == NF ? 0 : 11), $i; print ""}'
  rm -rf "$tmp"
fi

echo
echo "== Jobs by Slurm state (sacct since $since) =="
if [ $# -gt 0 ]; then
  acct=$(sacct -n -P -X -j "$(IFS=,; echo "$*")" -o State,Elapsed,TotalCPU,AllocCPUS)
else
  acct=$(sacct -n -P -X -u "$USER" -S "$since" -o State,Elapsed,TotalCPU,AllocCPUS)
fi
echo "$acct" | awk -F'|' 'NF {split($1, w, " "); c[w[1]]++} END {for (st in c) printf "%6d %s\n", c[st], st}' | sort -rn
rows=$(echo "$acct" | awk -F'|' '
  function secs(t,   d, p, n, s) {
    d = 0
    if (index(t, "-")) { split(t, p, "-"); d = p[1]; t = p[2] }
    n = split(t, p, ":")
    s = (n == 3) ? p[1] * 3600 + p[2] * 60 + p[3] : p[1] * 60 + p[2]
    return d * 86400 + s
  }
  $1 == "COMPLETED" && secs($2) > 0 { printf "%.3f %d\n", secs($3) / (secs($2) * $4), secs($2) }')
echo "completed tasks: median CPU efficiency (TotalCPU / Elapsed / CPUs): $(echo "$rows" | awk 'NF {print $1}' | median)"
echo "completed tasks: median elapsed seconds: $(echo "$rows" | awk 'NF {print $2}' | median)"
