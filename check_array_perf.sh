#!/usr/bin/env bash
# Checks whether running Slurm jobs make good progress.
#
# Usage: check_array_perf.sh [-w SECONDS] [-s SINCE] [JOBID...]
#   -w  sampling window per node (default 10)
#   -n  max nodes to ssh to, chosen at random; 0 means all (default 3)
#   -N  comma-separated nodes to probe instead of a random sample
#   -s  sacct start time for finished tasks (default now-1days)
#   JOBID  job ids to look at (default: every job you are running)
#
# Part 1 ssh's to each node running your jobs and samples your busy processes: those
# holding a .spec/.speck file open, using at least 5% CPU, in state D, or doing at least
# 1 MB/min of I/O. Processes holding spec files (simulation tasks) are OK when CPU% is
# high and the same output files stay open for the whole window (FDS equal), and SLOW
# when in state D, at low CPU%, or reopening files (FDS changing). Other processes
# (merge jobs, for example) are OK at high CPU%, IO when they read or write at low CPU%,
# STALLED in state D without I/O, and IDLE otherwise. RSS_MB is resident memory and
# IO_MB/min is rchar+wchar.
# Part 2 counts jobs by Slurm state from sacct, for the arrays you have running or pending
# (or the given JOBIDs), lists the longest-running jobs against their time limit with
# their sstat memory and CPU use, and gives the elapsed time of the completed ones.

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

# Runs on a compute node.
# Prints: JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB_PER_MIN CMD WCHAN THREADS AGE_MIN RSS_MB IO_MB_PER_MIN
probe() {
  local w=$1 tick page pid fd t rest n bytes sz comm wchan thr start el up rss io
  tick=$(getconf CLK_TCK)
  page=$(getconf PAGESIZE)
  snap() {
    for p in /proc/[0-9]*; do
      [ -O "$p" ] || continue
      pid=${p#/proc/}
      [ "$pid" = "$$" ] && continue
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
      rest=$(sed 's/^.*) //' "$p/stat" 2>/dev/null) || continue
      comm=$(tr ' ' _ <"$p/comm" 2>/dev/null)
      wchan=$(cat "$p/wchan" 2>/dev/null)
      thr=$(cat "$p"/task/*/stat 2>/dev/null | sed 's/^.*) //' | cut -c1 | sort | uniq -c | awk '{printf "%s%s", $2, $1}')
      start=$(echo "$rest" | awk '{print $20}')
      up=$(cut -d' ' -f1 /proc/uptime)
      el=$(awk -v u="$up" -v s="$start" -v t="$tick" 'BEGIN {printf "%d", (u - s / t) / 60}')
      rss=$(awk -v pg="$page" '{printf "%d", $2 * pg / 1048576}' "$p/statm" 2>/dev/null)
      io=$(awk '/^(rchar|wchar):/ {s += $2} END {print s + 0}' "$p/io" 2>/dev/null)
      echo "$pid $rest $n $bytes ${comm:-?} ${wchan:-0} ${thr:-?} $el ${rss:-0} ${io:-0}" | awk '{print $1, $2, $13 + $14, $(NF-7), $(NF-6), $(NF-5), $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
    done
  }
  snap >/tmp/.perf0.$$
  sleep "$w"
  snap >/tmp/.perf1.$$
  awk -v w="$w" -v tick="$tick" '
    NR == FNR { t0[$1] = $3; n0[$1] = $4; b0[$1] = $5; io0[$1] = $11; next }
    ($1 in t0) {
      cpu = ($3 - t0[$1]) / tick / w * 100
      iomin = ($11 - io0[$1]) / 1048576 / w * 60
      if (!($4 > 0 || n0[$1] > 0 || cpu >= 5 || $2 == "D" || iomin >= 1)) next
      cmd = "grep -o \"job_[0-9]*\" /proc/" $1 "/cgroup 2>/dev/null | head -1 | cut -d_ -f2"
      job = ""; cmd | getline job; close(cmd)
      if (job == "") job = "-"
      printf "%s %s %s %.0f %d %d %.1f %s %s %s %s %s %.1f\n", job, $1, $2, cpu, n0[$1], $4, ($5 - b0[$1]) / 1048576 / w * 60, $6, $7, $8, $9, $10, iomin
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
    echo "NODE JOB PID STATE CPU% FDS_BEFORE FDS_AFTER MB/min CMD WCHAN THREADS AGE_MIN RSS_MB IO_MB/min VERDICT"
    for node in $nodes; do
      if [ -s "$tmp/$node.err" ]; then echo "$node ERROR $(head -1 "$tmp/$node.err")"; fi
      echo "$node has $(echo "$jobs" | awk -v n="$node" '$2 == n' | wc -l) running jobs, $(awk '$5 > 0 || $6 > 0' "$tmp/$node" | wc -l) of $(wc -l <"$tmp/$node") busy processes with .spec/.speck files open" >&2
      awk -v node="$node" '{
        if ($5 > 0 || $6 > 0) v = ($3 != "D" && $4 >= 70 && $5 == $6) ? "OK" : "SLOW"
        else if ($3 == "D" && $13 < 1) v = "STALLED"
        else if ($4 >= 70) v = "OK"
        else if ($13 >= 1) v = "IO"
        else v = "IDLE"
        print node, $0, v}' "$tmp/$node"
    done
  } | awk '{for (i = 1; i <= NF; i++) printf "%-*s ", (i == NF ? 0 : 11), $i; print ""}'
  rm -rf "$tmp"
fi

echo
# Scope: the given job ids, else the arrays that still have a running or pending task.
if [ $# -gt 0 ]; then
  scope=$(IFS=,; echo "$*")
else
  scope=$(squeue -u "$USER" -h -t R,PD -o %F | sort -u | paste -sd, -)
fi
if [ -n "$scope" ]; then
  echo "== Jobs by Slurm state (sacct, jobs $scope) =="
  acct=$(sacct -n -P -X -j "$scope" -o State,Elapsed)
else
  echo "== Jobs by Slurm state (sacct since $since; nothing running or pending) =="
  acct=$(sacct -n -P -X -u "$USER" -S "$since" -o State,Elapsed)
fi
echo "$acct" | awk -F'|' 'NF {split($1, w, " "); c[w[1]]++} END {for (st in c) printf "%6d %s\n", c[st], st}' | sort -rn
secs=$(echo "$acct" | awk -F'|' '
  function secs(t,   d, p, n, s) {
    d = 0
    if (index(t, "-")) { split(t, p, "-"); d = p[1]; t = p[2] }
    n = split(t, p, ":")
    s = (n == 3) ? p[1] * 3600 + p[2] * 60 + p[3] : p[1] * 60 + p[2]
    return d * 86400 + s
  }
  $1 == "COMPLETED" { printf "%d\n", secs($2) }' | sort -n)
echo "$secs" | awk 'NF {a[++n] = $1; sum += $1}
  function tm(q,   k, i, t) { k = int(n * q); if (k < 1) k = 1; for (i = 1; i <= k; i++) t += a[i]; return t / k / 60 }
  END {
  if (!n) { print "no completed tasks"; exit }
  printf "completed tasks (n=%d), elapsed: mean %.1f min  median %.1f  p10 %.1f  p90 %.1f  min %.1f  max %.1f\n", n, sum / n / 60,
    a[int((n + 1) / 2)] / 60, a[int(n * 0.1) + 1] / 60, a[int(n * 0.9) + 1] / 60, a[1] / 60, a[n] / 60
  printf "trimmed means (mean of the fastest q of tasks): tm99 %.1f min  tm90 %.1f min\n", tm(0.99), tm(0.9) }'

if [ -n "$scope" ]; then
  running=$(sacct -n -P -X -j "$scope" -s R -o JobID,JobName,Elapsed,Timelimit | awk -F'|' '
    function secs(t,   d, p, n, s) {
      d = 0
      if (index(t, "-")) { split(t, p, "-"); d = p[1]; t = p[2] }
      n = split(t, p, ":")
      s = (n == 3) ? p[1] * 3600 + p[2] * 60 + p[3] : p[1] * 60 + p[2]
      return d * 86400 + s
    }
    NF {
      e = secs($3); l = ($4 ~ /^[0-9]/) ? secs($4) : 0
      printf "%d %s %s %s %s %s\n", e, $1, $2, $3, $4, l ? sprintf("%.0f%%", e / l * 100) : "-"
    }' | sort -rn | head -5 | cut -d' ' -f2-)
  if [ -n "$running" ]; then
    echo
    echo "== Longest-running jobs =="
    { echo "JOBID NAME ELAPSED LIMIT LIMIT_USED"; echo "$running"; } | awk '{printf "%-16s %-32s %-12s %-12s %s\n", $1, $2, $3, $4, $5}'
    echo
    echo "== Their resource use (sstat) =="
    for id in $(echo "$running" | awk '{print $1}'); do
      sstat -n -P -a -j "$id" -o JobID,MaxRSS,AveCPU,MaxDiskRead,MaxDiskWrite 2>/dev/null
    done | awk -F'|' 'BEGIN {print "JOBID MAXRSS AVECPU MAXDISKREAD MAXDISKWRITE"} {for (i = 1; i <= 5; i++) if ($i == "") $i = "-"; print $1, $2, $3, $4, $5}' | awk '{printf "%-20s %-12s %-14s %-14s %s\n", $1, $2, $3, $4, $5}'
  fi
fi
