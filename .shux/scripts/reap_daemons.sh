#!/usr/bin/env bash
# Stop shux daemons left behind by an interrupted run (issue #179).
#
# Scope is exactly `shux_daemon_pids` — a `__daemon` whose argv starts with THIS
# checkout's path. A daemon from another checkout, or any client invocation, is never a
# candidate. A daemon of yours that is still in use IS one, which is why this is
# developer-invoked and never automatic: run it when no session of yours is working.

set -euo pipefail

usage() { echo "usage: .shux/scripts/reap_daemons.sh [--dry-run]" >&2; exit 2; }

# Count the arguments rather than matching `''`, which cannot tell "no argument" from an
# empty one — `reap_daemons.sh ""` fell through to a full destructive reap.
dry_run=0
case "$#" in
  0) ;;
  1) [ "$1" = "--dry-run" ] || usage; dry_run=1 ;;
  *) usage ;;
esac

# shellcheck disable=SC2034  # consumed by lib/proc_scope.sh
# `pwd -P`: a daemon's argv is always the physical path (see lib/proc_scope.sh).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# shellcheck source=lib/proc_scope.sh disable=SC1091
. "$(dirname "${BASH_SOURCE[0]}")/lib/proc_scope.sh"

# shellcheck disable=SC2207  # mapfile is bash 4+; macOS ships bash 3.2
pids=($(shux_daemon_pids))

# Every descendant of a pid, listed while it is still the root of their tree. A pane's
# shell forks too, and what it forks can ignore the SIGHUP that ends the shell.
descendant_pids() {
  ps -axo pid=,ppid= | awk -v root="$1" '
    { parent[NR] = $2; pid[NR] = $1 }
    END {
      tree[root] = 1
      do {
        grew = 0
        for (i = 1; i <= NR; i++) {
          if ((parent[i] in tree) && !(pid[i] in tree)) { tree[pid[i]] = 1; print pid[i]; grew = 1 }
        }
      } while (grew)
    }'
}

# When a pid started, which with the pid names one process: pids are reused.
start_time() {
  ps -p "$1" -o lstart= 2>/dev/null | tr -s ' ' || true
}

if [ "${#pids[@]}" -eq 0 ]; then
  echo "no shux daemons from ${REPO_ROOT} are running"
  exit 0
fi

for pid in "${pids[@]}"; do
  describe_pid "${pid}"
done

# Recorded before anything is signalled: SIGKILL destroys the only evidence of who owned
# these. Once the daemon is gone its panes are orphans, adopted by init or by whatever
# subreaper is nearest, with nothing connecting them to it. See the sweep below.
descendants=()
for pid in "${pids[@]}"; do
  for child in $(descendant_pids "${pid}"); do
    descendants+=("${child}=$(start_time "${child}")")
  done
done

if [ "${dry_run}" -eq 1 ]; then
  echo "--dry-run: ${#pids[@]} daemon(s) would be stopped"
  exit 0
fi

terminate_pids "${pids[@]}"

# Report on what is actually gone, and only about the pids we targeted. Re-scanning the
# machine instead counted a daemon that STARTED during the reap — shux auto-starts one on
# first use, so any concurrent command in this checkout did it — as one that "survived
# TERM+KILL".
survivors=()
for pid in "${pids[@]}"; do
  pid_is_gone "${pid}" || survivors+=("${pid}")
done
if [ "${#survivors[@]}" -gt 0 ]; then
  echo "reap: ${#survivors[@]} daemon(s) survived TERM+KILL: ${survivors[*]}" >&2
  for pid in "${survivors[@]}"; do
    describe_pid "${pid}" >&2
  done
  exit 1
fi

# A daemon too wedged to service SIGTERM is SIGKILLed, and SIGKILL runs none of its pane
# teardown: its pane children are reparented to init, holding the PTYs the daemon held.
# Reaping without this turns one leaked daemon into several leaked shells and calls it
# success — manufacturing precisely what `no_leak_guard.sh` exists to catch.
#
# Attribution is LINEAGE, not timing. "Appeared between two snapshots" is not ownership:
# anything else in this checkout that orphans a process during the reap window looks
# identical, and since shux auto-starts a daemon on first use, a concurrent command mints
# one that passes every test a timing sweep applies — measured, an earlier version of
# this killed exactly that. Each pid below descended from a daemon we stopped, and still
# has the start time it had then, so it is that process and not one reusing its pid. Its
# parent now says nothing: init, a subreaper, or a zombie daemon not yet torn down.
stranded=()
set +u
for entry in "${descendants[@]:-}"; do
  [ -n "${entry}" ] || continue
  pid="${entry%%=*}"
  pid_is_gone "${pid}" && continue
  [ "$(start_time "${pid}")" = "${entry#*=}" ] || continue
  stranded+=("${pid}")
done
set -u

# The live children of any pid in a space-padded set, not already in it.
children_outside() {
  ps -axo pid=,ppid= | awk -v set="$1" 'index(set, " " $2 " ") && !index(set, " " $1 " ") { print $1 }'
}

if [ "${#stranded[@]}" -gt 0 ]; then
  # Frozen before they are killed: a stranded process that outlived SIGHUP may still be
  # forking, and a child born after the walk above would outlive the reap. Stopped
  # parents keep their children's ppid pointing at them, so the walk below reaches a
  # fixpoint. Lineage still decides: each pid added is a child of a stopped one.
  frozen=" "
  pending=("${stranded[@]}")
  while [ "${#pending[@]}" -gt 0 ]; do
    for pid in "${pending[@]}"; do
      case "${frozen}" in *" ${pid} "*) continue ;; esac
      kill -STOP "${pid}" >/dev/null 2>&1 || true
      frozen="${frozen}${pid} "
    done
    # shellcheck disable=SC2207  # mapfile is bash 4+; macOS ships bash 3.2
    pending=($(children_outside "${frozen}"))
  done
  # shellcheck disable=SC2206  # split on the spaces that pad the set
  stranded=(${frozen})
  echo "reap: stopping ${#stranded[@]} process(es) stranded by the reaped daemon(s)"
  for pid in "${stranded[@]}"; do
    describe_pid "${pid}"
  done
  terminate_pids "${stranded[@]}"
  left=()
  for pid in "${stranded[@]}"; do
    pid_is_gone "${pid}" || left+=("${pid}")
  done
  if [ "${#left[@]}" -gt 0 ]; then
    echo "reap: ${#left[@]} stranded process(es) survived KILL: ${left[*]}" >&2
    exit 1
  fi
fi

echo "reaped ${#pids[@]} shux daemon(s)"
