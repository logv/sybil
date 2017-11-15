#!/usr/bin/env bash

set -euo pipefail
HOSTINFO=`cat /dev/stdin`

# the host info file should be a list of hosts in the format
# hostname [pwd] [path/to/sybil]

# the sybil binary needs to support "aggregate" feature
# for this to work
SYBIL_BIN=`which sybil`

cmd=${*:-version}
echo "command to run is: \`sybil ${cmd}\`"

function cleanup() {
  echo "cleaning up files in ${BASEDIR}"
  rm ${BASEDIR} -rf
}

# creates a temporary dir with the output and results for this query
function setup_dirs() {
  BASEDIR=`mktemp -d -p .`
  RESULT_DIR="${BASEDIR}/results"
  OUTPUT_DIR="${BASEDIR}/output"

  COMBINE_LOG="${BASEDIR}/master.log"
  COMBINE_RESULTS="${BASEDIR}/master.results"

  # remove commented lines from the hostfile and replace hostfile name
  echo "${HOSTINFO}" | sed '/^\s*#/d' > "${BASEDIR}/hosts"
  HOSTFILE="${BASEDIR}/hosts"

  echo "*** host info is"
  cat ${HOSTFILE}
  echo ""


  trap "cleanup" EXIT
  echo "collecting results in ${BASEDIR}"

  mkdir ${RESULT_DIR}
  mkdir ${OUTPUT_DIR}
}

# print the contents of a file if it is ascii
function print_file() {
  if file ${1} | grep -E -i "ascii|unicode" > /dev/null; then
    echo "${*:2}"
    cat ${1}
    echo ""
  fi

}

# run the jobs remotely
function run_remote_commands() {
  while read hostline; do
    row=(${hostline})
    working_dir=${row[1]:-~}
    host=${row[0]}
    bin=${row[2]:-sybil}

    full_cmd="ssh -C ${host} \"cd ${working_dir} && ${bin} query ${cmd}\""
    echo "running command on ${host}"


    bash -c "${full_cmd}" > ${RESULT_DIR}/${host}.results 2> ${OUTPUT_DIR}/${host}.log && \
      echo "${host} finished"&
  done < ${HOSTFILE}

}

function print_remote_results() {
  # print the job outputs
  while read hostline; do
    IFS=" " row=(${hostline})
    host=${row[0]}
    bin=${row[2]:-sybil}
    cmd=${2:-version}
    result_file=${RESULT_DIR}/${host}.results
    log_file=${OUTPUT_DIR}/${host}.log

    print_file ${log_file} "*** ${host} output"
    print_file ${result_file} "*** ${host} output"
  done < ${HOSTFILE}

}

function aggregate_results() {
  ${SYBIL_BIN} aggregate ${RESULT_DIR} -debug > ${COMBINE_RESULTS} 2> ${COMBINE_LOG}
  print_file ${COMBINE_LOG} "*** aggregator output"
  print_file ${COMBINE_RESULTS} "*** combined results"
}


# main entry point
function main() {
  setup_dirs
  run_remote_commands
  wait # wait for all jobs to finish
  print_remote_results
  aggregate_results
}


main
