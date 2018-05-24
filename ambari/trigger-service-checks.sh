#!/bin/bash

all_components="hdfs,yarn,mapreduce2,hbase,hive,webhcat,pig,storm,falcon,oozie,zookeeper,tez,sqoop,ambari_metrics,atlas,kafka,knox,spark,smartsense,ranger,ranger_kms,zookeeper,flume"

set -e

function print_help() {
  if [[ -n ${1} ]]; then
    exit_code=${1}
  else
    exit_code=0
  fi

  echo "usage: $(basename ${0}) [OPTIONS]"
  echo ""
  echo "Required parameters:"
  echo " -u AMBARI_USER    Ambari admin user name"
  echo " -s AMBARI_URI     Base URI for the Ambari server"
  echo " -n CLUSTER        Cluster name"
  echo ""
  echo "Optional parameters:"
  echo " -p AMBARI_PASS    Ambari admin user's password"
  echo " -c COMPONENT      Components (comma separated) to trigger service checks for (default = all)"
  echo "                   Options:"
  echo "                     - all"
  IFS=","
  for component in ${components}; do
    echo "                     - ${component}"
  done
  IFS=" "
  echo " -h                Help screen"
  echo ""

  exit ${exit_code}
}

function print_err() {
  echo "ERROR: ${1}"
  echo ""
  print_help 1
}

while getopts ":u:p:c:s:n:h" OPT; do
  shopt -s nocasematch
  case ${OPT} in
    u)
      ambari_user="${OPTARG}"
      ;;
    p)
      ambari_pass="${OPTARG}"
      ;;
    c)
      components="${OPTARG}"
      ;;
    s)
      ambari_uri="${OPTARG}"
      ;;
    n)
      cluster_name="${OPTARG}"
      ;;
    h)
      print_help
      ;;
  esac
done

if [[ -z ${ambari_user} ]]; then
  print_err "-u not specified and is required"
elif [[ -z ${ambari_uri} ]]; then
  print_err "-s not specified and is required"
fi

if [[ "${components}" == "all" ]]; then
  components="${all_components}"
fi

if [[ -n "${ambari_pass}" ]]; then
  ambari_pass_arg=":${ambari_pass}"
else
  ambari_pass_arg=""
fi

IFS=","
for component in ${components}; do
  sn="${component^^}"
  case ${component} in
    hdfs|yarn|mapreduce2|hbase|hive|webhcat|pig|storm|falcon|oozie|tez|sqoop|ambari_metrics|atlas|kafka|knox|spark|smartsense|ranger|ranger_kms|flume)
      cmd="${sn}_SERVICE_CHECK"
      ;;
    zookeeper)
      cmd="ZOOKEEPER_QUORUM_SERVICE_CHECK"
      ;;
    *)
      echo "WARN: invalid component ${component} passed, skipping"
      continue
      ;;
  esac

  curl -s \
    -k \
    -H "X-Requested-By: ambari" \
    -u ${ambari_user}${ambari_pass_arg} \
    -X POST \
    -d "{\"RequestInfo\":{\"context\":\"${sn} Service Check\",\"command\":\"${cmd}\"},\"Requests/resource_filters\":[{\"service_name\":\"${sn}\"}]}" \
    ${ambari_uri}/api/v1/clusters/${cluster_name}/requests
done
