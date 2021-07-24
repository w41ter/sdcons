#!/bin/bash

function build_env() {
  pushd $(pwd) 2>&1 >/dev/null
  dir=$(pwd)
  mkdir -p ${dir}/chaos_run/
  mkdir -p ${dir}/chaos_run/log
  mkdir -p ${dir}/chaos_run/config
  mkdir -p ${dir}/chaos_run/server

  cd ${dir}/chaos_run
  ln -s ${dir}/target/debug/kv-server
  ln -s ${dir}/target/debug/kv-client

  echo "{" >cons_named_file
  echo "{" >kv_named_file
  echo "{" >snapshot_named_file

  base_port=6000
  for id in {1..3}; do
    port_step=$((1000 * $id))
    kv_port=$(($base_port + $port_step))
    cons_port=$(($kv_port + 1))
    snapshot_port=$(($kv_port + 2))

    mkdir -p ${dir}/chaos_run/server/$id/data
    mkdir -p ${dir}/chaos_run/server/$id/wal
    cat >./config/debug-${id}.toml <<EOF
id=${id}
kv_port=${kv_port}
cons_port=${cons_port}
snapshot_port=${snapshot_port}
named_file="cons_named_file"
snapshot_named_file="snapshot_named_file"
data_dir="server/${id}/data"
wal_dir="server/${id}/wal"
EOF

    if [[ $id == 3 ]]; then
      echo "    \"${id}\": \"0.0.0.0:${snapshot_port}\"" >>snapshot_named_file
      echo "    \"${id}\": \"0.0.0.0:${cons_port}\"" >>cons_named_file
      echo "    \"${id}\": \"0.0.0.0:${kv_port}\"" >>kv_named_file
    else
      echo "    \"${id}\": \"0.0.0.0:${snapshot_port}\"," >>snapshot_named_file
      echo "    \"${id}\": \"0.0.0.0:${cons_port}\"," >>cons_named_file
      echo "    \"${id}\": \"0.0.0.0:${kv_port}\"," >>kv_named_file
    fi
  done

  echo "}" >>kv_named_file
  echo "}" >>cons_named_file
  echo "}" >>snapshot_named_file

  cat >./config/client-debug.toml <<EOF
named_file="kv_named_file"
EOF

  popd >/dev/null
}

if [ ! -d "./chaos_run" ]; then
  build_env
fi

cd ./chaos_run

while [[ $# != 0 ]]; do
  case $1 in
  servers)
    state=1
    ulimit -c unlimited
    RUST_BACKTRACE=${state} ./kv-server --config=config/debug-1.toml >>log/1.log 2>&1 &
    RUST_BACKTRACE=${state} ./kv-server --config=config/debug-2.toml >>log/2.log 2>&1 &
    RUST_BACKTRACE=${state} ./kv-server --config=config/debug-3.toml >>log/3.log 2>&1 &
    exit 0
    ;;
  stop)
    ps -ef | awk '/kv-server/ && !/grep/{print $2}' | xargs kill -9
    exit 0
    ;;
  clean)
    rm -rf ./chaos_run/
    exit 0
    ;;
  client)
    ./kv-client --config=config/client-debug.toml <<EOF
put b b expect b
put a c if not exists
get a
get b
delete a
delete b
get a
get b
put b b expect b
put a c if not exists
get a
get b
delete a
delete b
EOF
    exit 0
    ;;
  *)
    break
    ;;
  esac
done

echo "unknow command $1"
echo "  servers - start servers"
echo "  client  - start a client"
echo "  stop    - stop servers"
echo "  clean   - clean all staled data"
exit 1
