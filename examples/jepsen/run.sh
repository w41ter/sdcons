#!/bin/bash

mkdir -p libs
binary_dir=`pwd`
binary_dir=${binary_dir%'/examples/jepsen'}/target/debug/
ln -sf ${binary_dir}/libnative_sdk.so `pwd`/libs

cd docker
bash ./up.sh

cd ../
lein run test --time-limit 300 \
  --num-keys 2 \
  --concurrency 2 \
  --test-count 2 \
  --nodes-file docker/hosts \
  --ssh-private-key ~/.ssh/id_rsa \
  --working-dir=/root/run/

cd docker
bash ./down.sh

cd ../
rm -rf libs

