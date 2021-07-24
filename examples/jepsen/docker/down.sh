#!/bin/bash

docker-compose -f docker-compose.yml down

rm -rf secret
rm -rf hosts
# sudo rm -rf run


