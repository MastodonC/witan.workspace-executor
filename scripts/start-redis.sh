#!/usr/bin/env bash
docker rm witan-redis
docker run -p 6379:6379 --name witan-redis -d redis
