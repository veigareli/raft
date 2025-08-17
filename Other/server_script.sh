#!/bin/bash

ports=(2000 2010 2020 2030 2040 2050 2060 2070 2080)

for port in "${ports[@]}"
do
  osascript <<EOF
tell application "Terminal"
    do script "cd \"$(pwd)\" && go run server/raftserver.go 127.0.0.1:$port servers_script.txt"
end tell
EOF
done

