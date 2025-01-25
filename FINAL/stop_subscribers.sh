#!/bin/bash

# Kill all python processes running subscriber scripts
for pid_file in pid_cluster*_subscriber*.pid; do
    if [ -f "$pid_file" ]; then
        pid=$(cat "$pid_file")
        echo "Stopping process $pid..."
        kill $pid 2>/dev/null
        rm $pid_file
    fi
done

echo "All subscribers stopped"
