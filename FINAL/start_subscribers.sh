#!/bin/bash

# Function to check if a process is running
check_process() {
    local pid=$1
    if ps -p $pid > /dev/null; then
        return 0  # Process is running
    else
        return 1  # Process is not running
    fi
}

# Function to start a subscriber and store its PID
start_subscriber() {
    local script=$1
    echo "Starting $script..."
    python $script &
    echo $! > "pid_${script%.py}.pid"  # Store PID in a file
    sleep 2  # Wait a bit between starts
}

# Clear any existing PID files
rm -f pid_*.pid

# Start all subscribers
start_subscriber "cluster1_subscriber1.py"
start_subscriber "cluster1_subscriber2.py"
start_subscriber "cluster2_subscriber1.py"
start_subscriber "cluster2_subscriber2.py"
start_subscriber "cluster3_subscriber1.py"
start_subscriber "cluster3_subscriber2.py"

echo "All subscribers started. Monitoring processes..."

# Monitor the processes
while true; do
    all_running=true
    for script in cluster{1,2,3}_subscriber{1,2}.py; do
        pid_file="pid_${script%.py}.pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if ! check_process $pid; then
                echo "WARNING: $script (PID: $pid) has stopped. Restarting..."
                start_subscriber $script
            fi
        fi
    done
    sleep 5
done
