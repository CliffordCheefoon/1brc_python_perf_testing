#!/bin/bash

# Loop 5 times
for run in {1..5}; do
    echo "Starting run $run"
    # Recursively scan for all files, excluding results.log and run_test.sh
    for file in $(find . -type f -not -name "results.log" -not -name "run_test.sh"); do
        echo "Running docker compose for $file in run $run" 
        # flush cache to ensure more consistent results
        echo 3 > /proc/sys/vm/drop_caches
        # Run docker compose and record output
        docker compose -f "$file" up --abort-on-container-exit >> results.log 2>&1
        echo "Finished $file in run $run"
    done
    echo "Completed run $run"
done
