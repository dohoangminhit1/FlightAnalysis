#!/bin/bash

# --- Configuration ---
# !!! IMPORTANT: Change this to the actual path of your data directory on Ubuntu !!!
DATADIR="/path/to/your/local/data" # Example: /home/your_user/bigdata/minh/data

# Check if DATADIR exists
if [ ! -d "$DATADIR" ]; then
  echo "Error: Data directory '$DATADIR' not found."
  echo "Please update the DATADIR variable in the script."
  exit 1
fi

echo "🔄 Copying local CSV files from '$DATADIR' into container 'hadoop-namenode'..."

# Loop through all CSV files in the DATADIR
for f in "$DATADIR"/*.csv; do
  # Check if file exists (handles cases where no *.csv files are found)
  if [ -f "$f" ]; then
    # Extract the filename from the full path
    filename=$(basename "$f")
    echo "  → Copying '$filename' into container..."
    docker cp "$f" "hadoop-namenode:/tmp/$filename"
    # Check the exit code of the docker cp command
    if [ $? -ne 0 ]; then
      echo "Error: Failed to copy '$filename' to container."
      exit $? # Exit script if copy fails
    fi
  fi
done

echo "✅ Copy to container finished."
echo "⏳ Pushing data from container's /tmp into HDFS /data..."

# Ensure the HDFS target directory exists ONCE before the loop
# The -p flag prevents errors if the directory already exists
echo "  → Ensuring HDFS directory '/data' exists..."
docker exec hadoop-namenode hdfs dfs -mkdir -p /data
if [ $? -ne 0 ]; then
  echo "Warning: Could not create HDFS /data directory (it might already exist, or there's an HDFS issue)."
  # Decide if you want to exit here. Often, it's okay if it exists.
fi


# Loop through the files again to put them into HDFS and clean up /tmp
for f in "$DATADIR"/*.csv; do
   if [ -f "$f" ]; then
    filename=$(basename "$f")
    echo "  → Uploading '$filename' to HDFS /data/ ..."
    # Use -f to overwrite if the file already exists in HDFS
    docker exec hadoop-namenode hdfs dfs -put -f "/tmp/$filename" /data/
    if [ $? -ne 0 ]; then
      echo "Error: Failed to upload '$filename' to HDFS."
      # Optionally keep the tmp file for inspection:
      # echo "      Temporary file kept in container: /tmp/$filename"
      exit $? # Exit script if upload fails
    fi

    # Clean up the temporary file from the container
    echo "  → Cleaning up /tmp/$filename from container..."
    docker exec hadoop-namenode rm -f "/tmp/$filename"
    if [ $? -ne 0 ]; then
      # This is usually less critical, so maybe just warn
      echo "Warning: Failed to clean up /tmp/$filename from container."
    fi
  fi
done

echo "✅ HDFS upload and cleanup finished."
echo "🚀 All done!"

exit 0 # Explicitly signal success