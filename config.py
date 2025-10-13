"""Configuration for the Jellyfin AV1 transcoding watchdog.

All paths can be absolute. If relative, they are resolved relative to the
directory containing this config file at runtime.
"""

# --- Paths ---
# List of remote directories to scan for media (NFS mounts)
MEDIA_DIRECTORIES = [
    "~/jellyfin_test/movies"
    # "/mnt/jellyfin/movies",
    #"/mnt/jellyfin/tv_shows",
]

# Local directory on the VM for temporary work
TRANSCODE_TEMP_PATH = "/tmp/transcoding/"

# Path to the HandBrake preset file
HANDBRAKE_PRESET_FILE = "AV1_MKV_Stereo.json"
HANDBRAKE_PRESET_NAME = "AV1_MKV_Stereo"

# Path to the state file for tracking inspected files
INSPECTED_FILES_LOG = "inspected_files.log"

# --- Transcoding Rules ---
# File size in Gigabytes. Files larger than this will be transcoded.
MAX_FILE_SIZE_GB = 0.005

# Video codec to check for. Files with this codec are considered "passed".
TARGET_CODEC = "av1"

# --- File Extensions ---
# Video file extensions to scan for
VIDEO_EXTENSIONS = (".mkv", ".mp4", ".avi", ".mov", ".webm")



