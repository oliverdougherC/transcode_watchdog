import os
import sys
import json
import shlex
import logging
import subprocess
import shutil
from datetime import datetime
from typing import Optional, List

try:
    # When executed as a package: python -m transcode_watchdog.main
    from .config import (
        MEDIA_DIRECTORIES,
        TRANSCODE_TEMP_PATH,
        HANDBRAKE_PRESET_FILE,
        HANDBRAKE_PRESET_NAME,
        INSPECTED_FILES_LOG,
        MAX_FILE_SIZE_GB,
        TARGET_CODEC,
        VIDEO_EXTENSIONS,
    )
except Exception:
    # When executed directly from the folder: python main.py
    from config import (
        MEDIA_DIRECTORIES,
        TRANSCODE_TEMP_PATH,
        HANDBRAKE_PRESET_FILE,
        HANDBRAKE_PRESET_NAME,
        INSPECTED_FILES_LOG,
        MAX_FILE_SIZE_GB,
        TARGET_CODEC,
        VIDEO_EXTENSIONS,
    )


def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def setup_logging(base_dir: str) -> logging.Logger:
    logger = logging.getLogger("transcode_watchdog")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid duplicate handlers if re-imported
    if logger.handlers:
        return logger

    log_path = os.path.join(base_dir, "activity.log")
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_path)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(fmt)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


def resolve_path(base_dir: str, path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(base_dir, path)


def load_inspected_files(log_path: str) -> set:
    if not os.path.exists(log_path):
        return set()
    inspected = set()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            p = line.strip()
            if p:
                inspected.add(p)
    return inspected


def append_inspected_file(log_path: str, file_path: str) -> None:
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_path + "\n")


def verify_dependencies(logger: logging.Logger) -> bool:
    required = ["ffprobe", "HandBrakeCLI", "rsync"]
    missing = [cmd for cmd in required if shutil.which(cmd) is None]
    if missing:
        logger.critical(f"Missing required tools: {', '.join(missing)}")
        logger.critical("Ensure they are installed and available in PATH.")
        return False
    logger.info("All required CLI tools found: ffprobe, HandBrakeCLI, rsync")
    return True


def scan_media_files(directories: list, extensions: tuple):
    for directory in directories:
        for root, _dirs, files in os.walk(directory):
            for name in files:
                if name.lower().endswith(extensions):
                    yield os.path.join(root, name)


def run_cmd(logger: logging.Logger, cmd: list, check: bool = False) -> subprocess.CompletedProcess:
    logger.info(f"Running: {' '.join(shlex.quote(c) for c in cmd)}")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        logger.warning(
            "Command failed (rc=%s): %s\nSTDOUT: %s\nSTDERR: %s",
            result.returncode,
            " ".join(shlex.quote(c) for c in cmd),
            result.stdout.decode(errors="replace"),
            result.stderr.decode(errors="replace"),
        )
        if check:
            result.check_returncode()
    return result


def ffprobe_json(logger: logging.Logger, path: str) -> Optional[dict]:
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
    ]
    res = run_cmd(logger, cmd)
    if res.returncode != 0:
        return None
    try:
        return json.loads(res.stdout.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None


def inspect_file(logger: logging.Logger, file_path: str, inspected_log_path: str) -> bool:
    info = ffprobe_json(logger, file_path)
    if not info:
        logger.info(f"Inspection failed to read metadata; queueing for transcode: {file_path}")
        return True

    streams = info.get("streams", [])
    fmt = info.get("format", {})
    video_codec = None
    for stream in streams:
        if stream.get("codec_type") == "video":
            video_codec = stream.get("codec_name")
            break

    try:
        size_bytes = int(fmt.get("size", 0))
    except (TypeError, ValueError):
        size_bytes = 0

    size_limit_bytes = int(MAX_FILE_SIZE_GB * (1024 ** 3))
    if (video_codec == TARGET_CODEC) and (size_bytes < size_limit_bytes):
        logger.info(
            f"PASS: {file_path} (codec={video_codec}, size={size_bytes} < {size_limit_bytes})"
        )
        append_inspected_file(inspected_log_path, file_path)
        return False

    reasons = []
    if video_codec != TARGET_CODEC:
        reasons.append(f"codec is {video_codec}")
    if size_bytes >= size_limit_bytes:
        reasons.append("file size exceeds limit")
    logger.info(f"QUEUE: {file_path} (reasons: {', '.join(reasons) or 'unknown'})")
    return True


def verify_transcode(logger: logging.Logger, original_path: str, new_path: str) -> bool:
    # Health check
    health = run_cmd(logger, ["ffprobe", "-v", "error", "-hide_banner", new_path])
    if health.returncode != 0:
        logger.error(f"Health check failed for {new_path}")
        return False

    orig = ffprobe_json(logger, original_path)
    new = ffprobe_json(logger, new_path)
    if not orig or not new:
        logger.error("Failed to read metadata for verification")
        return False

    def extract_meta(meta: dict):
        fmt = meta.get("format", {})
        streams = meta.get("streams", [])
        try:
            duration = float(fmt.get("duration", 0))
        except (TypeError, ValueError):
            duration = 0.0
        v = sum(1 for s in streams if s.get("codec_type") == "video")
        a = sum(1 for s in streams if s.get("codec_type") == "audio")
        sub = sum(1 for s in streams if s.get("codec_type") == "subtitle")
        return duration, v, a, sub

    d1, v1, a1, s1 = extract_meta(orig)
    d2, v2, a2, s2 = extract_meta(new)

    if abs(d1 - d2) > 1.0:
        logger.error(f"Duration mismatch: original={d1:.3f}s new={d2:.3f}s")
        return False
    # Enforce only video/audio stream count parity; subtitle differences are allowed
    if (v1, a1) != (v2, a2):
        logger.error(
            f"Stream count mismatch (video/audio): orig(v{v1},a{a1}) vs new(v{v2},a{a2})"
        )
        return False
    if s1 != s2:
        logger.info(
            f"Subtitle track count changed: orig s{s1} -> new s{s2} (allowed)"
        )
    return True


def safe_replace(logger: logging.Logger, source_path: str, new_local_path: str) -> bool:
    source_dir = os.path.dirname(source_path)
    source_filename = os.path.basename(source_path)
    temp_remote_path = os.path.join(source_dir, f"{source_filename}.tmp")
    old_remote_path = os.path.join(source_dir, f"{source_filename}.old")

    try:
        # Step 6a: rsync new file to temp path on remote
        rsync_cmd = [
            "rsync",
            "-avh",
            "--progress",
            new_local_path,
            temp_remote_path,
        ]
        res = run_cmd(logger, rsync_cmd)
        if res.returncode != 0:
            logger.error("rsync to temp failed")
            return False

        # Step 6b and 6c: atomic swap
        os.rename(source_path, old_remote_path)
        os.rename(temp_remote_path, source_path)

        # Step 6d: remove old
        os.remove(old_remote_path)
        return True
    except Exception as e:
        logger.critical(f"Safe replace failed: {e}")
        # Attempt best-effort rollback
        try:
            if os.path.exists(temp_remote_path) and not os.path.exists(source_path):
                os.rename(temp_remote_path, source_path)
        except Exception:
            pass
        return False


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logger = setup_logging(base_dir)
    logger.info("Starting Jellyfin AV1 Transcoding Watchdog")

    if not verify_dependencies(logger):
        sys.exit(1)

    # Resolve paths possibly relative to repo
    preset_path = resolve_path(base_dir, HANDBRAKE_PRESET_FILE)
    inspected_log_path = resolve_path(base_dir, INSPECTED_FILES_LOG)

    ensure_dir(os.path.expanduser(TRANSCODE_TEMP_PATH))

    # Load state
    inspected_files = load_inspected_files(inspected_log_path)
    logger.info(f"Loaded {len(inspected_files)} previously inspected files")

    # Prepare and log media directories
    expanded_media_dirs: List[str] = []
    for d in MEDIA_DIRECTORIES:
        exp = os.path.abspath(os.path.expanduser(d))
        if not os.path.isdir(exp):
            logger.warning(f"Media directory not found, skipping: {exp}")
            continue
        expanded_media_dirs.append(exp)

    logger.info(
        f"Scanning {len(expanded_media_dirs)} directories: {expanded_media_dirs}"
    )

    # Scan and inspect
    transcode_queue: List[str] = []
    discovered_count = 0
    for media_path in scan_media_files(expanded_media_dirs, VIDEO_EXTENSIONS):
        discovered_count += 1
        if media_path in inspected_files:
            logger.info(f"SKIP inspected: {media_path}")
            continue
        try:
            needs_transcode = inspect_file(logger, media_path, inspected_log_path)
        except Exception as e:
            logger.error(f"Inspection error for {media_path}: {e}")
            needs_transcode = True
        if needs_transcode:
            transcode_queue.append(media_path)

    logger.info(f"Discovered {discovered_count} candidate files before filtering")
    logger.info(f"Queue length: {len(transcode_queue)}")

    # Process queue
    for source_path in transcode_queue:
        try:
            source_name = os.path.basename(source_path)
            local_source_path = os.path.join(TRANSCODE_TEMP_PATH, source_name)
            name_no_ext, _ext = os.path.splitext(source_name)
            local_output_path = os.path.join(TRANSCODE_TEMP_PATH, f"{name_no_ext}.av1.mkv")

            # Copy to local temp
            rsync_cmd = [
                "rsync",
                "-avh",
                "--progress",
                source_path,
                local_source_path,
            ]
            res = run_cmd(logger, rsync_cmd)
            if res.returncode != 0:
                logger.error(f"Failed to rsync source to local temp: {source_path}")
                continue

            # Transcode with HandBrakeCLI
            hb_cmd = [
                "HandBrakeCLI",
                "--preset-import-file",
                preset_path,
                "-i",
                local_source_path,
                "-o",
                local_output_path,
                "--preset",
                HANDBRAKE_PRESET_NAME,
            ]
            hb_res = run_cmd(logger, hb_cmd)
            if hb_res.returncode != 0 or not os.path.exists(local_output_path):
                logger.error(f"Transcode failed for {source_path}")
                if os.path.exists(local_output_path):
                    try:
                        os.remove(local_output_path)
                    except Exception:
                        pass
                continue

            # Verify integrity
            if not verify_transcode(logger, local_source_path, local_output_path):
                logger.error("Verification failed; deleting transcoded file")
                try:
                    os.remove(local_output_path)
                except Exception:
                    pass
                continue

            # Compare sizes
            try:
                original_size = os.path.getsize(local_source_path)
                new_size = os.path.getsize(local_output_path)
            except OSError as e:
                logger.error(f"Failed to stat files: {e}")
                try:
                    os.remove(local_output_path)
                except Exception:
                    pass
                continue

            if new_size >= original_size:
                logger.info(
                    f"Not space-efficient (new {new_size} >= orig {original_size}); skipping replace"
                )
                try:
                    os.remove(local_output_path)
                except Exception:
                    pass
                continue

            # Safe replace on remote
            if not safe_replace(logger, source_path, local_output_path):
                logger.error("Safe replace failed; leaving original untouched")
                try:
                    os.remove(local_output_path)
                except Exception:
                    pass
                continue

            # Cleanup local temp files
            try:
                if os.path.exists(local_source_path):
                    os.remove(local_source_path)
            except Exception:
                pass
            try:
                if os.path.exists(local_output_path):
                    os.remove(local_output_path)
            except Exception:
                pass

            # Mark original as inspected now that it has been replaced
            append_inspected_file(inspected_log_path, source_path)
            logger.info(f"SUCCESS: Replaced {source_path}")

        except Exception as e:
            logger.exception(f"Unhandled error processing {source_path}: {e}")


if __name__ == "__main__":
    main()


