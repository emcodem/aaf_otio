
####
#  aaftimelineparser.py
# emcdem@ffasatrans.com
# initial commit: 24.10.2025
# License: GPL or the one that comes closest to GPL that the used libraries allow
# Description: parses aaf timeline, can use bmxtranswrap to create a consolidated copy of the pieces in the timeline
####

import argparse
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "venv/Lib/site-packages/"))

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List
import opentimelineio as otio
from opentimelineio.media_linker import MediaLinker
from opentimelineio.schema import ExternalReference
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymediainfo import MediaInfo
args = None
logging.basicConfig(
    level=logging.DEBUG,                   # minimum level to log
    format="%(asctime)s [%(levelname)s] %(message)s"
)
@dataclass
class CutClip:
    path: Path
    start: float
    duration: float
    bmx_start_frames: int = 0
    bmx_duration_frames: int = 0

class CutClipList(list):
    def append(self, item: CutClip):
        # checks if path is already in list, if yes expand start and duration 
                
        if not isinstance(item, CutClip):
            raise TypeError(
                f"Only CutClip instances can be appended, got {type(item).__name__}"
            )

        # Search for an existing clip with the same path
        for existing in self:
            if existing.path == item.path:
                existing_end = existing.start + existing.duration
                new_end = item.start + item.duration

                # Update start if new item starts earlier
                if item.start < existing.start:
                    existing.start = item.start
                    logging.debug("Updating start %s to: %i",item.path,existing.start)

                # Update duration if new item ends later
                if new_end > existing_end:
                    existing.duration = new_end - existing.start
                    logging.debug("Updating duration %s to: %i",item.path,existing.duration)

                # Don’t append a duplicate
                return
            
        # No existing clip with same path
        super().append(item)

def run_command(cmd):
    """
    Runs a command, captures stdout/stderr, and returns a dict with all info.
    """
    try:
        result = subprocess.run(
            cmd,
            shell=True,               # use True for cross-platform shell commands
            stdout=subprocess.PIPE,   # capture stdout
            stderr=subprocess.PIPE,   # capture stderr
            text=True                 # return strings instead of bytes
        )
        return {
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "success": result.returncode == 0
        }
    except Exception as e:
        return {
            "cmd": cmd,
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
            "success": False
        }


# on some python interpreters, pkg_resources is not available
try:
    import pkg_resources
except ImportError:
    pkg_resources = None

__doc__ = """ Python wrapper around OTIO to convert timeline files between \
formats.

Available adapters: {}
""".format(otio.adapters.available_adapter_names())

def _parsed_args():
    """ parse commandline arguments with argparse """

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-i',
        '--input',
        type=str,
        required=True,
        help='path to input file',
    )
    parser.add_argument(
        '-o',
        '--output',
        type=str,
        required=True,
        help='path to output file',
    )
    parser.add_argument(
        '-s',
        '--source',
        type=str,
        required=False,
        help='folder containing files. The timeline will output only a media name like "filename". We are looking for "filename.mxf" in this source path',
    )
    parser.add_argument(
        '-b',
        '--bmx',
        type=str,
        required=False,
        help='path to bmx executable, e.g. c:\\temp\\bmxtranswrap.exe',
    )
    parser.add_argument(
        '-ha',
        '--handle',
        type=int,
        required=False,
        help='for bmx command, add this amount of frames before and after each partial to restore',
    )
    result = parser.parse_args()
    
    if not result.input:
        parser.error("-i/--input is a required argument")
    if not result.output:
        parser.error("-o/--output is a required argument")

    return result


def main():
    """Parse arguments and convert the files."""
    global args
    args = _parsed_args()
  
    in_adapter = otio.adapters.from_filepath(args.input).name

    result_tl = otio.adapters.read_from_file(
        args.input,
        in_adapter,
    )
    ffconcat_clips = []
    bmx_clips = CutClipList()
    for _t in result_tl.tracks:
        for item in _t:
            if (_t.kind != "Video"):
                continue
            if isinstance(item, otio.schema.Clip):
                sr = item.source_range
                _path = _resolve_media(args.source,item.name)
                logging.debug(f"  Clip: {_path or '(unnamed)'}")
                if sr:
                    logging.debug(f"    Source range: start={sr.start_time.to_seconds()}, duration={sr.duration.to_seconds()}")
                    ffconcat_clips.append(
                        #todo:ffconcat has outpoint, not duration
                        CutClip(path=_path, start=sr.start_time.to_seconds(), duration=sr.duration.to_seconds())
                        )
                    bmx_clips.append(
                        #bmx wants edit units
                        CutClip(path=_path, start=sr.start_time.to_seconds(), duration=sr.duration.to_seconds())
                        )
                    
                else:
                    logging.debug("    Source range: None")

    logging.info(generate_ffconcat(ffconcat_clips))
    bmx_cmds = (generate_bmx(bmx_clips,args.output,args.bmx))
    execute_bmx(bmx_cmds)


def _resolve_media(path,trackname):
    #returns either trackname or if a file was found, the full path
    if path is None:
        return trackname
    for ext in (".mxf", ".mp4"):
        path = Path(path)
        _current = path / f"{trackname}{ext}"
        if (_current).exists():
            return(_current)
    return trackname

def generate_ffconcat(clips):
    lines = ["ffconcat version 1.0"]
    for clip in clips:
        # FFmpeg expects forward slashes even on Windows
        path_str = str(clip.path).replace("\\", "/")
        lines.append(f"file '{path_str}'")
        lines.append(f"inpoint {clip.start}")
        lines.append(f"outpoint {round(clip.duration + clip.start,3)}")
    logging.debug("\n" + "\n".join(lines))
    logging.info("\n" + "\n".join(lines))
    return "\n".join(lines)

def get_source_rate(filepath):
    media_info = MediaInfo.parse(filepath)
    _parsed = float(media_info.video_tracks[0].frame_rate)
    return _parsed

def generate_bmx(clips,output_path,bmxtranswrap):
    #for each clip, generate a bmx command for shell exec
    output_path = Path(output_path)
    _cmds = []
    apply_handle(clips,args.handle)
    for clip in clips:
        # FFmpeg expects forward slashes even on Windows
        
        _out_file = output_path / Path(clip.path).name
        #_orig_rate = get_source_rate(str(clip.path)) # mediainfo todo: add timeline rate as default

        bmxargs = [
                   str (bmxtranswrap) + " -t op1a -o \""+str(_out_file)+"\" --start ",
                   clip.bmx_start_frames," --dur ",
                   clip.bmx_duration_frames,
                   " \""+str(clip.path)+"\""]
        bmxargs = [str(x) for x in bmxargs]
        bmxargs = " ".join(bmxargs)
        _cmds.append (bmxargs)
    return (_cmds)

def apply_handle(clips: List[CutClip], handle: int = 0) -> None:
    """
    Modifies start and duration of each CutClip 
    """
    for clip in clips:
        orig_framerate = get_source_rate(str(clip.path))
        start_frames = clip.start * orig_framerate
        duration_frames = clip.duration * orig_framerate
        reduction = min(handle, start_frames)  # cannot reduce below 0
        clip.bmx_start_frames = start_frames - reduction
        clip.bmx_duration_frames = handle + reduction + duration_frames
        clip.bmx_start_frames = round(start_frames - reduction)
        clip.bmx_duration_frames = round(handle + reduction + duration_frames)
        logging.debug("calculated bmx start and duration: %i, %i", clip.bmx_start_frames, clip.bmx_duration_frames)

def execute_bmx(cmds):
    #execute all bmx cmds parallel
    results = []
    with ThreadPoolExecutor() as executor:
        {logging.debug("Executing: " +"\n" + cmd) for cmd in cmds}
        future_to_cmd = {executor.submit(run_command, cmd): cmd for cmd in cmds}
        for future in as_completed(future_to_cmd):
            results.append(future.result())
    #check results        
    failed = [r for r in results if not r["success"]]
    if failed:
        logging.error("The following commands failed:")
        for r in failed:
            logging.error(f"- Command: {r['cmd']}")
            logging.error(f"  Return code: {r['returncode']}")
            logging.error(f"  stderr: {r['stderr'].strip()}")
            logging.error("-" * 40)

    if any(not r["success"] for r in results):
        logging.error("One or more commands failed!")
        sys.exit(2)
    else:
        logging.debug("All commands succeeded!") 
        sys.exit(0)      

if __name__ == '__main__':
    try:
        main()
    except otio.exceptions.OTIOError as err:
        logging.error("ERROR: " + str(err) + "\n")
        sys.exit(1)
