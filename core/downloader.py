import asyncio
import os
from pathlib import Path
from typing import Literal, Any, Optional

from boosty.api import download_file, get_content_length
from boosty.wrappers.media_pool import MediaPool
from core.defs import ContentType
from core.logger import logger
from core.meta import write_video_metadata
from core.utils import create_dir_if_not_exists
from core.stat_tracker import stat_tracker


class Downloader:
    def __init__(
            self,
            media_pool: MediaPool,
            base_path: Path,
            max_parallel_downloads: int = 10,
            save_meta: bool = False,
    ):
        self.media_pool = media_pool
        self.base_path = base_path
        self._max_parallel_downloads = max_parallel_downloads
        self._save_meta = save_meta
        self._semaphore = asyncio.Semaphore(max_parallel_downloads)

    async def _download_file_if_not_exists(
            self,
            file_url: str,
            path: Path,
            metadata: Optional[dict[str, Any]] = None
    ) -> Literal["downloaded", "passed", "incomplete"]:
        part_path = Path(str(path) + ".part")
        if part_path.exists():
            logger.info(f"Found incomplete file part, deleting: {part_path}")
            try:
                part_path.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete incomplete part file: {part_path} ({e})")

        if os.path.isfile(path):
            expected_len = None
            if ".m3u8" not in file_url.lower():
                expected_len = await get_content_length(file_url)
            if expected_len:
                actual_len = os.path.getsize(path)
                if actual_len < expected_len * 0.8:
                    logger.warning("File already exists but appears to be incomplete or corrupted")
                    stat_tracker.add_incomplete_file(str(path))
                    logger.debug(f"Skip saving file {path}: already exists but appears incomplete")
                    return "incomplete"
            logger.debug(f"Skip saving file {path}: already exists")
            return "passed"

        logger.info(f"Will save file: {path}")
        result = await download_file(file_url, part_path)
        if not result:
            raise Exception(f"Failed to download file {file_url}")
        if result:
            try:
                os.replace(part_path, path)
            except Exception as e:
                logger.warning(f"Failed to finalize download for {path}: {e}")
                raise
        if self._save_meta and result and metadata:
            logger.info(f"Write metadata to file {path}")
            await write_video_metadata(path, metadata)
        return "downloaded"

    async def _get_file_and_raise_stat(
            self,
            url: str,
            path_file: Path,
            _t: Literal["p", "v", "a", "f"],
            metadata: dict[str, Any] | None = None
    ):
        meta = metadata
        match _t:
            case "p":
                passed = stat_tracker.add_passed_photo
                downloaded = stat_tracker.add_downloaded_photo
                error = stat_tracker.add_error_photo
                meta = None
            case "v":
                passed = stat_tracker.add_passed_video
                downloaded = stat_tracker.add_downloaded_video
                error = stat_tracker.add_error_video
            case "a":
                passed = stat_tracker.add_passed_audio
                downloaded = stat_tracker.add_downloaded_audio
                error = stat_tracker.add_error_audio
                meta = None
            case "f":
                passed = stat_tracker.add_passed_file
                downloaded = stat_tracker.add_downloaded_file
                error = stat_tracker.add_error_file
                meta = None
            case _:
                logger.warning(f"Unknown _t: {_t}")
                return

        async with self._semaphore:
            try:
                status = await self._download_file_if_not_exists(url, path_file, meta)
                if status == "downloaded":
                    downloaded()
                else:
                    passed()
                return status
            except Exception as e:
                error()
                return "error"

    async def download_by_content_type(self, content_type: ContentType):
        match content_type:
            case ContentType.IMAGE:
                await self.download_photos()
                return
            case ContentType.VIDEO:
                await self.download_videos()
                return
            case ContentType.AUDIO:
                await self.download_audios()
                return

    async def download_photos(self):
        tasks = []
        paths = []
        photo_path = self.base_path / "photos"
        create_dir_if_not_exists(photo_path)
        images = self.media_pool.get_images()
        for image in images:
            path = photo_path / (image["id"] + ".jpg")
            tasks.append(self._get_file_and_raise_stat(image["url"], path, "p"))
            paths.append(path)
        results = await asyncio.gather(*tasks)
        incomplete_paths = [str(p) for p, r in zip(paths, results) if r == "incomplete"]
        return results, incomplete_paths

    async def download_videos(self):
        tasks = []
        paths = []
        video_path = self.base_path / "videos"
        create_dir_if_not_exists(video_path)
        videos = self.media_pool.get_videos()
        for video in videos:
            path = video_path / (video["id"] + ".mp4")
            tasks.append(self._get_file_and_raise_stat(video["url"], path, "v", video.get("meta")))
            paths.append(path)
        results = await asyncio.gather(*tasks)
        incomplete_paths = [str(p) for p, r in zip(paths, results) if r == "incomplete"]
        return results, incomplete_paths

    async def download_audios(self):
        tasks = []
        paths = []
        audio_path = self.base_path / "audios"
        create_dir_if_not_exists(audio_path)
        audios = self.media_pool.get_audios()
        for audio in audios:
            path = audio_path / (audio["id"] + ".mp3")
            tasks.append(self._get_file_and_raise_stat(audio["url"], path, "a"))
            paths.append(path)
        results = await asyncio.gather(*tasks)
        incomplete_paths = [str(p) for p, r in zip(paths, results) if r == "incomplete"]
        return results, incomplete_paths

    async def download_files(self):
        tasks = []
        paths = []
        files_path = self.base_path / "files"
        create_dir_if_not_exists(files_path)
        files = self.media_pool.get_files()
        for file in files:
            path = files_path / file["title"]
            tasks.append(self._get_file_and_raise_stat(file["url"], path, "f"))
            paths.append(path)
        results = await asyncio.gather(*tasks)
        incomplete_paths = [str(p) for p, r in zip(paths, results) if r == "incomplete"]
        return results, incomplete_paths
