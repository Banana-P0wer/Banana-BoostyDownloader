import sys
from pathlib import Path

from core.sync_data import SyncData
from welcome import print_welcome

try:
    import asyncio
    import os.path

    from boosty.api import get_profile_stat
    from core.config import conf
    from core.exceptions import SyncCancelledExc, ConfigMalformedExc
    from core.logger import logger
    from core.utils import parse_boosty_link, parse_bool, print_summary, create_dir_if_not_exists, print_colorized
    from core.launchers import fetch_and_save_media, fetch_and_save_posts, fetch_and_save_lonely_post
    from core.stat_tracker import stat_tracker
except Exception as e:
    print(f"[{e.__class__.__name__}] App stopped ({e})")
    input("Press enter to exit...")
    sys.exit(1)


async def main():
    links_queue = None
    if conf.links_file:
        links_path = Path(conf.links_file)
        try:
            raw_lines = links_path.read_text(encoding="utf-8").splitlines()
        except Exception as e:
            logger.critical(f"Failed to read links file: {links_path} ({e})")
            raise ConfigMalformedExc
        parsed_links = []
        invalid_links = []
        for line in raw_lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if (stripped.startswith('"') and stripped.endswith('"')) or (
                stripped.startswith("'") and stripped.endswith("'")
            ):
                stripped = stripped[1:-1].strip()
            creator_name, post_id = parse_boosty_link(stripped)
            if not post_id:
                invalid_links.append(stripped)
                continue
            parsed_links.append((creator_name, post_id, stripped))
        if invalid_links:
            logger.warning("Skipped invalid links:\n" + "\n".join(invalid_links))
        if not parsed_links:
            logger.critical("No valid post links found in file.")
            raise ConfigMalformedExc
        links_queue = parsed_links
        parsed_creator_name = links_queue[0][0]
    else:
        raw_creator_name = conf.post_link or conf.creator_name or input("Enter creator boosty link or user name > ")
        parsed_creator_name, post_id_from_link = parse_boosty_link(raw_creator_name)
        if conf.desired_post_id is None and post_id_from_link:
            conf.desired_post_id = post_id_from_link
        if parsed_creator_name.replace(" ", "") == "":
            logger.critical("Empty creator name, exiting.")
            raise ConfigMalformedExc
    use_cookie_in = True
    if not conf.ready_to_auth():
        print_colorized("Attention", "Without authorization, many files may not be available for download. We recommend that you fill the 'auth' field in the configuration file, following the instructions from README.md", warn=True)
        if parse_bool(input("Do you want to continue without authorization? (y/n) > ")):
            use_cookie_in = False
        else:
            logger.info("Ok.")
            raise SyncCancelledExc
    print_summary(
        creator_name=parsed_creator_name,
        use_cookie=use_cookie_in,
        sync_dir=str(conf.sync_dir),
        need_load_video=conf.need_load_video,
        need_load_photo=conf.need_load_photo,
        need_load_audio=conf.need_load_audio,
        need_load_files=conf.need_load_files,
        storage_type=conf.storage_type,
    )
    if not conf.auto_confirm_download:
        if not parse_bool(input("Proceed? (y/n) > ")):
            raise SyncCancelledExc

    if not os.path.isdir(conf.sync_dir):
        logger.critical(f"Path {conf.sync_dir} does not exist. Create it and try again.")
        raise ConfigMalformedExc

    if links_queue:
        print_colorized("starting sync", conf.storage_type)
        print(f"syncing {len(links_queue)} posts:")
        max_creator_len = max(24, max(len(c) for c, _, _ in links_queue))
        for creator_name, post_id, _ in links_queue:
            print(f"{creator_name:<{max_creator_len}} {post_id}")

        succeeded_links = []
        skipped_links = []
        failed_links = []
        incomplete_links = []
        incomplete_files_all = []

        sync_data_by_creator = {}
        sync_data_errors = set()
        if conf.sync_offset_save:
            unique_creators = {creator for creator, _, _ in links_queue}
            for creator_name in unique_creators:
                base_path = conf.sync_dir / creator_name
                cache_path = base_path / "__cache__"
                sync_data_file_path = cache_path / conf.default_sd_file_name
                create_dir_if_not_exists(base_path)
                create_dir_if_not_exists(cache_path)
                try:
                    sync_data_by_creator[creator_name] = await SyncData.get_or_create_sync_data(
                        sync_data_file_path,
                        creator_name
                    )
                except Exception as e:
                    logger.error(f"Failed to prepare sync data for {creator_name}: {e}")
                    sync_data_errors.add(creator_name)

        post_parallel = max(1, int(conf.max_download_parallel))
        post_semaphore = asyncio.Semaphore(post_parallel)

        async def process_link(creator_name: str, post_id: str, raw_link: str):
            async with post_semaphore:
                if creator_name in sync_data_errors:
                    return raw_link, "error", []
                base_path = conf.sync_dir / creator_name
                cache_path = base_path / "__cache__"
                create_dir_if_not_exists(base_path)
                create_dir_if_not_exists(cache_path)
                sync_data = sync_data_by_creator.get(creator_name) if conf.sync_offset_save else None
                try:
                    status, incomplete_files = await fetch_and_save_lonely_post(
                        creator_name=creator_name,
                        post_id=post_id,
                        use_cookie=use_cookie_in,
                        base_path=base_path,
                        cache_path=cache_path,
                        sync_data=sync_data
                    )
                except Exception as e:
                    logger.error(f"Failed to process link {raw_link}: {e}")
                    return raw_link, "error", []
                return raw_link, status, incomplete_files

        tasks = [process_link(creator_name, post_id, raw_link) for creator_name, post_id, raw_link in links_queue]
        results = await asyncio.gather(*tasks)
        for raw_link, status, incomplete_files in results:
            if incomplete_files:
                if raw_link not in incomplete_links:
                    incomplete_links.append(raw_link)
                for file_path in incomplete_files:
                    if file_path not in incomplete_files_all:
                        incomplete_files_all.append(file_path)
            if status == "downloaded":
                succeeded_links.append(raw_link)
            elif status == "skipped":
                skipped_links.append(raw_link)
            elif status == "ok":
                succeeded_links.append(raw_link)
            else:
                failed_links.append(raw_link)

        from core.defs import AsciiCommands
        if failed_links:
            logger.info(
                f"{AsciiCommands.COLORIZE_ERROR.value}Unable to download the following links:"
                f"{AsciiCommands.COLORIZE_DEFAULT.value}\n" + "\n".join(failed_links)
            )
        if skipped_links:
            logger.info(
                f"{AsciiCommands.COLORIZE_WARNING.value}Skipped (already downloaded) links:"
                f"{AsciiCommands.COLORIZE_DEFAULT.value}\n" + "\n".join(skipped_links)
            )
            if incomplete_files_all:
                logger.warning(
                    "The following files appear to be incomplete or corrupted:\n"
                    + "\n".join(incomplete_files_all)
                )
                print("Would you like to delete these files and try downloading them again to avoid potential issues?")
                if parse_bool(input("Delete and re-download these files? (yes/no): ")):
                    for file_path in incomplete_files_all:
                        try:
                            Path(file_path).unlink()
                        except Exception as e:
                            logger.warning(f"Failed to delete file {file_path}: {e}")
                    stat_tracker.clear_incomplete_files()
                    for link in incomplete_links:
                        creator_name, post_id = parse_boosty_link(link)
                        if not post_id:
                            continue
                        base_path = conf.sync_dir / creator_name
                        cache_path = base_path / "__cache__"
                        sync_data_file_path = cache_path / conf.default_sd_file_name
                        create_dir_if_not_exists(base_path)
                        create_dir_if_not_exists(cache_path)
                        sync_data = None
                        if conf.sync_offset_save:
                            try:
                                sync_data = await SyncData.get_or_create_sync_data(sync_data_file_path, creator_name)
                            except Exception as e:
                                logger.error(f"Failed to prepare sync data for {creator_name}: {e}")
                                continue
                        await fetch_and_save_lonely_post(
                            creator_name=creator_name,
                            post_id=post_id,
                            use_cookie=use_cookie_in,
                            base_path=base_path,
                            cache_path=cache_path,
                            sync_data=sync_data
                        )
        if succeeded_links:
            logger.info(
                f"{AsciiCommands.COLORIZE_HIGHLIGHT.value}Downloaded the following links:"
                f"{AsciiCommands.COLORIZE_DEFAULT.value}\n" + "\n".join(succeeded_links)
            )
        if conf.final_statistics_table:
            unique_creators = {creator for creator, _, _ in links_queue}
            if len(unique_creators) == 1:
                await get_profile_stat(next(iter(unique_creators)))
            stat_tracker.show_summary()
        return

    base_path: Path = conf.sync_dir / parsed_creator_name
    cache_path = base_path / "__cache__"
    sync_data_file_path = cache_path / conf.default_sd_file_name

    print_colorized("starting sync", conf.storage_type)
    if conf.desired_post_id:
        print("syncing 1 post:")
        print(f"{parsed_creator_name:<24} {conf.desired_post_id}")

    create_dir_if_not_exists(base_path)
    create_dir_if_not_exists(cache_path)

    sync_data = None
    if conf.sync_offset_save:
        sync_data = await SyncData.get_or_create_sync_data(sync_data_file_path, parsed_creator_name)

    if conf.desired_post_id:
        await fetch_and_save_lonely_post(
            creator_name=parsed_creator_name,
            post_id=conf.desired_post_id,
            use_cookie=use_cookie_in,
            base_path=base_path,
            cache_path=cache_path,
            sync_data=sync_data
        )
    elif conf.storage_type == "media":
        image_start_offset = None
        video_start_offset = None
        audio_start_offset = None
        if sync_data:
            rt_photo_offset = await sync_data.get_runtime_photo_offset()
            rt_video_offset = await sync_data.get_runtime_video_offset()
            rt_audio_offset = await sync_data.get_runtime_audio_offset()
            if any((rt_photo_offset, rt_audio_offset, rt_video_offset)):
                print_colorized("Oops", "Seems like your last sync ends unexpected", warn=True)
                if parse_bool(input("Shall we pick up where we left off? (y/n) > ")):
                    if rt_photo_offset:
                        image_start_offset = rt_photo_offset
                    if rt_video_offset:
                        video_start_offset = rt_video_offset
                    if rt_audio_offset:
                        audio_start_offset = rt_audio_offset
        await fetch_and_save_media(
            creator_name=parsed_creator_name,
            use_cookie=use_cookie_in,
            base_path=base_path,
            sync_data=sync_data,
            image_start_offset=image_start_offset,
            audio_start_offset=audio_start_offset,
            video_start_offset=video_start_offset,
        )
    elif conf.storage_type == "post":
        start_offset = None
        if sync_data:
            rt_posts_offset = await sync_data.get_runtime_posts_offset()
            if rt_posts_offset:
                print_colorized("Oops", "Seems like your last sync ends unexpected", warn=True)
                if parse_bool(input("Shall we pick up where we left off? (y/n) > ")):
                    start_offset = rt_posts_offset
        await fetch_and_save_posts(
            creator_name=parsed_creator_name,
            use_cookie=use_cookie_in,
            base_path=base_path,
            cache_path=cache_path,
            start_offset=start_offset,
            sync_data=sync_data
        )
    await get_profile_stat(parsed_creator_name)
    if conf.final_statistics_table:
        stat_tracker.show_summary()


if __name__ == "__main__":
    try:
        print_welcome()
        asyncio.run(main())
    except Exception as e:
        print(f"[{e.__class__.__name__}] App stopped with: {e.__class__.__name__}")
        if conf.debug:
            raise e
    finally:
        input("\nPress enter to exit...")
