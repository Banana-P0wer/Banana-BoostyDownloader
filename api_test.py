import argparse
import asyncio
import re
from typing import Any, Optional

import aiohttp
import yaml


BOOSTY_API_BASE_URL = "https://api.boosty.to"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",  # noqa: E501
    "Sec-Ch-Ua": '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
}


def parse_boosty_link(raw_input: str) -> tuple[str, Optional[str]]:
    match = re.search(r"boosty\.to/([^/\s]+)/?(?:posts/([0-9a-fA-F-]+))?", raw_input)
    if match is None:
        return raw_input, None
    return match.group(1), match.group(2)


def load_auth_from_config(path: str) -> tuple[Optional[str], Optional[str]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        auth = data.get("auth", {})
        return auth.get("cookie"), auth.get("authorization")
    except Exception:
        return None, None


def find_hash_fields(obj: dict[str, Any]) -> dict[str, Any]:
    hash_fields = {}
    for key, value in obj.items():
        lower = key.lower()
        if any(token in lower for token in ("hash", "md5", "sha", "checksum")):
            hash_fields[key] = value
    return hash_fields


def is_playlist_url(url: str) -> bool:
    lowered = url.lower()
    return ".m3u8" in lowered or "video.m3u8" in lowered


async def fetch_post_json(
    session: aiohttp.ClientSession,
    creator_name: str,
    post_id: str,
    cookie: Optional[str],
    authorization: Optional[str],
) -> dict[str, Any]:
    headers = dict(DEFAULT_HEADERS)
    if cookie and authorization:
        headers["Cookie"] = cookie
        headers["Authorization"] = authorization
    url = f"{BOOSTY_API_BASE_URL}/v1/blog/{creator_name}/post/{post_id}"
    async with session.get(url, headers=headers) as resp:
        print(f"POST API status: {resp.status}")
        resp.raise_for_status()
        return await resp.json()


async def get_content_length(
    session: aiohttp.ClientSession,
    url: str,
) -> tuple[Optional[int], dict[str, str]]:
    headers = dict(DEFAULT_HEADERS)
    # Try HEAD first
    async with session.head(url, headers=headers, allow_redirects=True) as resp:
        if resp.status in (200, 206):
            length = resp.headers.get("Content-Length")
            return (int(length) if length else None), dict(resp.headers)

    # Fallback: range request
    headers["Range"] = "bytes=0-0"
    async with session.get(url, headers=headers, allow_redirects=True) as resp:
        if resp.status in (200, 206):
            length = resp.headers.get("Content-Length")
            return (int(length) if length else None), dict(resp.headers)
    return None, {}


def format_check_status(available: bool) -> str:
    return "YES" if available else "NO"


async def main():
    parser = argparse.ArgumentParser(description="Test Boosty API media metadata.")
    parser.add_argument("link", help="Boosty post link")
    parser.add_argument("--limit", type=int, default=3, help="Limit number of media items to inspect")
    parser.add_argument("--config", default="config.yml", help="Path to config.yml with auth")
    args = parser.parse_args()

    creator, post_id = parse_boosty_link(args.link)
    if not post_id:
        raise SystemExit("Invalid link: post id not found")

    cookie, authorization = load_auth_from_config(args.config)

    async with aiohttp.ClientSession() as session:
        post = await fetch_post_json(session, creator, post_id, cookie, authorization)

        signed_query = post.get("signedQuery", "")
        items = []

        for media in post.get("data", []):
            media_type = media.get("type", "unknown")
            hash_fields = find_hash_fields(media)
            size = media.get("size")
            media_id = media.get("id")

            if media_type == "ok_video":
                for url_info in media.get("playerUrls", []):
                    url = url_info.get("url")
                    if url:
                        items.append(
                            {
                                "type": "video",
                                "id": media_id,
                                "url": url,
                                "size": size,
                                "hash_fields": hash_fields,
                            }
                        )
            elif media_type in ("image", "audio_file", "file"):
                url = media.get("url")
                if url:
                    if media_type in ("audio_file", "file"):
                        url = url + signed_query
                    items.append(
                        {
                            "type": media_type,
                            "id": media_id,
                            "url": url,
                            "size": size,
                            "hash_fields": hash_fields,
                        }
                    )

        print(f"Found media items: {len(items)} (showing up to {args.limit})")
        warned_items = 0
        for i, item in enumerate(items[: args.limit], start=1):
            print("-" * 80)
            print(f"{i}. type={item['type']} id={item['id']}")
            print(f"   size(from API)={item.get('size')}")
            print(f"   url={item['url']}")
            if item["hash_fields"]:
                print(f"   hash fields={item['hash_fields']}")
            else:
                print("   hash fields=NONE")

            length, headers = await get_content_length(session, item["url"])
            print(f"   Content-Length={length}")
            for header_key in ("ETag", "Content-MD5", "Content-Range"):
                if header_key in headers:
                    print(f"   {header_key}={headers[header_key]}")

            has_hash = bool(item["hash_fields"])
            has_length = length is not None
            has_size = item.get("size") is not None
            playlist = is_playlist_url(item["url"])

            print("   verification order: hash > content-length > api-size")
            print(
                "   available checks: "
                f"hash={format_check_status(has_hash)}, "
                f"content-length={format_check_status(has_length)}, "
                f"api-size={format_check_status(has_size)}"
            )

            warnings = []
            if playlist:
                warnings.append("HLS playlist URL (.m3u8); Content-Length is not the media size.")
            if not has_hash:
                warnings.append("No hash fields in API response.")
            if not has_length:
                warnings.append("No Content-Length from server (HEAD/Range).")
            if not has_size:
                warnings.append("No size field in API response.")
            if has_length and has_size and item["size"] != length:
                warnings.append(f"Size mismatch: api-size={item['size']} vs content-length={length}")

            if warnings:
                warned_items += 1
                for w in warnings:
                    print(f"   WARN: {w}")

        if warned_items:
            print("-" * 80)
            print(f"WARN: {warned_items} item(s) have potential verification gaps.")


if __name__ == "__main__":
    asyncio.run(main())
