import logging
from datetime import datetime

from core.config import conf
from core.defs import AsciiCommands

try:
    from tqdm import tqdm
    _TQDM_AVAILABLE = True
except Exception:
    tqdm = None
    _TQDM_AVAILABLE = False


class ColorizingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        if original_levelname == "WARNING":
            record.levelname = (
                f"{AsciiCommands.COLORIZE_WARNING.value}"
                f"{original_levelname}"
                f"{AsciiCommands.COLORIZE_DEFAULT.value}"
            )
        elif original_levelname in ("ERROR", "CRITICAL"):
            record.levelname = (
                f"{AsciiCommands.COLORIZE_ERROR.value}"
                f"{original_levelname}"
                f"{AsciiCommands.COLORIZE_DEFAULT.value}"
            )
        formatted = super().format(record)
        record.levelname = original_levelname
        return formatted


logger = logging.getLogger("boosty_downloader")
logger.setLevel(logging.DEBUG if conf.debug else logging.INFO)

if conf.progress_bar and _TQDM_AVAILABLE:
    class TqdmHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            msg = self.format(record)
            tqdm.write(msg)
    stream_handler = TqdmHandler()
else:
    stream_handler = logging.StreamHandler()
plain_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
color_formatter = ColorizingFormatter("%(asctime)s [%(levelname)s] %(message)s")

stream_handler.setFormatter(color_formatter)
logger.addHandler(stream_handler)

if conf.save_logs_to_file:
    filename = f"boosty_downloader_{datetime.timestamp(datetime.now())}_launch.log"
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(plain_formatter)
    logger.addHandler(file_handler)
