import sys
import subprocess
import logging
from pathlib import Path
import os
import datetime
import time

BASE = Path(__file__).resolve().parent
CRAW_DIR = BASE  # 루트 디렉토리로 설정

LOG_PATH = BASE / "daily_crawl.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8")
    ]
)
logger = logging.getLogger("daily_crawl")

# 실행할 스크립트 목록 - 상대 경로로 수정
SCRIPTS = [
    # BASE / "craw" / "category" / "craw_danawa_all_categories.py",
    BASE / "craw" / "items" / "A_link_filter.py",
    BASE / "craw" / "items" / "B_in_link_get_items.py",
]

# 개별 스크립트 실행 최대 시간(초). 0이면 무제한
SCRIPT_TIMEOUT = int(os.environ.get("SCRIPT_TIMEOUT", "0"))

def check_file_exists(path):
    if not os.path.exists(path):
        logger.error(f"파일이 존재하지 않음: {path}")
        return False
    return True

def run_script(path: Path, timeout: int = SCRIPT_TIMEOUT):
    if not check_file_exists(path):
        return
        
    logger.info(f"=== 스크립트 실행 시작: {path} ===")
    try:
        with subprocess.Popen(
            [sys.executable, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        ) as proc:
            start = time.time()
            assert proc.stdout is not None
            for line in proc.stdout:
                logger.info(line.rstrip())
                if timeout and (time.time() - start) > timeout:
                    proc.kill()
                    raise TimeoutError(f"스크립트 타임아웃 초과({timeout}s): {path}")
            ret = proc.wait()
            if ret != 0:
                raise subprocess.CalledProcessError(ret, proc.args)
        logger.info(f"=== 스크립트 실행 종료: {path} (성공) ===")
    except Exception as e:
        logger.error(f"스크립트 실행 실패: {path} - {e}")
        raise

def main():
    logger.info("=== daily_crawl 시작: %s ===", datetime.datetime.now().isoformat())
    if not CRAW_DIR.exists():
        logger.error("CRAW_DIR가 존재하지 않습니다: %s", CRAW_DIR)
        logger.error("기존 크롤러 폴더를 CrawD/craw로 복사하세요.")
        return
    for s in SCRIPTS:
        run_script(s, SCRIPT_TIMEOUT)
    logger.info("=== daily_crawl 종료 ===")

if __name__ == "__main__":
    main()
