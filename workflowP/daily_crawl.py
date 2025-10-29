import sys
import subprocess
import logging
from pathlib import Path
import os
import datetime
import time
import platform

BASE = Path(__file__).resolve().parent
CRAW_DIR = BASE  # 루트 디렉토리로 설정

LOG_PATH = BASE / "daily_crawl.log"

# 기본 로거/핸들러 구성: 파일은 매 실행 시 초기화(mode='w')
logger = logging.getLogger("daily_crawl")
logger.setLevel(logging.INFO)

# 포맷터: 타임스탬프 고정
_formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 콘솔 핸들러
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.INFO)
_sh.setFormatter(_formatter)
logger.addHandler(_sh)

# 파일 핸들러: 항상 덮어쓰기로 초기화
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8", mode="w")
_fh.setLevel(logging.INFO)
_fh.setFormatter(_formatter)
logger.addHandler(_fh)

# ANSI 색상 도우미 (GitHub Actions 콘솔 가독성)
class C:
    RESET = "\x1b[0m"
    BOLD = "\x1b[1m"
    DIM = "\x1b[2m"
    RED = "\x1b[31m"
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    BLUE = "\x1b[34m"

def color(msg: str, c: str) -> str:
    # 파일 로그에도 ANSI가 기록되지만, GitHub Actions 가독성을 우선
    return f"{c}{msg}{C.RESET}"

# 실행할 스크립트 목록 - 상대 경로로 수정
SCRIPTS = [
    # BASE / "craw" / "category" / "craw_danawa_all_categories.py",
    BASE / "craw" / "items" / "A_link_filter.py",
    BASE / "craw" / "items" / "B_in_link_get_items.py",
]

# 개별 스크립트 실행 최대 시간(초). 0이면 무제한
SCRIPT_TIMEOUT = int(os.environ.get("SCRIPT_TIMEOUT", "0"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

def check_file_exists(path):
    if not os.path.exists(path):
        logger.error(f"파일이 존재하지 않음: {path}")
        return False
    return True

def _kill_tree(proc: subprocess.Popen):
    try:
        if platform.system() == "Windows":
            # 전체 프로세스 트리 강제 종료
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            try:
                os.killpg(os.getpgid(proc.pid), 9)
            except Exception:
                proc.kill()
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass

def run_script(path: Path, timeout: int = SCRIPT_TIMEOUT, max_retries: int = MAX_RETRIES) -> bool:
    if not check_file_exists(path):
        return False

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        start_ts = datetime.datetime.now().isoformat()
        logger.info(color(f"=== 스크립트 실행 시작[{attempt}/{max_retries}]: {path} @ {start_ts} ===", C.BLUE))
        try:
            with subprocess.Popen(
                [sys.executable, str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=(platform.system() != "Windows"),
                creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if platform.system()=="Windows" else 0),
            ) as proc:
                start = time.time()
                assert proc.stdout is not None
                for line in proc.stdout:
                    msg = line.rstrip()
                    low = msg.lower()
                    # 하위 스크립트의 체크포인트/중간 저장 로그 강조
                    if ("💾" in msg) or ("체크포인트" in msg) or ("중간 저장" in msg) or ("checkpoint" in low):
                        logger.info(color(msg, C.YELLOW))
                    else:
                        logger.info(msg)
                    if timeout and (time.time() - start) > timeout:
                        _kill_tree(proc)
                        raise TimeoutError(f"스크립트 타임아웃 초과({timeout}s): {path}")
                ret = proc.wait()
                if ret != 0:
                    raise subprocess.CalledProcessError(ret, proc.args)
            end_ts = datetime.datetime.now().isoformat()
            logger.info(color(f"=== 스크립트 실행 종료: {path} (성공) @ {end_ts} ===", C.GREEN))
            return True
        except Exception as e:
            err_ts = datetime.datetime.now().isoformat()
            logger.error(color(f"스크립트 실행 실패[{attempt}/{max_retries}]: {path} - {e} @ {err_ts}", C.RED))
            if attempt < max_retries:
                time.sleep(min(5, attempt * 2))
            else:
                logger.error(color(f"최대 재시도 도달: {path}", C.RED))
                return False

def _write_github_summary(summary: str):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(summary)
            if not summary.endswith("\n"):
                f.write("\n")
    except Exception:
        pass

def main():
    start_iso = datetime.datetime.now().isoformat()
    logger.info(color(f"=== daily_crawl 시작 @ {start_iso} ===", C.BOLD))
    if not CRAW_DIR.exists():
        logger.error("CRAW_DIR가 존재하지 않습니다: %s", CRAW_DIR)
        logger.error("기존 크롤러 폴더를 CrawD/craw로 복사하세요.")
        return

    success, failed = [], []
    for s in SCRIPTS:
        ok = run_script(s, SCRIPT_TIMEOUT, MAX_RETRIES)
        if ok:
            success.append(s)
        else:
            failed.append(s)
        # 중간 실패라도 다음 작업 계속 진행

    end_iso = datetime.datetime.now().isoformat()
    # 요약 출력(색상)
    logger.info(color("=== 실행 요약 ===", C.BOLD))
    logger.info(color(f"성공: {len(success)}", C.GREEN))
    for p in success:
        logger.info(color(f"  ✔ {p}", C.GREEN))
    logger.info(color(f"실패: {len(failed)}", C.RED))
    for p in failed:
        logger.info(color(f"  ✖ {p}", C.RED))
    logger.info(color(f"로그 파일: {LOG_PATH}", C.BLUE))
    logger.info(color(f"=== daily_crawl 종료 @ {end_iso} ===", C.BOLD))

    # GitHub Actions Step Summary 작성(있을 경우)
    md = [
        "## Daily Crawl Summary",
        f"- Start: {start_iso}",
        f"- End: {end_iso}",
        f"- Success: {len(success)}",
        f"- Failed: {len(failed)}",
    ]
    if success:
        md.append("### Succeeded")
        md += [f"- {p}" for p in success]
    if failed:
        md.append("### Failed")
        md += [f"- {p}" for p in failed]
    _write_github_summary("\n".join(md))

if __name__ == "__main__":
    main()
