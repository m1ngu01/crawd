import logging
import json
import re
import time
import os
import datetime
from selenium.webdriver.chrome.service import Service
from pathlib import Path
from multiprocessing import Pool, cpu_count, Manager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from A_link_filter import to_list

# ================== 상수 ==================
THIS_FILE = Path(__file__).resolve()
PROJ_ROOT = THIS_FILE.parents[2]
DATA_DIR = PROJ_ROOT / "craw" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = DATA_DIR / "quick_text_probe_parallel"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LEGACY_JSON_PATH = DATA_DIR / "quick_text_probe_parallel.json"
MANIFEST_PATH = OUTPUT_DIR / "manifest.json"
STATE_PATH = OUTPUT_DIR / "state.json"
STATUS_PATH = DATA_DIR / "quick_text_probe_parallel.status.json"
JSON_PART_RECORDS = max(1, int(os.environ.get("JSON_PART_RECORDS", "500")))

PAGELOAD_TIMEOUT = int(os.environ.get("PAGELOAD_TIMEOUT", "10"))
IMPLICIT_WAIT = int(os.environ.get("IMPLICIT_WAIT", "2"))
WAIT_TIMEOUT = int(os.environ.get("WAIT_TIMEOUT", "10"))
SAMPLE_N = int(os.environ.get("SAMPLE_N", "7000"))
WORKERS = int(os.environ.get("WORKERS", "4"))  # 병렬 실행 개수
CHECKPOINT_N = int(os.environ.get("CHECKPOINT_N", "10"))  # 중간 저장 단위
# 배치(청크) 크기: 기본 10 → 10개 단위로 부모가 결과 수신/체크포인트 가능
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))

LIST_SELECTORS = [
    "div.main_prodlist.main_prodlist_list > ul > li"
]
# 목록형 보기 버튼 (리스트형 전환)
LIST_VIEW_BUTTON_SELECTOR = (
    "#danawa_content > div.product_list_wrap > div > div.prod_list_tab > div > "
    "div.view_opt > ul > li.type_item"
)

# ================== 로깅 ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s][%(processName)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ================== 유틸 ==================
def clean_text(s: str) -> str:
    """텍스트 정제"""
    if not s:
        return ""
    s = re.sub(r"[ \t\r\f]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def parse_float(value: str):
    """문자열에서 실수 추출 (없으면 None)"""
    if not value:
        return None
    cleaned = value.replace(",", "")
    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None

def parse_int(value: str):
    """문자열에서 정수 추출 (없으면 None)"""
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def short_exception(exc: Exception) -> str:
    """
    Selenium 예외 등에서 스택트레이스가 포함된 메시지를 간결하게 정리한다.
    """
    text = ""
    for attr in ("msg", "message"):
        candidate = getattr(exc, attr, None)
        if isinstance(candidate, str) and candidate.strip():
            text = candidate
            break
    if not text:
        text = str(exc) if exc else ""
    if "Stacktrace:" in text:
        text = text.split("Stacktrace:", 1)[0]
    if text.lower().startswith("message"):
        parts = text.split(":", 1)
        if len(parts) == 2:
            text = parts[1]
    text = text.strip()
    if not text:
        text = exc.__class__.__name__ if exc else ""
    return text

def ensure_list_view(driver, page_url=None):
    """목록형(리스트) 보기로 전환"""
    if not page_url:
        try:
            page_url = driver.current_url
        except Exception:
            page_url = None
    url_for_log = page_url or "<unknown>"

    try:
        list_button = WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, LIST_VIEW_BUTTON_SELECTOR))
        )
    except Exception as exc:
        log.warning("목록형 보기 탭을 찾지 못했습니다 (url=%s): %s", url_for_log, short_exception(exc))
        return False

    try:
        current_class = list_button.get_attribute("class") or ""
        if "selected" in current_class.split():
            return True

        driver.execute_script("arguments[0].click();", list_button)

        def _list_view_selected(d):
            try:
                refreshed = d.find_element(By.CSS_SELECTOR, LIST_VIEW_BUTTON_SELECTOR)
                cls = refreshed.get_attribute("class") or ""
                return "selected" in cls.split()
            except Exception:
                return False

        WebDriverWait(driver, WAIT_TIMEOUT).until(_list_view_selected)
        time.sleep(0.5)
        return True
    except Exception as exc:
        log.warning("목록형 보기 전환 실패 (url=%s): %s", url_for_log, short_exception(exc))
        return False

def find_product_items(driver):
    """상품 리스트 탐색 (로드 대기 포함)"""
    for sel in LIST_SELECTORS:
        try:
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, sel))
            )
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return els, sel
        except Exception:
            pass
    return [], ""

# ================== 워커 함수 ==================
def worker(args):
    """링크 리스트 한 묶음을 병렬로 크롤링"""
    if len(args) == 4:
        link_batch, start_index, total, skipped = args
    else:
        link_batch, start_index, total = args
        skipped = 0

    progress_total = total - skipped
    if progress_total <= 0:
        progress_total = len(link_batch) or 1

    if skipped:
        progress_total_display = f"{total - skipped}"
    else:
        progress_total_display = str(progress_total)
    results = []

    # 크롬 옵션 설정
    service = Service(log_path=os.devnull)
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--headless=new")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--disable-logging")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-default-apps")
    options.add_argument("--disable-breakpad")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--mute-audio")

    options.add_experimental_option("excludeSwitches", [
        "enable-logging",
        "enable-automation",
        "enable-blink-features=AutomationControlled"
    ])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGELOAD_TIMEOUT)
    driver.implicitly_wait(IMPLICIT_WAIT)

    # 진행도 출력 폭 계산 (예: 1250 -> 폭 5 에 맞춰 우측 언더스코어 패딩)
    width = max(5, len(str(progress_total)))

    for idx, r in enumerate(link_batch, start=0):
        cur = start_index + idx
        cur_disp = f"{cur}{' ' * (width - len(str(cur)))}"
        prog_str = f"진행도 [{cur_disp}/ {progress_total_display}]"
        link = r.get("link")
        path = [r.get(f"{i}차", "") for i in range(1, 5)]
        result = {"link": link, "path": path, "ok": False, "products": []}

        try:
            driver.get(link)
            time.sleep(2)

            ensure_list_view(driver, page_url=link)

            # 상품 리스트 탐색
            items, used_sel = find_product_items(driver)
            if not items:
                continue

            for item in items[:30]:  # 최대 30개
                try:
                    # ================== 이미지 ==================
                    img_el = item.find_element(
                        By.CSS_SELECTOR,
                        "div.prod_main_info > div.thumb_image > a.thumb_link > img"
                    )
                    image = img_el.get_attribute("data-original") or img_el.get_attribute("src")

                    # ================== 상품명 ==================
                    prod_anchor = item.find_element(
                        By.CSS_SELECTOR,
                        "div.prod_main_info > div.prod_info > p > a"
                    )
                    prod_name = clean_text(prod_anchor.text)
                    prod_link = prod_anchor.get_attribute("href") or ""

                    # ================== 스펙/태그 ==================
                    tags_css = ", ".join([
                        "div.prod_main_info div.prod_info div.spec-box[data-simple-description-open-area='Y'] div.spec_list",
                        "div.prod_main_info div.prod_info div.spec-box:not([style*='display:none']) div.spec_list",
                        "div.prod_info div.spec-box[data-simple-description-open-area='Y'] div.spec_list",
                        "div.prod_info div.spec-box:not([style*='display:none']) div.spec_list",
                    ])
                    tags_elem = item.find_elements(By.CSS_SELECTOR, tags_css)
                    tags = clean_text(tags_elem[0].text if tags_elem else "")

                    # ================== 가격 ==================
                    price_els = item.find_elements(
                        By.CSS_SELECTOR,
                        "div.prod_main_info > div.prod_pricelist > ul > li p.price_sect > a > strong"
                    )
                    price = clean_text(price_els[0].text if price_els else "")

                    # ================== 평점/리뷰 ==================
                    score_els = item.find_elements(
                        By.CSS_SELECTOR,
                        "div.prod_info > div.prod_sub_info > div > div > a > div > span.text__score"
                    )
                    raw_score = clean_text(score_els[0].text if score_els else "")
                    rating = parse_float(raw_score)

                    review_els = item.find_elements(
                        By.CSS_SELECTOR,
                        "div.prod_info > div.prod_sub_info > div > div > a > div > div.text__review > span.text__number"
                    )
                    raw_review_count = clean_text(review_els[0].text if review_els else "")
                    review_count = parse_int(raw_review_count)

                    rating_weighted = None
                    if rating is not None and review_count is not None:
                        rating_weighted = round(rating * review_count, 2)

                    # ================== 저장 ==================
                    result["products"].append({
                        "link": prod_link,
                        "image": image,
                        "prod_name": prod_name,
                        "tags": tags,
                        "price": price,
                        "rating": rating,
                        "review_count": review_count,
                        "rating_weighted": rating_weighted,
                        "raw_rating_text": raw_score,
                        "raw_review_text": raw_review_count,
                    })

                except Exception as e:
                    log.debug("item parse error: %s", e, exc_info=True)
                    continue

            result.update({
                "ok": True,
                "list_selector": used_sel,
                "product_count": len(result["products"])
            })
            results.append(result)
            log.info(f"✅ {len(result['products'])}개 완료 | {prog_str} - {path[1] if len(path) > 1 else path[0]}")

        except Exception as e:
            log.warning(f"❌ {path[-1] if path[-1] else link} 에러: {short_exception(e)}")

    driver.quit()
    return results

# ================== 메인 ==================
def _read_existing_results():
    """
    기존 결과를 로드한다. 분할 저장(manifest 기반) 또는 레거시 단일 JSON 모두 지원.
    """
    results = []
    manifest = None
    if MANIFEST_PATH.exists():
        try:
            with MANIFEST_PATH.open("r", encoding="utf-8") as mf:
                manifest = json.load(mf)
        except Exception as exc:
            log.warning("manifest 읽기 실패: %s", exc)
            manifest = None
        if manifest:
            for part in manifest.get("parts", []):
                filename = part.get("file")
                if not filename:
                    continue
                part_path = OUTPUT_DIR / filename
                if not part_path.exists():
                    continue
                with part_path.open("r", encoding="utf-8") as pf:
                    for line in pf:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            results.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            return results
    if LEGACY_JSON_PATH.exists():
        try:
            with LEGACY_JSON_PATH.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as exc:
            log.warning("레거시 JSON 로드 실패: %s", exc)
            return []
    return results

def _chunk_list(items, size):
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]

def _write_sharded_results(data):
    """
    데이터를 JSONL 파트로 분할 저장하고 manifest/state/status를 갱신한다.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().isoformat()
    tmp_parts = []
    part_entries = []
    for index, chunk in enumerate(_chunk_list(data, JSON_PART_RECORDS), start=1):
        filename = f"part_{index:05}.jsonl"
        final_path = OUTPUT_DIR / filename
        tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            for row in chunk:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")
        tmp_parts.append((tmp_path, final_path))
        part_entries.append({"file": filename, "count": len(chunk)})

    for existing in OUTPUT_DIR.glob("part_*.jsonl"):
        try:
            existing.unlink()
        except OSError:
            pass

    for tmp_path, final_path in tmp_parts:
        tmp_path.replace(final_path)

    manifest = {
        "parts": part_entries,
        "total_count": len(data),
        "updated_at": timestamp,
        "part_size_limit": JSON_PART_RECORDS,
    }
    tmp_manifest = MANIFEST_PATH.with_suffix(".tmp")
    with tmp_manifest.open("w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2, ensure_ascii=False)
    tmp_manifest.replace(MANIFEST_PATH)

    state = {
        "links": [item.get("link") for item in data if isinstance(item, dict) and item.get("ok")],
        "updated_at": timestamp,
    }
    tmp_state = STATE_PATH.with_suffix(".tmp")
    with tmp_state.open("w", encoding="utf-8") as sf:
        json.dump(state, sf, indent=2, ensure_ascii=False)
    tmp_state.replace(STATE_PATH)

    if LEGACY_JSON_PATH.exists():
        try:
            LEGACY_JSON_PATH.unlink()
        except OSError:
            pass

def _write_status(processed_links, pending_links, skipped_links, total_links, eligible_links, complete_total):
    payload = {
        "timestamp": datetime.datetime.now().isoformat(),
        "processed_links": processed_links,
        "pending_links": pending_links,
        "skipped_links": skipped_links,
        "total_links": total_links,
        "eligible_links": eligible_links,
        "complete_total": complete_total,
    }
    tmp_status = STATUS_PATH.with_suffix(".tmp")
    with tmp_status.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    tmp_status.replace(STATUS_PATH)

def main():
    rows = to_list()
    if not rows:
        log.warning("필터된 링크가 없습니다.")
        return

    # 중복 제거
    uniq, seen = [], set()
    for r in rows:
        lk = r.get("link", "")
        if lk and lk not in seen:
            uniq.append(r)
            seen.add(lk)

    # 🔹 기존 결과 로드 및 재시작 스킵 구성
    prev_results = _read_existing_results()
    prev_links = {r.get("link") for r in prev_results if isinstance(r, dict) and r.get("ok")}

    # 🔹 처리 개수 제한 (deterministic)
    total = min(SAMPLE_N, len(uniq)) if SAMPLE_N > 0 else len(uniq)
    uniq = uniq[:total]

    # 🔹 재시작 스킵 적용
    todo = [r for r in uniq if r.get("link") not in prev_links]
    skipped = len(uniq) - len(todo)
    log.info(f"총 {len(rows)}개 중 상위 {total}개 링크 병렬 점검 시작 (이전 완료 {skipped}개 스킵)")

    # 🔹 병렬 처리 분할
    chunk_size = max(1, min(BATCH_SIZE, len(todo))) if todo else 1
    raw_chunks = [todo[i:i + chunk_size] for i in range(0, len(todo), chunk_size)]
    # 각 청크의 시작 인덱스(1-based)와 총 개수를 함께 전달하여 전역 진행도를 계산
    chunks = []
    start = 1
    for batch in raw_chunks:
        chunks.append((batch, start, total, skipped))
        start += len(batch)
    log.info(f"각 프로세스당 {chunk_size}개 링크 처리 예정")

    # 🔹 병렬 실행
    manager = Manager()
    shared_results = manager.list()  # 병렬 안전 수집
    lock = manager.Lock()

    pending_initial = len(todo)
    last_checkpoint_at = 0

    def _maybe_checkpoint():
        nonlocal last_checkpoint_at
        current_total = len(prev_results) + len(shared_results)
        if CHECKPOINT_N > 0 and current_total - last_checkpoint_at >= CHECKPOINT_N:
            with lock:
                current_shared = list(shared_results)
                data = list(prev_results) + current_shared
                _write_sharded_results(data)
                last_checkpoint_at = current_total
                pending_links = max(0, pending_initial - len(current_shared))
                _write_status(len(current_shared), pending_links, skipped, len(rows), len(uniq), len(data))
                log.info(f"💾 체크포인트 저장 ({current_total}개) → {OUTPUT_DIR}")

    if todo:
        with Pool(WORKERS) as pool:
            for batch_results in pool.imap_unordered(worker, chunks):
                with lock:
                    for item in batch_results:
                        shared_results.append(item)
                _maybe_checkpoint()

    # 🔹 최종 저장 (이전 + 신규)
    final_shared = list(shared_results)
    final_data = list(prev_results) + final_shared
    force_write = bool(final_shared) or not MANIFEST_PATH.exists() or LEGACY_JSON_PATH.exists()
    if force_write:
        _write_sharded_results(final_data)
    else:
        log.info("💾 신규 결과 없음, 기존 분할 파일 유지")
    pending_links = max(0, pending_initial - len(final_shared))
    _write_status(len(final_shared), pending_links, skipped, len(rows), len(uniq), len(final_data))
    log.info(f"✅ 병렬 크롤링 완료: 신규 {len(final_shared)}개, 누적 {len(final_data)}개 저장 → {OUTPUT_DIR}")

"""단일 실행 엔트리"""
if __name__ == "__main__":
    main()
