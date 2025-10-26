import logging
import json
import re
import time
import os
from selenium.webdriver.chrome.service import Service
from pathlib import Path
from multiprocessing import Pool, cpu_count
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
OUT_JSON = DATA_DIR / "quick_text_probe_parallel.json"

PAGELOAD_TIMEOUT = int(os.environ.get("PAGELOAD_TIMEOUT", "10"))
IMPLICIT_WAIT = int(os.environ.get("IMPLICIT_WAIT", "2"))
WAIT_TIMEOUT = int(os.environ.get("WAIT_TIMEOUT", "10"))
SAMPLE_N = int(os.environ.get("SAMPLE_N", "7000"))
WORKERS = int(os.environ.get("WORKERS", "4"))  # 병렬 실행 개수

LIST_SELECTORS = [
    "div.main_prodlist.main_prodlist_list > ul > li"
]

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
    link_batch, start_index, total = args
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
    width = max(5, len(str(total)))

    for idx, r in enumerate(link_batch, start=0):
        cur = start_index + idx
        cur_disp = f"{cur}{' ' * (width - len(str(cur)))}"
        prog_str = f"진행도 [{cur_disp}/ {total}]"
        link = r.get("link")
        path = [r.get(f"{i}차", "") for i in range(1, 5)]
        result = {"link": link, "path": path, "ok": False, "products": []}

        try:
            driver.get(link)
            time.sleep(2)

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
                    prod_name = clean_text(
                        item.find_element(
                            By.CSS_SELECTOR,
                            "div.prod_main_info > div.prod_info > p > a"
                        ).text
                    )

                    # ================== 스펙/태그 ==================
                    tags_css = ", ".join([
                        "div.prod_main_info > div.prod_info > div.spec-box.spec-box--full > div.spec_list",
                        "div.prod_main_info > div.prod_info > div.spec_list",
                        "div.spec_list",
                    ])
                    tags_elem = item.find_elements(By.CSS_SELECTOR, tags_css)
                    tags = clean_text(tags_elem[0].text if tags_elem else "")

                    # ================== 가격 ==================
                    price_els = item.find_elements(
                        By.CSS_SELECTOR,
                        "div.prod_main_info > div.prod_pricelist > ul > li p.price_sect > a > strong"
                    )
                    price = clean_text(price_els[0].text if price_els else "")

                    # ================== 저장 ==================
                    result["products"].append({
                        "image": image,
                        "prod_name": prod_name,
                        "tags": tags,
                        "price": price
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
            log.warning(f"❌ {path[-1] if path[-1] else link} 에러: {e}")

    driver.quit()
    return results

# ================== 메인 ==================
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

    # 🔹 처리 개수 제한 (deterministic)
    total = min(SAMPLE_N, len(uniq)) if SAMPLE_N > 0 else len(uniq)
    uniq = uniq[:total]
    log.info(f"총 {len(rows)}개 중 상위 {total}개 링크 병렬 점검 시작")

    # 🔹 병렬 처리 분할
    chunk_size = len(uniq) // WORKERS or 1
    raw_chunks = [uniq[i:i + chunk_size] for i in range(0, len(uniq), chunk_size)]
    # 각 청크의 시작 인덱스(1-based)와 총 개수를 함께 전달하여 전역 진행도를 계산
    chunks = []
    start = 1
    for batch in raw_chunks:
        chunks.append((batch, start, total))
        start += len(batch)
    log.info(f"각 프로세스당 {chunk_size}개 링크 처리 예정")

    # 🔹 병렬 실행
    with Pool(WORKERS) as pool:
        results = pool.map(worker, chunks)

    # 🔹 결과 합치기
    merged = [r for batch in results for r in batch]

    # 🔹 JSON 저장
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    log.info(f"✅ 병렬 크롤링 완료: 총 {len(merged)}개 링크 결과 저장 → {OUT_JSON}")

"""단일 실행 엔트리"""
if __name__ == "__main__":
    main()
