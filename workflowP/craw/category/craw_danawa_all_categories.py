# craw/category/craw_danawa_all_categories.py
import logging
from pathlib import Path
import time
import json
import csv

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException

# ================== 경로 설정 ==================
THIS_FILE = Path(__file__).resolve()
PROJ_ROOT = THIS_FILE.parents[2]            # GiftStandard/
DATA_DIR = PROJ_ROOT / "craw" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = DATA_DIR / "danawa_category_rows.csv"
JSON_PATH = DATA_DIR / "danawa_category_rows.json"

# ================== 로그 설정 ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ================== 속도/대기 상수 ==================
HOVER_DELAY = 0.05          # hover 후 아주 짧은 대기
WAIT_TIMEOUT = 1            # 패널 표시 대기 최대 시간
WAIT_POLL_INTERVAL = 0.05   # 패널 탐색 주기

# ================== 유틸 함수 ==================
def clean_category_text(driver, el):
    """
    span.category__depth__txt의 '직계 텍스트 노드'만 추출하여
    <span class='icom'>인기메뉴</span> 같은 보조 텍스트 제거.
    """
    try:
        if el.tag_name.lower() == "span" and "category__depth__txt" in (el.get_attribute("class") or ""):
            txt_span = el
        else:
            txt_span = el.find_element(By.CSS_SELECTOR, "span.category__depth__txt")

        direct_text = driver.execute_script(
            """
            const node = arguments[0];
            if (!node) return '';
            const parts = [];
            for (const child of node.childNodes) {
              if (child.nodeType === Node.TEXT_NODE) {
                parts.push(child.nodeValue);
              }
            }
            return parts.join('').trim();
            """,
            txt_span
        )
        if direct_text:
            return " ".join(direct_text.split())

        full = txt_span.get_attribute("innerText") or ""
        for ic in txt_span.find_elements(By.CSS_SELECTOR, "span.icom, span[class*='ico']"):
            t = (ic.get_attribute("innerText") or ic.text or "").strip()
            if t:
                full = full.replace(t, "")
        return " ".join(full.split())
    except StaleElementReferenceException:
        return ""
    except Exception:
        try:
            return (el.text or "").strip()
        except Exception:
            return ""

def visible_only(elems):
    return [e for e in elems if getattr(e, "is_displayed", lambda: False)()]

def hover(actions, el, pause=HOVER_DELAY):
    try:
        actions.move_to_element(el).perform()
        if pause:
            time.sleep(pause)
    except Exception as e:
        logger.warning(f"hover 실패: {e}")

def get_panel(driver, el, class_keywords):
    """
    현재 el의 형제/같은 li 하위에서 class_keywords를 포함하는 div 패널 반환.
    """
    try:
        return driver.execute_script(
            """
            const el = arguments[0];
            const keywords = arguments[1];
            function match(node){
              if (!node || !node.className) return false;
              const cls = String(node.className);
              return keywords.every(k => cls.includes(k));
            }
            // 형제 우선
            let n = el.nextElementSibling;
            while(n){
              if (n.tagName === 'DIV' && match(n)) return n;
              n = n.nextElementSibling;
            }
            // 같은 li 내부
            const li = el.closest('li');
            if (li){
              const divs = li.querySelectorAll(':scope > div');
              for (const d of divs){
                if (match(d)) return d;
              }
            }
            return null;
            """,
            el, class_keywords
        )
    except Exception:
        return None

def wait_panel(driver, el, class_keywords, timeout=WAIT_TIMEOUT, poll_interval=WAIT_POLL_INTERVAL):
    end = time.time() + timeout
    while time.time() < end:
        panel = get_panel(driver, el, class_keywords)
        try:
            if panel and panel.is_displayed():
                return panel
        except Exception:
            pass
        time.sleep(poll_interval)
    return None

# ================== 메인 ==================
def main():
    logger.info("🔍 Danawa 전체 카테고리 크롤링 시작")
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    actions = ActionChains(driver)
    driver.get("https://www.danawa.com/")
    driver.implicitly_wait(3)

    rows = []
    try:
        first_menus = driver.find_elements(By.CSS_SELECTOR, "#sectionLayer > li > a")
        for first_menu in first_menus:
            try:
                first_text = clean_category_text(driver, first_menu)
            except StaleElementReferenceException:
                continue
            if not first_text:
                continue

            logger.info(f"1차: {first_text}")

            # 1차 → 2차
            hover(actions, first_menu)
            second_panel = wait_panel(driver, first_menu, ["category__2depth"])
            if not second_panel:
                continue

            second_items = visible_only(second_panel.find_elements(By.CSS_SELECTOR, "ul > li > a"))
            for second in second_items:
                try:
                    second_text = clean_category_text(driver, second)
                    if not second_text:
                        continue
                    logger.info(f"  └─ 2차: {second_text}")

                    # 2차 → 3차
                    hover(actions, second)
                    third_panel = wait_panel(driver, second, ["category__3depth"])

                    if not third_panel:
                        href = (second.get_attribute("href") or "").strip()
                        rows.append({"1차": first_text, "2차": second_text, "3차": "", "4차": "", "link": href})
                        logger.info(f"      [링크] {href}")
                        continue

                    third_items = visible_only(third_panel.find_elements(By.CSS_SELECTOR, "ul > li > a"))
                    for third in third_items:
                        try:
                            third_text = clean_category_text(driver, third)
                            if not third_text:
                                continue
                            logger.info(f"        └─ 3차: {third_text}")

                            # 3차 → 4차
                            hover(actions, third)
                            fourth_panel = wait_panel(driver, third, ["category__4depth"])

                            if not fourth_panel:
                                href = (third.get_attribute("href") or "").strip()
                                rows.append(
                                    {"1차": first_text, "2차": second_text, "3차": third_text, "4차": "", "link": href}
                                )
                                logger.info(f"            [링크] {href}")
                                continue

                            fourth_items = visible_only(fourth_panel.find_elements(By.CSS_SELECTOR, "ul > li > a"))
                            for fourth in fourth_items:
                                fourth_text = clean_category_text(driver, fourth)
                                if not fourth_text:
                                    continue
                                href = (fourth.get_attribute("href") or "").strip()
                                rows.append(
                                    {"1차": first_text, "2차": second_text, "3차": third_text, "4차": fourth_text, "link": href}
                                )
                                logger.info(f"            └─ 4차: {fourth_text} -> {href}")
                        except StaleElementReferenceException:
                            logger.debug("3차 카테고리 요소가 갱신되어 건너뜀")
                            continue
                except StaleElementReferenceException:
                    logger.debug("2차 카테고리 요소가 갱신되어 건너뜀")
                    continue

    finally:
        driver.quit()

    # 저장
    headers = ["1차", "2차", "3차", "4차", "link"]
    with CSV_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    with JSON_PATH.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    logger.info(
        f"✅ 완료: 총 {len(rows)}개 항목 | CSV 저장 경로: {CSV_PATH} | JSON 저장 경로: {JSON_PATH}"
    )

if __name__ == "__main__":
    logger.info("================= Danawa 카테고리 크롤러 시작 =================")
    main()
