# craw/items/get_items_to_link.py
from pathlib import Path
import csv

# =============== 경로 설정 ===============
THIS_FILE = Path(__file__).resolve()
PROJ_ROOT = THIS_FILE.parents[2]                          # GiftStandard/
CSV_PATH = PROJ_ROOT / "craw" / "data" / "danawa_category_rows.csv"

# =============== 필터 설정 ===============
DANAWA_LIST_PREFIX = "https://prod.danawa.com/list/?cate="
EXCLUDE_FIRST_CATEGORY = "로켓배송관"   # 1차 == 이 값일 때 제외

def iter_rows(filter_prefix: str = DANAWA_LIST_PREFIX):
    """
    CSV_PATH에서 행을 읽고:
      1) link 가 filter_prefix 로 시작하지 않으면 제외
      2) 1차 카테고리가 EXCLUDE_FIRST_CATEGORY 와 같으면 제외
    """
    with CSV_PATH.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            link = row.get("link", "")
            first_cat = row.get("1차", "")
            # 조건 1: prefix 확인
            if filter_prefix and not link.startswith(filter_prefix):
                continue
            # 조건 2: 특정 1차 카테고리 제외
            # if first_cat == EXCLUDE_FIRST_CATEGORY:
            #     continue
            yield row

def to_list(filter_prefix: str = DANAWA_LIST_PREFIX):
    return list(iter_rows(filter_prefix))

def main():
    rows = to_list()
    print(f"총 {len(rows)}개 (prefix={DANAWA_LIST_PREFIX}, exclude={EXCLUDE_FIRST_CATEGORY})")
    # 샘플 10개 출력
    # for r in rows[:10]:
    #     print(f"{r['1차']} > {r['2차']} > {r['3차']} > {r['4차']} :: {r['link']}")

if __name__ == "__main__":
    main()
