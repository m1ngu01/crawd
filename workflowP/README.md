workflowP

설명:
- 이 폴더는 Danawa 크롤러를 하루에 한 번 실행하도록 정리한 구조입니다.
- 핵심 진입점: workflowP/daily_crawl.py
- 기존 크롤러 스크립트(craw 폴더)를 workflowP/craw로 복사해야 합니다.

로컬 실행 방법:
1. workflowP 디렉터리에 기존 craw 폴더를 복사:
   복사 예시 (PowerShell):
   Copy-Item -Recurse -Path "..\craw" -Destination ".\craw"

2. 가상환경 생성 및 활성화:
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1  (PowerShell) 또는 .\.venv\Scripts\activate.bat (CMD)

3. 의존성 설치:
   pip install -r workflowP/requirements.txt

4. 수동 실행:
   python workflowP/daily_crawl.py

GitHub Actions:
- 리포지토리에 `.github/workflows/daily_crawl.yml` 파일이 포함되어 매일 자동 실행됩니다.
- Selenium 크롤러가 브라우저를 필요로 하므로 워크플로에서 Chrome을 설치하고 headless로 실행합니다.

주의사항:
- 카테고리 스크립트 경로는 `craw/category/craw_danawa_all_categories.py` 입니다.
- 아이템 스크립트는 환경변수(`WORKERS`, `SAMPLE_N`, `PAGELOAD_TIMEOUT` 등)로 동작을 조절할 수 있습니다.
