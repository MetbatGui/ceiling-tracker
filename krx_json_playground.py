import os
from dotenv import load_dotenv
from src.infrastructure.krx_adapter import KrxDirectStockInfoAdapter

def run_playground():
    load_dotenv()
    adapter = KrxDirectStockInfoAdapter()
    
    print("\nTesting MDCSTAT01701 without isuCd")
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
        'locale': 'ko_KR',
        'isuCd': '', # Empty?
        'isuSrtCd': '005930',
        'strtDd': '20260201',
        'endDd': '20260228',
        'adjStkPrc_isNo': 'Y',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }
    
    res = adapter.session.post(url, headers=headers, data=payload)
    print("status:", res.status_code)
    try:
        data = res.json()
        output = data.get('output', [])
        print(f"Items found: {len(output)}")
    except Exception as e:
        print("Error:", e)
        
    payload['isuCd'] = 'KR7005930003'
    res = adapter.session.post(url, headers=headers, data=payload)
    print("status with KR7...:", res.status_code)
    print("items:", len(res.json().get('output', [])))
        
if __name__ == "__main__":
    run_playground()
