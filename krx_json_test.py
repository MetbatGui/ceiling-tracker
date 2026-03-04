import os
import requests
from dotenv import load_dotenv

def test_krx_json():
    load_dotenv()
    session = requests.Session()
    
    # Optional Login (KRX might allow this anonymously, but let's try with it)
    index_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }
    session.get(index_url, headers=headers)
    print("Initial GET Cookies:", session.cookies.get_dict())
    
    # Fetch JSON Data
    data_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    data_headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        'Sec-Ch-Ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    payload = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT02401',
        'locale': 'ko_KR',
        'mktId': 'STK',
        'invstTpCd': '9000',
        'strtDd': '20260303',
        'endDd': '20260303',
        'share': '1',
        'money': '3',
        'csvxls_isNo': 'false',
    }
    
    response = session.post(data_url, headers=data_headers, data=payload)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("Response Snippet:", response.text[:500])
    else:
        print("Response Text:", response.text)

if __name__ == "__main__":
    test_krx_json()
