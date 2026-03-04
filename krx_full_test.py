import os
import requests
from dotenv import load_dotenv

def run_test():
    load_dotenv()
    _session = requests.Session()
    
    _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
    _LOGIN_JSP  = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
    _LOGIN_URL  = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 초기 세션 발급
    print("1. Hit MDCCOMS001.cmd")
    _session.get(_LOGIN_PAGE, headers={"User-Agent": _UA}, timeout=15)
    
    print("2. Hit login.jsp")
    _session.get(_LOGIN_JSP, headers={"User-Agent": _UA, "Referer": _LOGIN_PAGE}, timeout=15)

    payload = {
        "mbrNm": "", "telNo": "", "di": "", "certType": "",
        "mbrId": os.getenv("KRX_USERNAME"), "pw": os.getenv("KRX_PASSWORD"),
    }
    headers = {"User-Agent": _UA, "Referer": _LOGIN_PAGE}

    # 로그인 POST
    print("3. POST Login")
    resp = _session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
    data = resp.json()
    error_code = data.get("_error_code", "")
    print(f"Login Response: {data}")

    # CD011 중복 로그인 처리
    if error_code == "CD011":
        print("Handling CD011...")
        payload["skipDup"] = "Y"
        resp = _session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")
        print(f"Re-Login Response: {data}")

    print("Login Success?", error_code == "CD001")
    print("Session Cookies:", _session.cookies.get_dict())
    
    # ----------------------------------------------------
    # 데이터 요청 테스트 (Generate OTP or JSON)
    # ----------------------------------------------------
    print("4. Test getJsonData.cmd")
    data_url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    data_headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        'User-Agent': _UA,
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # KRX Client session required
    _session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
    _session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
    
    json_payload = {
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
    
    response = _session.post(data_url, headers=data_headers, data=json_payload)
    print("JSON Status:", response.status_code)
    print("JSON Snippet:", response.text[:500])

if __name__ == "__main__":
    run_test()
