import os
import requests
from dotenv import load_dotenv

def test_krx_otp():
    load_dotenv()
    session = requests.Session()
    
    # 1. Login
    login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    login_headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    login_payload = {
        'mbrNm': '',
        'telNo': '',
        'di': '',
        'certType': '',
        'mbrId': os.getenv('KRX_USERNAME'),
        'pw': os.getenv('KRX_PASSWORD')
    }
    
    session.post(login_url, headers=login_headers, data=login_payload)
    print("Logged in. Cookies:", session.cookies.get_dict())

    
    # Set required client session cookie
    session.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
    session.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
    
    otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_headers = {
        'Accept': 'text/plain, */*; q=0.01',
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
    
    otp_payload = {
        'locale': 'ko_KR',
        'mktId': 'STK',
        'invstTpCd': '9000',
        'strtDd': '20260303',
        'endDd': '20260303',
        'share': '1',
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
    }
    
    response = session.post(otp_url, headers=otp_headers, data=otp_payload)
    print(f"OTP Status Code: {response.status_code}")
    print(f"OTP Response Text (OTP Code): {response.text}")

if __name__ == "__main__":
    test_krx_otp()
