import os
import httpx
from dotenv import load_dotenv

def run_test():
    load_dotenv()
    
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc',
        'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    # Enable HTTP/2
    client = httpx.Client(http2=True, headers=headers)
    
    print("1. Hit index page")
    client.get('https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201')
    
    print("2. Login")
    login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    login_payload = {
        'mbrNm': '', 'telNo': '', 'di': '', 'certType': '',
        'mbrId': os.getenv('KRX_USERNAME'),
        'pw': os.getenv('KRX_PASSWORD')
    }
    
    res = client.post(login_url, data=login_payload)
    print("Login Response:", res.text)
    
    client.cookies.set('mdc.client_session', 'true', domain='data.krx.co.kr')
    client.cookies.set('lang', 'ko_KR', domain='data.krx.co.kr')
    print("Cookies:", dict(client.cookies))
    
    print("3. Get OTP")
    otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
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
    
    response = client.post(otp_url, data=otp_payload)
    print("OTP Status:", response.status_code)
    print("OTP Response:", response.text)

if __name__ == "__main__":
    run_test()
