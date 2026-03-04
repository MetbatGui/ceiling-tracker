import requests

def test_krx_login():
    session = requests.Session()
    
    url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://data.krx.co.kr',
        'Referer': 'https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    payload = {
        'mbrNm': '',
        'telNo': '',
        'di': '',
        'certType': '',
        'mbrId': 'zeya9643',
        'pw': 'chlwltjr43!'
    }
    
    # Send login request
    response = session.post(url, headers=headers, data=payload)
    
    print(f"Status Code: {response.status_code}")
    print(f"Response URL: {response.url}")
    print(f"Response Text: {response.text}")
    print(f"Cookies: {session.cookies.get_dict()}")

if __name__ == "__main__":
    test_krx_login()
