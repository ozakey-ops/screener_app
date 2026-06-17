"""
KRX 주식 스크리너 - 모바일 웹앱 v1.0
────────────────────────────────────────────────────────────────
실행: python screener_app.py
접속: 같은 Wi-Fi의 스마트폰에서  http://{컴퓨터IP}:5000
────────────────────────────────────────────────────────────────
"""

import os, sys, time, glob, json, socket, threading, zipfile, io
import xml.etree.ElementTree as ET
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, render_template_string
from functools import lru_cache

# ═══════════════════════════════════════════════════════════════
#  설정
# ═══════════════════════════════════════════════════════════════

from dotenv import load_dotenv
load_dotenv()
KRX_API_KEY  = os.environ.get("KRX_API_KEY", "")
DART_API_KEY = os.environ.get("DART_API_KEY", "")
KRX_BASE     = "https://data-dbg.krx.co.kr"
DART_BASE    = "https://opendart.fss.or.kr/api"
CACHE_TTL    = 3600   # 1시간 캐시

app = Flask(__name__)
_cache        = {}   # {key: (timestamp, data)}
_corp_map     = {}   # stock_code(6자리) → DART corp_code(8자리)

# 연말 시총 캐시: {year(int): {stock_code: mktcap(원)}}
_year_end_mktcap: dict = {}
_year_end_lock   = threading.Lock()
_ye_single: dict = {}   # {(stock_code, year): mktcap} — 단일종목 캐시
_ye_single_lock  = threading.Lock()

def get_corp_map():
    """DART corpCode.xml 다운로드 → stock_code→corp_code 매핑 (1회 캐시)"""
    global _corp_map
    if _corp_map:
        return _corp_map
    try:
        print("[DART] corpCode.xml 다운로드 중...")
        r = requests.get(f"{DART_BASE}/corpCode.xml",
                         params={"crtfc_key": DART_API_KEY}, timeout=30)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        xml_bytes = z.read("CORPCODE.xml")
        root = ET.fromstring(xml_bytes)
        for item in root.findall("list"):
            sc = (item.findtext("stock_code") or "").strip()
            cc = (item.findtext("corp_code")  or "").strip()
            if sc and cc and sc != " ":
                _corp_map[sc] = cc
        print(f"[DART] corp_map 완료: {len(_corp_map)}개 상장사")
    except Exception as e:
        print(f"[DART] corpCode.xml 실패: {e}")
    return _corp_map

# ═══════════════════════════════════════════════════════════════
#  데이터 수집 유틸
# ═══════════════════════════════════════════════════════════════

def recent_biz_day(offset=0):
    d = datetime.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    for _ in range(abs(offset)):
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def cached(key, fn, ttl=CACHE_TTL):
    now = time.time()
    if key in _cache and now - _cache[key][0] < ttl:
        return _cache[key][1]
    data = fn()
    _cache[key] = (now, data)
    return data


def krx_fetch(endpoint, bas_dd):
    url = KRX_BASE + endpoint
    try:
        r = requests.get(url, params={"basDd": bas_dd},
                         headers={"AUTH_KEY": KRX_API_KEY, "Accept": "application/json"},
                         timeout=15)
        r.raise_for_status()
        return r.json().get("OutBlock_1", [])
    except Exception:
        return []


def fetch_ye_mktcap_single(stock_code: str, year: int) -> float:
    """KRX에서 특정 종목의 연말 시총만 수집 — 전종목 대신 단일 종목 API 파라미터 사용.
    isuCd 필터 지원 시 해당 종목만, 미지원 시 전체에서 필터링."""
    code6 = stock_code.zfill(6)
    for delta in range(1, 12):
        candidate = datetime(year, 12, 31) - timedelta(days=delta)
        if candidate.weekday() >= 5:
            continue
        date_str = candidate.strftime("%Y%m%d")
        for ep in ["/svc/apis/sto/stk_bydd_trd", "/svc/apis/sto/ksq_bydd_trd"]:
            try:
                url = f"{KRX_BASE}{ep}"
                # 단일종목 파라미터 시도
                r = requests.get(url,
                    params={"basDd": date_str, "isuCd": code6},
                    headers={"AUTH_KEY": KRX_API_KEY, "Accept": "application/json"},
                    timeout=15)
                rows = r.json().get("OutBlock_1", [])
                # API가 단일종목 필터를 지원하면 1건, 미지원이면 전체 반환
                for row in rows:
                    cd = str(row.get("ISU_CD", "")).zfill(6)[:6]
                    if cd == code6:
                        mc = to_num(row.get("MKTCAP", 0))
                        if mc > 0:
                            print(f"[KRX-YE] {code6} {year}연말 → {date_str} 시총={mc/1e8:,.1f}억원")
                            return mc
            except Exception:
                continue
        print(f"[KRX-YE] {code6}/{date_str} 데이터 없음, 하루 앞으로 이동")
    print(f"[KRX-YE] {code6} {year}연말 시총 수집 실패")
    return 0.0


def get_ye_mktcap_single(stock_code: str, year: int) -> float:
    """단일종목 연말 시총 캐시 조회 → 없으면 수집"""
    key = (stock_code, year)
    if key not in _ye_single:
        with _ye_single_lock:
            if key not in _ye_single:
                _ye_single[key] = fetch_ye_mktcap_single(stock_code, year)
    return _ye_single.get(key, 0.0)


def prefetch_ye_mktcap_for_stock(stock_code: str, years: list):
    """특정 종목의 여러 연도 연말 시총을 병렬 수집"""
    missing = [y for y in years if (stock_code, y) not in _ye_single]
    if not missing:
        return
    print(f"[KRX-YE] {stock_code} 연말 시총 prefetch: {missing}")
    def _fetch(yr):
        get_ye_mktcap_single(stock_code, yr)
    with ThreadPoolExecutor(max_workers=4) as exe:
        list(exe.map(_fetch, missing))


def fetch_year_end_mktcap(year: int) -> dict:
    """KRX에서 해당 연도 12월 마지막 거래일 전종목 시총 수집
    Returns: {stock_code(6자리): mktcap(원)} — 데이터 없으면 {}
    """
    # Dec 31부터 최대 10일 전까지 거슬러 올라가며 거래일 탐색
    for delta in range(1, 12):  # 12월 30일부터 역순 탐색
        candidate = datetime(year, 12, 31) - timedelta(days=delta)
        if candidate.weekday() >= 5:
            continue   # 주말 건너뜀
        date_str = candidate.strftime("%Y%m%d")
        all_data: dict = {}
        for ep in ["/svc/apis/sto/stk_bydd_trd", "/svc/apis/sto/ksq_bydd_trd"]:
            rows = krx_fetch(ep, date_str)
            for row in rows:
                code   = str(row.get("ISU_CD", "")).zfill(6)[:6]
                mktcap = to_num(row.get("MKTCAP", 0))
                if code and mktcap > 0:
                    all_data[code] = mktcap
        if all_data:
            print(f"[KRX-YE] {year} 연말 → {date_str} 기준 {len(all_data)}종목 시총 수집")
            return all_data
        # 공휴일 등으로 데이터 없으면 하루 더 앞으로
        print(f"[KRX-YE] {year}/{date_str} 데이터 없음, 하루 앞으로 이동")
    print(f"[KRX-YE] {year} 연말 시총 데이터 수집 실패")
    return {}


def get_year_end_mktcap(stock_code: str, year: int) -> float:
    """특정 종목·연도 연말 시총(원) 반환. 미캐시면 KRX 수집 후 저장."""
    global _year_end_mktcap
    if year not in _year_end_mktcap:
        with _year_end_lock:
            if year not in _year_end_mktcap:          # double-check
                _year_end_mktcap[year] = fetch_year_end_mktcap(year)
    return _year_end_mktcap.get(year, {}).get(stock_code, 0.0)


def prefetch_year_end_mktcaps(years_needed: list):
    """필요한 연도들의 연말 시총을 병렬로 미리 수집 (최대 4개 동시)"""
    missing = [y for y in years_needed if y not in _year_end_mktcap]
    if not missing:
        return
    print(f"[KRX-YE] 연말 시총 prefetch: {missing}")
    def _fetch(year):
        get_year_end_mktcap("_prefetch_", year)   # 수집+캐싱만 목적
    with ThreadPoolExecutor(max_workers=4) as exe:
        list(exe.map(_fetch, missing))


def get_all_stocks():
    """KOSPI + KOSDAQ 종목 전체 반환 (캐시 1시간)"""
    def fetch():
        bas_dd = recent_biz_day(-1)
        rows = []
        for ep in ["/svc/apis/sto/stk_bydd_trd", "/svc/apis/sto/ksq_bydd_trd"]:
            data = krx_fetch(ep, bas_dd)
            if not data:
                data = krx_fetch(ep, recent_biz_day(-2))
            rows.extend(data)

        # 로컬 Excel 폴백
        if not rows:
            files = sorted(glob.glob("KRX_종목목록_*.xlsx"), reverse=True)
            if files:
                df = pd.read_excel(files[0], sheet_name="전체", dtype=str)
                return df.fillna("").to_dict("records")
        return rows

    return cached("stocks", fetch)


def to_num(v):
    """쉼표 제거 후 float 변환 (음수 부호 보존)"""
    try:
        s = str(v).replace(",", "").strip()
        return float(s) if s and s not in ("-", "") else 0.0
    except Exception:
        return 0.0


def normalize_stocks(raw):
    result = []
    for r in raw:
        mkt = str(r.get("MKT_NM", r.get("시장구분", "")))
        result.append({
            "code":   str(r.get("ISU_CD", r.get("종목코드", ""))).zfill(6)[:6],
            "name":   str(r.get("ISU_NM", r.get("종목명", ""))),
            "market": "KOSPI" if "유가" in mkt or "KOSPI" in mkt else
                      "KOSDAQ" if "코스닥" in mkt or "KOSDAQ" in mkt else mkt,
            "close":  to_num(r.get("TDD_CLSPRC", r.get("종가", 0))),
            "chg":    to_num(r.get("CMPPREVDD_PRC", r.get("전일대비", 0))),
            "chg_rt": to_num(r.get("FLUC_RT", r.get("등락률", 0))),
            "volume": to_num(r.get("ACC_TRDVOL", r.get("거래량", 0))),
            "mktcap": to_num(r.get("MKTCAP", r.get("시가총액", 0))),
            "shares": to_num(r.get("LIST_SHRS", r.get("상장주식수", 0))),
            "sector": str(r.get("SECT_TP_NM", r.get("소속부", ""))),
        })
    return result


# ═══════════════════════════════════════════════════════════════
#  Flask API
# ═══════════════════════════════════════════════════════════════

@app.route("/api/stocks")
def api_stocks():
    raw    = get_all_stocks()
    stocks = normalize_stocks(raw)
    bas_dd = recent_biz_day(-1)
    date_disp = f"{bas_dd[:4]}.{bas_dd[4:6]}.{bas_dd[6:]}"
    return jsonify({"count": len(stocks), "data": stocks, "date": date_disp})


@app.route("/api/screener")
def api_screener():
    """
    쿼리 파라미터로 필터링:
      market=KOSPI|KOSDAQ|ALL
      mktcap_min, mktcap_max  (억원)
      chg_rt_min, chg_rt_max  (%)
      volume_min              (주)
      q                       (종목명/코드 검색)
      sort=mktcap|chg_rt|volume|close  desc
      page, size
    """
    raw    = get_all_stocks()
    stocks = normalize_stocks(raw)

    # 필터
    market     = request.args.get("market", "ALL")
    q          = request.args.get("q", "").strip().lower()
    cap_min    = float(request.args.get("mktcap_min", 0))   * 1e8
    cap_max    = float(request.args.get("mktcap_max", 9e15))* 1e8
    chg_min    = float(request.args.get("chg_rt_min", -99))
    chg_max    = float(request.args.get("chg_rt_max",  99))
    vol_min    = float(request.args.get("volume_min",   0))
    sort_by    = request.args.get("sort", "mktcap")
    page       = max(1, int(request.args.get("page", 1)))
    size       = min(100, int(request.args.get("size", 50)))

    filtered = [
        s for s in stocks
        if (market == "ALL" or s["market"] == market)
        and (not q or q in s["name"].lower() or q in s["code"])
        and cap_min  <= s["mktcap"]  <= cap_max
        and chg_min  <= s["chg_rt"]  <= chg_max
        and s["volume"] >= vol_min
    ]

    # 정렬
    reverse = sort_by not in ("code", "name")
    filtered.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)

    total  = len(filtered)
    start  = (page - 1) * size
    paged  = filtered[start: start + size]

    return jsonify({"total": total, "page": page, "size": size, "data": paged})


@app.route("/api/debug/search/<name>")
def api_debug_search(name):
    """종목명으로 raw 데이터 검색 (디버그용) — /api/debug/search/삼성전자"""
    raw = get_all_stocks()
    stocks = normalize_stocks(raw)
    hits = [s for s in stocks if name in s["name"]]
    return jsonify({
        "query": name,
        "total_stocks": len(stocks),
        "hits": len(hits),
        "data": hits[:20],
        "sample_names": [s["name"] for s in stocks[:10]],
    })


@app.route("/api/debug/dart/<code>")
def api_debug_dart(code):
    """DART 연결 단계별 진단 — /api/debug/dart/005930"""
    out = {"stock_code": code, "steps": {}}

    # 1) corp_code 조회 (corpCode.xml 매핑)
    corp_map  = get_corp_map()
    corp_code = corp_map.get(code, "")
    out["steps"]["corp_map"] = {
        "map_size": len(corp_map),
        "corp_code": corp_code or "없음",
    }

    if not corp_code:
        out["error"] = "corp_code 없음 → DART 미등록 종목"
        return jsonify(out)

    # 2) 최근 사업연도 4카테고리 샘플
    cur_year = datetime.today().year - 1  # 직전연도 (사업보고서 확정)
    sample = {}
    for cat in ["M210000", "M220000", "M230000", "M250000"]:
        try:
            r = requests.get(f"{DART_BASE}/fnlttCmpnyIndx.json", params={
                "crtfc_key":   DART_API_KEY,
                "corp_code":   corp_code,
                "bsns_year":   str(cur_year),
                "reprt_code":  "11011",
                "idx_cl_code": cat,
            }, timeout=12)
            js = r.json()
            sample[cat] = {
                "status": js.get("status"),
                "count":  len(js.get("list", [])),
                "items":  [{"nm": x.get("idx_nm"), "val": x.get("idx_val")}
                           for x in js.get("list", [])[:5]],
            }
            time.sleep(0.2)
        except Exception as e:
            sample[cat] = {"error": str(e)}

    out["steps"]["fnlttCmpnyIndx"] = {"year": cur_year, "cats": sample}

    # 3) 캐시 상태
    dart_key = f"dart_{code}"
    if dart_key in _cache:
        ts, data = _cache[dart_key]
        out["steps"]["cache"] = {
            "hit": True,
            "age_sec": round(time.time() - ts),
            "years_in_data": list(data.keys()) if isinstance(data, dict) else type(data).__name__,
        }
    else:
        out["steps"]["cache"] = {"hit": False}

    return jsonify(out)


@app.route("/api/debug/dart-metrics/<code>")
def api_debug_dart_metrics(code):
    """DART가 실제 반환하는 지표명 전체 확인 — /api/debug/dart-metrics/005930"""
    corp_code = get_corp_map().get(code, "")
    if not corp_code:
        return jsonify({"error": "corp_code 없음"})

    cur_year = datetime.today().year - 1
    out = {"stock_code": code, "corp_code": corp_code, "year": cur_year, "cats": {}}
    cat_labels = {
        "M210000": "수익성", "M220000": "안정성",
        "M230000": "성장성", "M250000": "시장가치"
    }
    for cat, label in cat_labels.items():
        for reprt_code in ["11011", "11014", "11012"]:
            try:
                r = requests.get(f"{DART_BASE}/fnlttCmpnyIndx.json", params={
                    "crtfc_key": DART_API_KEY, "corp_code": corp_code,
                    "bsns_year": str(cur_year), "reprt_code": reprt_code,
                    "idx_cl_code": cat,
                }, timeout=12)
                js = r.json()
                if js.get("status") == "000" and js.get("list"):
                    out["cats"][f"{cat}({label})"] = {
                        "reprt_code": reprt_code,
                        "metrics": [
                            {"nm": x.get("idx_nm"), "val": x.get("idx_val")}
                            for x in js["list"]
                        ]
                    }
                    break
                time.sleep(0.2)
            except Exception as e:
                out["cats"][f"{cat}({label})"] = {"error": str(e)}
    return jsonify(out)


@app.route("/api/debug/dart-clear/<code>")
def api_debug_dart_clear(code):
    """DART 캐시 강제 삭제 — /api/debug/dart-clear/005930"""
    for key in [f"dart_raw_{code}"]:
        _cache.pop(key, None)
    return jsonify({"cleared": f"dart_raw_{code}"})


@app.route("/api/debug/dart-raw/<code>")
def api_debug_dart_raw(code):
    """수집된 원시 재무데이터 확인 + 이자비용 계정명 실시간 조회
    사용법: /api/debug/dart-raw/005930
    """
    _cache.pop(f"dart_raw_{code}", None)   # 캐시 초기화 후 새로 수집
    raw_fin = fetch_dart_raw(code)
    if not raw_fin:
        return jsonify({"error": "데이터 없음 (corp_code 미매핑 또는 DART 오류)"}), 404

    # 최신연도 이자비용 상세
    years = sorted(raw_fin.keys())
    latest = raw_fin[years[-1]] if years else {}

    # 이자비용 관련 계정 원시 검색 (캐시 없이 DART 직접)
    corp_code = get_corp_map().get(code, "")
    interest_candidates = []
    if corp_code:
        try:
            r = requests.get(f"{DART_BASE}/fnlttSinglAcntAll.json", params={
                "crtfc_key": DART_API_KEY, "corp_code": corp_code,
                "bsns_year": str(datetime.today().year - 1),
                "reprt_code": "11011", "fs_div": "CFS",
            }, timeout=15)
            js = r.json()
            for item in js.get("list", []):
                nm = (item.get("account_nm") or "").strip()
                if any(k in nm for k in ["이자","금융비용","금융원가","차입원가"]):
                    interest_candidates.append({
                        "sj_div": item.get("sj_div"),
                        "account_nm": nm,
                        "amount": item.get("thstrm_amount"),
                    })
        except Exception as e:
            interest_candidates = [{"error": str(e)}]

    return jsonify({
        "code": code,
        "years_collected": years,
        "latest_year_raw": latest,
        "interest_related_accounts": interest_candidates,
    })


@app.route("/api/debug/ye-stock/<code>")
def api_debug_ye_stock(code):
    """종목별 연도별 연말시총 + 배당수익률 확인 — /api/debug/ye-stock/009150"""
    code = code.zfill(6)
    # 캐시된 연말시총에서 해당 종목만 추출
    result = {}
    for yr in sorted(set(k[1] for k in _ye_single if k[0] == code)):
        mc = _ye_single.get((code, yr), 0)
        result[str(yr)] = {
            "mktcap_원": mc,
            "mktcap_억": round(mc / 1e8, 1) if mc else None,
        }

    # DART 재무 데이터 있으면 배당+scale도 같이 출력
    cache_key = f"dart_{code}"
    raw_fin = _cache.get(cache_key, {}).get("data")
    if raw_fin:
        stocks_raw = get_all_stocks()
        stock_info = next((s for s in normalize_stocks(stocks_raw) if s["code"] == code), {})
        shares = stock_info.get("shares", 0)
        close  = stock_info.get("close",  0)
        for yr_str, d in raw_fin.items():
            yr_int   = int(yr_str)
            eq_raw   = d.get("equity")
            divs_raw = d.get("dividends")
            # scale 감지 (연말주가 기준)
            ye_mc = _year_end_mktcap.get(yr_int, {}).get(code, 0)
            yr_close = (ye_mc / shares) if ye_mc > 0 and shares > 0 else close
            detected_scale = 1_000_000
            if eq_raw and eq_raw > 0 and shares > 0 and yr_close > 0:
                for s in [1, 1_000, 1_000_000, 100_000_000]:
                    bps = eq_raw * s / shares
                    if bps > 0 and 0.05 <= (yr_close / bps) <= 50:
                        detected_scale = s
                        break
            entry = result.setdefault(yr_str, {})
            entry["yr_close_추정"] = round(yr_close) if yr_close else None
            entry["equity_raw"]   = eq_raw
            entry["scale"]        = detected_scale
            if divs_raw and ye_mc:
                # 배당 독립 scale 감지
                d = abs(divs_raw)
                div_scale = detected_scale
                for s in [1, 1_000, 1_000_000]:
                    yld = d * s / ye_mc * 100
                    if 0.01 <= yld <= 30:
                        div_scale = s
                        break
                divs_won = d * div_scale
                entry["div_scale"]     = div_scale
                entry["eq_scale"]      = detected_scale
                entry["divs_won"]      = round(divs_won)
                entry["배당수익률_pct"] = round(divs_won / ye_mc * 100, 2)

    return jsonify({"code": code, "years": result})


@app.route("/api/debug/year-end/<int:year>")
def api_debug_year_end(year):
    """특정 연도 연말 시총 수집 확인 — /api/debug/year-end/2023
    반환: 해당 연도 연말 기준 시총 상위 10종목 + 특정 종목 조회 예시
    """
    data = fetch_year_end_mktcap(year)
    if not data:
        return jsonify({"error": f"{year} 연말 데이터 없음"}), 404
    with _year_end_lock:
        _year_end_mktcap[year] = data
    # 시총 상위 10종목
    top10 = sorted(data.items(), key=lambda x: x[1], reverse=True)[:10]
    return jsonify({
        "year": year,
        "total_stocks": len(data),
        "top10_mktcap": [{"code": c, "mktcap_억": round(m / 1e8, 0)} for c, m in top10],
        "sample_005930": data.get("005930"),   # 삼성전자
        "sample_000660": data.get("000660"),   # SK하이닉스
    })


# ═══════════════════════════════════════════════════════════════
#  DART 재무제표 원시 데이터 수집 → 지표 직접 계산
# ═══════════════════════════════════════════════════════════════

# 계정과목 매핑: key → (sj_div_filter, [account_name_aliases])
_ACCT = {
    "revenue":     (None, ["매출액","수익(매출액)","영업수익","매출"]),
    "op_income":   (None, ["영업이익","영업이익(손실)"]),
    "net_income":  (None, ["당기순이익","당기순이익(손실)","분기순이익","분기순이익(손실)"]),
    "interest_exp":(None, ["이자비용",
                            "금융비용",
                            "금융원가",
                            "이자비용(금융원가)",
                            "금융비용합계",
                            "이자 및 할인료",
                            "차입원가",
                            "금융비용(이자비용)",
                            "이자비용및기타금융원가"]),
    # CF에서 이자지급 fallback (손익계산서에 없을 경우 사용)
    "interest_paid":("CF", ["이자의지급","이자지급","이자의 지급",
                             "이자지급액","이자 지급","이자비용지급",
                             "이자및배당금의지급"]),
    "total_assets":(None, ["자산총계"]),
    "equity":      (None, ["자본총계"]),
    "total_liab":  (None, ["부채총계"]),
    "cur_assets":  (None, ["유동자산"]),
    "cur_liab":    (None, ["유동부채"]),
    "cash":        (None, ["현금및현금성자산","현금 및 현금성자산"]),
    "depreciation":("CF", ["감가상각비","유형자산감가상각비",
                            "감가상각비 및 상각비","감가상각비와무형자산상각비"]),
    "dividends":   ("CF", ["배당금지급","현금배당금의지급",
                            "배당금의지급","배당금의 지급"]),
}


def _parse_amt(s):
    if s is None:
        return None
    try:
        v = float(str(s).replace(",", "").replace(" ", ""))
        return v
    except Exception:
        return None


def fetch_dart_raw(stock_code):
    """DART fnlttSinglAcntAll → 재무제표 원시 항목 15년치 수집"""
    corp_code = get_corp_map().get(stock_code, "")
    print(f"[DART] {stock_code} → corp_code={corp_code or '없음'}")
    if not corp_code:
        return {}

    cur_year = datetime.today().year
    years    = list(range(cur_year - 15, cur_year))
    result   = {}
    lock     = threading.Lock()

    def fetch_year(year):
        for fs_div in ["CFS", "OFS"]:          # 연결 우선, 개별 fallback
            try:
                time.sleep(0.2)
                r = requests.get(f"{DART_BASE}/fnlttSinglAcntAll.json", params={
                    "crtfc_key":  DART_API_KEY,
                    "corp_code":  corp_code,
                    "bsns_year":  str(year),
                    "reprt_code": "11011",      # 사업보고서
                    "fs_div":     fs_div,
                }, timeout=15)
                js    = r.json()
                items = js.get("list", [])
                if js.get("status") != "000" or not items:
                    continue

                raw = {}
                for item in items:
                    sj  = item.get("sj_div", "")
                    nm  = (item.get("account_nm") or "").strip()
                    amt = _parse_amt(item.get("thstrm_amount"))
                    if amt is None:
                        continue
                    for key, (sj_filter, aliases) in _ACCT.items():
                        if key in raw:
                            continue
                        if sj_filter and sj != sj_filter:
                            continue
                        if nm in aliases:
                            raw[key] = amt

                if raw.get("total_assets") or raw.get("revenue"):
                    # 이자비용 계정 못 찾으면 IS/CIS에서 후보 출력
                    if not raw.get("interest_exp") and not raw.get("interest_paid"):
                        candidates = [
                            (item.get("sj_div",""), (item.get("account_nm") or "").strip())
                            for item in items
                            if any(k in (item.get("account_nm") or "")
                                   for k in ["이자","금융비용","금융원가","차입"])
                        ]
                        if candidates:
                            print(f"[DART] {stock_code} {year} 이자 관련 계정명: {candidates[:10]}")
                    print(f"[DART] {stock_code} {year}({fs_div}): {list(raw.keys())}")
                    with lock:
                        result[year] = raw
                    return
            except Exception as e:
                print(f"[DART] {stock_code} {year} 오류: {e}")

    with ThreadPoolExecutor(max_workers=4) as exe:
        list(exe.map(fetch_year, years))

    print(f"[DART] {stock_code}: {len(result)}년치 수집 완료")
    return {str(k): v for k, v in sorted(result.items())}


def compute_ratios(raw_fin, stock):
    """원시 재무데이터 + 현재 주가 + KRX 연말 시총 → 5개 섹션 지표 계산"""
    stock_code = stock.get("code", "")
    mktcap = stock.get("mktcap", 0)   # 현재 시총 (원, KRX 실시간)
    shares = stock.get("shares", 0)   # 발행주식수
    close  = stock.get("close",  0)   # 현재가 (원)

    years = sorted(raw_fin.keys())    # "2011", "2012", ...

    # ── 해당 종목만 연말 시총 병렬 prefetch (단일종목 수집) ──
    all_years_int = [int(yr) for yr in years]
    if all_years_int and stock_code:
        prefetch_ye_mktcap_for_stock(stock_code, all_years_int)

    def r2(v):
        return round(v, 2) if v is not None else None

    def _yr_scale(eq_raw, yr_int):
        """연도별 DART 금액 단위 추정: 그 해 연말 주가 기준 PBR 0.05~50 구간"""
        if not eq_raw or eq_raw <= 0 or shares <= 0:
            return 1_000_000
        # 연말시총 → 그 해 주가 추정 (없으면 현재 주가 fallback)
        ye_mc   = _ye_single.get((stock_code, yr_int), 0)
        yr_close = (ye_mc / shares) if ye_mc > 0 else close
        if yr_close <= 0:
            return 1_000_000
        for s in [1, 1_000, 1_000_000, 100_000_000]:
            bps = eq_raw * s / shares
            if bps > 0 and 0.05 <= (yr_close / bps) <= 50:
                return s
        return 1_000_000

    def _detect_div_scale(divs_raw, ye_mktcap):
        """배당금 단위 독립 추정 — 연말시총 기준 배당수익률 0.01%~30% 구간에 맞는 scale 반환.
        DART CF(현금흐름표) 배당금이 BS/IS와 다른 단위로 제공되는 경우 방지."""
        if not divs_raw or not ye_mktcap or ye_mktcap <= 0:
            return None   # 판단 불가 → 호출자가 fallback
        d = abs(divs_raw)
        for s in [1, 1_000, 1_000_000]:
            yld = d * s / ye_mktcap * 100
            if 0.01 <= yld <= 30:   # 0.01%~30% 사이면 합리적인 배당수익률
                return s
        return 1   # 어느 scale도 범위 밖이면 원 단위(=1)로 처리

    # 최신연도 scale (밸류에이션 지표용)
    latest_scale = 1_000_000
    if years:
        latest_scale = _yr_scale(raw_fin[years[-1]].get("equity"), int(years[-1]))
    print(f"[DART] latest_scale={latest_scale:,} (shares={shares:.2e}, close={close:,})")

    ratios = {}
    for i, yr in enumerate(years):
        d  = raw_fin[yr]
        rv = {}

        rev     = d.get("revenue")
        op_inc  = d.get("op_income")
        net_inc = d.get("net_income")
        assets  = d.get("total_assets")
        equity  = d.get("equity")
        liab    = d.get("total_liab")
        cur_a   = d.get("cur_assets")
        cur_l   = d.get("cur_liab")
        int_exp = d.get("interest_exp") or d.get("interest_paid")  # CF fallback
        cash    = d.get("cash")
        depre   = d.get("depreciation")
        divs    = d.get("dividends")

        # ── 수익성 ─────────────────────────────────────────
        if net_inc is not None and equity and equity != 0:
            rv["ROE"] = r2(net_inc / equity * 100)
        if net_inc is not None and assets and assets != 0:
            rv["ROA"] = r2(net_inc / assets * 100)
        if op_inc is not None and rev and rev != 0:
            rv["영업이익률"] = r2(op_inc / rev * 100)

        # ── 재무건전성 ──────────────────────────────────────
        if liab is not None and equity and equity != 0:
            rv["부채비율"] = r2(liab / equity * 100)
        if cur_a is not None and cur_l and cur_l != 0:
            rv["유동비율"] = r2(cur_a / cur_l * 100)
        if op_inc is not None and int_exp and int_exp != 0:
            # int_exp는 이자비용(부호: 양수가 비용)
            denom = abs(int_exp)
            rv["이자보상배율"] = r2(op_inc / denom)
            print(f"[ICR] {yr}: op_income={op_inc:,.0f} / interest={denom:,.0f} = {rv['이자보상배율']}")
        else:
            print(f"[ICR] {yr}: op_income={op_inc} int_exp={int_exp} → 이자보상배율 계산 불가")

        # ── 성장성 (전기 대비) ──────────────────────────────
        if i > 0:
            prev     = raw_fin[years[i - 1]]
            prev_rev = prev.get("revenue")
            prev_op  = prev.get("op_income")
            if rev is not None and prev_rev and abs(prev_rev) > 0:
                rv["매출액증가율"] = r2((rev - prev_rev) / abs(prev_rev) * 100)
            if op_inc is not None and prev_op is not None and abs(prev_op) > 0:
                rv["영업이익증가율"] = r2((op_inc - prev_op) / abs(prev_op) * 100)

        # ── 배당수익률 (연도별 scale + KRX 연말 시총) ────────
        yr_int    = int(yr)
        yr_scale  = _yr_scale(equity, yr_int)
        if divs:
            ye_mktcap = get_ye_mktcap_single(stock_code, yr_int)
            if ye_mktcap > 0:
                # 배당금은 CF 제공 단위가 BS와 다를 수 있으므로 독립 감지
                div_scale = _detect_div_scale(divs, ye_mktcap)
                if div_scale is None:
                    div_scale = yr_scale  # 판단 불가 시 equity scale 사용
                divs_won = abs(divs) * div_scale
                rv["배당수익률"] = r2(divs_won / ye_mktcap * 100)
                print(f"[DIV] {yr}: 배당={divs_won:,.0f}원(div_scale={div_scale:,}/eq_scale={yr_scale:,}) / 연말시총={ye_mktcap:,.0f}원 = {rv['배당수익률']}%")
            elif mktcap > 0 and yr == years[-1]:
                div_scale = yr_scale
                divs_won  = abs(divs) * div_scale
                rv["배당수익률"] = r2(divs_won / mktcap * 100)
                print(f"[DIV] {yr}: fallback 현재시총 scale={div_scale:,} = {rv['배당수익률']}%")

        # ── 밸류에이션 (최신연도, latest_scale 사용) ──────────
        if yr == years[-1] and mktcap > 0:
            if net_inc:
                net_w = net_inc * latest_scale
                if net_w > 0:
                    rv["PER"] = r2(mktcap / net_w)
            if equity:
                eq_w = equity * latest_scale
                if eq_w > 0:
                    rv["PBR"] = r2(mktcap / eq_w)
            if op_inc and depre is not None and cash is not None and liab is not None:
                ebitda   = (op_inc + abs(depre)) * latest_scale
                net_debt = max(0, (liab - cash)) * latest_scale
                ev       = mktcap + net_debt
                if ebitda > 0:
                    rv["EV/EBITDA"] = r2(ev / ebitda)

        ratios[yr] = rv

    return ratios


@app.route("/api/stock/<code>")
def api_stock_detail(code):
    """단일 종목 상세 — DART 재무제표 직접 계산"""
    raw    = get_all_stocks()
    stocks = normalize_stocks(raw)
    stock  = next((s for s in stocks if s["code"] == code), None)
    if not stock:
        return jsonify({"error": "종목 없음"}), 404

    raw_fin          = cached(f"dart_raw_{code}", lambda: fetch_dart_raw(code), ttl=86400)
    stock["financial"] = compute_ratios(raw_fin, stock)   # 매번 계산 (현재 주가 반영)
    return jsonify(stock)


def fetch_dart_fin(stock_code):  # 하위호환 stub — 사용 안 함
    """DART fnlttCmpnyIndx로 15년 재무지표 수집 (4카테고리 병렬)"""
    corp_code = get_corp_map().get(stock_code, "")
    print(f"[DART] {stock_code} → corp_code={corp_code or '없음'}")
    if not corp_code:
        return {}

    cur_year = datetime.today().year
    years = list(range(cur_year - 15, cur_year))
    # 수익성 / 안정성 / 성장성 / 시장가치
    cats  = ["M210000", "M220000", "M230000", "M250000"]

    result = {}
    lock   = threading.Lock()

    # reprt_code 우선순위: 사업보고서 → 3분기 → 반기
    REPRT_ORDER = ["11011", "11014", "11012"]

    def fetch_one(year, cl_code):
        for reprt_code in REPRT_ORDER:
            try:
                time.sleep(0.15)  # DART rate limit 방지
                r = requests.get(f"{DART_BASE}/fnlttCmpnyIndx.json", params={
                    "crtfc_key":   DART_API_KEY,
                    "corp_code":   corp_code,
                    "bsns_year":   str(year),
                    "reprt_code":  reprt_code,
                    "idx_cl_code": cl_code,
                }, timeout=12)
                js   = r.json()
                items = js.get("list", [])
                if js.get("status") == "000" and items:
                    with lock:
                        if year not in result:
                            result[year] = {}
                        for item in items:
                            nm  = item.get("idx_nm", "")
                            val = item.get("idx_val")
                            if nm and val not in (None, "", "-", None):
                                result[year][nm] = val
                    return  # 데이터 찾으면 다음 reprt_code 불필요
            except Exception:
                pass

    tasks = [(y, c) for y in years for c in cats]
    with ThreadPoolExecutor(max_workers=3) as exe:
        futs = [exe.submit(fetch_one, y, c) for y, c in tasks]
        for f in as_completed(futs):
            pass  # 결과는 result dict에 직접 저장

    # 연도를 문자열 key로 직렬화
    final = {str(k): v for k, v in sorted(result.items())}
    print(f"[DART] {stock_code}: {len(final)}년치 수집 완료 / 예시 years={list(final.keys())[:5]}")
    return final


# ═══════════════════════════════════════════════════════════════
#  모바일 HTML (TradingView 스타일)
# ═══════════════════════════════════════════════════════════════

HTML = r"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>KRX 스크리너</title>
<style>
:root{
  --bg:#f0f3fa; --bg2:#ffffff; --bg3:#e8ecf5;
  --border:#dde1ec; --text:#131722; --sub:#5d6278;
  --green:#089981; --red:#e8394a; --blue:#1a6fe8;
  --accent:#d4a017; --radius:8px; --nav:56px;
  --shadow:0 2px 8px rgba(0,0,0,.07);
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  font-size:14px;overflow-x:hidden;padding-bottom:var(--nav)}
/* ── 상단바 ── */
.topbar{position:sticky;top:0;z-index:100;background:var(--bg2);
  border-bottom:1px solid var(--border);padding:10px 14px;display:flex;
  align-items:center;gap:10px}
.logo{font-size:16px;font-weight:700;color:var(--accent);white-space:nowrap}
.search-wrap{flex:1;position:relative}
.search{width:100%;background:var(--bg3);border:1px solid var(--border);
  color:var(--text);border-radius:20px;padding:7px 14px 7px 34px;
  font-size:13px;outline:none}
.search::placeholder{color:var(--sub)}
.search-icon{position:absolute;left:11px;top:50%;transform:translateY(-50%);
  color:var(--sub);font-size:14px;pointer-events:none}
/* ── 필터 칩 ── */
.filter-bar{display:flex;gap:6px;padding:10px 14px;overflow-x:auto;
  scrollbar-width:none;border-bottom:1px solid var(--border)}
.filter-bar::-webkit-scrollbar{display:none}
.chip{background:var(--bg3);border:1px solid var(--border);color:var(--sub);
  border-radius:16px;padding:5px 12px;font-size:12px;white-space:nowrap;cursor:pointer;
  transition:all .2s}
.chip.active{background:var(--blue);border-color:var(--blue);color:#fff}
.excl-toggle{display:flex;align-items:center;gap:4px;background:var(--bg3);
  border:1px solid var(--border);border-radius:16px;padding:5px 10px;
  font-size:12px;color:var(--sub);cursor:pointer;white-space:nowrap;transition:all .2s;
  user-select:none;-webkit-user-select:none}
.excl-toggle.on{border-color:var(--accent);color:var(--accent)}
.excl-toggle .excl-box{width:14px;height:14px;border:1.5px solid currentColor;
  border-radius:3px;display:flex;align-items:center;justify-content:center;
  font-size:10px;line-height:1;flex-shrink:0}
/* ── sticky 탭 묶음 ── */
.sticky-tabs{position:sticky;top:56px;z-index:90;background:var(--bg2)}
/* ── 정렬 탭 ── */
.sort-bar{display:flex;gap:0;border-bottom:1px solid var(--border);
  overflow-x:auto;scrollbar-width:none}
.sort-bar::-webkit-scrollbar{display:none}
.sort-btn{flex:0 0 auto;padding:9px 14px;font-size:12px;color:var(--sub);
  cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap}
.sort-btn.active{color:var(--text);border-color:var(--blue)}
.sort-arrow{margin-left:3px;font-size:11px;opacity:.8}
/* ── 컬럼 헤더 ── */
.col-header{display:flex;align-items:center;padding:5px 14px;gap:10px;
  background:var(--bg2);border-bottom:1px solid var(--border)}
.col-h-left{flex:0 0 auto;width:120px;font-size:10px;color:var(--sub)}
.col-h-data{display:flex;flex:1;gap:0}
.col-h-cell{flex:1;text-align:right;font-size:10px;color:var(--sub);padding:0 4px}
/* ── 위로 가기 버튼 ── */
#scrollTopBtn{position:fixed;top:62px;right:10px;z-index:500;
  width:32px;height:32px;border-radius:50%;border:none;cursor:pointer;
  background:rgba(26,111,232,.55);backdrop-filter:blur(6px);
  -webkit-backdrop-filter:blur(6px);
  color:#fff;font-size:14px;line-height:32px;text-align:center;
  box-shadow:0 2px 8px rgba(0,0,0,.2);
  opacity:0;pointer-events:none;transition:opacity .3s}
#scrollTopBtn.show{opacity:1;pointer-events:auto}
/* ── 종목 카드 ── */
.stock-list{padding:0 0 8px}
.stock-card{display:flex;align-items:center;padding:11px 14px;
  border-bottom:1px solid var(--border);cursor:pointer;transition:background .15s;gap:10px}
.stock-card:active{background:var(--bg3)}
.stock-left{flex:0 0 auto;min-width:0;width:120px}
.stock-name{font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.3}
.stock-code{font-size:10px;color:var(--sub);margin-top:3px;white-space:nowrap}
/* 4열 데이터 */
.stock-data{display:flex;flex:1;gap:0;align-items:center}
.data-col{flex:1;text-align:right;padding:0 5px}
.data-val{font-size:14px;font-weight:700;white-space:nowrap;line-height:1}
.data-val.sec{font-size:12px;font-weight:500;color:var(--sub)}
.up{color:var(--red)} .dn{color:var(--green)} .nc{color:var(--text)}
/* ── 마켓 배지 ── */
.badge{display:inline-block;padding:2px 6px;border-radius:4px;font-size:10px;
  font-weight:600;margin-left:5px;vertical-align:middle}
.badge-K{background:#deeaff;color:#1a6fe8}
.badge-Q{background:#d5f5ef;color:#089981}
/* ── 스크리너 패널 ── */
.panel-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:200}
.panel-overlay.open{display:block}
.panel{position:fixed;bottom:0;left:0;right:0;background:var(--bg2);
  border-radius:16px 16px 0 0;max-height:80vh;overflow-y:auto;z-index:201;
  transform:translateY(100%);transition:transform .3s ease}
.panel.open{transform:translateY(0)}
.panel-handle{width:40px;height:4px;background:var(--border);border-radius:2px;
  margin:12px auto 16px}
.panel-title{font-size:16px;font-weight:700;padding:0 18px 14px;border-bottom:1px solid var(--border)}
.filter-section{padding:16px 18px;border-bottom:1px solid var(--border)}
.filter-label{font-size:12px;color:var(--sub);margin-bottom:8px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px}
.filter-row{display:flex;gap:8px;align-items:center}
.filter-input{background:var(--bg3);border:1px solid var(--border);color:var(--text);
  border-radius:6px;padding:8px 10px;font-size:13px;flex:1;outline:none;width:100%}
.filter-input:focus{border-color:var(--blue)}
.range-sep{color:var(--sub);font-size:12px}
.apply-btn{width:calc(100% - 36px);margin:16px 18px;background:var(--blue);color:#fff;
  border:none;border-radius:8px;padding:13px;font-size:15px;font-weight:700;cursor:pointer}
/* ── 하단 네비 ── */
.nav{position:fixed;bottom:0;left:0;right:0;height:var(--nav);background:var(--bg2);
  border-top:1px solid var(--border);display:flex;z-index:100}
.nav-item{flex:1;display:flex;flex-direction:column;align-items:center;
  justify-content:center;cursor:pointer;color:var(--sub);transition:color .2s;gap:3px}
.nav-item.active{color:var(--blue)}
.nav-icon{font-size:20px}
.nav-label{font-size:10px;font-weight:500}
/* ── 상세 페이지 ── */
.page{display:none;height:calc(100vh - var(--nav));overflow-y:auto}
.page.active{display:block}
/* ── 종목 상세 슬라이드업 ── */
.detail-overlay{display:none;position:fixed;inset:0;background:var(--bg);z-index:300;
  overflow-y:auto;transform:translateY(100%);transition:transform .3s ease}
.detail-overlay.open{display:block;transform:translateY(0)}
.detail-header{position:sticky;top:0;background:var(--bg2);border-bottom:1px solid var(--border);
  padding:12px 14px;display:flex;align-items:center;gap:12px;z-index:1}
.back-btn{font-size:22px;cursor:pointer;color:var(--text);background:none;border:none;
  line-height:1;padding:0 4px}
.detail-body{padding:16px}
.detail-price-row{display:flex;justify-content:space-between;align-items:flex-end;
  padding:16px 0 20px;border-bottom:1px solid var(--border)}
.detail-price{font-size:32px;font-weight:800}
/* ── 재무 섹션 카드 ── */
.fin-section-card{background:var(--bg2);border-radius:14px;padding:16px;margin-top:12px;box-shadow:var(--shadow);border:1px solid var(--border)}
.fin-section-hd{display:flex;align-items:center;gap:8px;margin-bottom:12px}
.fin-section-bar{width:3px;height:16px;border-radius:2px;flex-shrink:0}
.fin-section-title{font-size:14px;font-weight:700}
.fin-metrics-row{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.fin-metric-chip{flex:1;min-width:68px;text-align:center;background:var(--bg);border:1px solid var(--border);border-radius:10px;padding:10px 5px;transition:box-shadow .15s}
.fin-metric-lbl{font-size:10px;color:var(--sub);margin-bottom:3px;white-space:nowrap}
.fin-metric-formula{font-size:8.5px;color:var(--sub);margin-top:3px;opacity:.7;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%}
.fin-metric-val{font-size:16px;font-weight:800;white-space:nowrap;color:var(--text)}
.fin-chart-wrap{position:relative;height:165px;margin-top:4px}
.fin-val-row .fin-metric-chip{padding:10px 5px}
.fin-val-row .fin-metric-val{font-size:20px}
.fin-val-row .fin-metric-lbl{font-size:11px;margin-bottom:5px}
/* ── 로딩 ── */
.loading{display:flex;align-items:center;justify-content:center;
  height:200px;flex-direction:column;gap:12px;color:var(--sub)}
.spinner{width:36px;height:36px;border:3px solid var(--border);
  border-top-color:var(--blue);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
/* ── 빈 상태 ── */
.empty{text-align:center;padding:60px 20px;color:var(--sub)}
.empty-icon{font-size:48px;margin-bottom:12px}
/* ── 스크롤 인디케이터 ── */
.more-btn{text-align:center;padding:16px;color:var(--blue);cursor:pointer;font-size:13px}
</style>
</head>
<body>

<!-- ── 상단바 ── -->
<div class="topbar">
  <div class="logo">📈 KRX</div>
  <div class="search-wrap">
    <span class="search-icon">🔍</span>
    <input class="search" type="search" placeholder="종목명 또는 코드 검색..." id="searchInput">
  </div>
  <div style="display:flex;flex-direction:column;align-items:flex-end;gap:2px;flex-shrink:0">
    <span id="asOfDate" style="font-size:10px;color:var(--sub);white-space:nowrap"></span>
    <span style="font-size:22px;cursor:pointer;color:var(--sub);line-height:1" onclick="openPanel()">⚙️</span>
  </div>
</div>

<!-- ── 마켓 필터 칩 ── -->
<div class="sticky-tabs">
<div class="filter-bar">
  <div class="chip active" data-market="ALL">전체</div>
  <div class="chip" data-market="KOSPI">KOSPI</div>
  <div class="chip" data-market="KOSDAQ">KOSDAQ</div>
  <div class="excl-toggle" id="exclToggle">
    <div class="excl-box" id="exclBox"></div>
    <span>우선·스팩 제외</span>
  </div>
</div>

<!-- ── 정렬 탭 ── -->
<div class="sort-bar">
  <div class="sort-btn active" data-sort="mktcap" data-dir="desc">시가총액<span class="sort-arrow">↑</span></div>
  <div class="sort-btn" data-sort="volume" data-dir="desc">거래량<span class="sort-arrow"></span></div>
  <div class="sort-btn" data-sort="chg_rt" data-dir="desc">등락률<span class="sort-arrow"></span></div>
  <div class="sort-btn" data-sort="close" data-dir="desc">현재가<span class="sort-arrow"></span></div>
</div>

<!-- ── 컬럼 헤더 ── -->
<div class="col-header">
  <div class="col-h-left">종목명</div>
  <div class="col-h-data">
    <div class="col-h-cell">현재가</div>
    <div class="col-h-cell">등락률</div>
    <div class="col-h-cell">거래량</div>
    <div class="col-h-cell">시가총액</div>
  </div>
</div>
</div> <!-- /sticky-tabs -->

<!-- ── 종목 리스트 ── -->
<div id="stockList" class="stock-list">
  <div class="loading"><div class="spinner"></div><span>종목 데이터 불러오는 중...</span></div>
</div>

<!-- ── 무한스크롤 sentinel은 JS에서 동적 삽입 ── -->

<!-- ── 스크리너 패널 ── -->
<div class="panel-overlay" id="panelOverlay" onclick="closePanel()"></div>
<div class="panel" id="panel">
  <div class="panel-handle"></div>
  <div class="panel-title">🔧 스크리너 설정</div>

  <div class="filter-section">
    <div class="filter-label">시가총액 (억원)</div>
    <div class="filter-row">
      <input class="filter-input" type="number" id="capMin" placeholder="최소">
      <span class="range-sep">~</span>
      <input class="filter-input" type="number" id="capMax" placeholder="최대">
    </div>
  </div>

  <div class="filter-section">
    <div class="filter-label">등락률 (%)</div>
    <div class="filter-row">
      <input class="filter-input" type="number" id="chgMin" placeholder="최소" value="-99">
      <span class="range-sep">~</span>
      <input class="filter-input" type="number" id="chgMax" placeholder="최대" value="99">
    </div>
  </div>

  <div class="filter-section">
    <div class="filter-label">최소 거래량 (주)</div>
    <input class="filter-input" type="number" id="volMin" placeholder="예: 100000">
  </div>

  <button class="apply-btn" onclick="applyFilter()">✅ 적용</button>
</div>

<!-- ── 종목 상세 ── -->
<button id="scrollTopBtn" onclick="(document.querySelector('.page')||document.documentElement).scrollTo({top:0,behavior:'smooth'})">▲</button>

<div class="detail-overlay" id="detailOverlay">
  <div class="detail-header">
    <button class="back-btn" onclick="closeDetail()">‹</button>
    <div>
      <div id="detailName" style="font-weight:700;font-size:16px"></div>
      <div id="detailCode" style="font-size:11px;color:var(--sub)"></div>
    </div>
    <div id="detailBadge" style="margin-left:auto"></div>
  </div>
  <div class="detail-body">
    <div class="detail-price-row">
      <div>
        <div id="detailPrice" class="detail-price"></div>
        <div id="detailChg" style="font-size:14px;margin-top:4px"></div>
      </div>
      <div style="text-align:right;color:var(--sub);font-size:12px">
        <div id="detailMktcap"></div>
        <div id="detailVol" style="margin-top:4px"></div>
      </div>
    </div>

    <div id="detailFin"></div>
  </div>
</div>

<script>
// ── 상태 ───────────────────────────────────────────────
let allStocks = [];
let filtered  = [];
let page      = 0;
const PAGE_SZ = 50;

let state = {
  market:  "ALL",
  sort:    "mktcap",
  sortDir: "desc",   // "desc" | "asc"
  q:       "",
  capMin:  0,    capMax:  9e15,  // 원 단위 (1조 = 1e12원)
  chgMin: -99,   chgMax:  99,
  volMin:  0,
  excl:    false,    // 우선주·스팩 제외
};

// ── 숫자 포맷 ──────────────────────────────────────────
const fmt = (n, d=0) => n==null ? "-" : Number(n).toLocaleString("ko-KR",{maximumFractionDigits:d});
const fmtCap = n => {
  // KRX MKTCAP 단위: 원 (1조 = 1e12원, 1억 = 1e8원)
  if (!n) return "-";
  if (n >= 1e12) return (n/1e12).toFixed(1) + "조";
  if (n >= 1e8)  return (n/1e8).toFixed(0)  + "억";
  return fmt(n);
};
const fmtVol = n => {
  if (!n) return "-";
  if (n >= 1e8) return (n/1e8).toFixed(1)+"억주";
  if (n >= 1e4) return (n/1e4).toFixed(0)+"만주";
  return fmt(n)+"주";
};

// ── 데이터 로드 ────────────────────────────────────────
async function loadStocks() {
  try {
    const res  = await fetch("/api/stocks");
    const json = await res.json();
    allStocks  = json.data || [];
    if (json.date) {
      const el = document.getElementById("asOfDate");
      if (el) el.textContent = json.date + " 기준";
    }
    applyFilter();
  } catch(e) {
    document.getElementById("stockList").innerHTML =
      '<div class="empty"><div class="empty-icon">⚠️</div><div>데이터 로드 실패<br><small>서버 연결을 확인하세요</small></div></div>';
  }
}

// ── 필터 & 정렬 ────────────────────────────────────────
function applyFilter() {
  // 입력 단위: 억원 → 내부 단위: 원 (1억 = 1e8원)
  state.capMin = parseFloat(document.getElementById("capMin").value||0) * 1e8;
  state.capMax = parseFloat(document.getElementById("capMax").value||9e7) * 1e8 || 9e15; // 기본: 9천조원(무제한)
  state.chgMin = parseFloat(document.getElementById("chgMin").value||-99);
  state.chgMax = parseFloat(document.getElementById("chgMax").value||99);
  state.volMin = parseFloat(document.getElementById("volMin").value||0);
  closePanel();
  renderList();
}

function getFiltered() {
  // 한글 NFC 정규화 (모바일 IME NFD 입력 대응)
  const q = state.q ? state.q.normalize("NFC") : "";
  let list = allStocks.filter(s => {
    const nm = (s.name || "").normalize("NFC");
    const nameMatch = !q || nm.includes(q) || nm.toLowerCase().includes(q.toLowerCase()) || (s.code||"").includes(q);
    const mktMatch  = (state.market === "ALL" || s.market === state.market);
    const capOk     = (s.mktcap || 0) >= state.capMin && (s.mktcap || 0) <= state.capMax;
    const chgOk     = (s.chg_rt ?? 0) >= state.chgMin && (s.chg_rt ?? 0) <= state.chgMax;
    const volOk     = (s.volume || 0) >= state.volMin;
    // 우선주: 종목코드 끝 1자리가 홀수(5,7 등), 또는 종목명 끝 "우"/"우B"/"우C"
    // 스팩주: 종목명에 "스팩" 포함
    const isPreferred = /우[A-Z]?$/.test(nm) || /[13579]$/.test(s.code || "");
    const isSpac      = nm.includes("스팩") || nm.toUpperCase().includes("SPAC");
    const exclOk     = !state.excl || (!isPreferred && !isSpac);
    return nameMatch && mktMatch && capOk && chgOk && volOk && exclOk;
  });

  const key = state.sort;
  const dir = state.sortDir === "asc" ? 1 : -1;
  list.sort((a, b) => ((a[key] ?? 0) - (b[key] ?? 0)) * dir);
  return list;
}

// ── 무한스크롤 sentinel ────────────────────────────────
let observer = null;

function renderList(reset=true) {
  if (reset) { filtered = getFiltered(); page = 0; }
  const start = page * PAGE_SZ;
  const slice = filtered.slice(start, start + PAGE_SZ);
  const total = filtered.length;

  const el = document.getElementById("stockList");
  // sentinel 제거 후 재추가
  const old = document.getElementById("sentinel");
  if (old) old.remove();
  if (observer) { observer.disconnect(); observer = null; }

  if (reset) el.innerHTML = "";

  if (filtered.length === 0) {
    el.innerHTML = '<div class="empty"><div class="empty-icon">🔍</div><div>조건에 맞는 종목이 없습니다</div></div>';
    return;
  }

  slice.forEach(s => {
    const cls  = s.chg_rt > 0 ? "up" : s.chg_rt < 0 ? "dn" : "nc";
    const sign = s.chg_rt > 0 ? "+" : "";
    const badge = s.market === "KOSPI"
      ? '<span class="badge badge-K">K</span>'
      : '<span class="badge badge-Q">Q</span>';

    const div = document.createElement("div");
    div.className = "stock-card";
    div.innerHTML = `
      <div class="stock-left">
        <div class="stock-name">${s.name}${badge}</div>
        <div class="stock-code">${s.code} · ${s.market}</div>
      </div>
      <div class="stock-data">
        <div class="data-col"><div class="data-val ${cls}">${fmt(s.close)}</div></div>
        <div class="data-col"><div class="data-val ${cls}">${sign}${fmt(s.chg_rt,2)}%</div></div>
        <div class="data-col"><div class="data-val sec">${fmtVol(s.volume)}</div></div>
        <div class="data-col"><div class="data-val sec">${fmtCap(s.mktcap)}</div></div>
      </div>`;
    div.onclick = () => openDetail(s);
    el.appendChild(div);
  });

  page++;

  if (reset) {
    const info = document.createElement("div");
    info.style.cssText = "padding:8px 14px;font-size:11px;color:var(--sub)";
    info.textContent = `총 ${fmt(total)}개 종목`;
    el.prepend(info);
  }

  // 다음 페이지 있으면 sentinel 달기
  if (page * PAGE_SZ < total) {
    const sentinel = document.createElement("div");
    sentinel.id = "sentinel";
    sentinel.style.cssText = "height:1px;margin-bottom:70px";
    el.appendChild(sentinel);
    observer = new IntersectionObserver(entries => {
      if (entries[0].isIntersecting) renderList(false);
    }, { threshold: 0.1 });
    observer.observe(sentinel);
  }
}

// ── 검색 ──────────────────────────────────────────────
let searchTimer;
let composing = false;
const searchEl = document.getElementById("searchInput");
searchEl.addEventListener("compositionstart", () => { composing = true; });
searchEl.addEventListener("compositionend",   e => {
  composing = false;
  state.q = e.target.value.trim();
  renderList();
});
searchEl.addEventListener("input", e => {
  if (composing) return;   // 한글 조합 중에는 무시
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    state.q = e.target.value.trim();
    renderList();
  }, 300);
});

// ── 마켓 칩 ──────────────────────────────────────────
document.querySelectorAll(".chip[data-market]").forEach(el => {
  el.addEventListener("click", () => {
    document.querySelectorAll(".chip[data-market]").forEach(c=>c.classList.remove("active"));
    el.classList.add("active");
    state.market = el.dataset.market;
    renderList();
  });
});

// ── 우선·스팩 제외 토글 ──────────────────────────────
document.getElementById("exclToggle").addEventListener("click", () => {
  state.excl = !state.excl;
  const tog = document.getElementById("exclToggle");
  const box = document.getElementById("exclBox");
  if (state.excl) {
    tog.classList.add("on");
    box.textContent = "✓";
  } else {
    tog.classList.remove("on");
    box.textContent = "";
  }
  renderList();
});

// ── 정렬 탭 (클릭 시 토글, 화살표 표시) ────────────────
document.querySelectorAll(".sort-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    const key = btn.dataset.sort;
    if (state.sort === key) {
      // 같은 탭 재클릭 → 방향 토글
      state.sortDir = state.sortDir === "desc" ? "asc" : "desc";
    } else {
      // 다른 탭 → 내림차순으로 초기화
      state.sort    = key;
      state.sortDir = "desc";
      document.querySelectorAll(".sort-btn").forEach(b => {
        b.classList.remove("active");
        b.querySelector(".sort-arrow").textContent = "";
      });
      btn.classList.add("active");
    }
    btn.querySelector(".sort-arrow").textContent = state.sortDir === "desc" ? "↑" : "↓";
    renderList();
  });
});

// ── 패널 ─────────────────────────────────────────────
function openPanel()  {
  document.getElementById("panelOverlay").classList.add("open");
  document.getElementById("panel").classList.add("open");
}
function closePanel() {
  document.getElementById("panelOverlay").classList.remove("open");
  document.getElementById("panel").classList.remove("open");
}

// ── 종목 상세 ─────────────────────────────────────────
async function openDetail(s) {
  const ov = document.getElementById("detailOverlay");
  document.getElementById("detailName").textContent  = s.name;
  document.getElementById("detailCode").textContent  = s.code + " · " + s.market;
  document.getElementById("detailBadge").innerHTML   =
    s.market === "KOSPI"
      ? '<span class="badge badge-K">KOSPI</span>'
      : '<span class="badge badge-Q">KOSDAQ</span>';

  const cls  = s.chg_rt > 0 ? "up" : s.chg_rt < 0 ? "dn" : "nc";
  const sign = s.chg_rt > 0 ? "+" : "";
  document.getElementById("detailPrice").className   = `detail-price ${cls}`;
  document.getElementById("detailPrice").textContent = fmt(s.close) + "원";
  document.getElementById("detailChg").className     = cls;
  document.getElementById("detailChg").textContent   =
    `${sign}${fmt(s.chg)}원 (${sign}${fmt(s.chg_rt,2)}%)`;
  document.getElementById("detailMktcap").textContent= "시총 " + fmtCap(s.mktcap);
  document.getElementById("detailVol").textContent   = "거래 " + fmtVol(s.volume);

  document.getElementById("detailFin").innerHTML =
    '<div class="loading"><div class="spinner"></div><span>재무 데이터 로딩...</span></div>';

  ov.classList.add("open");
  document.body.style.overflow = "hidden";

  try {
    const res = await fetch(`/api/stock/${s.code}`);
    const data = await res.json();
    renderFinancial(data.financial || {});
  } catch(e) {
    document.getElementById("detailFin").innerHTML =
      '<div style="color:var(--sub);text-align:center;padding:20px;font-size:13px">재무 데이터를 불러올 수 없습니다</div>';
  }
}

// ── Chart.js 관리 ──────────────────────────────────────
let _charts = [];
function _destroyCharts() { _charts.forEach(c => { try { c.destroy(); } catch(e){} }); _charts = []; }

function _getVal(fin, year, keys) {
  const d = fin[year] || fin[String(year)] || {};
  for (const k of keys) {
    const v = d[k];
    if (v != null && v !== "" && v !== "-") {
      const n = parseFloat(String(v).replace(/,/g, ""));
      if (!isNaN(n)) return n;
    }
  }
  return null;
}

function renderFinancial(fin) {
  _destroyCharts();
  const el = document.getElementById("detailFin");

  if (!fin || typeof fin !== "object" || Object.keys(fin).length === 0) {
    el.innerHTML = '<div style="color:var(--sub);text-align:center;padding:30px;font-size:13px">재무 데이터 없음 (DART 미등록)</div>';
    return;
  }

  const years = Object.keys(fin).sort();  // "2010", "2011", ...
  const labels = years.map(y => `'${y.slice(2)}`);

  // ── 섹션 정의 ──────────────────────────────────────────
  // Python compute_ratios 가 생성하는 key와 1:1 대응 (별칭 불필요)
  const SECTIONS = [
    {
      id: "val", title: "밸류에이션", color: "#2962ff",
      type: "bar", noHistChart: true,
      metrics: [
        { lbl: "PER",       keys: ["PER"],       unit: "배", formula: "시가총액 ÷ 당기순이익" },
        { lbl: "PBR",       keys: ["PBR"],       unit: "배", formula: "시가총액 ÷ 자기자본" },
        { lbl: "EV/EBITDA", keys: ["EV/EBITDA"], unit: "배", formula: "(시총+순차입금) ÷ (영업이익+감가상각)" },
      ]
    },
    {
      id: "prof", title: "수익성", color: "#26a69a", type: "line",
      metrics: [
        { lbl: "ROE",     keys: ["ROE"],     unit: "%", formula: "당기순이익 ÷ 자기자본 × 100" },
        { lbl: "ROA",     keys: ["ROA"],     unit: "%", formula: "당기순이익 ÷ 총자산 × 100" },
        { lbl: "영업이익률", keys: ["영업이익률"], unit: "%", formula: "영업이익 ÷ 매출액 × 100" },
      ]
    },
    {
      id: "growth", title: "성장성", color: "#f0b90b", type: "bar",
      metrics: [
        { lbl: "매출 성장률",    keys: ["매출액증가율"],   unit: "%", formula: "(당기매출 - 전기매출) ÷ |전기매출| × 100" },
        { lbl: "영업이익 성장률", keys: ["영업이익증가율"], unit: "%", formula: "(당기영업이익 - 전기) ÷ |전기| × 100" },
      ]
    },
    {
      id: "stable", title: "재무건전성", color: "#ef5350", type: "line",
      metrics: [
        { lbl: "부채비율",   keys: ["부채비율"],    unit: "%",  formula: "총부채 ÷ 자기자본 × 100" },
        { lbl: "유동비율",   keys: ["유동비율"],    unit: "%",  formula: "유동자산 ÷ 유동부채 × 100" },
        { lbl: "이자보상배율", keys: ["이자보상배율"], unit: "배", formula: "영업이익 ÷ 이자비용" },
      ]
    },
    {
      id: "mkt", title: "시장 지위", color: "#ab47bc", type: "bar",
      metrics: [
        { lbl: "배당수익률", keys: ["배당수익률"], unit: "%", formula: "배당금지급 ÷ 연말시가총액 × 100" },
      ]
    },
  ];

  const PALETTE = ["#1a6fe8","#089981","#e8394a","#d4a017","#7c4dff","#f57c00"];

  // HTML 생성
  let html = "";
  SECTIONS.forEach(sec => {
    const chartEl = sec.noHistChart
      ? ''
      : `<div class="fin-chart-wrap"><canvas id="cv_${sec.id}"></canvas></div>`;
    html += `
    <div class="fin-section-card">
      <div class="fin-section-hd">
        <div class="fin-section-bar" style="background:${sec.color}"></div>
        <span class="fin-section-title">${sec.title}</span>
      </div>
      <div class="fin-metrics-row${sec.noHistChart ? ' fin-val-row' : ''}" id="mr_${sec.id}"></div>
      ${chartEl}
    </div>`;
  });
  el.innerHTML = html;

  // Chart.js 로드 후 렌더
  function doRender() {
    SECTIONS.forEach(sec => {
      const latestYr = years[years.length - 1];
      const mrEl = document.getElementById(`mr_${sec.id}`);
      if (mrEl) {
        sec.metrics.forEach((m, mi) => {
          const v    = _getVal(fin, latestYr, m.keys);
          const disp = v != null ? `${v.toFixed(1)}${m.unit}` : "-";
          // noHistChart 섹션은 색상 강조 칩
          const col  = sec.noHistChart ? PALETTE[mi % PALETTE.length] : "";
          const style = sec.noHistChart && v != null
            ? `color:${col};font-size:18px;font-weight:800`
            : "";
          let trend = "";
          if (!sec.noHistChart && years.length >= 2) {
            const prev = _getVal(fin, years[years.length - 2], m.keys);
            if (v != null && prev != null) {
              trend = v > prev
                ? `<span style="font-size:10px;color:var(--green);margin-left:2px">▲</span>`
                : v < prev
                  ? `<span style="font-size:10px;color:var(--red);margin-left:2px">▼</span>`
                  : "";
            }
          }
          mrEl.innerHTML += `
            <div class="fin-metric-chip" title="${m.formula || ''}">
              <div class="fin-metric-lbl">${m.lbl}</div>
              <div class="fin-metric-val" style="${style}">${disp}${trend}</div>
              ${m.formula ? `<div class="fin-metric-formula">${m.formula}</div>` : ''}
            </div>`;
        });
      }

      // noHistChart 섹션은 차트 없이 종료
      if (sec.noHistChart) return;

      // 차트 데이터셋
      const datasets = sec.metrics.map((m, mi) => {
        const data = years.map(y => _getVal(fin, y, m.keys));
        const col  = PALETTE[mi % PALETTE.length];
        return {
          label: m.lbl,
          data,
          borderColor: col,
          backgroundColor: sec.type === "bar" ? col + "cc" : col + "22",
          borderRadius: sec.type === "bar" ? 4 : 0,
          borderWidth: sec.type === "bar" ? 0 : 2.5,
          pointRadius: sec.type === "line" ? 3 : 0,
          pointHoverRadius: 5,
          tension: 0.35,
          spanGaps: true,
          fill: sec.type === "line" && sec.metrics.length === 1,
        };
      });

      const canvas = document.getElementById(`cv_${sec.id}`);
      if (!canvas) return;

      const chart = new Chart(canvas, {
        type: sec.type,
        data: { labels, datasets },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: {
              display: sec.metrics.length > 1,
              labels: { color: "#5d6278", font: { size: 10 }, usePointStyle: true, boxWidth: 8, padding: 10 }
            },
            tooltip: {
              backgroundColor: "#ffffff",
              borderColor: "#dde1ec",
              borderWidth: 1,
              titleColor: "#131722",
              bodyColor: "#5d6278",
              callbacks: {
                label: ctx => {
                  const v = ctx.raw;
                  const unit = sec.metrics[ctx.datasetIndex]?.unit || "";
                  return ` ${ctx.dataset.label}: ${v != null ? v.toFixed(1) + unit : "-"}`;
                }
              }
            }
          },
          scales: {
            x: {
              ticks: { color: "#5d6278", font: { size: 9 }, maxTicksLimit: 8 },
              grid:  { color: "#e8ecf5" },
            },
            y: {
              ticks: { color: "#5d6278", font: { size: 9 }, maxTicksLimit: 5 },
              grid:  { color: "#e8ecf5" },
            }
          }
        }
      });
      _charts.push(chart);
    });
  }

  if (window.Chart) {
    doRender();
  } else {
    const s = document.createElement("script");
    s.src = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js";
    s.onload = doRender;
    document.head.appendChild(s);
  }
}

function closeDetail() {
  _destroyCharts();
  document.getElementById("detailOverlay").classList.remove("open");
  document.body.style.overflow = "";
}

// 뒤로가기 제스처
window.addEventListener("popstate", closeDetail);

// ── 위로 가기 버튼 ──
const _stBtn = document.getElementById('scrollTopBtn');
// .page div 내부 스크롤 감지 (window가 아님)
const _pageEl = document.querySelector('.page') || document.documentElement;
_pageEl.addEventListener('scroll', () => {
  if (_pageEl.scrollTop > 300) _stBtn.classList.add('show');
  else _stBtn.classList.remove('show');
}, {passive:true});

// ── 초기 로드 ─────────────────────────────────────────
loadStocks();

// 새로고침 (pull-to-refresh 시뮬레이션)
let startY = 0;
document.addEventListener("touchstart", e => { startY = e.touches[0].clientY; });
document.addEventListener("touchend",   e => {
  if (e.changedTouches[0].clientY - startY > 120 && window.scrollY === 0) {
    document.getElementById("stockList").innerHTML =
      '<div class="loading"><div class="spinner"></div><span>새로고침 중...</span></div>';
    _cache_bust = Date.now();
    loadStocks();
  }
});
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


# ═══════════════════════════════════════════════════════════════
#  서버 실행
# ═══════════════════════════════════════════════════════════════

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if __name__ == "__main__":
    missing = [p for p in ("flask", "requests", "pandas", "openpyxl")
               if not __import__("importlib").util.find_spec(p)]
    if missing:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        print("패키지 설치 완료. 다시 실행해 주세요.")
        sys.exit(0)

    ip   = get_local_ip()
    port = 5000

    print()
    print("=" * 55)
    print("  📱 KRX 주식 스크리너 - 모바일 웹앱")
    print("=" * 55)
    print(f"  PC 접속   : http://localhost:{port}")
    print(f"  모바일 접속: http://{ip}:{port}")
    print(f"  (같은 Wi-Fi에 연결된 스마트폰에서 접속)")
    print("=" * 55)
    print("  종료: Ctrl+C")
    print()

    app.run(host="0.0.0.0", port=port, debug=False)
