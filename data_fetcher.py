"""
経済指標データ取得モジュール
- 市場データ: yfinance
- CPI: FRED API (無料キー必要 → https://fred.stlouisfed.org/docs/api/api_key.html)
  環境変数 FRED_API_KEY に設定、またはこのファイルの FRED_API_KEY = "..." に直接入力
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# ====================================================
# FRED APIキー設定（CPIデータに必要）
# ====================================================
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")  # ← ここに直接入れてもOK

# ====================================================
# 指標定義
# ====================================================
INDICATORS = {
    "為替": {
        "USD/JPY": {"ticker": "USDJPY=X",  "unit": "円",    "decimals": 3, "data_from": "2003年〜"},
        "EUR/USD": {"ticker": "EURUSD=X",  "unit": "USD",   "decimals": 4, "data_from": "2003年〜"},
        "EUR/JPY": {"ticker": "EURJPY=X",  "unit": "円",    "decimals": 3, "data_from": "2003年〜"},
        "GBP/JPY": {"ticker": "GBPJPY=X",  "unit": "円",    "decimals": 3, "data_from": "2003年〜"},
        "AUD/JPY": {"ticker": "AUDJPY=X",  "unit": "円",    "decimals": 3, "data_from": "2003年〜"},
        "CHF/JPY": {"ticker": "CHFJPY=X",  "unit": "円",    "decimals": 3, "data_from": "2003年〜"},
    },
    "ドル指数": {
        "DXY (ドル指数)": {"ticker": "DX-Y.NYB", "unit": "pt", "decimals": 3, "data_from": "1971年〜"},
    },
    "米国株価指数": {
        "ダウ平均":      {"ticker": "^DJI",  "unit": "pt", "decimals": 2, "data_from": "1985年〜"},
        "S&P500":       {"ticker": "^GSPC", "unit": "pt", "decimals": 2, "data_from": "1927年〜"},
        "ナスダック100": {"ticker": "^NDX",  "unit": "pt", "decimals": 2, "data_from": "1985年〜"},
        "ラッセル2000":  {"ticker": "^RUT",  "unit": "pt", "decimals": 2, "data_from": "1987年〜"},
    },
    "日本株価指数": {
        "日経225": {"ticker": "^N225", "unit": "円", "decimals": 2, "data_from": "1965年〜"},
    },
    "米国金利": {
        "米30年債利回り":     {"ticker": "^TYX", "unit": "%", "decimals": 3, "data_from": "1977年〜"},
        "米10年債利回り":     {"ticker": "^TNX", "unit": "%", "decimals": 3, "data_from": "1962年〜"},
        "米5年債利回り":      {"ticker": "^FVX", "unit": "%", "decimals": 3, "data_from": "1962年〜"},
        "米短期金利(3ヶ月)": {"ticker": "^IRX", "unit": "%", "decimals": 3, "data_from": "1982年〜"},
    },
    "商品": {
        "金 先物":   {"ticker": "GC=F", "unit": "USD/oz",    "decimals": 2, "data_from": "※直近数ヶ月のみ"},
        "銀 先物":   {"ticker": "SI=F", "unit": "USD/oz",    "decimals": 3, "data_from": "※直近数ヶ月のみ"},
        "原油 先物": {"ticker": "CL=F", "unit": "USD/bbl",   "decimals": 2, "data_from": "※直近数ヶ月のみ"},
        "天然ガス":  {"ticker": "NG=F", "unit": "USD/MMBtu", "decimals": 3, "data_from": "※直近数ヶ月のみ"},
    },
    "恐怖指数": {
        "VIX": {"ticker": "^VIX", "unit": "pt", "decimals": 2, "data_from": "1990年〜"},
    },
}

# ティッカー逆引きマップ
TICKER_MAP: dict[str, str] = {}
UNIT_MAP:   dict[str, str] = {}
DEC_MAP:    dict[str, int] = {}
for _cat, _items in INDICATORS.items():
    for _name, _info in _items.items():
        TICKER_MAP[_name] = _info["ticker"]
        UNIT_MAP[_name]   = _info["unit"]
        DEC_MAP[_name]    = _info["decimals"]

# ====================================================
# キャッシュ（5分）
# ====================================================
_cache: dict = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 300


def _is_fresh(key: str) -> bool:
    return key in _cache and (time.time() - _cache_ts.get(key, 0)) < CACHE_TTL


# ====================================================
# 現在値取得
# ====================================================
def get_current_prices() -> dict:
    if _is_fresh("current"):
        return _cache["current"]

    all_tickers = [info["ticker"] for cat in INDICATORS.values() for info in cat.values()]
    result: dict = {}

    try:
        raw = yf.download(
            all_tickers, period="5d", interval="1d",
            auto_adjust=True, progress=False
        )
        close = raw["Close"] if isinstance(raw.columns, pd.MultiIndex) else raw[["Close"]]
        if close.index.tz is not None:
            close.index = close.index.tz_convert(None)
    except Exception as e:
        print(f"[ERROR] 現在値取得失敗: {e}")
        close = pd.DataFrame()

    for cat_name, cat_items in INDICATORS.items():
        result[cat_name] = {}
        for name, info in cat_items.items():
            ticker = info["ticker"]
            try:
                if isinstance(close.columns, pd.MultiIndex):
                    col = close[ticker].dropna()
                else:
                    col = close[ticker].dropna() if ticker in close.columns else pd.Series()

                current = prev = change = change_pct = None
                if len(col) >= 2:
                    current    = float(col.iloc[-1])
                    prev       = float(col.iloc[-2])
                    change     = current - prev
                    change_pct = (change / prev) * 100 if prev else 0
                elif len(col) == 1:
                    current = float(col.iloc[-1])

                dec = info["decimals"]
                result[cat_name][name] = {
                    "value":      round(current,    dec)     if current      is not None else None,
                    "change":     round(change,     dec)     if change       is not None else None,
                    "change_pct": round(change_pct, 2)       if change_pct   is not None else None,
                    "unit":       info["unit"],
                }
            except Exception:
                result[cat_name][name] = {
                    "value": None, "change": None, "change_pct": None, "unit": info["unit"]
                }

    _cache["current"]    = result
    _cache_ts["current"] = time.time()
    return result


# ====================================================
# 時系列データ取得
# ====================================================
def fetch_market_data(selected_names: list[str], interval: str = "1d",
                      start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """選択された指標の時系列データを取得して結合DataFrameを返す"""
    tickers = {name: TICKER_MAP[name] for name in selected_names if name in TICKER_MAP}
    if not tickers:
        return pd.DataFrame()

    ticker_list = list(tickers.values())
    name_by_ticker = {v: k for k, v in tickers.items()}

    yf_kwargs: dict = {"interval": interval, "auto_adjust": True, "progress": False}
    if start_date and end_date:
        yf_kwargs["start"] = start_date
        yf_kwargs["end"]   = end_date
    else:
        yf_kwargs["period"] = "1y"

    try:
        raw = yf.download(ticker_list, **yf_kwargs)
        if raw.empty:
            return pd.DataFrame()

        if isinstance(raw.columns, pd.MultiIndex):
            close = raw["Close"].copy()
        else:
            # 単一ティッカーの場合
            close = raw[["Close"]].copy()
            close.columns = ticker_list

        if close.index.tz is not None:
            close.index = close.index.tz_convert(None)

        close.index.name = "日付"
        close = close.rename(columns=name_by_ticker)
        # 選択順に並べ替え
        cols = [n for n in selected_names if n in close.columns]
        return close[cols]

    except Exception as e:
        print(f"[ERROR] 時系列データ取得失敗: {e}")
        return pd.DataFrame()


# ====================================================
# マクロ経済指標（FRED）
# transform:
#   "yoy_pct"  … レベル値→前年同月比(%)
#   "level"    … そのままの値（%表示など）
#   "mom_diff" … 前月差（千人など）
# ====================================================
MACRO_SERIES: dict[str, dict] = {
    # ── CPI ──────────────────────────────────────────
    "米CPI (前年同月比%)":         {"id": "CPIAUCSL",       "transform": "yoy_pct"},
    "米コアCPI (前年同月比%)":     {"id": "CPILFESL",        "transform": "yoy_pct"},
    "日本CPI (前年同月比%)":       {"id": "JPNCPIALLMINMEI", "transform": "yoy_pct"},
    # ── 雇用 ─────────────────────────────────────────
    "米失業率(%)":                 {"id": "UNRATE",          "transform": "level"},
    "米NFP 雇用者数変化(千人)":    {"id": "PAYEMS",          "transform": "mom_diff"},
    # ── 景気 ─────────────────────────────────────────
    "米GDP成長率 前期比年率(%)":   {"id": "A191RL1Q225SBEA", "transform": "level"},
  # ── 米国金融政策 ─────────────────────────────────
    "米FF金利(%)":                       {"id": "FEDFUNDS",         "transform": "level"},
    # ── 米国景気・消費 ───────────────────────────────
    "米鉱工業生産 前年比(%)":            {"id": "INDPRO",           "transform": "yoy_pct"},
    "米小売売上高 前年比(%)":            {"id": "RSAFS",            "transform": "yoy_pct"},
    "米消費者信頼感 ミシガン大(pt)":     {"id": "UMCSENT",          "transform": "level"},
    "米住宅着工件数(千戸)":              {"id": "HOUST",            "transform": "level"},
    # ── 日本経済 ─────────────────────────────────────
    "日本失業率(%)":                     {"id": "LRUNTTTTJPM156S",  "transform": "level"},
    "日本鉱工業生産 前年比(%)":          {"id": "JPNPROINDMISMEI",  "transform": "yoy_pct"},
}

# 後方互換（excel_generator 等から参照される場合に備え alias を残す）
CPI_SERIES = MACRO_SERIES


def fetch_cpi_data(start_date: str = None, end_date: str = None) -> tuple[pd.DataFrame | None, str | None]:
    """FRED API からマクロ経済指標を取得。(DataFrame, error_message) を返す"""
    FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
    if not FRED_API_KEY:
        return None, (
            "マクロ経済指標の取得にはFREDのAPIキーが必要です。\n"
            "https://fred.stlouisfed.org/docs/api/api_key.html で無料取得後、\n"
            "data_fetcher.py の FRED_API_KEY に設定してください。"
        )

    # yoy変換のため開始日を1年前倒し
    if start_date:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        start = (s - timedelta(days=366)).strftime("%Y-%m-%d")
    else:
        start = (datetime.now() - timedelta(days=366*2)).strftime("%Y-%m-%d")
    end = end_date or datetime.now().strftime("%Y-%m-%d")

    frames = []
    for col_name, meta in MACRO_SERIES.items():
        series_id = meta["id"]
        transform = meta["transform"]
        try:
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id":         series_id,
                    "api_key":           FRED_API_KEY,
                    "file_type":         "json",
                    "observation_start": start,
                    "observation_end":   end,
                },
                timeout=10,
            )
            resp.raise_for_status()
            obs = resp.json().get("observations", [])
            df = pd.DataFrame(obs)[["date", "value"]]
            df["date"]  = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.set_index("date").rename(columns={"value": col_name})

            if transform == "yoy_pct":
                df[col_name] = df[col_name].pct_change(12) * 100
            elif transform == "mom_diff":
                df[col_name] = df[col_name].diff()
            # "level" はそのまま

            frames.append(df)
        except Exception as e:
            print(f"[WARN] FRED系列 {series_id} 取得失敗: {e}")

    if not frames:
        return None, "マクロ経済指標データの取得に失敗しました"

    result = frames[0]
    for f in frames[1:]:
        result = result.join(f, how="outer")

    # 余分な1年分を除去
    cutoff = start_date or (datetime.now() - timedelta(days=366)).strftime("%Y-%m-%d")
    result = result[result.index >= cutoff].dropna(how="all")
    return result, None

# ====================================================
# 日銀短観データ取得（BOJ公式フラットファイル）
# ZIP: https://www.stat-search.boj.or.jp/info/co.zip
# ====================================================
# 系列コード: TK99[計算][業種4桁][項目3桁][種別][頻度2桁][実績][規模][明細3桁]
TANKAN_SERIES = {
    "短観 大企業製造業 業況DI":     "TK99F1000601GCQ01000",
    "短観 大企業非製造業 業況DI":   "TK99F2000601GCQ01000",
    "短観 中堅企業製造業 業況DI":   "TK99F1000601GCQ02000",
    "短観 中小企業製造業 業況DI":   "TK99F1000601GCQ03000",
    "短観 中小企業非製造業 業況DI": "TK99F2000601GCQ03000",
}

_tankan_cache: dict = {}
_tankan_cache_ts: dict[str, float] = {}
TANKAN_CACHE_TTL = 3600 * 6  # 6時間キャッシュ（短観は四半期発表）

def fetch_tankan_data(start_date: str = None, end_date: str = None) -> tuple[pd.DataFrame | None, str | None]:
    """
    日銀短観（企業短期経済観測調査）の業況判断DIを取得。
    BOJ公式フラットファイル（co.zip → co.csv）をダウンロードしてパース。
    四半期データ（3月・6月・9月・12月調査）。
    """
    import io, zipfile

    cache_key = f"{start_date}_{end_date}"
    if cache_key in _tankan_cache and (time.time() - _tankan_cache_ts.get(cache_key, 0)) < TANKAN_CACHE_TTL:
        return _tankan_cache[cache_key], None

    try:
        resp = requests.get(
            "https://www.stat-search.boj.or.jp/info/co.zip",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        return None, f"日銀短観ZIPダウンロード失敗: {e}"

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open("co.csv") as f:
                df_all = pd.read_csv(
                    f,
                    header=None,
                    names=["code", "freq", "period", "value"],
                    encoding="utf-8",
                    dtype=str,
                )
    except Exception as e:
        return None, f"日銀短観CSVパース失敗: {e}"

    # 対象系列のみ抽出（四半期）
    target_codes = set(TANKAN_SERIES.values())
    df_all = df_all[(df_all["code"].isin(target_codes)) & (df_all["freq"] == "Q")].copy()

    if df_all.empty:
        return None, "短観データが見つかりません（系列コードを確認してください）"

    df_all["value"] = pd.to_numeric(df_all["value"], errors="coerce")

    # 期間をdatetimeに変換（YYYYQQ → 四半期末月末日）
    def period_to_date(p: str):
        try:
            year  = int(p[:4])
            q     = int(p[4:6])  # 01=Q1, 02=Q2, 03=Q3, 04=Q4
            month = q * 3        # 3, 6, 9, 12
            return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
        except Exception:
            return pd.NaT

    df_all["date"] = df_all["period"].apply(period_to_date)
    df_all = df_all.dropna(subset=["date", "value"])

    # 系列コード → 日本語名
    code_to_name = {v: k for k, v in TANKAN_SERIES.items()}
    df_all["series_name"] = df_all["code"].map(code_to_name)

    # ピボット（日付 × 系列名）
    result = df_all.pivot_table(
        index="date", columns="series_name", values="value", aggfunc="first"
    )
    result.index.name = "日付"

    # 列を定義順に並べ替え
    ordered_cols = [c for c in TANKAN_SERIES.keys() if c in result.columns]
    result = result[ordered_cols]

    # 日付フィルタ
    if start_date:
        result = result[result.index >= pd.Timestamp(start_date)]
    if end_date:
        result = result[result.index <= pd.Timestamp(end_date)]

    result = result.sort_index().dropna(how="all")

    _tankan_cache[cache_key]    = result
    _tankan_cache_ts[cache_key] = time.time()

    return result, None
