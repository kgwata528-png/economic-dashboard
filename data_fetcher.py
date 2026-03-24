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
        "USD/JPY": {"ticker": "USDJPY=X",  "unit": "円",    "decimals": 3},
        "EUR/USD": {"ticker": "EURUSD=X",  "unit": "USD",   "decimals": 4},
        "EUR/JPY": {"ticker": "EURJPY=X",  "unit": "円",    "decimals": 3},
        "GBP/JPY": {"ticker": "GBPJPY=X",  "unit": "円",    "decimals": 3},
        "AUD/JPY": {"ticker": "AUDJPY=X",  "unit": "円",    "decimals": 3},
        "CHF/JPY": {"ticker": "CHFJPY=X",  "unit": "円",    "decimals": 3},
    },
    "ドル指数": {
        "DXY (ドル指数)": {"ticker": "DX-Y.NYB", "unit": "pt", "decimals": 3},
    },
    "米国株価指数": {
        "ダウ平均":      {"ticker": "^DJI",  "unit": "pt", "decimals": 2},
        "S&P500":       {"ticker": "^GSPC", "unit": "pt", "decimals": 2},
        "ナスダック100": {"ticker": "^NDX",  "unit": "pt", "decimals": 2},
        "ラッセル2000":  {"ticker": "^RUT",  "unit": "pt", "decimals": 2},
    },
    "日本株価指数": {
        "日経225": {"ticker": "^N225", "unit": "円", "decimals": 2},
    },
    "米国金利": {
        "米30年債利回り":     {"ticker": "^TYX", "unit": "%", "decimals": 3},
        "米10年債利回り":     {"ticker": "^TNX", "unit": "%", "decimals": 3},
        "米5年債利回り":      {"ticker": "^FVX", "unit": "%", "decimals": 3},
        "米短期金利(3ヶ月)": {"ticker": "^IRX", "unit": "%", "decimals": 3},
    },
    "商品": {
        "金 先物":   {"ticker": "GC=F", "unit": "USD/oz",    "decimals": 2},
        "銀 先物":   {"ticker": "SI=F", "unit": "USD/oz",    "decimals": 3},
        "原油 先物": {"ticker": "CL=F", "unit": "USD/bbl",   "decimals": 2},
        "天然ガス":  {"ticker": "NG=F", "unit": "USD/MMBtu", "decimals": 3},
    },
    "恐怖指数": {
        "VIX": {"ticker": "^VIX", "unit": "pt", "decimals": 2},
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
def fetch_market_data(selected_names: list[str], period: str, interval: str) -> pd.DataFrame:
    """選択された指標の時系列データを取得して結合DataFrameを返す"""
    tickers = {name: TICKER_MAP[name] for name in selected_names if name in TICKER_MAP}
    if not tickers:
        return pd.DataFrame()

    ticker_list = list(tickers.values())
    name_by_ticker = {v: k for k, v in tickers.items()}

    try:
        raw = yf.download(
            ticker_list, period=period, interval=interval,
            auto_adjust=True, progress=False
        )
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
}

# 後方互換（excel_generator 等から参照される場合に備え alias を残す）
CPI_SERIES = MACRO_SERIES


def fetch_cpi_data(period: str) -> tuple[pd.DataFrame | None, str | None]:
    """FRED API からマクロ経済指標を取得。(DataFrame, error_message) を返す"""
    FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
    if not FRED_API_KEY:
        return None, (
            "マクロ経済指標の取得にはFREDのAPIキーが必要です。\n"
            "https://fred.stlouisfed.org/docs/api/api_key.html で無料取得後、\n"
            "data_fetcher.py の FRED_API_KEY に設定してください。"
        )

    period_map = {"3mo": 3, "6mo": 6, "1y": 12, "3y": 36, "5y": 60, "10y": 120, "max": 900}
    months_back = period_map.get(period, 12)
    # yoy 変換のため取得期間を1年余分に確保（maxの場合は1960年から）
    if period == "max":
        start = "1960-01-01"
    else:
        start = (datetime.now() - timedelta(days=(months_back + 14) * 31)).strftime("%Y-%m-%d")
    end   = datetime.now().strftime("%Y-%m-%d")

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

    # 期間フィルタ（余分な1年分を除去）
    cutoff = (datetime.now() - timedelta(days=months_back * 31)).strftime("%Y-%m-%d")
    result = result[result.index >= cutoff].dropna(how="all")
    return result, None
