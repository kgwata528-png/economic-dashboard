"""
経済指標ダッシュボード
Flask Webアプリ

起動方法:
    pip install flask yfinance pandas openpyxl requests
    python app.py

ブラウザで http://localhost:5000 を開く
"""

import io
from datetime import datetime
from flask import Flask, render_template, jsonify, request, send_file

from data_fetcher import (
    get_current_prices,
    fetch_market_data,
    fetch_cpi_data,
    fetch_tankan_data,
    INDICATORS,
    TICKER_MAP,
)
from excel_generator import generate_excel

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html", indicators=INDICATORS)


@app.route("/api/current")
def api_current():
    """現在値をJSONで返す（5分キャッシュ）"""
    try:
        data = get_current_prices()
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/download", methods=["POST"])
def api_download():
    """選択された指標・期間・足種でExcelを生成してダウンロード"""
    body = request.get_json(force=True)
    selected   = body.get("selected", list(TICKER_MAP.keys()))
    interval   = body.get("interval", "1d")
    want_cpi   = body.get("include_cpi", True)
    start_date = body.get("start_date")
    end_date   = body.get("end_date")

    # 市場データ取得
    market_df = fetch_market_data(selected, interval=interval,
                                  start_date=start_date, end_date=end_date)

    # CPI取得
    cpi_df = None
    if want_cpi:
        cpi_df, cpi_err = fetch_cpi_data(start_date=start_date, end_date=end_date)
        if cpi_err:
            app.logger.warning(f"CPI取得スキップ: {cpi_err}")

    # 短観取得
    tankan_df = None
    if want_cpi:
        tankan_result, tankan_err = fetch_tankan_data()
        if tankan_err:
            app.logger.warning(f"短観取得スキップ: {tankan_err}")
        else:
            tankan_df = tankan_result

    excel_bytes = generate_excel(market_df, cpi_df, f"{start_date}〜{end_date}", interval, tankan_df=tankan_df)

    interval_label = {"1d": "daily", "1wk": "weekly", "1mo": "monthly"}.get(interval, interval)
    s = (start_date or "").replace("-", "")[2:]  # 250101
    e = (end_date   or "").replace("-", "")[2:]  # 260101
    filename = f"econ_{s}-{e}_{interval_label}.xlsx"

    return send_file(
        io.BytesIO(excel_bytes),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )

@app.route("/api/macro")
def api_macro():
    """FREDマクロ指標の最新値をJSONで返す"""
    from datetime import datetime, timedelta
    try:
        end   = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        df, err = fetch_cpi_data(start_date=start, end_date=end)
        if err:
            return jsonify({"ok": False, "error": err})
        result = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                latest = float(series.iloc[-1])
                prev   = float(series.iloc[-2]) if len(series) > 1 else None
                result[col] = {
                    "value":  round(latest, 2),
                    "date":   series.index[-1].strftime("%Y/%m"),
                    "change": round(latest - prev, 2) if prev is not None else None,
                }
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/tankan")
def api_tankan():
    """日銀短観の最新値をJSONで返す"""
    try:
        from data_fetcher import fetch_tankan_data
        df, err = fetch_tankan_data()
        if err:
            return jsonify({"ok": False, "error": err})
        result = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                latest = float(series.iloc[-1])
                prev   = float(series.iloc[-2]) if len(series) > 1 else None
                dt     = series.index[-1]
                q      = (dt.month - 1) // 3 + 1
                result[col] = {
                    "value":  round(latest, 1),
                    "date":   f"{dt.year}/Q{q}",
                    "change": round(latest - prev, 1) if prev is not None else None,
                }
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/macro")
def api_macro():
    """FREDマクロ指標の最新値をJSONで返す"""
    from datetime import datetime, timedelta
    try:
        end   = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        df, err = fetch_cpi_data(start_date=start, end_date=end)
        if err:
            return jsonify({"ok": False, "error": err})
        result = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                latest = float(series.iloc[-1])
                prev   = float(series.iloc[-2]) if len(series) > 1 else None
                result[col] = {
                    "value":  round(latest, 2),
                    "date":   series.index[-1].strftime("%Y/%m"),
                    "change": round(latest - prev, 2) if prev is not None else None,
                }
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/tankan")
def api_tankan():
    """日銀短観の最新値をJSONで返す"""
    try:
        from data_fetcher import fetch_tankan_data
        df, err = fetch_tankan_data()
        if err:
            return jsonify({"ok": False, "error": err})
        result = {}
        for col in df.columns:
            series = df[col].dropna()
            if len(series) > 0:
                latest = float(series.iloc[-1])
                prev   = float(series.iloc[-2]) if len(series) > 1 else None
                dt     = series.index[-1]
                q      = (dt.month - 1) // 3 + 1
                result[col] = {
                    "value":  round(latest, 1),
                    "date":   f"{dt.year}/Q{q}",
                    "change": round(latest - prev, 1) if prev is not None else None,
                }
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    print("=" * 50)
    print("  経済指標ダッシュボード起動中...")
    print(f"  ブラウザで http://localhost:{port} を開いてください")
    print("=" * 50)
    app.run(debug=debug, host="0.0.0.0", port=port)
