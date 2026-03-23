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
    selected  = body.get("selected", list(TICKER_MAP.keys()))  # 未選択なら全指標
    period    = body.get("period",   "1y")
    interval  = body.get("interval", "1d")
    want_cpi  = body.get("include_cpi", True)

    # 市場データ取得
    market_df = fetch_market_data(selected, period, interval)

    # CPI取得
    cpi_df = None
    if want_cpi:
        cpi_df, cpi_err = fetch_cpi_data(period)
        if cpi_err:
            app.logger.warning(f"CPI取得スキップ: {cpi_err}")

    excel_bytes = generate_excel(market_df, cpi_df, period, interval)

    interval_label = {"1d": "日足", "1wk": "週足", "1mo": "月足"}.get(interval, interval)
    period_label   = {
        "3mo": "3M", "6mo": "6M", "1y": "1Y",
        "3y": "3Y",  "5y": "5Y",  "10y": "10Y",
    }.get(period, period)
    filename = f"economic_data_{period_label}_{interval_label}_{datetime.now().strftime('%Y%m%d')}.xlsx"

    return send_file(
        io.BytesIO(excel_bytes),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    print("=" * 50)
    print("  経済指標ダッシュボード起動中...")
    print(f"  ブラウザで http://localhost:{port} を開いてください")
    print("=" * 50)
    app.run(debug=debug, host="0.0.0.0", port=port)
