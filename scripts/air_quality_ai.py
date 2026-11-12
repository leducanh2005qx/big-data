"""AI summaries and Vietnamese Q&A for the air-quality dashboard.

The service queries real InfluxDB observations, computes bounded statistics,
and only sends those statistics to an optional OpenAI-compatible endpoint.
Summaries are cached in memory and persisted to InfluxDB so Grafana can query
them with its existing datasource without calling the AI API on every refresh.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
import unicodedata
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

try:
    from scripts.air_quality_features import (
        CITY_LABELS as FEATURE_CITY_LABELS,
        build_demo_alerts,
        build_email_preview,
        collect_alerts,
        detect_anomalies,
        email_status,
        forecast_aqi,
        recommend_cities,
        send_alert_email,
    )
except ModuleNotFoundError:  # Supports running this file directly inside the Docker image.
    from air_quality_features import (  # type: ignore[no-redef]
        CITY_LABELS as FEATURE_CITY_LABELS,
        build_demo_alerts,
        build_email_preview,
        collect_alerts,
        detect_anomalies,
        email_status,
        forecast_aqi,
        recommend_cities,
        send_alert_email,
    )


LOGIC_VERSION = "2.0"
DEFAULT_HOURS = 24
MAX_HOURS = 168
SUMMARY_REFRESH_SECONDS = int(os.getenv("AI_SUMMARY_REFRESH_SECONDS", "600"))
ALERT_EMAIL_REFRESH_SECONDS = int(os.getenv("ALERT_EMAIL_REFRESH_SECONDS", "600"))
ALERT_EMAIL_COOLDOWN_SECONDS = int(os.getenv("ALERT_EMAIL_COOLDOWN_SECONDS", "3600"))
SUMMARY_MEASUREMENT = "air_quality_ai_summary"
SUMMARY_RETENTION_POLICY = os.getenv("INFLUXDB_RAW_RETENTION_POLICY", "raw_7d")
DEFAULT_DASHBOARD = "air-quality-overview"
DASHBOARD_CONTEXTS = {
    "air-quality-overview": {
        "title": "Hệ thống giám sát chất lượng không khí",
        "description": "Dashboard tổng quan về AQI, PM2.5, PM10, xu hướng theo thời gian, so sánh thành phố và bản đồ.",
        "metrics": ["AQI", "PM2.5", "PM10", "CO", "NO2", "SO2", "O3"],
    },
}

CITY_ALIASES = {
    "hanoi": "Hanoi", "ha noi": "Hanoi", "hà nội": "Hanoi",
    "haiphong": "Hai Phong", "hai phong": "Hai Phong", "hải phòng": "Hai Phong",
    "halong": "Ha Long", "ha long": "Ha Long", "hạ long": "Ha Long",
    "vinh": "Vinh",
    "hue": "Hue", "huế": "Hue",
    "hcm": "Ho Chi Minh City", "tphcm": "Ho Chi Minh City",
    "tp.hcm": "Ho Chi Minh City", "ho chi minh": "Ho Chi Minh City",
    "ho chi minh city": "Ho Chi Minh City", "thành phố hồ chí minh": "Ho Chi Minh City",
    "danang": "Da Nang", "da nang": "Da Nang", "đà nẵng": "Da Nang",
    "quynhon": "Quy Nhon", "quy nhon": "Quy Nhon", "quy nhơn": "Quy Nhon",
    "nhatrang": "Nha Trang", "nha trang": "Nha Trang",
    "dalat": "Da Lat", "da lat": "Da Lat", "đà lạt": "Da Lat",
    "bienhoa": "Bien Hoa", "bien hoa": "Bien Hoa", "biên hòa": "Bien Hoa",
    "cantho": "Can Tho", "can tho": "Can Tho", "cần thơ": "Can Tho",
    "buonmathuot": "Buon Ma Thuot", "buon ma thuot": "Buon Ma Thuot", "buôn ma thuột": "Buon Ma Thuot",
    "caobang": "Cao Bang", "cao bang": "Cao Bang", "cao bằng": "Cao Bang",
    "tuyenquang": "Tuyen Quang", "tuyen quang": "Tuyen Quang", "tuyên quang": "Tuyen Quang",
    "dienbien": "Dien Bien", "dien bien": "Dien Bien", "điện biên": "Dien Bien",
    "laichau": "Lai Chau", "lai chau": "Lai Chau", "lai châu": "Lai Chau",
    "sonla": "Son La", "son la": "Son La", "sơn la": "Son La",
    "laocai": "Lao Cai", "lao cai": "Lao Cai", "lào cai": "Lao Cai",
    "thainguyen": "Thai Nguyen", "thai nguyen": "Thai Nguyen", "thái nguyên": "Thai Nguyen",
    "langson": "Lang Son", "lang son": "Lang Son", "lạng sơn": "Lang Son",
    "quangninh": "Quang Ninh", "quang ninh": "Quang Ninh", "quảng ninh": "Quang Ninh",
    "bacninh": "Bac Ninh", "bac ninh": "Bac Ninh", "bắc ninh": "Bac Ninh",
    "phutho": "Phu Tho", "phu tho": "Phu Tho", "phú thọ": "Phu Tho",
    "hungyen": "Hung Yen", "hung yen": "Hung Yen", "hưng yên": "Hung Yen",
    "ninhbinh": "Ninh Binh", "ninh binh": "Ninh Binh", "ninh bình": "Ninh Binh",
    "thanhhoa": "Thanh Hoa", "thanh hoa": "Thanh Hoa", "thanh hóa": "Thanh Hoa",
    "nghean": "Nghe An", "nghe an": "Nghe An", "nghệ an": "Nghe An",
    "hatinh": "Ha Tinh", "ha tinh": "Ha Tinh", "hà tĩnh": "Ha Tinh",
    "quangtri": "Quang Tri", "quang tri": "Quang Tri", "quảng trị": "Quang Tri",
    "quangngai": "Quang Ngai", "quang ngai": "Quang Ngai", "quảng ngãi": "Quang Ngai",
    "gialai": "Gia Lai", "gia lai": "Gia Lai",
    "khanhhoa": "Khanh Hoa", "khanh hoa": "Khanh Hoa", "khánh hòa": "Khanh Hoa",
    "daklak": "Dak Lak", "dak lak": "Dak Lak", "đắk lắk": "Dak Lak",
    "lamdong": "Lam Dong", "lam dong": "Lam Dong", "lâm đồng": "Lam Dong",
    "dongnai": "Dong Nai", "dong nai": "Dong Nai", "đồng nai": "Dong Nai",
    "tayninh": "Tay Ninh", "tay ninh": "Tay Ninh", "tây ninh": "Tay Ninh",
    "dongthap": "Dong Thap", "dong thap": "Dong Thap", "đồng tháp": "Dong Thap",
    "vinhlong": "Vinh Long", "vinh long": "Vinh Long", "vĩnh long": "Vinh Long",
    "angiang": "An Giang", "an giang": "An Giang",
    "camau": "Ca Mau", "ca mau": "Ca Mau", "cà mau": "Ca Mau",
}
CITY_LABELS = {
    "Hanoi": "Hà Nội", "Hai Phong": "Hải Phòng", "Ha Long": "Hạ Long", "Vinh": "Vinh", "Hue": "Huế",
    "Ho Chi Minh City": "TP. Hồ Chí Minh", "Da Nang": "Đà Nẵng", "Quy Nhon": "Quy Nhơn",
    "Nha Trang": "Nha Trang", "Da Lat": "Đà Lạt", "Bien Hoa": "Biên Hòa", "Can Tho": "Cần Thơ",
    "Buon Ma Thuot": "Buôn Ma Thuột",
    "Cao Bang": "Cao Bằng", "Tuyen Quang": "Tuyên Quang", "Dien Bien": "Điện Biên", "Lai Chau": "Lai Châu",
    "Son La": "Sơn La", "Lao Cai": "Lào Cai", "Thai Nguyen": "Thái Nguyên", "Lang Son": "Lạng Sơn",
    "Quang Ninh": "Quảng Ninh", "Bac Ninh": "Bắc Ninh", "Phu Tho": "Phú Thọ", "Hung Yen": "Hưng Yên",
    "Ninh Binh": "Ninh Bình", "Thanh Hoa": "Thanh Hóa", "Nghe An": "Nghệ An", "Ha Tinh": "Hà Tĩnh",
    "Quang Tri": "Quảng Trị", "Quang Ngai": "Quảng Ngãi", "Gia Lai": "Gia Lai", "Khanh Hoa": "Khánh Hòa",
    "Dak Lak": "Đắk Lắk", "Lam Dong": "Lâm Đồng", "Dong Nai": "Đồng Nai", "Tay Ninh": "Tây Ninh",
    "Dong Thap": "Đồng Tháp", "Vinh Long": "Vĩnh Long", "An Giang": "An Giang", "Ca Mau": "Cà Mau",
}

CITY_ALIASES.update({"nam dinh": "Nam Dinh", "namdinh": "Nam Dinh"})
CITY_LABELS["Nam Dinh"] = "Nam Định"
CITY_LABELS["Thanh Hoa"] = "Thanh Hóa"

_cache: dict[tuple[object, ...], dict[str, object]] = {}
_cache_lock = threading.Lock()


HTML = """<!doctype html><html lang="vi"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Trợ lý chất lượng không khí</title>
<style>
:root{--bg:#08111f;--card:#111e31;--card2:#17263b;--line:#2b405d;--text:#e5edf7;--muted:#9fb0c5;--cyan:#22d3ee;--blue:#0891b2;--green:#34d399}
*{box-sizing:border-box}body{font:15px system-ui,-apple-system,sans-serif;background:radial-gradient(circle at top,#142844 0,var(--bg) 46%);color:var(--text);max-width:1180px;margin:0 auto;padding:24px}
.topbar{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:22px}.brand{display:flex;gap:12px;align-items:center}.logo{font-size:30px}.eyebrow{color:var(--cyan);font-size:12px;text-transform:uppercase;letter-spacing:1px}.brand h1{font-size:25px;margin:3px 0}.muted{color:var(--muted)}
button,.button{border:1px solid var(--line);border-radius:8px;padding:10px 13px;background:var(--card2);color:var(--text);cursor:pointer;text-decoration:none;display:inline-block;font-size:14px}button:hover,.button:hover{border-color:var(--cyan);background:#1b3854}.primary{background:var(--blue);border-color:var(--blue)}.danger{color:#fecaca}.status{font-size:13px;color:var(--green);white-space:nowrap}.status.offline{color:#fbbf24}
.context{background:linear-gradient(120deg,#123049,#142238);border:1px solid #245272;border-radius:12px;padding:16px;margin-bottom:18px}.context h2{margin:0 0 5px;font-size:17px}.chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}.chip{border:1px solid #35627e;border-radius:999px;padding:5px 10px;color:#b8eaff;font-size:13px}
.layout{display:grid;grid-template-columns:285px 1fr;gap:18px}.card{background:rgba(17,30,49,.9);border:1px solid var(--line);border-radius:12px;padding:17px}.card h2{font-size:17px;margin:0 0 12px}.side button{width:100%;text-align:left;margin:5px 0}.summary{white-space:pre-wrap;line-height:1.6;color:#dbeafe;min-height:95px}.messages{min-height:300px;max-height:480px;overflow:auto;padding:4px 2px}.message{max-width:88%;padding:12px 14px;border-radius:12px;margin:10px 0;white-space:pre-wrap;line-height:1.55}.message.user{margin-left:auto;background:#075985;border-bottom-right-radius:3px}.message.bot{background:var(--card2);border-bottom-left-radius:3px}.message small{display:block;color:#9cc9df;margin-bottom:4px}.composer{display:flex;gap:10px;margin-top:14px}.composer textarea{flex:1;min-height:62px;resize:vertical;background:#0c1728;color:var(--text);border:1px solid var(--line);border-radius:9px;padding:12px;font:inherit}.actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}.hint{font-size:13px;color:var(--muted);margin-top:10px}
@media(max-width:760px){body{padding:14px}.topbar{align-items:flex-start;flex-direction:column}.layout{grid-template-columns:1fr}.messages{max-height:none}.composer{flex-direction:column}.message{max-width:96%}}
</style>
<header class="topbar"><div class="brand"><div class="logo">🌫️</div><div><div class="eyebrow">Trợ lý dữ liệu Big Data</div><h1>Phân tích chất lượng không khí</h1></div></div><div><span id="health" class="status">● Đang kiểm tra kết nối</span></div></header>
<section class="context"><h2 id="contextTitle">Ngữ cảnh dashboard</h2><div id="contextDescription" class="muted">Đang nhận ngữ cảnh từ Grafana...</div><div id="chips" class="chips"></div><div class="actions"><button class="primary" onclick="openGrafana()">← Quay về Grafana</button><button onclick="reloadContext()">↻ Làm mới dữ liệu</button><button onclick="demoEmail()">✉ Gửi thử email demo</button><button class="danger" onclick="clearChat()">Xóa cuộc trò chuyện</button></div></section>
<main class="layout"><aside class="side"><section class="card"><h2>Phân tích nhanh</h2><button onclick="loadSummary('overview')">📌 Tóm tắt dashboard</button><button onclick="quick('Dashboard này nói về những chỉ số gì?')">❓ Dashboard này nói về gì?</button><button onclick="quick('Tình hình AQI hiện tại như thế nào?')">🟢 Phân tích AQI</button><button onclick="quick('PM2.5 và PM10 hiện đang ở mức nào?')">🫁 Phân tích bụi mịn</button><button onclick="quick('Dự báo AQI trong 24 giờ tới như thế nào?')">🔮 Dự báo AQI</button><button onclick="quick('Có điểm bất thường hoặc tăng đột biến nào không?')">⚠️ Phát hiện bất thường</button><button onclick="quick('Tôi muốn đi du lịch, nên chọn thành phố nào?')">🧳 Tư vấn chọn thành phố</button><button onclick="quick('AQI đang có xu hướng tăng hay giảm?')">📈 Xu hướng AQI</button><button onclick="quick('Thành phố nào đang ô nhiễm nhất trong phạm vi dashboard?')">🏙️ So sánh thành phố</button><p class="hint">Các câu hỏi nhanh vẫn dùng đúng thành phố và khoảng thời gian đang chọn trên Grafana.</p></section><section class="card" style="margin-top:18px"><h2>Bản tóm tắt</h2><div id="summary" class="summary">Đang tải...</div></section></aside>
<section class="card"><h2>Trò chuyện với trợ lý</h2><div id="messages" class="messages"><div class="message bot"><small>Trợ lý</small>Tôi đang sẵn sàng. Hãy hỏi về các chỉ số đang có trong dashboard này.</div></div><form class="composer" onsubmit="event.preventDefault();ask()"><textarea id="q" placeholder="Ví dụ: Hà Nội và Đà Lạt, nơi nào có không khí sạch hơn?" aria-label="Nhập câu hỏi"></textarea><button class="primary" type="submit">Gửi câu hỏi</button></form><p class="hint">Enter trong ô nhập để xuống dòng. Bấm nút “Gửi câu hỏi” để bắt đầu phân tích.</p></section></main>
<script>
const params=new URLSearchParams(location.search);
const dashboardContext={dashboard:params.get('dashboard')||'air-quality-overview',city:params.get('city')||'all',source:params.get('source')||'all',from:params.get('from')||'',to:params.get('to')||''};
const dashboardTitle={'air-quality-overview':'Hệ thống giám sát chất lượng không khí'}[dashboardContext.dashboard]||dashboardContext.dashboard;
const cityLabel={'all':'Tất cả thành phố','Hanoi':'Hà Nội','Da Lat':'Đà Lạt','Da Nang':'Đà Nẵng','Ho Chi Minh City':'TP. Hồ Chí Minh'}[dashboardContext.city]||dashboardContext.city;
document.getElementById('contextTitle').textContent='Đang phân tích: '+dashboardTitle;
document.getElementById('contextDescription').textContent='Trợ lý chỉ trả lời theo bộ lọc của dashboard hiện tại, không tự mở rộng sang dữ liệu ngoài phạm vi.';
[['Dashboard',dashboardTitle],['Thành phố',cityLabel],['Nguồn dữ liệu',dashboardContext.source==='all'?'Tất cả':dashboardContext.source],['Thời gian',dashboardContext.from&&dashboardContext.to?'Theo bộ lọc Grafana':'24 giờ gần nhất']].forEach(([name,value])=>{let el=document.createElement('span');el.className='chip';el.textContent=name+': '+value;document.getElementById('chips').appendChild(el)});
function hours(){let a=Number(dashboardContext.from),b=Number(dashboardContext.to);return a&&b&&b>a?Math.max(1,Math.min(168,Math.ceil((b-a)/3600000))):24}
function summaryUrl(scope){let q=new URLSearchParams({hours:String(hours())});if(dashboardContext.city&&dashboardContext.city!=='all')q.set('city',dashboardContext.city);return '/summary/'+scope+'?'+q.toString()}
async function loadSummary(scope){let box=document.getElementById('summary');box.textContent='Đang phân tích...';try{let r=await fetch(summaryUrl(scope));let x=await r.json();box.textContent=x.summary||x.error||'Chưa có dữ liệu.'}catch(e){box.textContent='Không kết nối được service AI.'}}
function addMessage(role,text){let el=document.createElement('div');el.className='message '+role;let label=document.createElement('small');label.textContent=role==='user'?'Bạn':'Trợ lý';el.appendChild(label);let body=document.createElement('span');body.textContent=text;el.appendChild(body);let box=document.getElementById('messages');box.appendChild(el);box.scrollTop=box.scrollHeight}
function quick(question){document.getElementById('q').value=question;ask()}
async function ask(){let input=document.getElementById('q'),question=input.value.trim();if(!question)return;input.value='';addMessage('user',question);let status=document.getElementById('health');status.textContent='● Đang phân tích...';status.className='status';try{let r=await fetch('/ask',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:question,dashboard_context:dashboardContext})});let x=await r.json();addMessage('bot',x.answer||x.error||'Không nhận được câu trả lời.');status.textContent=x.mode==='ai'?'● AI trực tuyến':'● Phân tích dữ liệu thật';if(x.mode!=='ai')status.className='status offline'}catch(e){addMessage('bot','Không kết nối được service AI. Hãy kiểm tra Docker và thử lại.');status.textContent='● Mất kết nối';status.className='status offline'} }
function clearChat(){document.getElementById('messages').innerHTML='<div class="message bot"><small>Trợ lý</small>Tôi đang sẵn sàng. Hãy hỏi về các chỉ số đang có trong dashboard này.</div>'}
function reloadContext(){loadSummary('overview');document.getElementById('health').textContent='● Đã làm mới dữ liệu'}
async function demoEmail(){try{let r=await fetch('/email/demo',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({send:true})});let x=await r.json();let text=x.sent?'Đã gửi email demo thành công.':('Chưa gửi email thật: '+(x.reason||'đang ở chế độ preview')+String.fromCharCode(10,10)+((x.preview&&x.preview.body)||''));addMessage('bot',text)}catch(e){addMessage('bot','Không tạo được email demo. Hãy kiểm tra service AI.')}}
function openGrafana(){let q=new URLSearchParams({orgId:'1'});if(dashboardContext.city&&dashboardContext.city!=='all')q.set('var-city',dashboardContext.city);if(dashboardContext.source&&dashboardContext.source!=='all')q.set('var-source',dashboardContext.source);if(dashboardContext.from&&dashboardContext.to){q.set('from',dashboardContext.from);q.set('to',dashboardContext.to)}window.location.href='http://localhost:3000/d/air-quality-overview/he-thong-giam-sat-chat-luong-khong-khi?'+q.toString()}
async function checkHealth(){try{let r=await fetch('/health');let x=await r.json();let s=document.getElementById('health');s.textContent=x.ai_configured?'● Service hoạt động':'● Service hoạt động (fallback)';s.className=x.ai_configured?'status':'status offline'}catch(e){document.getElementById('health').textContent='● Không kết nối service AI';document.getElementById('health').className='status offline'}}
checkHealth();loadSummary('overview');
</script></html>"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def parse_hours(value: str | None) -> int:
    try:
        hours = int(value or DEFAULT_HOURS)
    except ValueError as error:
        raise ValueError("hours phải là số nguyên") from error
    if not 1 <= hours <= MAX_HOURS:
        raise ValueError(f"hours phải nằm trong khoảng 1-{MAX_HOURS}")
    return hours


def normalize_city(value: str | None) -> str | None:
    if value is None or not value.strip() or value.strip().lower() in {"all", "*", "$__all"}:
        return None
    city = CITY_ALIASES.get(value.strip().lower())
    if city is None:
        raise ValueError("city phải là một thành phố đã cấu hình trong OPENMETEO_LOCATIONS")
    return city


def influx_config() -> tuple[str, str, str, str, str]:
    return (
        os.getenv("INFLUXDB_HOST", "influxdb"),
        os.getenv("INFLUXDB_PORT", "8086"),
        os.getenv("INFLUXDB_DATABASE", "air_quality"),
        os.getenv("INFLUXDB_ADMIN_USER", ""),
        os.getenv("INFLUXDB_ADMIN_PASSWORD", ""),
    )


def influx_query(query: str) -> dict[str, object]:
    host, port, database, user, password = influx_config()
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    body = urlencode({"db": database, "q": query}).encode("utf-8")
    request = Request(
        f"http://{host}:{port}/query",
        data=body,
        headers={"Authorization": f"Basic {token}", "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(request, timeout=10) as response:
            payload = json.load(response)
    except HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"InfluxDB query failed: {detail or error}") from error
    error = payload.get("results", [{}])[0].get("error")
    if error:
        raise RuntimeError(f"InfluxDB query failed: {error}")
    return payload


def influx_write_summary(summary: dict[str, object]) -> None:
    """Persist one summary as a string-field measurement for Grafana tables."""
    host, port, database, user, password = influx_config()
    token = base64.b64encode(f"{user}:{password}".encode()).decode()

    def tag(value: object) -> str:
        return str(value or "all").replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")

    def field(value: object) -> str:
        return json.dumps(str(value), ensure_ascii=False)

    scope = tag(summary["scope"])
    city = tag(summary.get("city"))
    time_range = tag(summary["time_range"])
    logic_version = tag(LOGIC_VERSION)
    fields = ",".join([
        f"summary={field(summary['summary'])}",
        f"highlights={field(json.dumps(summary.get('highlights', []), ensure_ascii=False))}",
        f"warnings={field(json.dumps(summary.get('warnings', []), ensure_ascii=False))}",
        f"mode={field(summary['mode'])}",
        f"data_updated_at={field(summary.get('data_updated_at'))}",
        f"generated_at={field(summary['generated_at'])}",
        f"response_time_ms={int(summary.get('response_time_ms') or 0)}i",
    ])
    timestamp = int(utc_now().timestamp() * 1_000_000_000)
    line = f"{SUMMARY_MEASUREMENT},scope={scope},city={city},time_range={time_range},logic_version={logic_version} {fields} {timestamp}"
    query = urlencode({"db": database, "rp": SUMMARY_RETENTION_POLICY})
    request = Request(
        f"http://{host}:{port}/write?{query}",
        data=line.encode("utf-8"),
        headers={"Authorization": f"Basic {token}", "Content-Type": "text/plain; charset=utf-8"},
        method="POST",
    )
    with urlopen(request, timeout=10):
        return


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def query_observations(hours: int, city: str | None = None, metric_type: str | None = None,
                       pollutants: list[str] | None = None) -> list[dict[str, object]]:
    clauses = [f"time > now() - {hours}h", "time <= now()"]
    if city:
        clauses.append(f'"city" = {_literal(city)}')
    if metric_type:
        clauses.append(f'"metric_type" = {_literal(metric_type)}')
    if pollutants:
        clauses.append("(" + " OR ".join(f'\"pollutant\" = {_literal(item)}' for item in pollutants) + ")")
    query = (
        'SELECT "value" FROM "raw_7d"."air_quality_observation" WHERE '
        + " AND ".join(clauses)
        + ' GROUP BY "city","pollutant","metric_type","source","data_mode","aqi_standard"'
    )
    payload = influx_query(query)
    result: list[dict[str, object]] = []
    response = payload.get("results", [{}])[0]
    for series in response.get("series", []):
        tags = series.get("tags", {})
        columns = series.get("columns", [])
        try:
            time_index, value_index = columns.index("time"), columns.index("value")
        except ValueError:
            time_index, value_index = 0, 1
        for values in series.get("values", []):
            if len(values) <= value_index or values[value_index] is None:
                continue
            try:
                numeric = float(values[value_index])
            except (TypeError, ValueError):
                continue
            row = dict(tags)
            row["time"] = values[time_index]
            row["value"] = numeric
            result.append(row)
    return result


def latest_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    latest: dict[tuple[object, ...], dict[str, object]] = {}
    for row in rows:
        key = (row.get("city"), row.get("pollutant"), row.get("metric_type"), row.get("source"), row.get("data_mode"), row.get("aqi_standard"))
        if key not in latest or str(row.get("time", "")) > str(latest[key].get("time", "")):
            latest[key] = row
    return list(latest.values())


def rows_fingerprint(rows: list[dict[str, object]]) -> str:
    content = json.dumps(sorted(rows, key=lambda item: json.dumps(item, sort_keys=True)), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def latest_data_time(rows: list[dict[str, object]]) -> str | None:
    times = [str(row["time"]) for row in rows if row.get("time")]
    return max(times) if times else None


def _number(value: float) -> float | int:
    return int(value) if value.is_integer() else round(value, 2)


def _aqi_category(value: float, standard: str | None) -> str:
    if standard != "US-AQI":
        return "Theo chuẩn " + (standard or "đang có trong dữ liệu")
    if value <= 50:
        return "Tốt"
    if value <= 100:
        return "Trung bình"
    if value <= 150:
        return "Không tốt cho nhóm nhạy cảm"
    if value <= 200:
        return "Không tốt"
    if value <= 300:
        return "Rất không tốt"
    return "Nguy hại"


def _stats_for(scope: str, rows: list[dict[str, object]], city: str | None, hours: int) -> dict[str, object]:
    current = latest_rows(rows)
    aqi_rows = [row for row in (rows if scope == "aqi-trend" else current) if row.get("metric_type") == "aqi"]
    us_aqi = [row for row in aqi_rows if row.get("aqi_standard") == "US-AQI"] or aqi_rows
    current_aqi = [row for row in current if row.get("metric_type") == "aqi"]
    current_us_aqi = [row for row in current_aqi if row.get("aqi_standard") == "US-AQI"] or current_aqi
    concentration = [row for row in (rows if scope == "particulate" else current) if row.get("metric_type") == "concentration"]
    aqi_by_city = {}
    for row in current_us_aqi:
        city_name = str(row.get("city") or "Không rõ")
        aqi_by_city[city_name] = {"value": _number(float(row["value"])), "standard": row.get("aqi_standard"), "time": row.get("time"), "source": row.get("source"), "data_mode": row.get("data_mode")}
    particulate: dict[str, dict[str, object]] = {}
    for pollutant in ("pm25", "pm10"):
        pollutant_rows = [row for row in concentration if row.get("pollutant") == pollutant]
        pollutant_rows.sort(key=lambda row: str(row.get("time", "")))
        values = [float(row["value"]) for row in pollutant_rows]
        if values:
            particulate[pollutant] = {"latest": _number(values[-1]), "average": _number(sum(values) / len(values)), "minimum": _number(min(values)), "maximum": _number(max(values)), "unit": "µg/m³", "samples": len(values), "latest_time": pollutant_rows[-1].get("time")}
    trend = None
    if scope == "aqi-trend" and us_aqi:
        us_aqi.sort(key=lambda row: str(row.get("time", "")))
        values = [float(row["value"]) for row in us_aqi]
        first, last = values[0], values[-1]
        trend = {
            "standard": us_aqi[0].get("aqi_standard"),
            "first": _number(first), "last": _number(last),
            "minimum": _number(min(values)), "maximum": _number(max(values)),
            "delta": _number(last - first), "direction": "tăng" if last > first else ("giảm" if last < first else "ổn định"),
            "minimum_time": us_aqi[values.index(min(values))].get("time"),
            "maximum_time": us_aqi[values.index(max(values))].get("time"),
            "samples": len(values),
        }
    return {
        "scope": scope,
        "city": city or "all",
        "time_range": f"{hours}h",
        "data_updated_at": latest_data_time(rows),
        "source_modes": sorted({f"{row.get('source')}:{row.get('data_mode')}" for row in rows}),
        "aqi_standard": "US-AQI" if any(row.get("aqi_standard") == "US-AQI" for row in current_us_aqi) else (current_us_aqi[0].get("aqi_standard") if current_us_aqi else None),
        "aqi_by_city": aqi_by_city,
        "particulate": particulate,
        "aqi_trend": trend,
        "sample_count": len(rows),
    }


def _fallback_summary(scope: str, stats: dict[str, object]) -> tuple[str, list[str], list[str]]:
    aqi_by_city = stats.get("aqi_by_city", {})
    particulate = stats.get("particulate", {})
    if not stats.get("sample_count"):
        return "Chưa có dữ liệu thật trong khoảng thời gian đã chọn.", [], ["Kiểm tra collector, InfluxDB và khoảng thời gian trên dashboard."]
    highlights: list[str] = []
    warnings: list[str] = []
    if aqi_by_city:
        highest_city, highest = max(aqi_by_city.items(), key=lambda item: float(item[1]["value"]))
        lowest_city, lowest = min(aqi_by_city.items(), key=lambda item: float(item[1]["value"]))
        standard = highest.get("standard")
        highlights.append(f"AQI cao nhất: {CITY_LABELS.get(highest_city, highest_city)} {_number(float(highest['value']))} ({standard or 'không rõ chuẩn'}).")
        highlights.append(f"AQI thấp nhất: {CITY_LABELS.get(lowest_city, lowest_city)} {_number(float(lowest['value']))}.")
        if standard == "US-AQI" and float(highest["value"]) > 100:
            warnings.append(f"AQI cao nhất thuộc mức {_aqi_category(float(highest['value']), standard)} theo US-AQI.")
    trend = stats.get("aqi_trend")
    if scope == "aqi-trend" and trend:
        highlights.insert(0, f"AQI {trend['direction']} từ {trend['first']} lên {trend['last']} ({trend['delta']:+g}); cực đại {trend['maximum']} và cực tiểu {trend['minimum']} theo {trend['standard']}.")
        if trend["standard"] == "US-AQI" and float(trend["maximum"]) > 100:
            warnings.insert(0, f"Trong khoảng đã chọn, AQI cực đại thuộc mức {_aqi_category(float(trend['maximum']), trend['standard'])} theo US-AQI.")
    for pollutant, values in particulate.items():
        highlights.append(f"{pollutant.upper()} trung bình {values['average']} µg/m³, cao nhất {values['maximum']} µg/m³.")
    if not warnings:
        warnings.append("Chưa phát hiện cảnh báo theo ngưỡng US-AQI từ dữ liệu AQI đang có.")
    if scope == "overview":
        summary = "Tổng quan: " + " ".join(highlights[:3])
    elif scope == "particulate":
        summary = "Phân tích bụi mịn: " + " ".join(highlights)
    elif scope == "compare":
        summary = "So sánh thành phố: " + " ".join(highlights[:2])
    else:
        summary = "Xu hướng AQI cần được đọc cùng chuỗi thời gian; dữ liệu hiện có " + "; ".join(highlights[:2])
    return summary, highlights, warnings


def _ai_text(scope: str, stats: dict[str, object]) -> str | None:
    key = os.getenv("AI_API_KEY", "").strip()
    if not key:
        return None
    endpoint = os.getenv("AI_API_URL", "https://api.openai.com/v1/chat/completions")
    context = json.dumps(stats, ensure_ascii=False, separators=(",", ":"))
    instructions = (
        "Bạn là chuyên gia phân tích chất lượng không khí. Trả lời hoàn toàn bằng tiếng Việt. "
        "Chỉ dùng các thống kê trong CONTEXT, không bịa số, không suy đoán nguyên nhân ô nhiễm. "
        "Phân biệt model_estimate với measured, và nói rõ chuẩn AQI nếu có. Trả lời ngắn gọn 2-4 câu."
    )
    if scope == "question":
        instructions += " Nếu câu hỏi yêu cầu cao nhất, thấp nhất hoặc một thành phố cụ thể, chỉ trả lời đúng kết quả được hỏi, không liệt kê các thành phố còn lại. Nếu người dùng so sánh hai thành phố, chỉ so sánh đúng hai thành phố đó; nêu AQI, PM2.5, PM10 nếu có và kết luận ngắn gọn. Không gọi AQI là tỷ lệ phần trăm; nếu có phần trăm thì phải ghi rõ đó chỉ là chênh lệch tương đối."
    body = json.dumps({"model": os.getenv("AI_MODEL", "gpt-4o-mini"), "temperature": 0.2, "messages": [
        {"role": "system", "content": instructions},
        {"role": "user", "content": f"Phạm vi phân tích: {scope}\nCONTEXT JSON:\n{context}"},
    ]}, ensure_ascii=False).encode("utf-8")
    request = Request(endpoint, data=body, headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.load(response)
        return str(payload["choices"][0]["message"]["content"]).strip()
    except Exception:
        return None


def summary_for(scope: str, city: str | None = None, hours: int = DEFAULT_HOURS) -> dict[str, object]:
    if scope in {"overview", "compare"}:
        rows = query_observations(hours, city)
    elif scope == "aqi-trend":
        rows = query_observations(hours, city, "aqi")
    elif scope == "particulate":
        rows = query_observations(hours, city, "concentration", ["pm25", "pm10"])
    else:
        raise ValueError("scope không hợp lệ")
    fingerprint = rows_fingerprint(rows)
    data_updated_at = latest_data_time(rows)
    key = (scope, city or "all", hours, data_updated_at, fingerprint, LOGIC_VERSION)
    with _cache_lock:
        cached = _cache.get(key)
    if cached:
        return cached
    started = time.perf_counter()
    stats = _stats_for(scope, rows, city, hours)
    fallback, highlights, warnings = _fallback_summary(scope, stats)
    ai_text = _ai_text(scope, stats)
    mode = "ai" if ai_text else ("data_summary" if os.getenv("AI_API_KEY", "").strip() == "" else "fallback")
    result: dict[str, object] = {
        "mode": mode,
        "scope": scope,
        "city": city or "all",
        "time_range": f"{hours}h",
        "summary": ai_text or fallback,
        "highlights": highlights,
        "warnings": warnings,
        "data_updated_at": data_updated_at,
        "generated_at": iso_now(),
        "response_time_ms": round((time.perf_counter() - started) * 1000, 2),
    }
    with _cache_lock:
        _cache[key] = result
        if len(_cache) > 100:
            del _cache[next(iter(_cache))]
    try:
        influx_write_summary(result)
    except Exception:
        pass
    return result


def _context_hours(context: dict[str, object]) -> int:
    try:
        start = int(str(context.get("from", "")))
        end = int(str(context.get("to", "")))
        if end > start > 0:
            return max(1, min(MAX_HOURS, int((end - start) / 3_600_000) + 1))
    except (TypeError, ValueError):
        pass
    return DEFAULT_HOURS


def _context_city(context: dict[str, object]) -> str | None:
    value = str(context.get("city", "") or "").strip()
    if not value or value.lower() in {"all", ".*", "$__all"}:
        return None
    try:
        return normalize_city(value)
    except ValueError:
        return None


def latest_context(hours: int = 168, city: str | None = None) -> list[dict[str, object]]:
    rows = latest_rows(query_observations(hours, city, pollutants=["pm25", "pm10", "us_aqi"]))
    return [{"city": row.get("city"), "pollutant": row.get("pollutant"), "value": row.get("value"), "event_time": row.get("time"), "source": row.get("source"), "data_mode": row.get("data_mode"), "aqi_standard": row.get("aqi_standard")} for row in rows]


def metric_label(pollutant: str) -> str:
    return {"pm25": "PM2.5", "pm10": "PM10", "us_aqi": "AQI"}.get(pollutant, pollutant.upper())


def _fold_text(value: str) -> str:
    """Chuẩn hóa tiếng Việt để câu hỏi có hoặc không có dấu đều được hiểu."""
    try:
        repaired = value.encode("latin-1").decode("utf-8")
        if any(mark in value for mark in ("Ã", "Ä", "áº", "á»")):
            value = repaired
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    text = unicodedata.normalize("NFD", value.lower())
    text = text.replace("đ", "d")
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    return text.replace("đ", "d")


def _mentioned_cities(question: str) -> list[str]:
    folded = _fold_text(question)
    found: list[str] = []
    for alias, city in sorted(CITY_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if _fold_text(alias) in folded and city not in found:
            found.append(city)
    return found


def _latest_by_city_metric(rows: list[dict[str, object]]) -> dict[tuple[object, object], dict[str, object]]:
    latest: dict[tuple[object, object], dict[str, object]] = {}
    for row in rows:
        key = (row.get("city"), row.get("pollutant"))
        if key not in latest or str(row.get("event_time", "")) > str(latest[key].get("event_time", "")):
            latest[key] = row
    return latest


def comparison_answer(question: str, rows: list[dict[str, object]], cities: list[str]) -> str:
    """So sánh đúng các thành phố người dùng nêu, không liệt kê toàn bộ database."""
    latest = _latest_by_city_metric([row for row in rows if row.get("city") in cities])
    if not latest:
        return "Chưa có dữ liệu phù hợp cho các thành phố được hỏi."

    city_metrics: dict[str, dict[str, dict[str, object]]] = {city: {} for city in cities}
    for (city, pollutant), row in latest.items():
        if city in city_metrics and pollutant in {"us_aqi", "pm25", "pm10"}:
            city_metrics[city][str(pollutant)] = row

    lines = ["So sánh dữ liệu mới nhất (Open-Meteo):"]
    available_cities: list[str] = []
    for city in cities:
        metrics = city_metrics[city]
        if not metrics:
            lines.append(f"- {CITY_LABELS.get(city, city)}: chưa có dữ liệu.")
            continue
        available_cities.append(city)
        values: list[str] = []
        aqi = metrics.get("us_aqi")
        if aqi:
            aqi_value = float(aqi["value"])
            standard = str(aqi.get("aqi_standard") or "US-AQI")
            values.append(f"AQI {_number(aqi_value)} ({_aqi_category(aqi_value, standard)})")
        for pollutant in ("pm25", "pm10"):
            row = metrics.get(pollutant)
            if row:
                values.append(f"{metric_label(pollutant)} {_number(float(row['value']))} µg/m³")
        times = [str(row.get("event_time")) for row in metrics.values() if row.get("event_time")]
        time_text = f"; thời điểm {max(times)}" if times else ""
        lines.append(f"- {CITY_LABELS.get(city, city)}: {', '.join(values)}{time_text}.")

    aqi_values = [(city, city_metrics[city]["us_aqi"]) for city in available_cities if "us_aqi" in city_metrics[city]]
    if len(aqi_values) >= 2:
        cleanest, cleanest_row = min(aqi_values, key=lambda item: float(item[1]["value"]))
        most_polluted, most_polluted_row = max(aqi_values, key=lambda item: float(item[1]["value"]))
        clean_value = float(cleanest_row["value"])
        polluted_value = float(most_polluted_row["value"])
        relative = ((polluted_value - clean_value) / clean_value * 100) if clean_value else None
        conclusion = (
            f"Kết luận: {CITY_LABELS.get(cleanest, cleanest)} đang có AQI thấp hơn, phù hợp hơn nếu ưu tiên không khí sạch. "
            f"{CITY_LABELS.get(most_polluted, most_polluted)} có AQI cao hơn"
        )
        if relative is not None:
            conclusion += f" khoảng {_number(relative)}% so với {CITY_LABELS.get(cleanest, cleanest)}"
        lines.append(conclusion + ".")
    else:
        lines.append("Lưu ý: chưa đủ AQI của cả hai thành phố để kết luận thành phố nào ít ô nhiễm hơn.")
    lines.append("Lưu ý: hệ thống không gọi đây là 'tỷ lệ ô nhiễm %' vì AQI và nồng độ bụi là các chỉ số khác nhau; phần trăm trên chỉ là mức chênh lệch AQI tương đối.")
    return "\n".join(lines)


def deterministic_answer(question: str, rows: list[dict[str, object]]) -> str:
    if not rows:
        return "Chưa có dữ liệu PM2.5/PM10 trong 7 ngày gần nhất. Hãy kiểm tra collector và khoảng thời gian trên dashboard."
    lowered = question.lower()
    mentioned = _mentioned_cities(question)
    if len(mentioned) >= 2:
        return comparison_answer(question, rows, mentioned)
    selected_city = mentioned[0] if mentioned else None
    folded = _fold_text(question)
    requested_pollutant = None
    if "pm2.5" in folded or "pm25" in folded or "bui min" in folded:
        requested_pollutant = "pm25"
    elif "pm10" in folded:
        requested_pollutant = "pm10"
    elif "aqi" in folded or "chat luong khong khi" in folded:
        requested_pollutant = "us_aqi"

    selected = [row for row in rows if (selected_city is None or row.get("city") == selected_city) and (requested_pollutant is None or row.get("pollutant") == requested_pollutant)]
    if not selected:
        scope = CITY_LABELS.get(selected_city, selected_city) if selected_city else "phạm vi đã hỏi"
        return f"Chưa có dữ liệu phù hợp cho {scope}."

    # Keep one newest value per city and pollutant before ranking or answering.
    newest = {}
    for row in selected:
        key = (row.get("city"), row.get("pollutant"))
        if key not in newest or str(row.get("event_time", "")) > str(newest[key].get("event_time", "")):
            newest[key] = row
    selected = list(newest.values())
    asks_highest = any(term in folded for term in ("cao nhat", "lon nhat", "nhieu nhat", "o nhiem nhat"))
    asks_lowest = any(term in folded for term in ("thap nhat", "nho nhat", "it nhat"))
    if asks_highest or asks_lowest:
        answer = (max if asks_highest else min)(selected, key=lambda row: float(row["value"]))
        label = CITY_LABELS.get(str(answer.get("city")), str(answer.get("city")))
        pollutant = metric_label(str(answer.get("pollutant", "")))
        return f"{label} có {pollutant} {'cao nhất' if asks_highest else 'thấp nhất'}: {answer.get('value')} µg/m³; thời điểm bản ghi: {answer.get('event_time')}."
    if selected_city and requested_pollutant:
        answer = selected[0]
        label = CITY_LABELS.get(selected_city, selected_city)
        unit = "AQI" if requested_pollutant == "us_aqi" else "µg/m³"
        return f"{label}: {metric_label(requested_pollutant)} = {answer.get('value')} {unit}; thời điểm bản ghi: {answer.get('event_time')}."
    if selected_city:
        label = CITY_LABELS.get(selected_city, selected_city)
        values = []
        for pollutant in ("us_aqi", "pm25", "pm10"):
            row = next((item for item in selected if item.get("pollutant") == pollutant), None)
            if row:
                unit = "AQI" if pollutant == "us_aqi" else "µg/m³"
                values.append(f"{metric_label(pollutant)} {row.get('value')} {unit}")
        if values:
            latest_time = max(str(row.get("event_time", "")) for row in selected)
            return f"{label} hiện có: {', '.join(values)}; thời điểm cập nhật gần nhất: {latest_time}."
    answer = max(selected, key=lambda row: float(row["value"]))
    label = CITY_LABELS.get(str(answer.get("city")), str(answer.get("city")))
    return f"Giá trị nổi bật trong dữ liệu: {label} — {metric_label(str(answer.get('pollutant', '')))} = {answer.get('value')}; thời điểm: {answer.get('event_time')}."


def ai_answer(question: str, context: str) -> str | None:
    return _ai_text("question", {"question": question, "context": context})


def dashboard_context_payload(context: object) -> dict[str, object]:
    raw = context if isinstance(context, dict) else {}
    dashboard = str(raw.get("dashboard", DEFAULT_DASHBOARD))
    definition = DASHBOARD_CONTEXTS.get(dashboard, DASHBOARD_CONTEXTS[DEFAULT_DASHBOARD])
    city = _context_city(raw)
    return {
        "dashboard": dashboard,
        "title": definition["title"],
        "description": definition["description"],
        "metrics": definition["metrics"],
        "city": city or "all",
        "hours": _context_hours(raw),
        "source": str(raw.get("source", "all") or "all"),
    }


def dashboard_scope_answer(question: str, context: dict[str, object]) -> str | None:
    folded = _fold_text(question)
    asks_scope = any(term in folded for term in (
        "chi so nao", "chi so gi", "theo doi gi", "noi ve gi", "phan tich gi", "dashboard nay",
    ))
    if not asks_scope:
        return None
    metrics = ", ".join(str(metric) for metric in context.get("metrics", []))
    return (
        f"Bạn đang xem dashboard {context.get('title')}. Dashboard này tập trung vào: {metrics}. "
        f"Nó gồm chỉ số hiện tại, xu hướng theo thời gian, so sánh thành phố, bản đồ và trạng thái cập nhật dữ liệu. "
        f"Ngữ cảnh hiện tại: {context.get('city')} trong khoảng {context.get('hours')} giờ gần nhất."
    )


def feature_question_answer(question: str, context: dict[str, object]) -> str | None:
    """Answer forecast/anomaly/travel intents with bounded real-data features."""
    folded = _fold_text(question)
    mentioned_cities = _mentioned_cities(question)
    if len(mentioned_cities) >= 2:
        # A named comparison must stay scoped to those locations; travel intent
        # must not turn it into a ranking across the entire database.
        return None
    city = mentioned_cities[0] if len(mentioned_cities) == 1 else (None if context.get("city") == "all" else str(context.get("city")))
    hours = int(context.get("hours") or DEFAULT_HOURS)
    try:
        if any(term in folded for term in ("nen di", "du lich", "chon thanh pho", "thanh pho nao sach", "di dau")):
            result = recommend_cities(hours, city=city, limit=3)
            recommendations = result.get("recommendations", [])
            if not recommendations:
                return "Chưa có đủ AQI/PM2.5/PM10 để tư vấn chọn thành phố trong phạm vi dashboard."
            lines = ["Gợi ý chọn thành phố theo dữ liệu đang có (không phải tư vấn y tế):"]
            for index, item in enumerate(recommendations, 1):
                metrics = []
                if item.get("aqi") is not None:
                    metrics.append(f"AQI {item['aqi']}")
                if item.get("pm25") is not None:
                    metrics.append(f"PM2.5 {item['pm25']} µg/m³")
                lines.append(f"{index}. {item['label']} — điểm sạch {item['score']}/100; {', '.join(metrics)}.")
            lines.append("Điểm càng cao nghĩa là mức ô nhiễm tương đối càng thấp trong nhóm có dữ liệu.")
            return "\n".join(lines)
        if any(term in folded for term in ("du bao", "du doan", "sap toi", "ngay mai", "trong 24 gio")):
            result = forecast_aqi(city=city, horizon=24, history_hours=max(24, hours))
            cities = result.get("cities", {})
            if not cities:
                return "Chưa có đủ chuỗi AQI theo giờ để tạo dự báo."
            selected = list(cities.values()) if city else list(cities.values())[:3]
            lines = ["Dự báo AQI 24 giờ tới (ước tính theo xu hướng lịch sử, không phải số đo chắc chắn):"]
            for item in selected:
                forecast = item["forecast"]
                end = forecast[-1]
                lines.append(f"- {item['label']}: hiện {item['current_aqi']} ({item['current_category']}), dự báo cuối kỳ khoảng {end['aqi']} ({end['category']}).")
            return "\n".join(lines)
        if any(term in folded for term in ("bat thuong", "dot bien", "tang dot bien", "giam bat thuong")):
            result = detect_anomalies(hours=max(24, hours), city=city)
            anomalies = result.get("anomalies", [])
            if not anomalies:
                return "Chưa phát hiện bất thường rõ rệt theo phương pháp median-MAD trên dữ liệu theo giờ trong khoảng đã chọn."
            lines = ["Các điểm bất thường đang phát hiện:"]
            for item in anomalies[:5]:
                metric = {"us_aqi": "AQI", "pm25": "PM2.5", "pm10": "PM10"}.get(str(item["pollutant"]), str(item["pollutant"]).upper())
                lines.append(f"- {item['label']} — {metric} {item['value']} ({item['direction']}, điểm bất thường {item['robust_score']}); thời điểm {item['time']}.")
            return "\n".join(lines)
    except Exception as error:
        return f"Chưa thể chạy phân tích mở rộng lúc này: {error}"
    return None


def health_payload() -> dict[str, object]:
    return {"status": "ok", "ai_configured": bool(os.getenv("AI_API_KEY", "").strip()), "model": os.getenv("AI_MODEL", "gpt-4o-mini"), "generated_at": iso_now()}


def refresh_summaries() -> None:
    for scope in ("overview", "aqi-trend", "particulate", "compare"):
        try:
            summary_for(scope, hours=DEFAULT_HOURS)
        except Exception as error:
            print(f"AI summary refresh failed for {scope}: {error}", flush=True)


def summary_worker(stop: threading.Event) -> None:
    while not stop.is_set():
        refresh_summaries()
        stop.wait(max(60, SUMMARY_REFRESH_SECONDS))


def email_alert_worker(stop: threading.Event) -> None:
    last_signature = ""
    last_sent_at = 0.0
    while not stop.is_set():
        if os.getenv("ALERT_EMAIL_ENABLED", "false").lower() in {"1", "true", "yes"}:
            try:
                alerts = collect_alerts(DEFAULT_HOURS)
                signature = json.dumps(alerts, sort_keys=True, ensure_ascii=False)
                now = time.time()
                if alerts and (signature != last_signature or now - last_sent_at >= ALERT_EMAIL_COOLDOWN_SECONDS):
                    result = send_alert_email(alerts)
                    print(f"Email alert check: sent={result.get('sent')} reason={result.get('reason', 'delivered')} alerts={len(alerts)}", flush=True)
                    last_signature, last_sent_at = signature, now
            except Exception as error:
                print(f"Email alert worker failed: {error}", flush=True)
        stop.wait(max(60, ALERT_EMAIL_REFRESH_SECONDS))


def _send_json(handler: BaseHTTPRequestHandler, payload: dict[str, object], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/health":
            _send_json(self, health_payload())
            return
        if parsed.path in {"/forecast", "/anomalies", "/recommendations", "/email/status", "/email/preview"}:
            try:
                params = parse_qs(parsed.query)
                if parsed.path == "/email/status":
                    _send_json(self, email_status())
                    return
                if parsed.path == "/email/preview":
                    alerts = collect_alerts(DEFAULT_HOURS)
                    _send_json(self, {"alerts": alerts, "preview": build_email_preview(alerts), "email": email_status()})
                    return
                city = normalize_city(params.get("city", [None])[0])
                hours = parse_hours(params.get("hours", [None])[0])
                if parsed.path == "/forecast":
                    horizon = max(1, min(48, int(params.get("horizon", ["24"])[0])))
                    _send_json(self, forecast_aqi(city=city, horizon=horizon, history_hours=hours))
                elif parsed.path == "/anomalies":
                    _send_json(self, detect_anomalies(hours=hours, city=city))
                else:
                    limit = max(1, min(20, int(params.get("limit", ["5"])[0])))
                    _send_json(self, recommend_cities(hours=hours, city=city, limit=limit))
            except Exception as error:
                _send_json(self, {"error": str(error)}, 400)
            return
        scope_by_path = {
            "/summary/overview": "overview",
            "/summary/aqi-trend": "aqi-trend",
            "/summary/particulate": "particulate",
            "/summary/compare": "compare",
        }
        if parsed.path not in scope_by_path:
            self.send_error(404)
            return
        try:
            params = parse_qs(parsed.query)
            city = normalize_city(params.get("city", [None])[0])
            hours = parse_hours(params.get("hours", [None])[0])
            _send_json(self, summary_for(scope_by_path[parsed.path], city, hours))
        except Exception as error:
            _send_json(self, {"error": str(error)}, 400)

    def do_POST(self):  # noqa: N802
        if self.path not in {"/ask", "/email/demo"}:
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                request_data = json.loads(raw.decode("utf-8"))
            except UnicodeDecodeError:
                request_data = json.loads(raw.decode("cp1258"))
            if self.path == "/email/demo":
                alerts = build_demo_alerts()
                preview = build_email_preview(alerts)
                wants_send = bool(request_data.get("send", False))
                demo_send_allowed = os.getenv("ALERT_EMAIL_DEMO_SEND", "false").lower() in {"1", "true", "yes"}
                if wants_send and demo_send_allowed:
                    result = send_alert_email(alerts)
                else:
                    result = {"sent": False, "reason": "demo_preview_only" if not demo_send_allowed else "send_not_requested", "preview": preview, "email": email_status()}
                _send_json(self, result)
                return
            question = str(request_data.get("question", "")).strip()
            if not question:
                raise ValueError("Câu hỏi không được để trống")
            requested_context = request_data.get("dashboard_context", {})
            context_info = dashboard_context_payload(requested_context)
            rows = latest_context(int(context_info["hours"]), None if context_info["city"] == "all" else str(context_info["city"]))
            fallback = feature_question_answer(question, context_info) or dashboard_scope_answer(question, context_info) or deterministic_answer(question, rows)
            ai_context = fallback + "\n\nNgữ cảnh dashboard hiện tại:\n" + json.dumps(context_info, ensure_ascii=False)
            # The model can invent a different city despite receiving grounded
            # rows. Keep exact city comparisons deterministic and data-bound.
            generated = None if len(_mentioned_cities(question)) >= 2 else ai_answer(question, ai_context)
            answer = generated or fallback
            mode = "ai" if generated else ("fallback" if os.getenv("AI_API_KEY", "").strip() else "data_summary")
            _send_json(self, {"answer": answer, "mode": mode, "dashboard_context": context_info})
        except Exception as error:
            _send_json(self, {"error": str(error)}, 500)

    def log_message(self, format, *args):  # noqa: A002
        return


def main() -> None:
    host = os.getenv("AI_HOST", "0.0.0.0")
    port = int(os.getenv("AI_PORT", "8090"))
    stop = threading.Event()
    if os.getenv("AI_AUTOPUBLISH", "true").lower() in {"1", "true", "yes"}:
        threading.Thread(target=summary_worker, args=(stop,), daemon=True).start()
    if os.getenv("ALERT_EMAIL_WORKER_ENABLED", "true").lower() in {"1", "true", "yes"}:
        threading.Thread(target=email_alert_worker, args=(stop,), daemon=True).start()
    print(f"Trợ lý chất lượng không khí: http://{host}:{port}", flush=True)
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()

