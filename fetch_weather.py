#!/usr/bin/env python3
"""
四川电力交易天气简报 v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
工程化原则：
  1. 容错金字塔 — 单站失败→跳过 / 单模型失败→降级 / 全失败→告警
  2. 输出双通道 — Markdown(WeChat) + HTML(Web ECharts)
  3. 质量标记 — 每份产出标注数据完整性和可信度
  4. 运行日志 — 循环日志记录每阶段耗时和失败详情
  5. 0 外部依赖 — chart 内嵌, HTML 自包含, Nginx 纯静态
"""

import json, yaml, sys, os, time, logging, traceback
from datetime import date, timedelta
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ─── 路径 ───
DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(DIR, "data")
LOG_DIR   = os.path.join(DIR, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ─── 日志 — 3 文件循环覆盖 ───
LOG_FILES = [os.path.join(LOG_DIR, f"weather_brief.log.{i}") for i in range(1, 4)]

def _setup_logging():
    """选取最旧日志文件写入"""
    log_file = min(LOG_FILES, key=lambda f: os.path.getmtime(f) if os.path.exists(f) else 0)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stderr)],
    )
    return logging.getLogger("weather_brief")

log = _setup_logging()

# ─── 配置加载 + 校验 ───
def _load_config():
    cfg_path = os.path.join(DIR, "config.yaml")
    if not os.path.exists(cfg_path):
        log.critical("config.yaml 不存在")
        sys.exit(1)
    cfg = yaml.safe_load(open(cfg_path))
    # 校验必要字段
    required = ["thresholds", "basin_lag", "load_cities", "reservoirs", "solar", "wind_farms"]
    missing = [k for k in required if k not in cfg]
    if missing:
        log.critical(f"config.yaml 缺少字段: {missing}")
        sys.exit(1)
    log.info(f"配置加载: {len(cfg.get('load_cities',[]))}城市 + {len(cfg.get('reservoirs',[]))}水库 + ...")
    return cfg

CFG = _load_config()
TH  = CFG["thresholds"]
LAG = CFG["basin_lag"]
FORECAST_DAYS = 5  # D → D+4
API_DAYS      = 5
FORECAST_URL  = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL   = "https://archive-api.open-meteo.com/v1/archive"
API_TIMEOUT   = 10
API_RETRIES   = 1

# ═══════════════════════════════════════════
# ① 数据层 — API 调用
# ═══════════════════════════════════════════

def _api_fetch(lat, lon, model=None, archive=False):
    """单站单模型拉取，带超时+重试。失败抛异常，由上层捕获。"""
    for attempt in range(API_RETRIES + 1):
        try:
            if archive:
                today = date.today()
                ly_start = (today.replace(year=today.year-1) - timedelta(days=1)).isoformat()
                ly_end   = (today.replace(year=today.year-1) + timedelta(days=FORECAST_DAYS-1)).isoformat()
                params = (f"latitude={lat}&longitude={lon}"
                          f"&start_date={ly_start}&end_date={ly_end}"
                          f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
                          f"&timezone=Asia/Shanghai")
                if model: params += f"&models={model}"
                url = f"{ARCHIVE_URL}?{params}"
            else:
                params = (f"latitude={lat}&longitude={lon}"
                          f"&hourly=temperature_2m,precipitation,shortwave_radiation,wind_speed_80m,cloud_cover"
                          f"&timezone=Asia/Shanghai&forecast_days={API_DAYS}")
                if model: params += f"&models={model}"
                url = f"{FORECAST_URL}?{params}"
            with urlopen(url, timeout=API_TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            if attempt < API_RETRIES:
                time.sleep(2)
    raise RuntimeError(f"API 失败 lat={lat} lon={lon} model={model} archive={archive}")

def _parse_forecast(data):
    """逐小时 → {date: {high,low,rain,rad,wind,cloud}}"""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    days = defaultdict(list)
    for i, t in enumerate(times):
        days[t[:10]].append({
            "temp": hourly.get("temperature_2m", [None])[i],
            "precip": hourly.get("precipitation", [None])[i],
            "rad": hourly.get("shortwave_radiation", [None])[i],
            "wind": hourly.get("wind_speed_80m", [None])[i],
            "cloud": hourly.get("cloud_cover", [None])[i],
        })
    result = {}
    for day, vals in sorted(days.items()):
        temps  = [v["temp"] for v in vals if v["temp"] is not None]
        rains  = [v["precip"] for v in vals if v["precip"] is not None]
        rads   = [v["rad"] for v in vals if v["rad"] is not None]
        winds  = [v["wind"] for v in vals if v["wind"] is not None]
        clouds = [v["cloud"] for v in vals if v["cloud"] is not None]
        result[day] = {
            "high":  max(temps) if temps else 0,
            "low":   min(temps) if temps else 0,
            "rain":  sum(rains) if rains else 0,
            "rad":   sum(rads)/len(rads) if rads else 0,
            "wind":  sum(winds)/len(winds)/3.6 if winds else 0,  # km/h→m/s
            "cloud": sum(clouds)/len(clouds) if clouds else 0,
        }
    return result

def _parse_archive(data):
    """日级档案 → {date: {high,low,rain}}"""
    daily = data.get("daily", {})
    times = daily.get("time", [])
    result = {}
    for i, t in enumerate(times):
        result[t] = {
            "high": daily.get("temperature_2m_max", [None])[i],
            "low":  daily.get("temperature_2m_min", [None])[i],
            "rain": daily.get("precipitation_sum", [None])[i],
        }
    return result

def _build_station_list():
    """从 config 构建统一站点列表 [{name, lat, lon, cat}]"""
    stations = []
    for st in CFG.get("load_cities", []):      stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"load"})
    for st in CFG.get("basin_hotspots", []):   stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"hotspot"})
    for st in CFG.get("industrial_zones", []): stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"industrial"})
    for st in CFG.get("reservoirs", []):       stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"reservoir"})
    for st in CFG.get("upstream", []):         stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"upstream"})
    for st in CFG.get("snowmelt", []):         stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"snowmelt"})
    for st in CFG.get("solar", []):            stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"solar"})
    for st in CFG.get("wind_farms", []):       stations.append({"name":st[0], "lat":st[1], "lon":st[2], "cat":"wind"})
    return stations

# ═══════════════════════════════════════════
# ② 核心拉取 + 容错合并
# ═══════════════════════════════════════════

def fetch_all():
    """拉取全站 ECMWF + CMA + Archive，带容错和质量追踪。
    
    返回: {
        "results": {name: {ecmwf:{date:{...}}, cma:{...}, archive:{...}}},
        "quality": {ecmwf_ok:N, cma_ok:N, archive_ok:N, total_stations:N, failed_stations:[...]}
    }
    """
    stations = _build_station_list()
    total = len(stations)
    quality = {"ecmwf_ok": 0, "cma_ok": 0, "archive_ok": 0, "total": total, "failed": []}
    results = {}
    
    def _one_station(st):
        r = {}
        # ECMWF
        try:
            r["ecmwf"] = _parse_forecast(_api_fetch(st["lat"], st["lon"], model="ecmwf_ifs"))
        except Exception:
            r["ecmwf"] = None
        # CMA
        try:
            r["cma"] = _parse_forecast(_api_fetch(st["lat"], st["lon"], model="cma_grapes_global"))
        except Exception:
            r["cma"] = None
        # Archive
        try:
            r["archive"] = _parse_archive(_api_fetch(st["lat"], st["lon"], model="ecmwf_ifs", archive=True))
        except Exception:
            r["archive"] = None
        return st["name"], r
    
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(_one_station, st): st["name"] for st in stations}
        for fut in as_completed(futures):
            try:
                name, r = fut.result()
                results[name] = r
                if r.get("ecmwf"): quality["ecmwf_ok"] += 1
                if r.get("cma"):   quality["cma_ok"] += 1
                if r.get("archive"): quality["archive_ok"] += 1
            except Exception:
                name = futures[fut]
                quality["failed"].append(name)
    
    quality["ecmwf_fail"] = total - quality["ecmwf_ok"]
    quality["cma_fail"]   = total - quality["cma_ok"]
    quality["archive_fail"] = total - quality["archive_ok"]
    return {"results": results, "quality": quality}

# ═══════════════════════════════════════════
# ③ 格式化辅助
# ═══════════════════════════════════════════

def _fc(results, name, model="ecmwf"):
    """安全取某站某模型的逐日数据"""
    r = results.get(name, {})
    m = r.get(model) or {}
    today = date.today()
    keys = [(today + timedelta(days=i)).isoformat() for i in range(FORECAST_DAYS)]  # D→D+4
    return [m.get(k, {}) for k in keys]

def _fv(results, name, model, field, default=0):
    """安全取某站某模型某字段的逐日值列表"""
    return [d.get(field, default) if d else default for d in _fc(results, name, model)]

def _archive_val(results, name, field):
    """去年同期某字段逐日值"""
    r = results.get(name, {})
    a = r.get("archive") or {}
    today = date.today()
    vals = []
    for i in range(FORECAST_DAYS):
        ly_key = (today.replace(year=today.year-1) + timedelta(days=i)).isoformat()  # D→D+4
        vals.append(a[ly_key].get(field) if ly_key in a else None)
    return vals

def _temp_icon(high):
    if high >= TH["temperature"]["forced_cooling"]: return "⚡"
    if high >= TH["temperature"]["cooling_support"]: return "🔥"
    if high >= TH["temperature"]["mild_cooling"]: return "🟡"
    return "🟢"

def _rain_dir(rain):
    if rain >= TH["rainfall"]["large"]:  return "↑较大"
    if rain >= TH["rainfall"]["medium"]: return "↑中幅"
    if rain >= TH["rainfall"]["small"]:  return "↑小幅"
    return "平稳"

def _solar_judge(rad):
    if rad >= TH["radiation"]["strong"]: return "压价明显"
    if rad >= TH["radiation"]["normal"]: return "正常"
    return "偏弱"

def _wind_judge(wind):
    if wind >= TH["wind"]["strong"]: return "强"
    if wind >= TH["wind"]["normal"]: return "正常"
    if wind >= TH["wind"]["low"]:    return "偏低"
    return "静风"

def _divergence(v1, v2):
    """双模型分歧检测"""
    if v1 is None or v2 is None: return False
    avg = (abs(v1) + abs(v2)) / 2
    if avg < 1: return False
    return abs(v1 - v2) / avg > 0.3

def _build_judgements(results):
    """生成综合研判文本 — Markdown 和 Web 共用"""
    cd_eh = _fv(results, "成都", "ecmwf", "high")
    cd_ch = _fv(results, "成都", "cma", "high")
    fc_line = TH["temperature"]["forced_cooling"]
    t0, t1, t2 = max(cd_eh[0], cd_ch[0]), max(cd_eh[1], cd_ch[1]), max(cd_eh[2], cd_ch[2])
    judgements = []
    
    if t0 >= fc_line and t1 >= fc_line - 2:
        s = f"❶ 今明高温支撑晚峰→D+1~D+2偏紧"
        if t2 < 30: s += "，后天降温→D+3负荷回落"
        judgements.append(s)
    elif t2 < 30:
        judgements.append(f"❶ 后天降温→D+3负荷回落")
    
    for rs in CFG.get("reservoirs", []):
        if len(rs) > 4 and rs[4]:
            er = _fv(results, rs[0], "ecmwf", "rain")[0]
            cr = _fv(results, rs[0], "cma", "rain")[0]
            avg = (er+cr)/2; lag = LAG.get(rs[3], "")
            if avg >= TH["rainfall"]["medium"]:
                div = " ⚠分歧" if _divergence(er, cr) else ""
                judgements.append(f"❷ {rs[0]}{lag}{avg:.0f}mm{div}→入库改善偏空")
                break
    
    total_rain = sum(_fv(results, r[0], "ecmwf", "rain")[0] for r in CFG.get("reservoirs", []))
    if total_rain < 30:
        judgements.append(f"❸ 降雨整体偏少→来水无突变")
    else:
        judgements.append(f"❸ 关注来水增加后的偏空压力")
    
    ly_high = _archive_val(results, "成都", "high")
    if ly_high[0] is not None:
        diff = max(cd_eh[0], cd_ch[0]) - ly_high[0]
        if abs(diff) >= 3:
            warmer = "强于" if diff > 0 else "弱于"
            judgements.append(f"❹ 今年高温{warmer}去年(+{abs(diff):.0f}°C→供需偏紧)")
    
    return judgements

def _quality_tag(q):
    e = q["ecmwf_ok"]; c = q["cma_ok"]; a = q["archive_ok"]; t = q["total"]
    ok_ratio = (e + c) / (2 * t) if t > 0 else 0
    if ok_ratio >= 0.95: grade = "高"
    elif ok_ratio >= 0.7: grade = "中"
    else: grade = f"低(E{e}/{t} C{c}/{t})"
    return f"E{e}/{t} C{c}/{t} A{a}/{t} | 可信度:{grade}"

# ═══════════════════════════════════════════
# ④ Markdown 简报生成
# ═══════════════════════════════════════════

def format_markdown(fetched):
    """生成 WeChat Markdown 简报"""
    results = fetched["results"]
    quality = fetched["quality"]
    today = date.today()
    today_str = today.strftime("%m-%d")
    td = today_str
    
    # ─── 全模型挂了的兜底 ───
    if quality["ecmwf_ok"] == 0 and quality["cma_ok"] == 0:
        return f"━━━ 四川交易天气简报 {td} ━━━\n\n⚠ 气象数据暂不可用\n请参考昨日报告或手动查看中央气象台\n\n生成时间: {td} 08:30"

    lines = [f"━━━ 四川交易天气简报 {td} ━━━"]
    
    # ─── 📌 概况 ───
    cd_eh = _fv(results, "成都", "ecmwf", "high")
    cd_ch = _fv(results, "成都", "cma", "high")
    
    hot_count = sum(1 for hs in CFG.get("basin_hotspots", [])
                    if max(_fv(results, hs[0], "ecmwf", "high")[0],
                           _fv(results, hs[0], "cma", "high")[0]) >= TH["temperature"]["forced_cooling"])
    
    max_r_st, max_r_v = "", 0
    for rs in CFG.get("reservoirs", []):
        er = _fv(results, rs[0], "ecmwf", "rain")[0]
        cr = _fv(results, rs[0], "cma", "rain")[0]
        avg = (er+cr)/2
        if avg > max_r_v: max_r_v = avg; max_r_st = rs[0]
    
    parts = []
    t0, t1, t2 = max(cd_eh[0], cd_ch[0]), max(cd_eh[1], cd_ch[1]), max(cd_eh[2], cd_ch[2])
    if t0 >= TH["temperature"]["cooling_support"]: parts.append("今明高温" if t1 >= TH["temperature"]["cooling_support"] else "今天高温")
    if t2 < 30: parts.append("后天降温")
    fc_label = TH["temperature"]["forced_cooling"]; parts.append(f"≥{fc_label}°C {hot_count}站")
    parts.append(f"{max_r_st}雨{max_r_v:.0f}mm" if max_r_st else "无明显降雨")
    lines.append(f"\n📌 概况  {' | '.join(parts)}")
    
    # ─── 🌡️ 负荷城市 ───
    lines.append(f"\n🌡️ 负荷城市（D→D+2 气温 ECMWF/CMA）")
    for st in CFG.get("load_cities", []):
        eh = _fv(results, st[0], "ecmwf", "high")
        ch = _fv(results, st[0], "cma", "high")
        if all(h==0 for h in eh[:3]+ch[:3]): continue
        e_part = "/".join(f"{h:.0f}" for h in eh[:3]) if any(h for h in eh[:3]) else "-"
        c_part = "/".join(f"{h:.0f}" for h in ch[:3]) if any(h for h in ch[:3]) else "-"
        icons = "".join(_temp_icon(max(eh[i], ch[i])) for i in range(3) if eh[i] or ch[i])
        lines.append(f"  {st[0]:4s}  {e_part}  |  {c_part}  {icons}")
    
    hs_parts = []
    for st in CFG.get("basin_hotspots", []):
        eh = _fv(results, st[0], "ecmwf", "high")[0]
        ch = _fv(results, st[0], "cma", "high")[0]
        hs_parts.append(f"{st[0]}{max(eh,ch):.0f}")
    lines.append(f"  盆地: {' '.join(hs_parts)}°C")
    
    ind_parts = []
    for st in CFG.get("industrial_zones", []):
        eh = _fv(results, st[0], "ecmwf", "high")[0]
        ch = _fv(results, st[0], "cma", "high")[0]
        ind_parts.append(f"{st[0]}{max(eh,ch):.0f}")
    lines.append(f"  工业区: {' '.join(ind_parts)}°C")
    lines.append(f"  ≥{fc_label}°C站次: 今{hot_count}站")
    
    # ─── 💧 水电区域 ───
    lines.append(f"\n💧 水电区域（降雨mm→入库滞后 / ECMWF·CMA）")
    for rs in CFG.get("reservoirs", []):
        er = _fv(results, rs[0], "ecmwf", "rain")[0]
        cr = _fv(results, rs[0], "cma", "rain")[0]
        basin = rs[3] if len(rs) > 3 else ""
        lag = LAG.get(basin, "")
        edir = _rain_dir(er); cdir = _rain_dir(cr)
        div = " ⚠" if _divergence(er, cr) else ""
        pillar = "🔷" if len(rs) > 4 and rs[4] else "  "
        lines.append(f"  {pillar}{rs[0]:6s}  E{er:4.0f}/C{cr:3.0f}mm  {lag:5s}  {edir}/{cdir}{div}")
    
    up_parts = []
    for st in CFG.get("upstream", []):
        er = _fv(results, st[0], "ecmwf", "rain")[0]
        cr = _fv(results, st[0], "cma", "rain")[0]
        up_parts.append(f"{st[0]}{max(er,cr):.0f}")
    lines.append(f"  上游: {' '.join(up_parts)}mm")
    
    snow_parts = []; snow_ok = True
    for st in CFG.get("snowmelt", []):
        el = _fv(results, st[0], "ecmwf", "low")[0]; cl = _fv(results, st[0], "cma", "low")[0]
        mn = min(el, cl); snow_parts.append(f"{st[0]}{mn:.0f}")
        if mn < 0: snow_ok = False
    lines.append(f"  🏔 融雪: {' '.join(snow_parts)}°C → {'正常' if snow_ok else '⚠低温'}")
    
    # ─── ☀️ 光伏 ───
    lines.append(f"\n☀️ 光伏（辐照度W/m² D→D+1 / ECMWF·CMA）")
    for st in CFG.get("solar", []):
        er = _fv(results, st[0], "ecmwf", "rad"); cr = _fv(results, st[0], "cma", "rad")
        lines.append(f"  {st[0]:6s}  E{er[0]:4.0f}→{er[1]:4.0f}/C{cr[0]:4.0f}→{cr[1]:4.0f}  {_solar_judge(er[0])}/{_solar_judge(cr[0])}")
    
    # ─── 💨 风电 ───
    lines.append(f"\n💨 风电（风速m/s D→D+1 / ECMWF·CMA）")
    for st in CFG.get("wind_farms", []):
        ew = _fv(results, st[0], "ecmwf", "wind"); cw = _fv(results, st[0], "cma", "wind")
        lines.append(f"  {st[0]:6s}  E{ew[0]:3.1f}→{ew[1]:3.1f}/C{cw[0]:3.1f}→{cw[1]:3.1f}  {_wind_judge(ew[0])}/{_wind_judge(cw[0])}")
    
    # ─── 📊 综合研判 ───
    lines.append(f"\n📊 综合研判")
    for j in _build_judgements(results):
        lines.append(f"  {j}")
    
    lines.append(f"\n数据: ECMWF+CMA | {_quality_tag(quality)} | {td} 08:30")
    return "\n".join(lines)


# ═══════════════════════════════════════════
# ⑤ 仪表盘数据导出 + HTML 生成
# ═══════════════════════════════════════════

def _build_dashboard_data(fetched):
    """构建 ECharts 仪表盘所需的完整 JSON 数据"""
    results = fetched["results"]
    today = date.today()
    day_labels = [(today + timedelta(days=i)).strftime("%m/%d") for i in range(FORECAST_DAYS)]  # D→D+4
    
    # ── 温度卡片 ──
    temp_cards = []
    for st in CFG.get("load_cities", []):
        eh = _fv(results, st[0], "ecmwf", "high")
        ch = _fv(results, st[0], "cma", "high")
        if all(h==0 for h in eh[:FORECAST_DAYS]) and all(h==0 for h in ch[:FORECAST_DAYS]): continue
        mx = max(eh[0], ch[0])
        icon = _temp_icon(mx)
        temp_cards.append({"name": st[0], "temp": int(mx), "icon": icon})
    
    # ── 温度趋势(D→D+4) ──
    temp_trend = {"days": day_labels, "series": []}
    for st in CFG.get("load_cities", []):
        eh = _fv(results, st[0], "ecmwf", "high")
        ch = _fv(results, st[0], "cma", "high")
        if all(h==0 for h in eh[:FORECAST_DAYS]) and all(h==0 for h in ch[:FORECAST_DAYS]): continue
        temp_trend["series"].append({"name": f"{st[0]}(E)", "data": [round(h,1) for h in eh[:FORECAST_DAYS]], "type": "ecmwf"})
        temp_trend["series"].append({"name": f"{st[0]}(C)", "data": [round(h,1) for h in ch[:FORECAST_DAYS]], "type": "cma"})
    
    # ── 水库降雨 ──
    rain_data = {"stations": [], "ecmwf": [], "cma": []}
    for rs in CFG.get("reservoirs", []):
        er = _fv(results, rs[0], "ecmwf", "rain")[0]
        cr = _fv(results, rs[0], "cma", "rain")[0]
        rain_data["stations"].append(rs[0])
        rain_data["ecmwf"].append(round(max(er,0), 1))
        rain_data["cma"].append(round(max(cr,0), 1))
    
    # ── 融雪 ──
    snow = []
    snow_ok = True
    for st in CFG.get("snowmelt", []):
        el = _fv(results, st[0], "ecmwf", "low")[0]
        cl = _fv(results, st[0], "cma", "low")[0]
        mn = int(min(el, cl))
        if mn < 0: snow_ok = False
        snow.append({"name": st[0], "temp": mn})
    
    # ── 上游来水 ──
    upstream = []
    for st in CFG.get("upstream", []):
        er = _fv(results, st[0], "ecmwf", "rain")[0]
        cr = _fv(results, st[0], "cma", "rain")[0]
        upstream.append({"name": st[0], "rain": round(max(er,cr), 1)})
    
    # ── 光伏 ──
    solar = {"days": day_labels, "series": []}
    for st in CFG.get("solar", []):
        er = _fv(results, st[0], "ecmwf", "rad")
        cr = _fv(results, st[0], "cma", "rad")
        solar["series"].append({"name": f"{st[0]}(E)", "data": [round(v) for v in er[:FORECAST_DAYS]], "type": "ecmwf"})
        solar["series"].append({"name": f"{st[0]}(C)", "data": [round(v) for v in cr[:FORECAST_DAYS]], "type": "cma"})
    
    # ── 风电 ──
    wind = {"days": day_labels, "series": []}
    for st in CFG.get("wind_farms", []):
        ew = _fv(results, st[0], "ecmwf", "wind")
        cw = _fv(results, st[0], "cma", "wind")
        wind["series"].append({"name": f"{st[0]}(E)", "data": [round(v,1) for v in ew[:FORECAST_DAYS]], "type": "ecmwf"})
        wind["series"].append({"name": f"{st[0]}(C)", "data": [round(v,1) for v in cw[:FORECAST_DAYS]], "type": "cma"})
    
    # ── 研判文本 ──
    judgements = _build_judgements(results)
    
    # ── 盆地高温 ──
    hotspots = []
    for hs in CFG.get("basin_hotspots", []):
        eh = _fv(results, hs[0], "ecmwf", "high")[0]
        ch = _fv(results, hs[0], "cma", "high")[0]
        hotspots.append({"name": hs[0], "temp": int(max(eh, ch))})
    
    return {
        "date": today.strftime("%m/%d"),
        "quality": _quality_tag(fetched["quality"]),
        "elapsed": "—",
        "temp_cards": temp_cards,
        "temp_trend": temp_trend,
        "rain_data": rain_data,
        "snow": snow,
        "snow_ok": snow_ok,
        "upstream": upstream,
        "solar": solar,
        "wind": wind,
        "hotspots": hotspots,
        "judgements": judgements,
    }


def generate_html(fetched, elapsed=0.0):
    """生成 ECharts 仪表盘 HTML。自包含，单文件。"""
    data = _build_dashboard_data(fetched)
    data["elapsed"] = f"{elapsed:.0f}s"
    data_json = json.dumps(data, ensure_ascii=False)
    
    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>四川交易天气简报 {data["date"]}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0f0f1a;color:#d0d0d0;font:14px/1.6 -apple-system,PingFang SC,Microsoft YaHei,sans-serif;min-height:100vh}}
.header{{background:linear-gradient(135deg,#1a1a3e,#0d1b3e);padding:20px 32px;display:flex;align-items:center;justify-content:space-between;border-bottom:2px solid #2a2a4a;flex-wrap:wrap;gap:12px}}
.header h1{{font-size:22px;color:#fff;letter-spacing:1px}}
.header .meta{{display:flex;gap:16px;font-size:13px;color:#aaa;flex-wrap:wrap}}
.header .meta span{{background:#1e2d4a;padding:4px 12px;border-radius:5px}}
.container{{max-width:1300px;margin:0 auto;padding:16px 24px}}

.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:20px}}
.card{{background:linear-gradient(135deg,#1e2d4a,#162040);border-radius:12px;padding:14px 16px;text-align:center;border:1px solid #2a3a5a;transition:all 0.2s}}
.card:hover{{border-color:#4FC3F7;transform:translateY(-2px);box-shadow:0 4px 16px rgba(79,195,247,.15)}}
.card .name{{font-size:13px;color:#aaa;margin-bottom:6px}}
.card .temp{{font-size:32px;font-weight:700;margin:6px 0}}
.card .icon{{font-size:14px}}
.card.hot .temp{{color:#FF5252}}
.card.warm .temp{{color:#FFB74D}}
.card.mild .temp{{color:#FFD740}}
.card.cool .temp{{color:#4FC3F7}}

.chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}}
.chart-box{{background:#161625;border-radius:12px;padding:16px;border:1px solid #2a2a4a}}
.chart-box.full{{grid-column:1/-1}}
.chart-box h3{{font-size:15px;color:#ccc;margin-bottom:10px;font-weight:500}}
.chart-box .chart{{width:100%;height:300px}}
.chart-box .chart.tall{{height:360px}}

.info-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:16px}}
.info-card{{background:#161625;border-radius:10px;padding:14px 18px;border:1px solid #2a2a4a}}
.info-card h4{{font-size:13px;color:#888;margin-bottom:8px}}
.info-card .val{{font-size:14px;color:#ddd;line-height:1.7}}

.judge{{background:linear-gradient(135deg,#1a2a1a,#1a1a2e);border-radius:12px;padding:20px 24px;border:1px solid #2a4a2a;margin-bottom:16px}}
.judge h3{{font-size:15px;color:#81C784;margin-bottom:12px}}
.judge p{{font-size:14px;line-height:2;color:#c0c0c0;margin:3px 0}}

.footer{{text-align:center;color:#555;font-size:12px;padding:20px 0 32px}}

/* 平板 */
@media(max-width:1024px){{
  .container{{padding:12px 16px}}
  .header{{padding:16px 20px}}
  .chart-box .chart{{height:260px}}
  .chart-box .chart.tall{{height:300px}}
}}

/* 手机 */
@media(max-width:768px){{
  .chart-grid{{grid-template-columns:1fr}}
  .cards{{grid-template-columns:repeat(4,1fr)}}
  .card .temp{{font-size:22px}}
  .header h1{{font-size:18px}}
  .header{{flex-direction:column;align-items:flex-start;padding:14px 16px}}
  .header .meta{{gap:8px;font-size:11px}}
  .chart-box .chart{{height:240px}}
  .chart-box .chart.tall{{height:260px}}
  .info-row{{grid-template-columns:1fr}}
}}

/* 大屏 */
@media(min-width:1600px){{
  .container{{max-width:1500px}}
  .cards{{grid-template-columns:repeat(8,1fr)}}
  .chart-grid{{grid-template-columns:1fr 1fr}}
  .chart-box .chart{{height:340px}}
  .chart-box .chart.tall{{height:400px}}
}}
</style>
</head>
<body>
<div class="header">
  <h1>🌤️ 四川电力交易天气简报 {data["date"]}</h1>
  <div class="meta">
    <span>ECMWF + CMA</span><span>{data["quality"]}</span><span>耗时 {data["elapsed"]}</span>
  </div>
</div>
<div class="container">

<!-- 温度卡片 -->
<div class="cards">
'''

    for card in data["temp_cards"]:
        t = card["temp"]
        fc = TH["temperature"]["forced_cooling"]
        cs = TH["temperature"]["cooling_support"]
        mc = TH["temperature"]["mild_cooling"]
        cls = "hot" if t >= fc else "warm" if t >= cs else "mild" if t >= mc else "cool"
        html += f'<div class="card {cls}"><div class="name">{card["name"]}</div><div class="temp">{t}°</div><div class="icon">{card["icon"]}</div></div>\n'

    html += '</div>\n\n'

    # ── 温度趋势图 ──
    html += '''<div class="chart-grid">
<div class="chart-box full"><h3>📈 负荷城市 D→D+4 气温热力图 (ECMWF/CMA取高)</h3>
<div class="chart tall" id="chart_temp"></div></div>
</div>

<!-- 水库 -->
<div class="chart-grid">
<div class="chart-box full"><h3>💧 水库 当日降雨 (ECMWF深蓝 / CMA浅蓝)</h3>
<div class="chart" id="chart_rain"></div></div>
</div>

<!-- 光伏 -->
<div class="chart-grid">
<div class="chart-box full"><h3>☀️ 光伏 D→D+4 (辐照度 W/m²)</h3>
<div class="chart" id="chart_solar"></div></div>
</div>

<!-- 风电 -->
<div class="chart-grid">
<div class="chart-box full"><h3>💨 风电 D→D+4 (风速 m/s)</h3>
<div class="chart" id="chart_wind"></div></div>
</div>

<!-- 补充信息行 -->
<div class="info-row">
'''

    # 盆地高温
    hs_text = "  ".join(f"{h['name']}{h['temp']}°" for h in data["hotspots"])
    html += f'<div class="info-card"><h4>🔥 盆地腹地高温</h4><div class="val">{hs_text}</div></div>\n'

    # 融雪
    sn_text = "  ".join(f"{s['name']}{s['temp']}°" for s in data["snow"])
    sn_status = "正常" if data["snow_ok"] else "⚠️低温"
    html += f'<div class="info-card"><h4>🏔 融雪监测</h4><div class="val">{sn_text}<br>→ {sn_status}</div></div>\n'

    # 上游来水
    up_text = "  ".join(f"{u['name']}{u['rain']:.0f}mm" for u in data["upstream"])
    html += f'<div class="info-card"><h4>💧 上游来水区</h4><div class="val">{up_text}<br>→ 来水温和</div></div>\n'

    html += '</div>\n\n'

    # ── 综合研判 ──
    if data["judgements"]:
        html += '<div class="judge"><h3>📊 综合研判</h3>\n'
        for j in data["judgements"]:
            html += f'<p>{j}</p>\n'
        html += '</div>\n\n'

    # ── ECharts 图表脚本 ──
    # 动态构建新能源系列（所有光伏+风电站）
    _solar_colors = ['#FFA726','#FFCC80','#FF9800','#FFE0B2','#F57C00','#FFECB3']
    _wind_colors  = ['#4FC3F7','#B3E5FC','#29B6F6','#E1F5FE','#03A9F4','#BBDEFB']
    _solar_js_parts = []
    for _si, _s in enumerate(data["solar"]["series"]):
        _c = _solar_colors[_si % len(_solar_colors)]
        _name = _s['name'].replace("'", "\\'")
        _data = json.dumps(_s['data'])
        _js = "{name:'%s',type:'bar',data:%s,itemStyle:{color:'%s',borderRadius:[3,3,0,0]},barCategoryGap:'20%%'}" % (_name, _data, _c)
        _solar_js_parts.append(_js)
    _solar_series_js = ",\n    ".join(_solar_js_parts)
    
    _wind_js_parts = []
    for _wi, _w in enumerate(data["wind"]["series"]):
        _c = _wind_colors[_wi % len(_wind_colors)]
        _name = _w['name'].replace("'", "\\'")
        _data = json.dumps(_w['data'])
        _style = "solid" if _w["type"] == "ecmwf" else "dashed"
        _js = "{name:'%s',type:'line',data:%s,lineStyle:{color:'%s',type:'%s',width:1.2},symbol:'circle',symbolSize:4}" % (_name, _data, _c, _style)
        _wind_js_parts.append(_js)
    _wind_series_js = ",\n    ".join(_wind_js_parts)
    
    st_count = len(_build_station_list())
    html += f'<div class="footer">ECMWF IFS + CMA GRAPES · {st_count}站 · {data["date"]} 08:30</div>'
    
    # ── ECharts 图表脚本 ──
    html += f'''</div>
<script>
const D = {data_json};

// 配色
const C_E = '#4FC3F7', C_C = '#FFB74D', C_BG = '#161625';

function makeChart(id, option) {{
  const el = document.getElementById(id);
  if (!el) return;
  const chart = echarts.init(el, null, {{renderer:'canvas'}});
  option.backgroundColor = C_BG;
  option.textStyle = {{color:'#aaa',fontSize:11}};
  chart.setOption(option);
  window.addEventListener('resize', ()=>chart.resize());
}}

// ① 温度趋势（热力图）
const _raw = D.temp_trend.series;
const _cityNames = [...new Set(_raw.map(function(s){{return s.name.replace(/\\(.\\)/,'');}}))];
const _heatData = [];
_cityNames.forEach(function(name, ci){{
  const e = _raw.find(function(s){{return s.name===name+'(E)';}})||{{data:[]}};
  const c = _raw.find(function(s){{return s.name===name+'(C)';}})||{{data:[]}};
  D.temp_trend.days.forEach(function(day, di){{
    _heatData.push([di, ci, Math.round(Math.max(e.data[di]||0, c.data[di]||0))]);
  }});
}});
makeChart('chart_temp', {{
  tooltip: {{formatter:function(p){{return p.data[2]+'°C  '+_cityNames[p.data[1]]+'  '+D.temp_trend.days[p.data[0]];}}}},
  grid: {{top:10,right:30,bottom:30,left:50}},
  xAxis: {{type:'category',data:D.temp_trend.days,axisLabel:{{fontSize:11,color:'#aaa'}},position:'top'}},
  yAxis: {{type:'category',data:_cityNames,axisLabel:{{fontSize:12,color:'#ddd'}},inverse:true}},
  visualMap: {{
    min:20,max:42,calculable:false,orient:'vertical',right:10,top:20,bottom:20,
    inRange:{{color:['#4FC3F7','#AED581','#FFD54F','#FF8A65','#FF5252']}},
    text:['高','低'],textStyle:{{color:'#aaa',fontSize:10}}
  }},
  series: [{{
    type:'heatmap',data:_heatData,label:{{show:true,formatter:function(p){{return p.data[2];}},fontSize:14,color:'#fff'}},
    emphasis:{{itemStyle:{{shadowBlur:10,shadowColor:'rgba(0,0,0,.5)'}}}}
  }}]
}});

// ② 水库降雨
makeChart('chart_rain', {{
  tooltip: {{trigger:'axis'}},
  grid: {{top:10,right:20,bottom:50,left:40}},
  xAxis: {{type:'category',data:D.rain_data.stations,axisLabel:{{rotate:30,fontSize:10,color:'#aaa'}}}},
  yAxis: {{type:'value',name:'mm'}},
  legend: {{bottom:0,textStyle:{{color:'#aaa',fontSize:10}}}},
  series: [
    {{name:'ECMWF',type:'bar',data:D.rain_data.ecmwf,itemStyle:{{color:C_E,borderRadius:[3,3,0,0]}},barGap:'10%'}},
    {{name:'CMA',type:'bar',data:D.rain_data.cma,itemStyle:{{color:C_C+'88',borderColor:C_C,borderRadius:[3,3,0,0]}}}}
  ]
}});

// ③ 光伏（所有站）
makeChart('chart_solar', {{
  tooltip: {{trigger:'axis'}},
  legend: {{type:'scroll',bottom:0,textStyle:{{color:'#aaa',fontSize:9}}}},
  grid: {{top:10,right:30,bottom:45,left:45}},
  xAxis: {{type:'category',data:D.solar.days}},
  yAxis: {{type:'value',name:'W/m²'}},
  series: [
    {_solar_series_js}
  ]
}});

// ④ 风电（所有站）
makeChart('chart_wind', {{
  tooltip: {{trigger:'axis'}},
  legend: {{type:'scroll',bottom:0,textStyle:{{color:'#aaa',fontSize:9}}}},
  grid: {{top:10,right:30,bottom:45,left:45}},
  xAxis: {{type:'category',data:D.wind.days}},
  yAxis: {{type:'value',name:'m/s'}},
  series: [
    {_wind_series_js}
  ]
}});
</script>
</body>
</html>'''
    return html


# ═══════════════════════════════════════════
# ⑥ 主入口
# ═══════════════════════════════════════════

def main():
    t0 = time.monotonic()
    log.info("=" * 40)
    log.info("天气简报开始")
    
    # ① 拉取
    t1 = time.monotonic()
    fetched = fetch_all()
    quality = fetched["quality"]
    dt_fetch = time.monotonic() - t1
    log.info(f"数据拉取: {dt_fetch:.1f}s | {_quality_tag(quality)}")
    if quality["failed"]:
        log.warning(f"失败站点: {quality['failed']}")
    
    # ② Markdown
    t2 = time.monotonic()
    md = format_markdown(fetched)
    md_path = os.path.join(DATA_DIR, "weather_brief_latest.md")
    with open(md_path, "w") as f:
        f.write(md)
    dt_md = time.monotonic() - t2
    log.info(f"Markdown: {len(md)}字 ({dt_md:.1f}s)")
    
    # ③ HTML（内嵌 ECharts 仪表盘）
    t3 = time.monotonic()
    html = generate_html(fetched, elapsed=dt_fetch)
    html_path = os.path.join(DATA_DIR, "index.html")
    with open(html_path, "w") as f:
        f.write(html)
    dt_html = time.monotonic() - t3
    log.info(f"HTML: {len(html)}字 ({dt_html:.1f}s)")
    
    # ④ 完成
    dt_total = time.monotonic() - t0
    log.info(f"完成: {dt_total:.1f}s | MD={md_path} | HTML={html_path}")
    
    # 输出 Markdown 到 stdout（供 cron 推送）
    print(md)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.critical(f"脚本异常退出:\n{traceback.format_exc()}")
        # 最后兜底——输出一行告警
        print(f"⚠ 天气简报生成失败 ({date.today().strftime('%m-%d')} 08:30)")
        sys.exit(1)
