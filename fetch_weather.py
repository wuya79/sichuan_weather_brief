#!/usr/bin/env python3
"""
四川电力交易天气简报 v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
工程化原则：
  1. 容错金字塔 — 单站失败→跳过 / 单模型失败→降级 / 全失败→告警
  2. 输出三通道 — Markdown(WeChat) + HTML(Web) + chart PNG
  3. 质量标记 — 每份产出标注数据完整性和可信度
  4. 运行日志 — 循环日志记录每阶段耗时和失败详情
  5. 0 外部依赖 — chart 内嵌, HTML 自包含, Nginx 纯静态
"""

import json, yaml, sys, os, time, logging, traceback
from datetime import datetime, date, timedelta
from urllib.request import urlopen
from urllib.error import URLError
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeout
from collections import defaultdict

# ─── 路径 ───
DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(DIR, "data")
CHART_DIR = os.path.join(DATA_DIR, "charts")
LOG_DIR   = os.path.join(DIR, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CHART_DIR, exist_ok=True)
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
FORECAST_DAYS = 4  # D → D+3
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
                ly_start = (today.replace(year=today.year-1) - timedelta(days=2)).isoformat()
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
                          f"&timezone=Asia/Shanghai&forecast_days={FORECAST_DAYS}")
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
    keys = [(today + timedelta(days=i)).isoformat() for i in range(FORECAST_DAYS)]
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
        ly_key = (today.replace(year=today.year-1) + timedelta(days=i)).isoformat()
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

def _quality_tag(q):
    """数据质量一行标记"""
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
                           _fv(results, hs[0], "cma", "high")[0]) >= 35)
    
    max_r_st, max_r_v = "", 0
    for rs in CFG.get("reservoirs", []):
        er = _fv(results, rs[0], "ecmwf", "rain")[0]
        cr = _fv(results, rs[0], "cma", "rain")[0]
        avg = (er+cr)/2 if er is not None and cr is not None else max(er or 0, cr or 0)
        if avg > max_r_v: max_r_v = avg; max_r_st = rs[0]
    
    parts = []
    t0, t1, t2 = max(cd_eh[0], cd_ch[0]), max(cd_eh[1], cd_ch[1]), max(cd_eh[2], cd_ch[2])
    if t0 >= 33: parts.append("今明高温" if t1 >= 33 else "今天高温")
    if t2 < 30: parts.append("后天降温")
    parts.append(f"≥35°C {hot_count}站")
    parts.append(f"{max_r_st}雨{max_r_v:.0f}mm" if max_r_st else "无明显降雨")
    lines.append(f"\n📌 概况  {' | '.join(parts)}")
    
    # ─── 🌡️ 负荷城市 ───
    lines.append(f"\n🌡️ 负荷城市（D→D+2 气温 ECMWF/CMA）")
    for st in CFG.get("load_cities", []):
        eh = _fv(results, st[0], "ecmwf", "high")
        ch = _fv(results, st[0], "cma", "high")
        if all(h==0 for h in eh[:3]+ch[:3]): continue
        e_part = "/".join(f"{h:.0f}" for h in eh[:3]) if eh[0] else "-"
        c_part = "/".join(f"{h:.0f}" for h in ch[:3]) if ch[0] else "-"
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
    lines.append(f"  ≥35°C站次: 今{hot_count}站")
    
    # ─── 💧 水电区域 ───
    lines.append(f"\n💧 水电区域（降雨mm→入库滞后 / ECMWF·CMA）")
    reservoir_cfg = {r[0]: r for r in CFG.get("reservoirs", [])}
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
    judgements = []
    n = 0
    
    # 负荷趋势
    if t0 >= 35 and t1 >= 33:
        n+=1; judgements.append(f"❶ 今明高温支撑晚峰→D+1~D+2偏紧")
    if t2 < 30:
        if not judgements: n+=1; judgements.append(f"❶ ")
        judgements[-1] += "，后天降温→D+3负荷回落" if "今明" in judgements[-1] else "后天降温→D+3负荷回落"
    
    # 来水
    for rs in CFG.get("reservoirs", []):
        if len(rs) > 4 and rs[4]:  # 龙头站
            er = _fv(results, rs[0], "ecmwf", "rain")[0]
            cr = _fv(results, rs[0], "cma", "rain")[0]
            avg = (er+cr)/2; lag = LAG.get(rs[3], "")
            if avg >= TH["rainfall"]["medium"]:
                n+=1; div = " ⚠分歧" if _divergence(er, cr) else ""
                judgements.append(f"❷ {rs[0]}{lag}{avg:.0f}mm{div}→入库改善偏空")
                break
    
    # 整体来水
    total_rain = sum(_fv(results, r[0], "ecmwf", "rain")[0] for r in CFG.get("reservoirs", []))
    n+=1
    if total_rain < 30:
        judgements.append(f"❸ 降雨整体偏少→来水无突变")
    else:
        judgements.append(f"❸ 关注来水增加后的偏空压力")
    
    # 去年同期
    ly_high = _archive_val(results, "成都", "high")
    if ly_high[0] is not None:
        diff = max(cd_eh[0], cd_ch[0]) - ly_high[0]
        if abs(diff) >= 3:
            n+=1
            judgements.append(f"❹ 今年高温{'强于' if diff>0 else '弱于'}去年(+{abs(diff):.0f}°C→供需偏紧)")
    
    for j in judgements:
        lines.append(f"  {j}")
    
    lines.append(f"\n数据: ECMWF+CMA | {_quality_tag(quality)} | {td} 08:30")
    return "\n".join(lines)


# ═══════════════════════════════════════════
# ⑤ 图表生成（matplotlib）
# ═══════════════════════════════════════════

def _matplotlib_setup():
    """配置 matplotlib 中文 + 暗色风格，失败则跳过图表"""
    try:
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib import pyplot as plt
        from matplotlib.font_manager import FontProperties
        # 中文字体
        _font_paths = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        ]
        _fp = None
        for _p in _font_paths:
            if os.path.exists(_p):
                _fp = FontProperties(fname=_p)
                break
        if _fp is None:
            log.warning("中文字体未找到，图表使用英文")
        # 暗色风格
        plt.style.use("dark_background")
        return plt, _fp
    except Exception as e:
        log.warning(f"matplotlib 初始化失败: {e}")
        return None, None


def generate_charts(fetched):
    """生成 3 张 PNG 图表。失败不崩主流程。"""
    plt, fp = _matplotlib_setup()
    if plt is None:
        log.warning("matplotlib 不可用，跳过图表生成")
        return []
    
    results = fetched["results"]
    today = date.today()
    day_labels = [(today + timedelta(days=i)).strftime("%m/%d") for i in range(FORECAST_DAYS)]
    x = range(len(day_labels))
    generated = []
    colors = {"ecmwf": "#4FC3F7", "cma": "#FFB74D"}  # 色盲友好
    
    # ── 图1: 负荷城市温度趋势 ──
    try:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        ax.set_title("负荷城市 D→D+3 气温趋势" + ("" if fp else " (ECMWF solid / CMA dashed)"), 
                     fontproperties=fp, fontsize=13, pad=12)
        for st in CFG["load_cities"]:
            eh = _fv(results, st[0], "ecmwf", "high")
            ch = _fv(results, st[0], "cma", "high")
            if all(h==0 for h in eh[:4]): continue
            ax.plot(x, eh[:4], color=colors["ecmwf"], linewidth=1.5, marker="o", markersize=4, label=f"{st[0]}(E)")
            ax.plot(x, ch[:4], color=colors["cma"], linewidth=1, linestyle="--", marker="s", markersize=3, alpha=0.7, label=f"{st[0]}(C)")
        ax.axhline(35, color="#EF5350", linewidth=1, linestyle=":", alpha=0.5)
        ax.text(3.5, 35.5, "35°C 强制冷线", color="#EF5350", fontsize=8, fontproperties=fp)
        ax.set_xticks(x); ax.set_xticklabels(day_labels, fontproperties=fp)
        ax.set_ylabel("°C", fontproperties=fp)
        ax.legend(loc="upper right", fontsize=7, ncol=2, prop=fp)
        ax.grid(alpha=0.15)
        path = os.path.join(CHART_DIR, "temp_trend.png")
        fig.tight_layout(); fig.savefig(path, dpi=80, facecolor="#1a1a2e")
        plt.close(fig); generated.append(path)
    except Exception as e:
        log.warning(f"温度图表失败: {e}")
    
    # ── 图2: 水库降雨柱状图 ──
    try:
        fig, ax = plt.subplots(figsize=(9, 4))
        ax.set_title("水库 72h 累计降雨" + ("" if fp else " (ECMWF solid / CMA hatched)"),
                     fontproperties=fp, fontsize=13, pad=12)
        reserves = CFG.get("reservoirs", [])
        names = [r[0] for r in reserves]
        e_vals = [_fv(results, r[0], "ecmwf", "rain")[0] for r in reserves]
        c_vals = [_fv(results, r[0], "cma", "rain")[0] for r in reserves]
        w = 0.35; xi = range(len(names))
        ax.bar([i-w/2 for i in xi], e_vals, w, color=colors["ecmwf"], alpha=0.9, label="ECMWF")
        ax.bar([i+w/2 for i in xi], c_vals, w, color=colors["cma"], alpha=0.7, label="CMA", hatch="//")
        ax.set_xticks(xi); ax.set_xticklabels(names, fontproperties=fp, rotation=30, ha="right", fontsize=9)
        ax.set_ylabel("mm", fontproperties=fp)
        ax.legend(loc="upper right", fontsize=8, prop=fp)
        ax.grid(axis="y", alpha=0.15)
        path = os.path.join(CHART_DIR, "rain_bars.png")
        fig.tight_layout(); fig.savefig(path, dpi=80, facecolor="#1a1a2e")
        plt.close(fig); generated.append(path)
    except Exception as e:
        log.warning(f"降雨图表失败: {e}")
    
    # ── 图3: 去年同期对比 ──
    try:
        fig, ax = plt.subplots(figsize=(8, 3.5))
        ax.set_title("成都气温 今年 vs 去年" + ("" if fp else ""), fontproperties=fp, fontsize=13, pad=12)
        x2 = range(FORECAST_DAYS)
        eh = _fv(results, "成都", "ecmwf", "high"); ch = _fv(results, "成都", "cma", "high")
        ly = _archive_val(results, "成都", "high")
        this_yr = [max(eh[i], ch[i]) for i in range(FORECAST_DAYS)]
        ax.plot(x2, this_yr, color="#4FC3F7", linewidth=2, marker="o", markersize=6, label="今年(ECMWF/CMA取高)")
        if ly[0] is not None:
            ax.plot(x2, ly[:FORECAST_DAYS], color="#81C784", linewidth=1.5, linestyle="--", marker="s", markersize=5, label="去年")
        ax.set_xticks(x2); ax.set_xticklabels(day_labels, fontproperties=fp)
        ax.set_ylabel("°C", fontproperties=fp)
        ax.legend(loc="upper right", fontsize=9, prop=fp)
        ax.grid(alpha=0.15)
        path = os.path.join(CHART_DIR, "yoy_compare.png")
        fig.tight_layout(); fig.savefig(path, dpi=80, facecolor="#1a1a2e")
        plt.close(fig); generated.append(path)
    except Exception as e:
        log.warning(f"同比图表失败: {e}")
    
    return generated


# ═══════════════════════════════════════════
# ⑥ HTML 生成
# ═══════════════════════════════════════════

def generate_html(fetched, chart_files):
    """生成自包含 HTML 页面，chart_files 为空也不崩"""
    quality = fetched["quality"]
    today = date.today().strftime("%m-%d")
    qt = _quality_tag(quality)
    charts_html = ""
    for cf in chart_files:
        fname = os.path.basename(cf)
        charts_html += f'<img src="charts/{fname}" alt="{fname}">\n'
    
    # 综合研判提取
    md = format_markdown(fetched)
    judge_lines = []
    in_judge = False
    for line in md.split("\n"):
        if "📊 综合研判" in line:
            in_judge = True; continue
        if in_judge:
            if line.strip().startswith("❶") or line.strip().startswith("❷") or line.strip().startswith("❸") or line.strip().startswith("❹"):
                judge_lines.append(line.strip())
            elif not line.strip() or line.startswith("数据:"):
                break
    
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>四川交易天气简报 {today}</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{background:#1a1a2e;color:#e0e0e0;font:14px/1.6 -apple-system,sans-serif;max-width:920px;margin:0 auto;padding:16px}}
  h1{{text-align:center;font-size:20px;margin:12px 0 20px;color:#fff}}
  img{{max-width:100%;display:block;margin:20px auto;border-radius:8px;border:1px solid #333}}
  .no-charts{{text-align:center;color:#888;padding:40px;font-size:16px}}
  .judgement{{background:#16213e;padding:16px 20px;border-radius:8px;margin:20px 0;line-height:1.8}}
  .judgement p{{margin:4px 0}}
  .footer{{text-align:center;color:#666;font-size:12px;margin-top:24px;padding-top:12px;border-top:1px solid #333}}
  .quality{{display:inline-block;background:#0f3460;color:#4FC3F7;padding:2px 10px;border-radius:4px;font-size:11px;margin-left:8px}}
</style>
</head>
<body>
<h1>🌤️ 四川交易天气简报 {today}</h1>
{charts_html if charts_html else '<div class="no-charts">📊 图表生成失败，请查看 Markdown 简报</div>'}
<div class="judgement">
{chr(10).join(f'<p>{l}</p>' for l in judge_lines) if judge_lines else '<p>研判数据暂缺</p>'}
</div>
<div class="footer">
  数据: ECMWF IFS + CMA GRAPES | 36站 | {today} 08:30
  <span class="quality">{qt}</span>
</div>
</body>
</html>"""
    return html


# ═══════════════════════════════════════════
# ⑦ 主入口
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
    
    # ③ 图表
    t3 = time.monotonic()
    chart_files = generate_charts(fetched)
    dt_chart = time.monotonic() - t3
    log.info(f"图表: {len(chart_files)}/3张 ({dt_chart:.1f}s)")
    
    # ④ HTML
    t4 = time.monotonic()
    html = generate_html(fetched, chart_files)
    html_path = os.path.join(DATA_DIR, "index.html")
    with open(html_path, "w") as f:
        f.write(html)
    dt_html = time.monotonic() - t4
    log.info(f"HTML: {len(html)}字 ({dt_html:.1f}s)")
    
    # ⑤ 完成
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
