# -*- coding: utf-8 -*-
"""
税务稽查工作台 · 权威外网自动抓取器（v29）

做什么
  1. 直连国家税务总局及可访问的省局门户、中国政府网等权威站点，列出候选文章链接；
  2. 逐篇打开官方内容页，读取标准 <meta name="pubdate"> 的真实发布日期、
     标题与正文首段（核心要点）——不依赖搜索排名，不用旧资料改日期冒充新闻；
  3. 按「真实发布日期」自动分成【本日新增】与【近 7 日权威发布】两组；
  4. 去重后把结构化结果写进同目录上一层 index.html 的
     /*__WB_LIVE_BEGIN__*/ ... /*__WB_LIVE_END__*/ 数据块（只替换这一块）；
  5. 同时留档 state/archive.json、state/seen.json、state/run.log。

边界（务必如实理解）
  · index.html 仍是纯 file:// 离线单页，打开即看，不联网、不含可点外链；
    联网抓取由本机这个后台脚本完成，抓完把「数据」写进 HTML，页面只负责展示。
  · 只有官方内容页里写着的发布日期才被采信；当天没有新发布就如实留空并写明，
    绝不会把往期材料改个日期当成今天的新闻。
  · 部分省局有 WAF（412/403）或偶发连接失败，会跳过并记进状态与日志，不影响其余源。
  · 定时依赖「本机/本环境持续运行」：环境休眠或关机期间不会执行；脚本每次启动会
    自动补抓（catch-up），把落后的那几天补回来。v29 起可改用 GitHub Actions 云端定时，
    由云端 runner 每天执行 --ci，不依赖本机常开（详见使用说明 v29 一节）。

用法（无需安装任何第三方库，Python 3.7+ 即可）
  python3 fetch_news.py            抓一次并写入 index.html（默认）
  python3 fetch_news.py --dry      只抓取并打印结果，不改写 index.html
  python3 fetch_news.py --daemon   常驻：每天 08:00 抓取；启动即补抓落后的日期
  python3 fetch_news.py --install  Linux/macOS：注册 crontab 每天 08:00（并立即抓一次）
  python3 fetch_news.py --date 2026-11-15   指定目标日期（补抓/测试用）
  python3 fetch_news.py --ci       CI 单次运行（GitHub Actions 用）：北京时间当天、
                                    单次抓取、写回 index.html、输出运行摘要，不常驻

Windows：用「任务计划程序」每天 08:00 执行 `python fetch_news.py` 即可。

v29 新增（GitHub Actions 云端每日自动更新）
  · --ci 模式：由 .github/workflows/daily-news.yml 每天北京时间 08:00 调用，
    抓完写回仓库里的 index.html 并由 Actions 自动提交，无需本机常开；
  · 二级来源「财经涉税媒体」：中国税务网、经济日报、央视网财经、证券时报、第一财经，
    与一级官方权威源分开统计、单独展示，明确标注「仅作线索 · 需人工复核」；
  · 空结果保护：境外 runner 若被 WAF/网络限制导致一条都没抓到，绝不写空数据覆盖
    已有真实结果，而是保留上一期数据并标记 fallback，如实说明原因。
"""
import sys, os, re, json, html, time, gzip, io, argparse, traceback
import urllib.request, urllib.parse, urllib.error
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
INDEX = HERE.parent / "index.html"
STATE = HERE / "state"
STATE.mkdir(exist_ok=True)
SEEN_F = STATE / "seen.json"
ARCH_F = STATE / "archive.json"
LAST_F = STATE / "last.json"
LOG_F = STATE / "run.log"
RUN_HOUR = 8                 # 每日定时抓取时刻（本地时间）
RECENT_DAYS = 7              # 「近 7 日」窗口
MAX_DETAIL = 90              # 单次最多精读的内容页数（控制耗时）
TIMEOUT = 12

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# 权威源：只列门户/栏目页，由它们发现当期文章
SEEDS = [
    ("国家税务总局·首页", "https://www.chinatax.gov.cn/"),
    ("国家税务总局·新闻发布", "https://www.chinatax.gov.cn/chinatax/n810219/index.html"),
    ("国家税务总局·政策法规库", "https://fgk.chinatax.gov.cn/zcfgk/"),
    ("国家税务总局江苏省税务局", "https://jiangsu.chinatax.gov.cn/"),
    ("国家税务总局浙江省税务局", "https://zhejiang.chinatax.gov.cn/"),
    ("国家税务总局上海市税务局", "https://shanghai.chinatax.gov.cn/"),
    ("国家税务总局河南省税务局", "https://henan.chinatax.gov.cn/"),
    ("国家税务总局安徽省税务局", "https://anhui.chinatax.gov.cn/"),
    ("中国政府网·最新政策", "https://www.gov.cn/zhengce/zuixin/"),
]

# 保底种子：已核验过的当期稽查/曝光类官方文章（内容页自带真实 pubdate）
PINNED = [
    ("广东税务部门曝光1起涉税中介偷税案件",
     "https://guangdong.chinatax.gov.cn/gdsw/swxw/2026-11/13/content_2f0f66ddbcf944a4ada9436dcf0dbd51.shtml"),
    ("关于对泉州德瑞服装制造有限公司《税务事项通知书（重大税收违法失信主体确定适用）》送达的公告",
     "https://fujian.chinatax.gov.cn/qzsswj/xxgk/sjxxgk/qtgg_23603/202611/t20261102_650865.htm"),
    ("关于对泉州池领服饰有限公司《税务处理事项告知书》送达的公告",
     "https://fujian.chinatax.gov.cn/qzsswj/xxgk/sjxxgk/qtgg_23603/202611/t20261103_650914.htm"),
    ("国家税务总局青岛市税务局第二稽查局送达公告2026年661号",
     "https://qingdao.chinatax.gov.cn/bsfw2019/ssgg/sdgg/dejcj/202611/t20261110_109687.html"),
    ("【中国税务报】交叉核验，扯下加油站申报假面具",
     "https://shandong.chinatax.gov.cn/art/2026/11/3/art_23_1520266.html"),
]

OK_DOMAINS = ("chinatax.gov.cn", "gov.cn", "mof.gov.cn", "news.cn",
              "people.com.cn", "samr.gov.cn", "court.gov.cn", "spp.gov.cn",
              "xinhuanet.com")
KEYWORD = re.compile(r"税|稽查|发票|虚开|偷税|骗税|退税|曝光|失信|纳税|征管|"
                     r"涉税|查处|违法|处罚|案例|公告|通告|财政|关税|费")
# 二级媒体源专用：必须真涉税，避免「消费 / 公告 / 典型案例」之类泛词把无关财经新闻收进来
MEDIA_KEYWORD = re.compile(r"税|发票|稽查|虚开|涉税|财税|关税|退税|骗税|偷税|纳税")
NAV_NOISE = re.compile(r"^(首页|网站地图|联系我们|登录|注册|English|更多|返回顶部|"
                       r"上一页|下一页|尾页|首页\s*>>|搜索|高级搜索|我要咨询|意见征集)$")
CONTENT_HINT = re.compile(r"/content|/art/|t20\d{6}|/20\d{2}[-/]|c\d{6,}|\.shtml|\.htm|/2026")
DATE_IN_URL = re.compile(r"/(20\d{2})[-/年.]?(\d{2})[-/月.]?(\d{2})|t(20\d{6})|/20\d{2}-(\d{2})[-/](\d{2})")

# ===== v29 · 二级来源：财经涉税媒体（仅作线索，不冒充官方权威发布） =====
MEDIA_SEEDS = [
    ("中国税务网·中国税务报", "https://www.ctaxnews.com.cn/"),
    ("经济日报·财经", "https://www.ce.cn/"),
    ("央视网财经", "https://finance.cctv.com/"),
    ("证券时报", "https://www.stcn.com/"),
    ("第一财经", "https://www.yicai.com/"),
]
MEDIA_DOMAINS = ("ctaxnews.com.cn", "ce.cn", "cctv.com", "stcn.com",
                 "yicai.com", "cnstock.com", "people.com.cn", "news.cn", "xinhuanet.com")
# 媒体文章 URL 形态与政府站不同（/article/detail/123.html、/news/103441000.html、ARTI…shtml）
MEDIA_HINT = re.compile(r"/detail/|/news/|/ARTI|/article/|/20\d{2}[-/]|\.shtml|\.html?|/content_")
MAX_MEDIA_DETAIL = 30          # 二级源单次最多精读页，控制 CI 耗时
MEDIA_NOTE = ("一级＝官方权威（*.gov.cn 等），逐篇打开官方内容页核验 pubdate；"
              "二级＝财经涉税媒体，日期多取自页面或 URL 推定，仅作线索、需人工复核，"
              "不进「本日新增 / 近 7 日权威发布」分组，也不自动写进内部收录台账。")


def beijing_today():
    """CI runner 是 UTC；用 UTC+8 明确算北京时间当天，不依赖系统 TZ。"""
    return (datetime.now(timezone.utc) + timedelta(hours=8)).date().isoformat()


def ok_media_host(u):
    h = host_of(u)
    return any(h == d or h.endswith("." + d) for d in MEDIA_DOMAINS)


def url_date(u):
    """从媒体 URL 里取 8 位/带分隔符日期；取不到返回空，绝不猜。"""
    for m in re.finditer(r"(20\d{2})[-/年.]?(\d{2})[-/月.]?(\d{2})", u or ""):
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return "%04d-%02d-%02d" % (y, mo, d)
    return ""


def log(msg):
    line = "[%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    try:
        with LOG_F.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def jload(p, default):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default


def jsave(p, obj):
    try:
        p.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as e:
        log("写状态失败 %s: %s" % (p.name, e))


def host_of(u):
    m = re.match(r"^https?://([^/:?#]+)", u or "", re.I)
    return (m.group(1) if m else "").lower()


def ok_host(u):
    h = host_of(u)
    return any(h == d or h.endswith("." + d) for d in OK_DOMAINS)


def get(url):
    """纯标准库 GET，返回 (status, text)。不做任何解析之外的动作。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Accept-Encoding": "identity",       # 避免处理压缩
        "Connection": "close",
    })
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            raw = r.read()
            status = r.status
            ctype = (r.headers.get("Content-Type") or "").lower()
    except urllib.error.HTTPError as e:
        return e.code, ""
    except Exception as e:
        return 0, ""
    enc = "utf-8"
    m = re.search(r"charset\s*=\s*[\"']?([\w\-]+)", ctype)
    if m:
        enc = m.group(1)
    else:
        head = raw[:2048].decode("ascii", "ignore")
        m2 = re.search(r"charset\s*=\s*[\"']?([\w\-]+)", head, re.I)
        if m2:
            enc = m2.group(1)
    if enc.lower() in ("gb2312", "gbk"):
        enc = "gb18030"
    try:
        return status, raw.decode(enc, "replace")
    except Exception:
        return status, raw.decode("utf-8", "replace")


def strip_tags(s):
    s = re.sub(r"<script[^>]*>.*?</script>", " ", s, flags=re.I | re.S)
    s = re.sub(r"<style[^>]*>.*?</style>", " ", s, flags=re.I | re.S)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    return " ".join(s.split())


def strip_tags_keep_br(s):
    """抓正文专用：把块级标签转成换行，保留段落结构，供阅读器分段展示。"""
    s = re.sub(r"<script[^>]*>.*?</script>|<style[^>]*>.*?</style>|<noscript[^>]*>.*?</noscript>",
               " ", s, flags=re.I | re.S)
    s = re.sub(r"<br\s*/?>|</p>|</div>|</li>|</h[1-6]>|</tr>|</article>|</section>", "\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    s = re.sub(r"[\t\u3000 ]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)
    return s.strip()


def meta_pubdate(h):
    """读官方内容页标准 pubdate；兼容属性顺序与常见别名。"""
    best = ""
    for tag in re.findall(r"<meta\b[^>]*>", h, flags=re.I)[:80]:
        attrs = dict((k.lower(), v) for k, v in
                     re.findall(r"([\w:\-]+)\s*=\s*[\"']([^\"']*)[\"']", tag))
        name = (attrs.get("name") or attrs.get("property") or "").lower()
        if name in ("pubdate", "publishdate", "publish_date", "article:published_time",
                    "weibo:article:create_at", "date", "dc.date", "contentdate", "pubtime"):
            v = attrs.get("content", "")
            if re.search(r"20\d{2}", v):
                best = v
                break
    if not best:
        for tag in re.findall(r"<meta\b[^>]*>", h, flags=re.I)[:80]:
            m = re.search(r"content\s*=\s*[\"']((?:20\d{2})[-/年]\d{1,2}[-/月]\d{1,2}[^\"']*)[\"']", tag, re.I)
            if m:
                best = m.group(1); break
    return norm_date(best)


def norm_date(s):
    if not s:
        return ""
    m = re.search(r"(20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})", s)
    if m:
        return "%04d-%02d-%02d" % (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", s)
    if m:
        return "%s-%s-%s" % (m.group(1), m.group(2), m.group(3))
    return ""


COLUMN_WORDS = ("媒体视点|最新文件|政策解读|总局政策解读|总局文件|工作动态|通知公告|税务要闻|"
                "曝光台|要闻|图片新闻|基层动态|政策法规|税收要闻|市局动态|县局动态|"
                "媒体聚焦|税务新闻|政策文件|网站首页|信息公开|专题专栏|图解|总局解读")


def page_title(h):
    m = re.search(r"<title[^>]*>(.*?)</title>", h, re.I | re.S)
    t = strip_tags(m.group(1)) if m else ""
    # 去尾部站点名，再去「省局名 + 栏目名」前缀，标题更接近原文主标题
    t = re.sub(r"\s*[_\-|｜]\s*[^_\-|｜]{2,30}$", "", t)
    t = re.sub(r"^国家税务总局[一-龥]{0,8}(?:省|市)?税务局(?:网站)?\s*", "", t)
    t = re.sub(r"^(?:" + COLUMN_WORDS + r")\s+", "", t)
    t = t.strip()
    m2 = re.search(r"<h1[^>]*>(.*?)</h1>", h, re.I | re.S)
    if (not t or len(t) < 6) and m2:
        t = re.sub(r"^(?:" + COLUMN_WORDS + r")\s+", "", strip_tags(m2.group(1))).strip()
    return t[:100]


def page_core(h):
    ps = re.findall(r"<p\b[^>]*>(.*?)</p>", h, re.I | re.S)
    for raw in ps:
        t = strip_tags(raw)
        if len(t) >= 40 and re.search(r"[\u4e00-\u9fa5]", t):
            return t[:420]
    # 兜底：部分省局正文放在 div（无 <p>），取第一段够长的中文文本
    for raw in re.findall(r"<div\b[^>]*>(.*?)</div>", h, re.I | re.S)[:120]:
        t = strip_tags(raw)
        if 60 <= len(t) <= 1600 and re.search(r"[\u4e00-\u9fa5]", t) and not re.search(
                r"^(首页|导航|网站地图|Copyright|版权所有|主办单位|备案号)", t):
            return t[:420]
    for tag in re.findall(r"<meta\b[^>]*>", h, flags=re.I):
        m = re.search(r"name\s*=\s*[\"']description[\"']\s+content\s*=\s*[\"']([^\"']{20,})[\"']", tag, re.I)
        if m:
            return html.unescape(m.group(1))[:420]
    return ""


_SKIP_LINE = ("首页", "导航", "网站地图", "版权所有", "主办单位", "备案号", "访问次数",
              "字体", "打印本页", "正文下载", "扫一扫", "网站纠错", "上一篇", "下一篇",
              "相关链接", "分享到", "责任编辑", "来源：", "发布时间", "【大", "【中", "【小")


def _clean_body(text):
    """按行过滤噪声行，保留中文正文段落。"""
    out = []
    for line in text.split("\n"):
        t = line.strip()
        if len(t) < 12 or not re.search(r"[\u4e00-\u9fa5]", t):
            continue
        if t.startswith(_SKIP_LINE):
            continue
        # 行内噪声：整行基本是链接串/日期戳/纯符号的跳过
        if re.fullmatch(r"[\d\-/:：.年月日\s]+", t):
            continue
        out.append(t)
    return "\n".join(out)


def page_body(h):
    """抓全文供页面内阅读器展示（不跳转浏览器）。抓不到就返回空串，前端如实降级为摘要。
    多容器策略：先取所有 <p>；不足时按「<article>/常见正文class容器/<td>/<div>」顺序，
    选中文字符最多、最像正文的容器。保留段落换行。上限 5000 字，避免撑爆 news.json。"""
    h = re.sub(r"<script.*?</script>|<style.*?</style>|<noscript.*?</noscript>", "", h, flags=re.I | re.S)

    # ① 所有 <p> 段落（覆盖大多数政务站）
    paras = []
    for raw in re.findall(r"<p\b[^>]*>(.*?)</p>", h, re.I | re.S):
        t = strip_tags_keep_br(raw).strip()
        if len(t) >= 18 and re.search(r"[\u4e00-\u9fa5]", t) and not t.startswith(_SKIP_LINE):
            paras.append(t)
    body = _clean_body("\n".join(paras))

    # ② 不足时按容器优先级取：article/section + 常见正文 class/id + td + div
    if len(body) < 120:
        containers = []
        # 语义标签
        for tag in ("article", "section"):
            containers += re.findall(r"<%s\b[^>]*>(.*?)</%s>" % (tag, tag), h, re.I | re.S)
        # 常见正文容器 class/id（TRS、政府站模板常用）
        for raw in re.findall(r"<(?:div|td)\b[^>]*(?:class|id)\s*=\s*[\"'][^\"']*(?:content|article|detail|TRS_Editor|zoom|conTxt|text|main|body)[^\"']*[\"'][^>]*>(.*?)</(?:div|td)>", h, re.I | re.S):
            containers.append(raw)
        # 兜底所有 div/td
        containers += re.findall(r"<(?:div|td)\b[^>]*>(.*?)</(?:div|td)>", h, re.I | re.S)[:260]
        cand = []
        for c in containers:
            t = _clean_body(strip_tags_keep_br(c))
            cn = len(re.findall(r"[\u4e00-\u9fa5]", t))
            if 60 <= len(t) <= 4000 and cn >= 40 and not t.startswith(_SKIP_LINE):
                # 正文密度：中文越多、噪声行占比越低越优先
                lines = [x for x in t.split("\n") if x]
                density = cn / max(1, len(lines))
                cand.append((cn, density, t))
        cand.sort(key=lambda x: (x[0], x[1]), reverse=True)
        if cand:
            body = cand[0][2]

    # ③ 再兜底 meta description
    if len(body) < 80:
        for tag in re.findall(r"<meta\b[^>]*>", h, re.I):
            m = re.search(r"name\s*=\s*[\"']description[\"']\s+content\s*=\s*[\"']([^\"']{30,})[\"']", tag, re.I)
            if m:
                body = html.unescape(m.group(1)); break
    return re.sub(r"\n{2,}", "\n", body).strip()[:5000]


def source_label(host, seed_name):
    if host.startswith("www.chinatax.gov.cn") or host == "chinatax.gov.cn":
        return "国家税务总局"
    if host.startswith("fgk.chinatax"):
        return "国家税务总局政策法规库"
    if host.endswith(".chinatax.gov.cn"):
        sub = host.split(".")[0]
        names = {"jiangsu": "江苏省", "zhejiang": "浙江省", "shanghai": "上海市",
                 "henan": "河南省", "anhui": "安徽省", "guangdong": "广东省",
                 "fujian": "福建省", "qingdao": "青岛市", "shandong": "山东省",
                 "beijing": "北京市", "sichuan": "四川省", "hunan": "湖南省"}
        return "国家税务总局%s税务局" % names.get(sub, sub)
    if "mof.gov.cn" in host:
        return "财政部"
    if "news.cn" in host or "xinhuanet" in host:
        return "新华社"
    if "people.com.cn" in host:
        return "人民日报"
    if "samr.gov.cn" in host:
        return "市场监管总局"
    if host.endswith("gov.cn") or host == "www.gov.cn":
        return "中国政府网"
    media = {"ctaxnews.com.cn": "中国税务网·中国税务报", "ce.cn": "经济日报",
             "cctv.com": "央视网财经", "stcn.com": "证券时报", "yicai.com": "第一财经",
             "cnstock.com": "上海证券报"}
    for k, v in media.items():
        if host == k or host.endswith("." + k):
            return v + "（财经媒体·二级）"
    return seed_name


def guess_cat(t, core):
    """主要按标题判类，避免正文里出现「国际标准」之类词把服务类新闻误判成国外税务。"""
    s = t or ""
    c = (core or "")[:120]
    if re.search(r"待核实|存疑|线索|征求意见稿", s): return "cat6"
    if re.search(r"国际|跨境|境外|OECD|CRS|金砖|税收协定|外贸|出海|出口退税|进博会", s): return "cat4"
    if re.search(r"曝光|典型案例|稽查案例|虚开|偷税|骗税|查处|失信|追缴|判决|行政处罚", s): return "cat2"
    if re.search(r"公告|通告|规定|办法|条例|总局令|基准|政策|解读|通知|决定|制度|修订草案|征管法|税法", s): return "cat1"
    if re.search(r"案例|典型|处罚|稽查|检查|违法|送达", s): return "cat2"
    if re.search(r"行业|数据|监管|中介|服务|发票|经济|运行|优化|助企|营商|税费|核验", s): return "cat3"
    if re.search(r"公告|通告|办法|政策解读", c): return "cat1"
    return "cat3"


def links_from(h, base, seed_name, media=False):
    out = []
    for m in re.finditer(r"<a\b([^>]*)>(.*?)</a>", h, re.I | re.S):
        attrs, inner = m.group(1), m.group(2)
        hm = re.search(r"href\s*=\s*[\"']([^\"']+)[\"']", attrs, re.I)
        if not hm:
            continue
        u = urllib.parse.urljoin(base, hm.group(1).strip())
        if u.startswith("javascript:") or u.startswith("#") or not u.startswith("http"):
            continue
        t = strip_tags(inner)
        tm = re.search(r"title\s*=\s*[\"']([^\"']+)[\"']", attrs, re.I)
        if (not t or len(t) < 8) and tm:
            t = html.unescape(tm.group(1)).strip()
        if media:
            # 媒体列表常把时间/阅读数拼在标题尾部，先清洗再判断
            t = re.sub(r"\s*\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}\s*$", "", t)
            t = re.sub(r"\s*\d+\s*\d+\s*(?:小时|分钟)前\s*$", "", t)
            t = re.sub(r"\s+\d{2,5}\s*$", "", t)
            t = re.sub(r"[_|｜-]\s*(财经频道|财经|证券|要闻|滚动|专题)\s*$", "", t)
        kw = MEDIA_KEYWORD if media else KEYWORD
        if len(t) < 8 or NAV_NOISE.match(t) or not kw.search(t):
            continue
        domain_ok = ok_media_host(u) if media else ok_host(u)
        hint = MEDIA_HINT if media else CONTENT_HINT
        if not domain_ok or not hint.search(u):
            continue
        out.append({"t": t[:100], "u": u, "seed": seed_name, "media": bool(media)})
    return out


def collect(seed_name, url, media=False):
    st, h = get(url)
    if st != 200 or not h:
        return {"name": seed_name, "ok": False, "status": st, "cand": 0, "items": []}
    ls = links_from(h, url, seed_name, media)
    # 优先当期路径、其次内容 ID 大的（新文章 ID 递增）
    def score(o):
        s = 0
        if DATE_IN_URL.search(o["u"]):
            s += 50
        m = re.search(r"c(\d{6,})|t(20\d{6})|art_(\d+)", o["u"])
        if m:
            s += min(int(next(g for g in m.groups() if g)) / 1e8, 20)
        return -s
    ls.sort(key=score)
    return {"name": seed_name, "ok": True, "status": st, "cand": len(ls),
            "items": ls[:24], "tier": "财经媒体（二级）" if media else "官方权威（一级）"}


def detail(o, target):
    st, h = get(o["u"])
    if st != 200 or not h:
        return None
    is_media = bool(o.get("media"))
    pub = meta_pubdate(h)
    basis = "官方内容页 pubdate 核验"
    if is_media:
        # 媒体页不猜正文日期：只认 meta pubdate 或 URL 里写明的日期
        if not pub:
            pub = url_date(o["u"])
            basis = "URL 路径日期推定 · 需人工复核" if pub else "未标注发布日期 · 需人工复核"
        else:
            basis = "媒体页 meta pubdate 核验 · 需人工复核"
    else:
        if not pub:
            # 兜底：正文前段日期；仍取不到就如实标为「未标注」，不冒充当天
            head = strip_tags(h[:6000])
            pub = norm_date(head[:1200]) or ""
            basis = "官方内容页 pubdate 核验" if pub else "官方原文·发布日期未标注"
    title = page_title(h) or o["t"]
    core = page_core(h)
    body = page_body(h)   # 全文：供手机页面内阅读器直接展开，不跳浏览器；抓不到为空，前端降级
    return {
        "t": title, "src": source_label(host_of(o["u"]), o.get("seed", "")),
        "url": o["u"], "pub": pub, "core": core, "body": body,
        "host": host_of(o["u"]), "cat": guess_cat(title, core),
        "seed": o.get("seed", ""),
        "tier": "财经媒体（二级·仅作线索）" if is_media else "官方权威（一级）",
        "tier_level": 2 if is_media else 1,
        "date_basis": basis,
        "verified": basis if is_media else ("官方内容页 pubdate 核验" if pub else "官方原文·发布日期未标注"),
    }


def run_once(target, ci=False):
    t0 = time.time()
    log("开始抓取，目标日期 %s" % target)
    seeds_report, cand = [], []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(collect, n, u): n for n, u in SEEDS}
        for f in as_completed(futs):
            try:
                r = f.result(); seeds_report.append(r)
                cand.extend(r["items"])
                log("源 %-24s status=%s 候选=%s" % (r["name"], r["status"], r["cand"]))
            except Exception as e:
                seeds_report.append({"name": futs[f], "ok": False, "status": 0, "cand": 0, "items": []})
                log("源 %s 异常 %s" % (futs[f], e))
    # ===== v29 · 二级来源：财经涉税媒体，单独采集、单独统计 =====
    media_report, media_cand = [], []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(collect, n, u, True): n for n, u in MEDIA_SEEDS}
        for f in as_completed(futs):
            try:
                r = f.result(); media_report.append(r); media_cand.extend(r["items"])
                log("媒体源 %-20s status=%s 候选=%s" % (r["name"], r["status"], r["cand"]))
            except Exception as e:
                media_report.append({"name": futs[f], "ok": False, "status": 0, "cand": 0,
                                     "items": [], "tier": "财经媒体（二级）"})
                log("媒体源 %s 异常 %s" % (futs[f], e))
    # 保底种子直接进精读队列
    for t, u in PINNED:
        cand.append({"t": t, "u": u, "seed": "已核验稽查/曝光条目"})

    seen_u, dedup = set(), []
    for o in cand:
        k = o["u"].split("#")[0]
        if k in seen_u:
            continue
        seen_u.add(k); dedup.append(o)
    dedup.sort(key=lambda o: (0 if DATE_IN_URL.search(o["u"]) else 1, 0 if "chinatax" in o["u"] else 1))
    queue = dedup[:MAX_DETAIL]
    log("候选去重后 %s 条，本次精读 %s 条" % (len(dedup), len(queue)))

    items = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(detail, o, target): o for o in queue}
        for f in as_completed(futs):
            try:
                d = f.result()
                if d:
                    items.append(d)
            except Exception:
                pass

    # ===== 二级源精读：只保留「近 7 日 + 有可核验日期」的涉税线索 =====
    m_seen_u, m_queue = set(), []
    for o in media_cand:
        k = o["u"].split("#")[0]
        if k in m_seen_u or not ok_media_host(o["u"]):
            continue
        m_seen_u.add(k); m_queue.append(o)
        if len(m_queue) >= MAX_MEDIA_DETAIL:
            break
    media_items = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(detail, o, target): o for o in m_queue}
        for f in as_completed(futs):
            try:
                d = f.result()
                if d and MEDIA_KEYWORD.search(d["t"]):
                    media_items.append(d)
            except Exception:
                pass

    # 去重：URL + 标题
    seen_arch = set(jload(SEEN_F, []))
    uniq, keys = [], set()
    for d in items:
        k = re.sub(r"\W", "", d["t"])[:45] + "|" + d["pub"]
        if k in keys:
            continue
        keys.add(k); uniq.append(d)
    uniq.sort(key=lambda d: (d["pub"] or "0000"), reverse=True)

    lo = (date.fromisoformat(target) - timedelta(days=RECENT_DAYS - 1)).isoformat()
    # 二级线索：按标题去重；只收近 7 日、且日期可核验的；无日期一律不收（不冒充）
    m_keys, hotspots = set(), []
    for d in sorted(media_items, key=lambda x: (x["pub"] or "0000"), reverse=True):
        k = re.sub(r"\W", "", d["t"])[:45] + "|" + d["pub"]
        if k in m_keys:
            continue
        if d["pub"] and lo <= d["pub"] <= target:
            m_keys.add(k); hotspots.append(d)
    hotspots = hotspots[:12]
    dated = [d for d in uniq if d["pub"] and lo <= d["pub"] <= target]
    undated = [d for d in uniq if not d["pub"]]
    today = [d for d in dated if d["pub"] == target]
    fresh = [d for d in dated if d["url"] not in seen_arch]

    # 历史留档
    arch = jload(ARCH_F, [])
    arch_map = {a.get("url"): a for a in arch if isinstance(a, dict)}
    for d in dated:
        arch_map[d["url"]] = d
    jsave(ARCH_F, list(arch_map.values())[-600:])
    jsave(SEEN_F, sorted(set(seen_arch) | {d["url"] for d in dated})[-3000:])

    ok_src = sum(1 for s in seeds_report if s["ok"])
    payload = {
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "target_date": target,
        "next_run": (date.fromisoformat(target) + timedelta(days=1)).isoformat() + " %02d:00" % RUN_HOUR,
        "run_hour": RUN_HOUR,
        "engine": "本地权威站点直抓 + 官方内容页 pubdate 核验",
        "sources": [{"name": s["name"], "ok": s["ok"], "status": s["status"], "cand": s["cand"]}
                    for s in seeds_report],
        "sources_ok": ok_src,
        "sources_total": len(seeds_report),
        "runner": ("GitHub Actions 云端定时" if os.environ.get("GITHUB_ACTIONS") == "true"
                   else ("本机 CI 等价预跑" if ci else "本机脚本")),
        "run_mode": "ci" if ci else "local",
        "tier_note": MEDIA_NOTE,
        "media": [{"name": s["name"], "ok": s["ok"], "status": s["status"], "cand": s["cand"]}
                  for s in media_report],
        "media_ok": sum(1 for s in media_report if s["ok"]),
        "media_total": len(media_report),
        "hotspots": hotspots,
        "scanned": len(dedup), "detailed": len(queue), "parsed": len(uniq),
        "today": today, "recent": dated, "undated": undated[:8],
        "fresh": fresh,
        "new_count": len(fresh),
        "status": "ok" if dated else ("partial" if uniq else "empty"),
        "note": ("本日（%s）权威站点无新增发布时如实留空；近 %s 日条目均标注各自真实发布日期，"
                 "不把往期材料改日期冒充当日新闻。" % (target, RECENT_DAYS)),
        "elapsed": round(time.time() - t0, 1),
    }
    log("完成：官方源 %s/%s，媒体源 %s/%s，精读 %s，解析 %s，近 %s 日 %s 条，"
        "本日 %s 条，二级线索 %s 条，耗时 %ss"
        % (ok_src, len(seeds_report), payload["media_ok"], payload["media_total"],
           len(queue), len(uniq), RECENT_DAYS, len(dated), len(today),
           len(hotspots), payload["elapsed"]))
    return payload


BEG = "/*__WB_LIVE_BEGIN__*/"
END = "/*__WB_LIVE_END__*/"


def existing_live(index_path):
    """读出 index.html 里已有的数据块，供空结果保护判断。"""
    try:
        txt = index_path.read_text(encoding="utf-8")
        a = txt.index(BEG); b = txt.index(END)
        m = re.search(r"window\.__WB_LIVE__=(.*?);", txt[a + len(BEG):b], re.S)
        return json.loads(m.group(1)) if m else None
    except Exception:
        return None


def ci_guard(p, index_path):
    """境外 runner 被 WAF / 网络限制时一条都抓不到：绝不写空数据覆盖已有真实结果。
    保留上一期 today/recent/target_date，只更新抓取尝试元信息并标记 fallback。"""
    if (p.get("recent") or []) or p.get("status") == "fallback":
        return p, False
    old = existing_live(index_path)
    if not old or not (old.get("recent") or []):
        return p, False
    merged = dict(old)
    merged.update({
        "fetched_at": p["fetched_at"],
        "runner": p["runner"], "run_mode": p["run_mode"],
        "status": "fallback",
        "fallback_reason": ("本次云端抓取未连通权威源（境外网络 / WAF 限制），未解析到有效条目；"
                            "已保留 %s 的真实抓取结果，未改写任何日期与条目。"
                            % (old.get("target_date") or "上期")),
        "media": p.get("media", old.get("media", [])),
        "media_ok": p.get("media_ok", 0), "media_total": p.get("media_total", 0),
        "hotspots": old.get("hotspots", []),
        "tier_note": old.get("tier_note", MEDIA_NOTE),
        "sources": p.get("sources", old.get("sources", [])),
        "sources_ok": p.get("sources_ok", 0), "sources_total": p.get("sources_total", 0),
        "elapsed": p["elapsed"], "next_run": p["next_run"],
        "ci_attempts": int(old.get("ci_attempts", 0)) + 1,
    })
    log("空结果保护生效：保留 %s 的真实数据，标记 fallback（第 %s 次）"
        % (old.get("target_date"), merged["ci_attempts"]))
    return merged, True


def public_mobile_json(p):
    """只导出可公开的手机新闻数据，绝不包含内部台账 / 脱敏索引 / 分析区。
    供 GitHub Actions commit 后经 jsDelivr CDN 分发给手机页面跨域读取。
    字段与页面 __WB_LIVE__ 渲染层一致，前端无需改造即可复用。"""
    keep = ("fetched_at", "target_date", "next_run", "run_hour", "engine",
            "sources", "sources_ok", "sources_total", "runner", "run_mode",
            "tier_note", "media", "media_ok", "media_total", "hotspots",
            "scanned", "detailed", "parsed", "today", "recent", "status",
            "note", "elapsed", "fallback_reason")
    out = {k: p[k] for k in keep if k in p}
    # 明确标注这是可公开分发的纯新闻子集，无任何内部资料
    out["scope"] = "public-news-only"
    out["privacy"] = "本文件仅含权威站点的公开新闻条目，均为可公开传播的信息"
    return out


def export_json(p, path):
    """写出手机专用公开 JSON（UTF-8，紧凑，前端可直接 JSON.parse）。"""
    obj = public_mobile_json(p)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8")
    log("已导出手机公开 JSON：%s（%s 字节，仅公开新闻）" % (path, path.stat().st_size))
    return obj


def ci_result(p, changed, guarded):
    return {"changed": bool(changed), "guarded": bool(guarded),
            "target_date": p.get("target_date"), "status": p.get("status"),
            "runner": p.get("runner"), "fetched_at": p.get("fetched_at"),
            "today": len(p.get("today") or []), "recent": len(p.get("recent") or []),
            "hotspots": len(p.get("hotspots") or []),
            "sources_ok": p.get("sources_ok"), "sources_total": p.get("sources_total"),
            "media_ok": p.get("media_ok"), "media_total": p.get("media_total"),
            "elapsed": p.get("elapsed"), "fallback_reason": p.get("fallback_reason", "")}


def write_step_summary(p, changed, guarded):
    """写 GitHub Actions 运行摘要：手机浏览器打开本次运行页即可看到当日结果。"""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        lines = [
            "## 税务稽查工作台 · 每日权威抓取（%s）" % p.get("target_date", ""),
            "",
            "| 项目 | 结果 |", "| --- | --- |",
            "| 运行状态 | `%s`（%s） |" % (p.get("status"), p.get("runner")),
            "| 抓取时间 | %s |" % p.get("fetched_at"),
            "| 官方权威源 | %s / %s 可用 |" % (p.get("sources_ok"), p.get("sources_total")),
            "| 财经媒体源 | %s / %s 可用 |" % (p.get("media_ok"), p.get("media_total")),
            "| 本日新增 | %s 条 |" % len(p.get("today") or []),
            "| 近 7 日权威发布 | %s 条 |" % len(p.get("recent") or []),
            "| 二级财经线索 | %s 条（仅作线索，需人工复核） |" % len(p.get("hotspots") or []),
            "| 是否改写 index.html | %s |" % ("是" if changed else "否"),
            "",
        ]
        if guarded:
            lines += ["> **空结果保护已生效**：" + p.get("fallback_reason", ""), ""]
        lines += ["### 本日新增（均为官方内容页核验的真实发布日期）", ""]
        for d in (p.get("today") or [])[:12]:
            lines.append("- `%s` %s ｜ %s" % (d.get("pub"), d.get("t"), d.get("src")))
        if not (p.get("today") or []):
            lines.append("- 本日各权威站点暂无新增税务发布（如实留空，不拿往期材料改日期冒充）。")
        lines += ["", "### 近 7 日权威发布（各自真实发布日期，不是今天的新闻）", ""]
        for d in (p.get("recent") or [])[:12]:
            lines.append("- `%s` %s ｜ %s" % (d.get("pub"), d.get("t"), d.get("src")))
        lines += ["", "> 一级＝官方权威，逐篇核验 pubdate；二级＝财经媒体，仅作线索。"
                  "原文链接只作纯文本引注，页面本身零联网、零可点外链。", ""]
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        log("写运行摘要失败：%s" % e)


def inject(payload, index_path):
    txt = index_path.read_text(encoding="utf-8")
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    data = data.replace("</", "<\\/").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    block = BEG + "\nwindow.__WB_LIVE__=" + data + ";\n" + END
    if BEG in txt and END in txt:
        a = txt.index(BEG); b = txt.index(END) + len(END)
        new = txt[:a] + block + txt[b:]
    else:
        # 标记不存在：插到 v28 渲染层之前的独立 script 里（幂等）
        anchor = "/*__WB_LIVE_SCRIPT__*/"
        script = '<script>\n' + block + '\n</script>\n' + anchor
        if anchor in txt:
            a = txt.index(anchor)
            # 替换掉上一个自动插入的 script（若有）
            start = txt.rfind("<script>", 0, a)
            if start != -1 and BEG in txt[start:a]:
                new = txt[:start] + script + txt[a + len(anchor):]
            else:
                new = txt[:a] + script + txt[a + len(anchor):]
        else:
            pos = txt.rfind("</body>")
            new = (txt[:pos] + script + txt[pos:]) if pos != -1 else (txt + script)
        log("未在页面找到数据块标记，已自动插入（建议保留标记以支持后续增量替换）")
    if new == txt:
        log("数据无变化，未改写文件")
        return False
    tmp = index_path.with_suffix(".html.tmp")
    tmp.write_text(new, encoding="utf-8")
    tmp.replace(index_path)
    log("已写入 %s（%s 字节）" % (index_path.name, index_path.stat().st_size))
    return True


def install_cron():
    import shutil, subprocess
    py = sys.executable
    script = str(Path(__file__).resolve())
    line = "0 %d * * * %s %s >> %s 2>&1" % (RUN_HOUR, py, script, str(LOG_F))
    # crontab 若可用则注册；否则提示用 --daemon
    if shutil.which("crontab"):
        try:
            cur = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            old = (cur.stdout or "").splitlines()
            old = [l for l in old if "fetch_news.py" not in l]
            old.append(line)
            p = subprocess.run(["crontab", "-"], input="\n".join(old) + "\n",
                               capture_output=True, text=True)
            if p.returncode == 0:
                log("已注册 crontab：每天 %02d:00 抓取" % RUN_HOUR)
                return True
            log("crontab 写入失败：%s" % (p.stderr or "").strip())
        except Exception as e:
            log("crontab 异常：%s" % e)
    log("本机没有 crontab（或注册失败）。请改用 `python3 fetch_news.py --daemon` 常驻补抓，"
        "Windows 请用任务计划程序。")
    return False


def daemon():
    log("常驻调度启动：每天 %02d:00 抓取；启动即补抓落后日期" % RUN_HOUR)
    last_date = jload(LAST_F, {}).get("date", "")
    while True:
        try:
            today = date.today().isoformat()
            now = datetime.now()
            # 跨天 / 环境休眠恢复后立即补抓（不限小时），避免一直等到次日 08:00
            due = (now.hour >= RUN_HOUR and last_date != today) or (last_date and last_date < today)
            if due or not last_date:
                p = run_once(today)
                inject(p, INDEX)
                jsave(LAST_F, {"date": today, "at": p["fetched_at"]})
                last_date = today
        except Exception:
            log("调度轮次异常：\n" + traceback.format_exc())
        time.sleep(60)


def main():
    ap = argparse.ArgumentParser(description="税务稽查工作台权威外网自动抓取器")
    ap.add_argument("--dry", action="store_true", help="只抓取打印，不改写 index.html")
    ap.add_argument("--daemon", action="store_true", help="常驻每日定时抓取")
    ap.add_argument("--install", action="store_true", help="注册系统 crontab 并立即抓一次")
    ap.add_argument("--date", default="", help="指定目标日期 YYYY-MM-DD")
    ap.add_argument("--ci", action="store_true",
                    help="CI 单次运行（GitHub Actions）：北京时间当天、写回文件、输出运行摘要")
    ap.add_argument("--export-json", default="", metavar="PATH",
                    help="额外导出手机专用公开新闻 JSON 到 PATH（不含内部资料）")
    a = ap.parse_args()
    if a.daemon:
        daemon(); return
    if a.install:
        install_cron()
    if a.ci:
        target = a.date or beijing_today()
        p = run_once(target, ci=True)
        p, guarded = ci_guard(p, INDEX)
        changed = inject(p, INDEX)
        jsave(LAST_F, {"date": target, "at": p["fetched_at"], "mode": "ci"})
        r = ci_result(p, changed, guarded)
        jsave(STATE / "ci_result.json", r)
        write_step_summary(p, changed, guarded)
        if a.export_json:
            export_json(p, a.export_json)
        print("CI_RESULT " + json.dumps(r, ensure_ascii=False))
        return
    target = a.date or date.today().isoformat()
    p = run_once(target)
    if not a.dry:
        inject(p, INDEX)
        jsave(LAST_F, {"date": target, "at": p["fetched_at"]})
        if a.export_json:
            export_json(p, a.export_json)
    else:
        jsave(STATE / "preview.json", p)
        print("\n--- 结果预览（--dry，未改写 index.html；完整结果见 state/preview.json）---")
        print(json.dumps({k: p[k] for k in
                          ("target_date", "status", "sources_ok", "sources_total",
                           "scanned", "detailed", "parsed", "elapsed")},
                         ensure_ascii=False, indent=1))
        print("本日新增 %s 条：" % len(p["today"]))
        for d in p["today"][:10]:
            print("  ", d["pub"], d["t"][:60])
        print("近 %s 日共 %s 条，前 10 条：" % (RECENT_DAYS, len(p["recent"])))
        for d in p["recent"][:10]:
            print("  ", d["pub"], "|", d["cat"], "|", d["t"][:58], "|", d["host"])


if __name__ == "__main__":
    main()
