import glob, json, os, time, datetime as dt
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# Optional: set CARTO_API_KEY in compose. Blank = free tiles without a key.
CARTO_API_KEY = os.environ.get("CARTO_API_KEY", "")
TILE_URL = "https://basemaps.cartocdn.com/rastertiles/dark_all/{z}/{x}/{y}.png"
if CARTO_API_KEY:
    TILE_URL += f"?key={CARTO_API_KEY}"

LAKEHOUSE_PATH = Path(os.environ.get("LAKEHOUSE_PATH", "/data/lakehouse"))
LAKEHOUSE_TABLE = os.environ.get("LAKEHOUSE_TABLE", "permits_current")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
OUT = os.environ.get("OUTPUT_DIR", "/output")
PERIOD = os.environ.get("PERIOD", "all")  # all|today|yesterday|week|month|30d|YYYY-MM-DD
ANCHOR_LATEST = os.environ.get("ANCHOR_LATEST", "true").lower() == "true"
# Hourly dashboard wants fresh data -> Cassandra first. Set false to prefer lakehouse.
PREFER_CASSANDRA = os.environ.get("PREFER_CASSANDRA", "true").lower() == "true"

COLS = ["permit_si_no", "borough", "neighborhood", "permit_type", "permit_status",
        "filing_date", "latitude", "longitude", "owner_business_name"]
BAD_OWNERS = {"", "N/A", "NA", "NONE", "NAN", "NULL", "UNKNOWN"}
ACRIS_BORO = {"1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN", "4": "QUEENS", "5": "STATEN ISLAND"}
PLUTO_BORO = {"MN": "MANHATTAN", "BX": "BRONX", "BK": "BROOKLYN", "QN": "QUEENS", "SI": "STATEN ISLAND"}

os.makedirs(OUT, exist_ok=True)
DARK = {"figure.facecolor": "#1f1f26", "axes.facecolor": "#1f1f26", "savefig.facecolor": "#1f1f26",
        "axes.edgecolor": "#444", "axes.labelcolor": "#cfcfd6", "axes.titlecolor": "#eee",
        "text.color": "#eee", "xtick.color": "#aaa", "ytick.color": "#aaa", "grid.color": "#33333c"}
sns.set_theme(style="darkgrid", rc=DARK)


# ---------------------------------------------------------------- loaders
def cassandra_session():
    from cassandra.cluster import Cluster
    for attempt in range(1, 21):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            return cluster, cluster.connect(KEYSPACE)
        except Exception as e:
            print(f"[{attempt}/20] Cassandra not ready: {e}")
            time.sleep(15)
    raise SystemExit("Could not connect to Cassandra.")


def load_table(table, cols="*"):
    cluster, s = cassandra_session()
    try:
        rows = list(s.execute(f"SELECT {cols} FROM {table}"))
    finally:
        cluster.shutdown()
    out = []
    for r in rows:
        rec = {}
        for k, v in r._asdict().items():
            rec[k] = str(v) if type(v).__name__ == "Date" else v
        out.append(rec)
    print(f"{table}: {len(out)} rows")
    return pd.DataFrame(out)


def load_permits_lakehouse():
    files = sorted(glob.glob(str(LAKEHOUSE_PATH / "silver" / LAKEHOUSE_TABLE / "dt=*" / "data.parquet")))
    if not files:
        return None
    frames = []
    for f in files:
        part = pd.read_parquet(f)
        part["_dt"] = Path(f).parent.name.replace("dt=", "")
        frames.append(part)
    d = pd.concat(frames, ignore_index=True).sort_values("_dt")
    d = d.drop_duplicates(subset="permit_si_no", keep="last")
    return d.reindex(columns=COLS)


def run(name, fn):
    """One broken chart must never kill the whole dashboard."""
    try:
        fn()
    except Exception as e:
        plt.close("all")
        print(f"!! {name} failed: {e!r}")


def save(name):
    plt.tight_layout()
    plt.savefig(f"{OUT}/{name}", dpi=130)
    plt.close()
    print("Saved", name)


# ---------------------------------------------------------------- data
df = None if PREFER_CASSANDRA else load_permits_lakehouse()
if df is None or df.empty:
    df = load_table("permits_current", ", ".join(COLS))
    if df.empty:
        df = load_permits_lakehouse()
if df is None or df.empty:
    raise SystemExit("No permit data anywhere.")

df["borough"] = df["borough"].astype(str).str.strip().str.upper()
df["filing_date"] = pd.to_datetime(df["filing_date"].astype(str), errors="coerce")
latest = df["filing_date"].max()
print("Latest filing_date:", latest)

try:
    sales = load_table("sales_acris")
except Exception as e:
    print("sales_acris load failed:", e); sales = pd.DataFrame()
try:
    pl = load_table("parcels_pluto")
except Exception as e:
    print("parcels_pluto load failed:", e); pl = pd.DataFrame()

if len(sales):
    sales["borough"] = sales["borough"].astype(str).map(ACRIS_BORO)
    sales["document_amt"] = pd.to_numeric(sales["document_amt"], errors="coerce")
    sales["rec"] = pd.to_datetime(sales["recorded_datetime"], utc=True, errors="coerce").dt.tz_convert(None)
if len(pl):
    pl["borough"] = pl["borough"].map(PLUTO_BORO)
    for col in ["assess_total", "bldg_area", "year_built", "latitude", "longitude"]:
        pl[col] = pd.to_numeric(pl[col], errors="coerce")
    pl["assess_psf"] = (pl["assess_total"] / pl["bldg_area"]).where(pl["bldg_area"] > 0)


def apply_period(d, period):
    if period == "all" or pd.isna(latest):
        return d
    a = latest.normalize() if ANCHOR_LATEST else pd.Timestamp.today().normalize()
    if period == "today": s = e = a
    elif period == "yesterday": s = e = a - pd.Timedelta(days=1)
    elif period == "week": s, e = a - pd.Timedelta(days=a.weekday()), a
    elif period == "month": s, e = a.replace(day=1), a
    elif period == "30d": s, e = a - pd.Timedelta(days=29), a
    else: s = e = pd.Timestamp(period)
    return d[(d["filing_date"] >= s) & (d["filing_date"] < e + pd.Timedelta(days=1))]


# ---------------------------------------------------------------- interactive map
def build_map():
    g = df[["latitude", "longitude", "filing_date"]].dropna(subset=["latitude", "longitude"])
    g = g[g["latitude"].between(40.4, 41.0) & g["longitude"].between(-74.3, -73.6)]
    permit_pts = [[round(float(r.latitude), 6), round(float(r.longitude), 6),
                   "" if pd.isna(r.filing_date) else r.filing_date.strftime("%Y-%m-%d")]
                  for r in g.itertuples()]

    pluto_pts = []
    if len(pl):
        p = pl.dropna(subset=["latitude", "longitude", "assess_psf"])
        p = p[p["latitude"].between(40.4, 41.0) & p["longitude"].between(-74.3, -73.6)]
        if len(p):
            cap = p["assess_psf"].quantile(0.95) or 1
            pluto_pts = [[round(float(r.latitude), 6), round(float(r.longitude), 6),
                          round(min(float(r.assess_psf) / cap, 1.0), 3)]
                         for r in p.itertuples()]

    html = MAP_HTML.replace("__PERMITS__", json.dumps(permit_pts)) \
                   .replace("__PLUTO__", json.dumps(pluto_pts)) \
                   .replace("__TILES__", TILE_URL)
    Path(f"{OUT}/permits_geo_heatmap.html").write_text(html, encoding="utf-8")
    print(f"Saved map: {len(permit_pts)} permit pts, {len(pluto_pts)} PLUTO pts")


MAP_HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NYC heatmap</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>
<style>
html,body,#map{height:100%;margin:0;background:#111}
#panel{position:absolute;top:10px;right:10px;z-index:1000;background:rgba(20,20,24,.92);color:#eee;
 padding:12px;border-radius:8px;font:13px system-ui,sans-serif;width:230px}
#panel button{background:#2a2a32;color:#eee;border:1px solid #444;border-radius:5px;padding:5px 8px;margin:2px;cursor:pointer}
#panel button.on{background:#e8590c;border-color:#e8590c}
#panel input[type=date],#panel select{background:#2a2a32;color:#eee;border:1px solid #444;border-radius:5px;padding:4px;width:100%;box-sizing:border-box;margin:3px 0}
#count{font-weight:600;margin:8px 0 2px}#label{color:#aaa;font-size:12px}
h4{margin:0 0 8px}
</style></head><body>
<div id="map"></div>
<div id="panel">
 <h4>NYC heatmap</h4>
 <select id="layer"><option value="permits">DOB permits</option><option value="pluto">PLUTO assessed $/sqft</option></select>
 <div id="permitCtl">
  <div id="btns"></div>
  <div style="margin-top:8px">Specific day<input type="date" id="day"></div>
  <div>From<input type="date" id="from"></div>
  <div>To<input type="date" id="to"></div>
  <button id="applyRange">Apply range</button>
  <label style="display:block;margin-top:8px"><input type="checkbox" id="anchor" checked> Anchor to latest date in data</label>
 </div>
 <div id="count"></div><div id="label"></div>
</div>
<script>
const pts = __PERMITS__;
const plutoPts = __PLUTO__;
const maxDate = pts.reduce((m,p)=>p[2]>m?p[2]:m,"");
const map = L.map("map").setView([40.73,-73.95],11);
L.tileLayer("__TILES__",{maxZoom:19,attribution:"&copy; CARTO &copy; OpenStreetMap contributors"}).addTo(map);
const heat = L.heatLayer([],{radius:12,blur:15}).addTo(map);
const pad=n=>String(n).padStart(2,"0");
const iso=d=>d.getFullYear()+"-"+pad(d.getMonth()+1)+"-"+pad(d.getDate());
const parse=s=>{const [y,m,d]=s.split("-").map(Number);return new Date(y,m-1,d)};
const anchor=()=>document.getElementById("anchor").checked&&maxDate?parse(maxDate):new Date();
let cur=null;
function bounds(kind){
 const a=anchor();let s=new Date(a),e=new Date(a);
 if(kind==="yesterday"){s.setDate(s.getDate()-1);e=new Date(s)}
 else if(kind==="week"){s.setDate(s.getDate()-((s.getDay()+6)%7))}
 else if(kind==="month"){s=new Date(a.getFullYear(),a.getMonth(),1)}
 else if(kind==="30d"){s.setDate(s.getDate()-29)}
 return [iso(s),iso(e)];
}
function show(s,e,name,btn){
 const sel=(s===null)?pts:pts.filter(p=>p[2]>=s&&p[2]<=e);
 heat.setLatLngs(sel.map(p=>[p[0],p[1],0.6]));
 document.getElementById("count").textContent=sel.length+" permits";
 document.getElementById("label").textContent=name+(s?" ("+(s===e?s:s+" to "+e)+")":"");
 document.querySelectorAll("#btns button").forEach(b=>b.classList.toggle("on",b===btn));
}
[["Today","today"],["Yesterday","yesterday"],["This week","week"],["This month","month"],["Last 30 days","30d"],["All time","all"]]
.forEach(([name,kind])=>{
 const b=document.createElement("button");b.textContent=name;
 b.onclick=()=>{
  const run=()=>{const [s,e]=kind==="all"?[null,null]:bounds(kind);show(s,e,name,b)};
  cur=run;run();
 };
 document.getElementById("btns").appendChild(b);
});
document.getElementById("day").onchange=e=>{const v=e.target.value;if(v){cur=null;show(v,v,"Day",null)}};
document.getElementById("applyRange").onclick=()=>{
 const f=document.getElementById("from").value,t=document.getElementById("to").value;
 if(f&&t){cur=null;show(f,t,"Range",null)}};
document.getElementById("anchor").onchange=()=>{if(cur)cur()};
document.getElementById("layer").onchange=e=>{
 if(e.target.value==="pluto"){
  document.getElementById("permitCtl").style.display="none";
  heat.setLatLngs(plutoPts.map(p=>[p[0],p[1],p[2]]));
  document.getElementById("count").textContent=plutoPts.length+" parcels";
  document.getElementById("label").textContent="Brighter = higher assessed $/sqft";
 } else {
  document.getElementById("permitCtl").style.display="block";
  document.querySelectorAll("#btns button")[5].click();
 }
};
document.querySelectorAll("#btns button")[5].click();
</script></body></html>"""


# ---------------------------------------------------------------- permits charts
def permits_charts():
    c = apply_period(df, PERIOD)
    print(f"Permit charts period '{PERIOD}': {len(c)} permits")

    d = c.dropna(subset=["borough", "filing_date"]).copy()
    if len(d):
        d["month"] = d["filing_date"].dt.to_period("M").astype(str)
        p = d.pivot_table(index="borough", columns="month", aggfunc="size", fill_value=0)
        p = p[sorted(p.columns)[-12:]]
        plt.figure(figsize=(max(8, len(p.columns) * 0.9), 5))
        sns.heatmap(p, annot=True, fmt="d", cmap="YlOrRd")
        plt.title("Permits by borough and filing month")
        save("permits_borough_month.png")

    d = c.dropna(subset=["borough", "permit_type"])
    if len(d):
        p = d.pivot_table(index="borough", columns="permit_type", aggfunc="size", fill_value=0)
        plt.figure(figsize=(max(8, len(p.columns) * 1.1), 5))
        sns.heatmap(p, annot=True, fmt="d", cmap="Blues")
        plt.title("Permits by borough and permit type")
        save("permits_borough_type.png")

    d = c.dropna(subset=["neighborhood", "permit_type"])
    if len(d):
        top = d["neighborhood"].value_counts().head(15).index
        p = d[d["neighborhood"].isin(top)].pivot_table(
            index="neighborhood", columns="permit_type", aggfunc="size", fill_value=0)
        plt.figure(figsize=(max(8, len(p.columns) * 1.1), 7))
        sns.heatmap(p, annot=True, fmt="d", cmap="viridis")
        plt.title("Top 15 neighborhoods by permit type")
        save("permits_neighborhood_type.png")


def permits_trend():
    t = df.dropna(subset=["filing_date"]).copy()
    if len(t):
        t["month"] = t["filing_date"].dt.to_period("M").astype(str)
        plt.figure(figsize=(9, 5))
        t.groupby("month").size().tail(36).plot(marker="o", color="#7950f2")
        plt.title("Permits filed per month"); plt.xticks(rotation=45)
        save("permits_trend.png")
    o = df.dropna(subset=["permit_type"])
    if len(o):
        plt.figure(figsize=(8, 5))
        o["permit_type"].value_counts().head(10).sort_values().plot.barh(color="#7950f2")
        plt.title("Top permit types")
        save("permits_types.png")


def owner_leaderboards():
    o = df["owner_business_name"].dropna().astype(str).str.strip()
    o = o[~o.str.upper().isin(BAD_OWNERS)]
    if len(o):
        plt.figure(figsize=(9, 6))
        o.value_counts().head(12).sort_values().plot.barh(color="#fa5252")
        plt.title("Top permit filers (owner business)")
        save("permits_top_owners.png")
    if len(pl):
        po = pl["owner_name"].dropna().astype(str).str.strip()
        po = po[~po.str.upper().isin(BAD_OWNERS)]
        if len(po):
            plt.figure(figsize=(9, 6))
            po.value_counts().head(12).sort_values().plot.barh(color="#12b886")
            plt.title("Owners with the most PLUTO parcels (sampled)")
            save("pluto_top_owners.png")


# ---------------------------------------------------------------- ACRIS charts
def acris_charts():
    if sales.empty:
        return
    deeds = sales[(sales["doc_type"] == "DEED") & (sales["document_amt"] > 0)]
    if len(deeds):
        m = deeds.groupby("borough")["document_amt"].median().sort_values()
        plt.figure(figsize=(8, 5)); m.plot.barh(color="#e8590c")
        plt.title("Median deed price by borough"); plt.xlabel("USD")
        save("acris_median_price.png")

        plt.figure(figsize=(8, 5))
        sns.histplot(deeds["document_amt"], bins=30, log_scale=True, color="#e8590c")
        plt.title("Deed price distribution (log scale)")
        save("acris_price_hist.png")

    v = sales.dropna(subset=["rec"]).copy()
    if len(v):
        v["month"] = v["rec"].dt.to_period("M").astype(str)
        plt.figure(figsize=(9, 5))
        v[v["doc_type"] == "DEED"].groupby("month").size().plot(marker="o", color="#e8590c")
        plt.title("Deeds recorded per month"); plt.xticks(rotation=45)
        save("acris_deed_volume.png")

    plt.figure(figsize=(8, 5))
    sales["doc_type"].value_counts().head(8).sort_values().plot.barh(color="#4c6ef5")
    plt.title("ACRIS document types")
    save("acris_doc_types.png")


# ---------------------------------------------------------------- PLUTO charts
def pluto_charts():
    if pl.empty:
        return
    d = pl.dropna(subset=["assess_psf", "borough"])
    if len(d):
        plt.figure(figsize=(8, 5))
        d.groupby("borough")["assess_psf"].median().sort_values().plot.barh(color="#12b886")
        plt.title("Median assessed value per sqft by borough"); plt.xlabel("USD / sqft")
        save("pluto_assess_psf.png")
    yb = pl[pl["year_built"] > 1800]
    if len(yb):
        plt.figure(figsize=(8, 5))
        sns.histplot(yb["year_built"], bins=20, color="#12b886")
        plt.title("Building age (year built)")
        save("pluto_year_built.png")


run("permits_charts", permits_charts)
run("permits_trend", permits_trend)
run("owner_leaderboards", owner_leaderboards)
run("acris_charts", acris_charts)
run("pluto_charts", pluto_charts)
run("map", build_map)


# ---------------------------------------------------------------- index.html
def kpi(label, value):
    return f'<div class="kpi"><div class="v">{value}</div><div class="l">{label}</div></div>'

k = [kpi("Permits", f"{len(df):,}")]
if len(sales):
    dd = sales[(sales["doc_type"] == "DEED") & (sales["document_amt"] > 0)]
    k.append(kpi("ACRIS records", f"{len(sales):,}"))
    if len(dd):
        k.append(kpi("Median deed", f"${dd['document_amt'].median():,.0f}"))
if len(pl):
    k.append(kpi("PLUTO parcels", f"{len(pl):,}"))
    if pl["assess_psf"].notna().any():
        k.append(kpi("Median assessed $/sqft", f"${pl['assess_psf'].median():,.0f}"))

SECTIONS = [
    ("Permits", [("permits_borough_month.png", "Permits by borough and month"),
                 ("permits_borough_type.png", "Permits by borough and type"),
                 ("permits_neighborhood_type.png", "Top 15 neighborhoods by permit type"),
                 ("permits_trend.png", "Permits filed per month"),
                 ("permits_types.png", "Top permit types"),
                 ("permits_top_owners.png", "Top permit filers")]),
    ("Sales (ACRIS)", [("acris_median_price.png", "Median deed price by borough"),
                       ("acris_deed_volume.png", "Deeds recorded per month"),
                       ("acris_price_hist.png", "Deed price distribution"),
                       ("acris_doc_types.png", "Document types")]),
    ("Parcels (PLUTO)", [("pluto_assess_psf.png", "Assessed value per sqft by borough"),
                         ("pluto_year_built.png", "Building age"),
                         ("pluto_top_owners.png", "Top parcel owners")]),
]
v = int(time.time())
body = ""
nav = ""
for title, items in SECTIONS:
    cards = "".join(f'<section class="card"><h3>{t}</h3><img src="{fn}?v={v}" alt="{t}"></section>'
                    for fn, t in items if os.path.exists(f"{OUT}/{fn}"))
    if cards:
        sid = title.split()[0].lower()
        nav += f'<a href="#{sid}">{title}</a>'
        body += (f'<h2 id="{sid}"><span>{title}</span>'
                 f'<button class="magbtn">Magnify</button></h2>'
                 f'<div class="grid compact">{cards}</div>')

INDEX = """<!DOCTYPE html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="300">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NYC Real Estate Dashboard</title>
<style>
:root{--bg:#0f1014;--card:#1a1b22;--line:#2a2b35;--text:#ececf1;--muted:#8d8fa0;--accent:#ff7a45}
*{box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:70px}
body{margin:0;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--text)}
header{position:sticky;top:0;z-index:50;display:flex;align-items:center;justify-content:space-between;
 gap:16px;flex-wrap:wrap;padding:12px 28px;background:rgba(15,16,20,.85);backdrop-filter:blur(10px);
 border-bottom:1px solid var(--line)}
.brand{font-weight:700;font-size:18px;letter-spacing:.2px}
.brand span{color:var(--accent)}
.upd{color:var(--muted);font-size:12px;display:flex;align-items:center;gap:6px}
.dot{width:8px;height:8px;border-radius:50%;background:#37d67a;box-shadow:0 0 8px #37d67a}
nav{display:flex;gap:8px;flex-wrap:wrap}
nav a{color:var(--muted);text-decoration:none;font-size:13px;padding:5px 12px;border:1px solid var(--line);
 border-radius:999px;transition:.15s}
nav a:hover{color:var(--text);border-color:var(--accent)}
.wrap{max-width:1500px;margin:0 auto;padding:20px 28px 60px}
.kpis{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.kpi{background:linear-gradient(145deg,var(--card),#15161c);border:1px solid var(--line);
 border-radius:14px;padding:16px 18px;border-top:3px solid var(--accent)}
.kpi .v{font-size:26px;font-weight:700}
.kpi .l{color:var(--muted);font-size:12px;margin-top:4px;text-transform:uppercase;letter-spacing:.6px}
h2{margin:34px 0 14px;font-size:15px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);
 display:flex;align-items:center;gap:10px}
h2::after{content:"";flex:1;height:1px;background:var(--line)}
.grid{display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(460px,1fr))}
.grid.compact{grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:12px}
.grid.compact .card{padding:8px}
.grid.compact .card h3{font-size:12px;margin-bottom:6px}
.grid.big{grid-template-columns:1fr;max-width:1100px;margin:0 auto;gap:22px}
.grid.big .card{padding:18px}
.grid.big .card h3{font-size:17px;margin-bottom:12px}
.magbtn{background:var(--card);color:var(--text);border:1px solid var(--line);border-radius:999px;
 padding:5px 14px;font-size:12px;cursor:pointer;transition:.15s;text-transform:none;letter-spacing:0}
.magbtn:hover{border-color:var(--accent);color:var(--accent)}
h2 .magbtn{order:2}
h2::after{order:1}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;
 transition:transform .15s,border-color .15s}
.card:hover{transform:translateY(-2px);border-color:#44465a}
.card h3{margin:0 0 10px;font-size:14px;font-weight:600}
.card img{width:100%;height:auto;border-radius:8px;cursor:zoom-in;display:block}
.map{padding:0;overflow:hidden}
.map iframe{width:100%;height:68vh;min-height:460px;border:0;display:block}
#lb{position:fixed;inset:0;background:rgba(0,0,0,.88);display:none;align-items:center;
 justify-content:center;z-index:100;cursor:zoom-out;padding:24px}
#lb img{max-width:95vw;max-height:92vh;border-radius:10px}
@media(max-width:560px){.grid{grid-template-columns:1fr}.wrap{padding:14px}header{padding:10px 14px}}
</style></head><body>
<header>
 <div class="brand">NYC <span>Real Estate</span> Dashboard</div>
 <nav><a href="#map">Map</a>__NAV__</nav>
 <button class="magbtn" id="magAll">Magnify all</button>
 <div class="upd"><span class="dot"></span>Updated __STAMP__ &middot; hourly</div>
</header>
<div class="wrap">
 <div class="kpis">__KPIS__</div>
 <h2 id="map">Heatmap</h2>
 <section class="card map"><iframe src="permits_geo_heatmap.html?v=__V__"></iframe></section>
 __BODY__
</div>
<div id="lb"><img alt=""></div>
<script>
const lb=document.getElementById("lb"),lbi=lb.querySelector("img");
document.querySelectorAll(".card img").forEach(i=>i.onclick=()=>{lbi.src=i.src;lb.style.display="flex"});
lb.onclick=()=>lb.style.display="none";
function setMode(g,big){g.classList.toggle("big",big);g.classList.toggle("compact",!big);
 const b=g.previousElementSibling.querySelector(".magbtn");if(b)b.textContent=big?"Back to overview":"Magnify"}
document.querySelectorAll("h2 .magbtn").forEach(b=>b.onclick=()=>{
 const g=b.parentElement.nextElementSibling,big=!g.classList.contains("big");
 setMode(g,big);if(big)b.parentElement.scrollIntoView({behavior:"smooth"})});
let allBig=false;
document.getElementById("magAll").onclick=function(){allBig=!allBig;
 document.querySelectorAll(".grid").forEach(g=>setMode(g,allBig));
 this.textContent=allBig?"Overview all":"Magnify all"};
document.onkeydown=e=>{if(e.key==="Escape")lb.style.display="none"};
</script></body></html>"""

page = (INDEX.replace("__STAMP__", dt.datetime.now().strftime("%b %d, %H:%M"))
             .replace("__KPIS__", "".join(k)).replace("__NAV__", nav)
             .replace("__V__", str(v)).replace("__BODY__", body))
Path(f"{OUT}/index.html").write_text(page, encoding="utf-8")
print("Saved index.html")
