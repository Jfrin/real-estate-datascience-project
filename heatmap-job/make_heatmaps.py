import glob
import json
import os
import time
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# >>> PASTE YOUR CARTO KEY BETWEEN THE QUOTES <<<
CARTO_API_KEY = "PASTE_YOUR_KEY_HERE"
TILE_URL = f"https://basemaps.cartocdn.com/rastertiles/dark_all/{{z}}/{{x}}/{{y}}.png?key={CARTO_API_KEY}"

LAKEHOUSE_PATH = Path(os.environ.get("LAKEHOUSE_PATH", "/data/lakehouse"))
LAKEHOUSE_TABLE = os.environ.get("LAKEHOUSE_TABLE", "permits_current")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
OUT = os.environ.get("OUTPUT_DIR", "/output")
# PNG charts: all | today | yesterday | week | month | 30d | YYYY-MM-DD
PERIOD = os.environ.get("PERIOD", "all")
ANCHOR_LATEST = os.environ.get("ANCHOR_LATEST", "true").lower() == "true"

COLS = ["permit_si_no", "borough", "neighborhood", "permit_type",
        "permit_status", "filing_date", "latitude", "longitude"]

os.makedirs(OUT, exist_ok=True)


def load_from_lakehouse():
    files = sorted(glob.glob(str(LAKEHOUSE_PATH / "silver" / LAKEHOUSE_TABLE / "dt=*" / "data.parquet")))
    if not files:
        return None
    frames = []
    for f in files:
        part = pd.read_parquet(f)
        part["_dt"] = Path(f).parent.name.replace("dt=", "")
        frames.append(part)
    df = pd.concat(frames, ignore_index=True).sort_values("_dt")
    df = df.drop_duplicates(subset="permit_si_no", keep="last")  # newest snapshot wins
    print(f"Lakehouse: {len(files)} snapshot(s), {len(df)} unique permits")
    return df.reindex(columns=COLS)


def load_from_cassandra():
    from cassandra.cluster import Cluster
    for attempt in range(1, 21):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(KEYSPACE)
            rows = list(session.execute(f"SELECT {', '.join(COLS)} FROM permits_current"))
            cluster.shutdown()
            if rows:
                print(f"Cassandra fallback: {len(rows)} rows")
                return pd.DataFrame([r._asdict() for r in rows])
            print(f"[{attempt}/20] permits_current is empty, waiting...")
        except Exception as e:
            print(f"[{attempt}/20] Cassandra not ready: {e}")
        time.sleep(15)
    raise SystemExit("No permit data in lakehouse or Cassandra.")


df = load_from_lakehouse()
if df is None or df.empty:
    print("Lakehouse empty - falling back to Cassandra.")
    df = load_from_cassandra()

df["borough"] = df["borough"].astype(str).str.strip().str.upper()
df["filing_date"] = pd.to_datetime(df["filing_date"].astype(str), errors="coerce")
latest = df["filing_date"].max()
print("Latest filing_date in data:", latest)


# ---------- period filter (used for the PNG charts) ----------
def apply_period(d, period):
    if period == "all" or pd.isna(latest):
        return d
    a = latest.normalize() if ANCHOR_LATEST else pd.Timestamp.today().normalize()
    if period == "today":
        s = e = a
    elif period == "yesterday":
        s = e = a - pd.Timedelta(days=1)
    elif period == "week":
        s, e = a - pd.Timedelta(days=a.weekday()), a
    elif period == "month":
        s, e = a.replace(day=1), a
    elif period == "30d":
        s, e = a - pd.Timedelta(days=29), a
    else:
        s = e = pd.Timestamp(period)
    return d[(d["filing_date"] >= s) & (d["filing_date"] < e + pd.Timedelta(days=1))]


# ---------- interactive map with period buttons ----------
g = df[["latitude", "longitude", "filing_date"]].dropna(subset=["latitude", "longitude"])
g = g[g["latitude"].between(40.4, 41.0) & g["longitude"].between(-74.3, -73.6)]
points = [[round(float(r.latitude), 6), round(float(r.longitude), 6),
           "" if pd.isna(r.filing_date) else r.filing_date.strftime("%Y-%m-%d")]
          for r in g.itertuples()]

HTML = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>NYC permits heatmap</title>
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
#panel input[type=date]{background:#2a2a32;color:#eee;border:1px solid #444;border-radius:5px;padding:4px;width:100%;box-sizing:border-box;margin:3px 0}
#count{font-weight:600;margin:8px 0 2px}#label{color:#aaa;font-size:12px}
h4{margin:0 0 8px}
</style></head><body>
<div id="map"></div>
<div id="panel">
 <h4>Permits heatmap</h4>
 <div id="btns"></div>
 <div style="margin-top:8px">Specific day<input type="date" id="day"></div>
 <div>From<input type="date" id="from"></div>
 <div>To<input type="date" id="to"></div>
 <button id="applyRange">Apply range</button>
 <label style="display:block;margin-top:8px"><input type="checkbox" id="anchor" checked> Anchor to latest date in data</label>
 <div id="count"></div><div id="label"></div>
</div>
<script>
const pts = __DATA__;
const maxDate = pts.reduce((m,p)=>p[2]>m?p[2]:m,"");
const map = L.map("map").setView([40.73,-73.95],11);
L.tileLayer("__TILES__",{maxZoom:19,attribution:"&copy; CARTO &copy; OpenStreetMap contributors"}).addTo(map);
const heat = L.heatLayer([],{radius:12,blur:15}).addTo(map);
const pad=n=>String(n).padStart(2,"0");
const iso=d=>d.getFullYear()+"-"+pad(d.getMonth()+1)+"-"+pad(d.getDate());
const parse=s=>{const [y,m,d]=s.split("-").map(Number);return new Date(y,m-1,d)};
const anchor=()=>document.getElementById("anchor").checked&&maxDate?parse(maxDate):new Date();
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
 b.onclick=()=>{const [s,e]=kind==="all"?[null,null]:bounds(kind);cur=()=>{const [s2,e2]=kind==="all"?[null,null]:bounds(kind);show(s2,e2,name,b)};show(s,e,name,b)};
 document.getElementById("btns").appendChild(b);
});
let cur=null;
document.getElementById("day").onchange=e=>{const v=e.target.value;if(v)show(v,v,"Day",null)};
document.getElementById("applyRange").onclick=()=>{
 const f=document.getElementById("from").value,t=document.getElementById("to").value;
 if(f&&t)show(f,t,"Range",null)};
document.getElementById("anchor").onchange=()=>{if(cur)cur()};
document.querySelectorAll("#btns button")[5].click();
</script></body></html>"""

html = (HTML.replace("__DATA__", json.dumps(points))
            .replace("__TILES__", TILE_URL))
Path(f"{OUT}/permits_geo_heatmap.html").write_text(html, encoding="utf-8")
print(f"Saved permits_geo_heatmap.html ({len(points)} points, period buttons in the page)")

# ---------- PNG charts (period from PERIOD env) ----------
c = apply_period(df, PERIOD)
print(f"PNG charts use period '{PERIOD}': {len(c)} permits")

d = c.dropna(subset=["borough", "filing_date"]).copy()
if len(d):
    d["month"] = d["filing_date"].dt.to_period("M").astype(str)
    pivot = d.pivot_table(index="borough", columns="month", aggfunc="size", fill_value=0)
    pivot = pivot[sorted(pivot.columns)[-12:]]
    plt.figure(figsize=(max(8, len(pivot.columns) * 0.9), 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="YlOrRd")
    plt.title("Permits by borough and filing month")
    plt.tight_layout()
    plt.savefig(f"{OUT}/permits_borough_month.png", dpi=150)
    plt.close()
    print("Saved permits_borough_month.png")

d = c.dropna(subset=["borough", "permit_type"])
if len(d):
    pivot = d.pivot_table(index="borough", columns="permit_type", aggfunc="size", fill_value=0)
    plt.figure(figsize=(max(8, len(pivot.columns) * 1.1), 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="Blues")
    plt.title("Permits by borough and permit type")
    plt.tight_layout()
    plt.savefig(f"{OUT}/permits_borough_type.png", dpi=150)
    plt.close()
    print("Saved permits_borough_type.png")

d = c.dropna(subset=["neighborhood", "permit_type"])
if len(d):
    top = d["neighborhood"].value_counts().head(15).index
    pivot = d[d["neighborhood"].isin(top)].pivot_table(
        index="neighborhood", columns="permit_type", aggfunc="size", fill_value=0)
    plt.figure(figsize=(max(8, len(pivot.columns) * 1.1), 7))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="viridis")
    plt.title("Top 15 neighborhoods by permit type")
    plt.tight_layout()
    plt.savefig(f"{OUT}/permits_neighborhood_type.png", dpi=150)
    plt.close()
    print("Saved permits_neighborhood_type.png")
