import os
import time
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import HeatMap
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")
OUT = os.environ.get("OUTPUT_DIR", "/output")
RETRIES = int(os.environ.get("RETRIES", "20"))
RETRY_SECONDS = int(os.environ.get("RETRY_SECONDS", "15"))

CARTO_API_KEY = os.environ.get("CARTO_API_KEY", "")
CARTO_TILE_URL = os.environ.get("CARTO_TILE_URL", "")  # use {api_key} as placeholder

QUERY = ("SELECT borough, neighborhood, permit_type, permit_status, "
         "filing_date, latitude, longitude FROM permits_current")

os.makedirs(OUT, exist_ok=True)


def load_permits():
    # Streaming jobs may not have created/filled the table yet, so retry.
    for attempt in range(1, RETRIES + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(KEYSPACE)
            rows = list(session.execute(QUERY))
            cluster.shutdown()
            if rows:
                return pd.DataFrame([r._asdict() for r in rows])
            print(f"[{attempt}/{RETRIES}] permits_current is empty, waiting...")
        except Exception as e:
            print(f"[{attempt}/{RETRIES}] Cassandra not ready: {e}")
        time.sleep(RETRY_SECONDS)
    raise SystemExit("No permit data available after retries.")


df = load_permits()
print(f"Loaded {len(df)} rows")

df["borough"] = df["borough"].astype(str).str.strip().str.upper()
df["filing_date"] = pd.to_datetime(df["filing_date"].astype(str), errors="coerce")

# 1) Geographic heatmap (interactive HTML)
g = df[["latitude", "longitude"]].dropna()
g = g[g["latitude"].between(40.4, 41.0) & g["longitude"].between(-74.3, -73.6)]
if len(g):
    if CARTO_API_KEY and CARTO_TILE_URL:
        m = folium.Map(location=[40.73, -73.95], zoom_start=11,
                       tiles=CARTO_TILE_URL.replace("{api_key}", CARTO_API_KEY),
                       attr="CARTO")
        print("Using CARTO tiles with API key")
    else:
        m = folium.Map(location=[40.73, -73.95], zoom_start=11, tiles="CartoDB dark_matter")
        print("Using free CartoDB dark_matter tiles")
    HeatMap(g.values.tolist(), radius=8, blur=12).add_to(m)
    m.save(f"{OUT}/permits_geo_heatmap.html")
    print(f"Saved permits_geo_heatmap.html ({len(g)} points)")
else:
    print("No valid NYC coordinates - skipping geographic heatmap.")

# 2) Borough x filing month (last 12 months with data)
d = df.dropna(subset=["borough", "filing_date"]).copy()
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

# 3) Borough x permit type
d = df.dropna(subset=["borough", "permit_type"])
if len(d):
    pivot = d.pivot_table(index="borough", columns="permit_type", aggfunc="size", fill_value=0)
    plt.figure(figsize=(max(8, len(pivot.columns) * 1.1), 5))
    sns.heatmap(pivot, annot=True, fmt="d", cmap="Blues")
    plt.title("Permits by borough and permit type")
    plt.tight_layout()
    plt.savefig(f"{OUT}/permits_borough_type.png", dpi=150)
    plt.close()
    print("Saved permits_borough_type.png")

# 4) Top 15 neighborhoods x permit type
d = df.dropna(subset=["neighborhood", "permit_type"])
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
else:
    print("No neighborhood data - skipping neighborhood heatmap.")
