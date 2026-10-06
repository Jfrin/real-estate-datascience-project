"""Query Cassandra, render heatmap + charts, write static page atomically."""
import os, tempfile, datetime as dt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs
from cassandra.cluster import Cluster

OUT = os.environ.get("DASHBOARD_DIR", "/opt/dashboard")
HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra").split(",")
KS = os.environ.get("CASSANDRA_KEYSPACE", "esra")
DAYS = 90

def q(session, cql, params=None):
    return pd.DataFrame(list(session.execute(cql, params or [])))

def main():
    session = Cluster(HOSTS).connect(KS)
    since = dt.date.today() - dt.timedelta(days=DAYS)
    # Assumed tables: neighborhood_daily_agg, rates_pmms, borough_daily_agg
    nb = q(session, "SELECT neighborhood_id, borough, date, median_ppsf, sale_count "
                    "FROM neighborhood_daily_agg WHERE date >= %s ALLOW FILTERING", [since])
    bo = q(session, "SELECT borough, date, median_ppsf, sale_count, permit_count "
                    "FROM borough_daily_agg WHERE date >= %s ALLOW FILTERING", [since])
    rt = q(session, "SELECT week, rate_30y, rate_15y FROM rates_pmms "
                    "WHERE week >= %s ALLOW FILTERING", [since])
    figs = []

    # 1. Heatmap: neighborhood x week, median $/sqft
    nb["date"] = pd.to_datetime(nb["date"])
    nb["week"] = nb["date"].dt.to_period("W").dt.start_time
    piv = nb.pivot_table(index="neighborhood_id", columns="week",
                         values="median_ppsf", aggfunc="median")
    piv = piv.loc[piv.mean(axis=1).sort_values(ascending=False).index[:40]]
    h = go.Figure(go.Heatmap(z=piv.values, x=piv.columns, y=piv.index,
                             colorscale="YlOrRd", colorbar_title="$/sqft"))
    h.update_layout(title="Median price per sqft: top 40 neighborhoods by week",
                    height=800, yaxis_autorange="reversed")
    figs.append(h)

    # 2. Borough trend
    figs.append(px.line(bo, x="date", y="median_ppsf", color="borough",
                        title="Median $/sqft by borough"))
    # 3. Sales volume
    figs.append(px.bar(bo.groupby("borough", as_index=False)["sale_count"].sum(),
                       x="borough", y="sale_count", title=f"Recorded sales, last {DAYS} days"))
    # 4. Permits
    figs.append(px.area(bo, x="date", y="permit_count", color="borough",
                        title="DOB permit activity"))
    # 5. Mortgage rates
    figs.append(px.line(rt, x="week", y=["rate_30y", "rate_15y"],
                        title="Freddie Mac PMMS mortgage rates (%)"))

    body = "\n".join(f'<section>{f.to_html(full_html=False, include_plotlyjs=False)}</section>'
                     for f in figs)
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="300"><title>ESRA Market Dashboard</title>
<script src="plotly.min.js"></script>
<style>body{{font-family:system-ui,sans-serif;margin:0;background:#f5f5f5}}
header{{padding:16px 24px;background:#1b2a41;color:#fff}}
main{{display:grid;grid-template-columns:repeat(auto-fit,minmax(520px,1fr));gap:16px;padding:16px}}
section{{background:#fff;border-radius:8px;padding:8px}}
section:first-child{{grid-column:1/-1}}</style></head><body>
<header><h2>NYC Market Dashboard</h2><small>Last updated {now} (hourly DAG)</small></header>
<main>{body}</main></body></html>"""

    os.makedirs(OUT, exist_ok=True)
    js = os.path.join(OUT, "plotly.min.js")
    if not os.path.exists(js):
        open(js, "w").write(get_plotlyjs())
    fd, tmp = tempfile.mkstemp(dir=OUT, suffix=".html")
    with os.fdopen(fd, "w") as f:
        f.write(html)
    os.chmod(tmp, 0o644)
    os.replace(tmp, os.path.join(OUT, "index.html"))  # atomic swap

if __name__ == "__main__":
    main()
