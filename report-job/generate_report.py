import os, glob, smtplib, datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle, Paragraph,
                                Spacer, Image, PageBreak)

# ---------------------------------------------------------------- config
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
REPORT_FROM = os.environ.get("REPORT_FROM_EMAIL", SMTP_USER)
REPORT_TO = os.environ["REPORT_TO_EMAIL"]

HEATMAP_DIR = os.environ.get("HEATMAP_DIR", "/heatmaps")      # mount ./heatmaps here
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8088")
SALES_LIMIT = int(os.environ.get("SALES_LIMIT", "300000"))

WORK_DIR = "/tmp/report"
PDF_PATH = f"{WORK_DIR}/daily_market_pulse.pdf"
os.makedirs(WORK_DIR, exist_ok=True)

BOROUGHS = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}


# ---------------------------------------------------------------- helpers
def find_col(df, *names):
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def norm_borough(v):
    s = str(v).strip()
    return BOROUGHS.get(s, s.title())


# ---------------------------------------------------------------- data
def fetch():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    s = cluster.connect(KEYSPACE)
    s.row_factory = dict_factory
    s.default_timeout = 120

    permits = pd.DataFrame(list(s.execute("SELECT * FROM permits_current")))
    sales = pd.DataFrame(list(s.execute(f"SELECT * FROM sales_acris LIMIT {SALES_LIMIT}")))

    def count(table):
        try:
            return s.execute(f"SELECT COUNT(*) AS n FROM {table}").one()["n"]
        except Exception:
            return None

    totals = {"ACRIS sales records": count("sales_acris"),
              "PLUTO parcels": count("parcels_pluto"),
              "DOB permits": len(permits)}
    s.shutdown()
    cluster.shutdown()
    return permits, sales, totals


# ---------------------------------------------------------------- charts
def _save(fig, name, title):
    path = f"{WORK_DIR}/{name}.png"
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    return (title, path)


def bar_chart(series, title, name, horizontal=False):
    if series is None or series.empty:
        return None
    fig, ax = plt.subplots(figsize=(7, 3.6))
    series.plot(kind="barh" if horizontal else "bar", ax=ax, color="#2b6cb0")
    ax.set_title(title)
    ax.set_xlabel("")
    ax.set_ylabel("")
    if horizontal:
        ax.invert_yaxis()
    return _save(fig, name, title)


def line_chart(series, title, name):
    if series is None or len(series) < 2:
        return None
    fig, ax = plt.subplots(figsize=(7, 3.6))
    series.plot(ax=ax, color="#2b6cb0", marker="o", markersize=3)
    ax.set_title(title)
    ax.set_xlabel("")
    ax.grid(alpha=0.3)
    return _save(fig, name, title)


def build_charts(permits, sales):
    charts, tables = [], []

    if not permits.empty:
        b = find_col(permits, "borough")
        if b:
            s = permits[b].map(norm_borough).value_counts()
            charts.append(bar_chart(s, "DOB permits by borough", "permits_borough"))
            tables.append(("DOB permits by borough",
                           s.rename_axis("Borough").reset_index(name="Permits")))

        t = find_col(permits, "job_type", "permit_type", "work_type")
        if t:
            s = permits[t].astype(str).value_counts().head(10)
            charts.append(bar_chart(s, f"Top permit types ({t})", "permits_type", horizontal=True))

        st = find_col(permits, "permit_status", "job_status", "status")
        if st:
            s = permits[st].astype(str).value_counts().head(10)
            charts.append(bar_chart(s, "Permits by status", "permits_status", horizontal=True))

        d = find_col(permits, "issuance_date", "issued_date", "filing_date", "approved_date")
        if d:
            days = pd.to_datetime(permits[d], errors="coerce").dt.normalize().dropna()
            s = days.value_counts().sort_index().tail(30)
            charts.append(line_chart(s, "Permits per day (last 30 days with data)", "permits_daily"))

    if not sales.empty:
        dc = find_col(sales, "doc_type")
        if dc:
            deeds = sales[sales[dc].astype(str).str.upper().str.contains("DEED", na=False)]
            if not deeds.empty:
                sales = deeds
        b = find_col(sales, "recorded_borough", "borough")
        amt = find_col(sales, "document_amt", "sale_price", "price")
        d = find_col(sales, "recorded_datetime", "document_date")
        if b:
            sales = sales.assign(_b=sales[b].map(norm_borough))
            counts = sales["_b"].value_counts()
            charts.append(bar_chart(counts, "Recorded deeds by borough (ACRIS)", "sales_borough"))
            tbl = counts.rename_axis("Borough").reset_index(name="Deeds")
            if amt:
                sales = sales.assign(_amt=pd.to_numeric(sales[amt], errors="coerce"))
                priced = sales[sales["_amt"] > 0]
                if not priced.empty:
                    med = priced.groupby("_b")["_amt"].median().round(0)
                    charts.append(bar_chart(med, "Median recorded price by borough ($)", "sales_median"))
                    tbl = tbl.merge(med.rename("Median Price ($)").reset_index()
                                    .rename(columns={"_b": "Borough"}), on="Borough", how="left")
            tables.append(("Recorded sales by borough (ACRIS)", tbl))
        if d:
            days = pd.to_datetime(sales[d], errors="coerce").dt.normalize().dropna()
            s = days.value_counts().sort_index().tail(30)
            charts.append(line_chart(s, "Deeds recorded per day (last 30 days with data)", "sales_daily"))

    return [c for c in charts if c], tables


# ---------------------------------------------------------------- heatmap snapshot
def heatmap_snapshots(max_items=3):
    """Use PNG/JPG heatmaps as-is; screenshot HTML heatmaps if playwright is installed."""
    found = []
    if not os.path.isdir(HEATMAP_DIR):
        return found

    files = glob.glob(f"{HEATMAP_DIR}/**/*", recursive=True)
    images = sorted([f for f in files if f.lower().endswith((".png", ".jpg", ".jpeg"))],
                    key=os.path.getmtime, reverse=True)[:max_items]
    for p in images:
        found.append((os.path.splitext(os.path.basename(p))[0].replace("_", " ").title(), p, None))

    if not images:
        htmls = sorted([f for f in files if f.lower().endswith(".html") and
                        os.path.basename(f).lower() != "index.html"],
                       key=os.path.getmtime, reverse=True)[:max_items]
        if not htmls:
            htmls = [f for f in files if f.lower().endswith(".html")][:1]
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as pw:
                browser = pw.chromium.launch(args=["--no-sandbox"])
                page = browser.new_page(viewport={"width": 1200, "height": 800})
                for h in htmls:
                    page.goto(f"file://{h}")
                    page.wait_for_timeout(4000)
                    out = f"{WORK_DIR}/snap_{os.path.basename(h)}.png"
                    page.screenshot(path=out)
                    found.append((os.path.splitext(os.path.basename(h))[0].replace("_", " ").title(),
                                  out, f"{DASHBOARD_URL}/{os.path.relpath(h, HEATMAP_DIR)}"))
                browser.close()
        except Exception as e:
            print(f"Heatmap screenshot skipped ({e}). Install playwright or have heatmap-job export PNGs.")
    return found


# ---------------------------------------------------------------- PDF
def rl_image(path, max_w=6.5 * inch, max_h=6.0 * inch):
    w, h = ImageReader(path).getSize()
    scale = min(max_w / w, max_h / h)
    return Image(path, width=w * scale, height=h * scale)


def build_pdf(totals, tables, charts, heatmaps, stamp):
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(PDF_PATH, pagesize=letter)
    el = [Paragraph("NYC Real-Estate Daily Market Pulse", styles["Title"]),
          Paragraph(stamp, styles["Normal"]), Spacer(1, 12)]

    for k, v in totals.items():
        el.append(Paragraph(f"{k}: {v:,}" if isinstance(v, int) else f"{k}: n/a", styles["Normal"]))
    el.append(Spacer(1, 14))

    for title, df in tables:
        el.append(Paragraph(title, styles["Heading2"]))
        data = [list(df.columns)] + df.head(15).astype(str).values.tolist()
        t = Table(data)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#333333")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTSIZE", (0, 0), (-1, -1), 10)]))
        el += [t, Spacer(1, 12)]

    el.append(PageBreak())
    el.append(Paragraph("Charts", styles["Heading1"]))
    for title, path in charts:
        el += [rl_image(path, max_h=3.6 * inch), Spacer(1, 10)]

    if heatmaps:
        el.append(PageBreak())
        el.append(Paragraph("Heatmap Snapshot", styles["Heading1"]))
        for title, path, _ in heatmaps:
            el += [Paragraph(title, styles["Heading3"]), rl_image(path), Spacer(1, 10)]
    doc.build(el)


# ---------------------------------------------------------------- email
def send_email(totals, tables, charts, heatmaps, stamp):
    root = MIMEMultipart("mixed")
    root["From"] = REPORT_FROM
    root["To"] = REPORT_TO
    root["Subject"] = f"NYC Real-Estate Daily Market Pulse | {dt.date.today():%b %d, %Y}"

    kpis = "".join(f"<li><b>{k}:</b> {v:,}</li>" if isinstance(v, int) else f"<li><b>{k}:</b> n/a</li>"
                   for k, v in totals.items())
    html = [f"<h2>NYC Real-Estate Daily Market Pulse</h2><p>{stamp}</p><ul>{kpis}</ul>"]
    for title, df in tables:
        html.append(f"<h3>{title}</h3>" + df.head(15).to_html(index=False, border=1))

    cid_images = []
    html.append("<h3>Charts</h3>")
    for i, (title, path) in enumerate(charts):
        html.append(f'<p><img src="cid:chart{i}" style="max-width:640px"></p>')
        cid_images.append((f"chart{i}", path))

    if heatmaps:
        html.append("<h3>Heatmap Snapshot</h3>")
        for i, (title, path, link) in enumerate(heatmaps):
            html.append(f"<p><b>{title}</b></p><p><img src=\"cid:heat{i}\" style=\"max-width:640px\"></p>")
            if link:
                html.append(f'<p><a href="{link}">Open interactive version</a></p>')
            cid_images.append((f"heat{i}", path))
    html.append(f'<p><a href="{DASHBOARD_URL}">Open full dashboard</a></p>')

    related = MIMEMultipart("related")
    related.attach(MIMEText("".join(html), "html"))
    for cid, path in cid_images:
        with open(path, "rb") as f:
            img = MIMEImage(f.read())
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=os.path.basename(path))
        related.attach(img)
    root.attach(related)

    with open(PDF_PATH, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="pdf")
    part.add_header("Content-Disposition", "attachment", filename="daily_market_pulse.pdf")
    root.attach(part)

    for title, df in tables:
        part = MIMEApplication(df.to_csv(index=False).encode(), _subtype="csv")
        fname = title.lower().replace(" ", "_").replace("(", "").replace(")", "") + ".csv"
        part.add_header("Content-Disposition", "attachment", filename=fname)
        root.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(root)


def main():
    stamp = f"Generated {dt.datetime.now():%Y-%m-%d %H:%M}"
    permits, sales, totals = fetch()
    charts, tables = build_charts(permits, sales)
    heatmaps = heatmap_snapshots()
    build_pdf(totals, tables, charts, heatmaps, stamp)
    send_email(totals, tables, charts, heatmaps, stamp)
    print(f"Report emailed: {len(charts)} charts, {len(tables)} tables, {len(heatmaps)} heatmap images.")


if __name__ == "__main__":
    main()