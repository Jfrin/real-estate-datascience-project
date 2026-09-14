import os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from cassandra.cluster import Cluster
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "nyc_realestate")

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
REPORT_FROM = os.environ.get("REPORT_FROM_EMAIL", SMTP_USER)
REPORT_TO = os.environ["REPORT_TO_EMAIL"]

PDF_PATH = "/tmp/daily_market_pulse.pdf"


def fetch_counts():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    from collections import Counter
    rows = session.execute("SELECT borough FROM permits_current")
    counts = Counter(r.borough for r in rows)
    permits_by_borough = [type("Row", (), {"borough": b, "cnt": c})() for b, c in counts.items()]
    
    total_sales = session.execute("SELECT COUNT(*) FROM sales_acris").one()[0]
    total_parcels = session.execute("SELECT COUNT(*) FROM parcels_pluto").one()[0]

    session.shutdown()
    cluster.shutdown()
    return permits_by_borough, total_sales, total_parcels


def build_pdf(permits_by_borough, total_sales, total_parcels):
    doc = SimpleDocTemplate(PDF_PATH, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = [
        Paragraph("NYC Real-Estate Daily Market Pulse", styles["Title"]),
        Spacer(1, 12),
        Paragraph(f"Total ACRIS sales records tracked: {total_sales}", styles["Normal"]),
        Paragraph(f"Total PLUTO parcels tracked: {total_parcels}", styles["Normal"]),
        Spacer(1, 16),
        Paragraph("DOB Permits by Borough", styles["Heading2"]),
    ]

    table_data = [["Borough", "Permit Count"]] + [[r.borough, r.cnt] for r in permits_by_borough]
    table = Table(table_data)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#333333")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
    ]))
    elements.append(table)

    doc.build(elements)


def send_email():
    msg = MIMEMultipart()
    msg["From"] = REPORT_FROM
    msg["To"] = REPORT_TO
    msg["Subject"] = "NYC Real-Estate Daily Market Pulse"
    msg.attach(MIMEText("See attached PDF for today's market pulse report.", "plain"))

    with open(PDF_PATH, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename="daily_market_pulse.pdf")
        msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)


def main():
    permits_by_borough, total_sales, total_parcels = fetch_counts()
    build_pdf(permits_by_borough, total_sales, total_parcels)
    send_email()
    print("Report generated and emailed successfully.")


if __name__ == "__main__":
    main()
