"""
Module d'export (CSV, Excel coloré, SQLite) et d'alertes email.
"""

import os
import logging
import smtplib
import sqlite3
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataExporter:
    def __init__(self, config: dict):
        self.config = config
        self.export_config = config.get("export", {})
        os.makedirs("data/results", exist_ok=True)

    def _ts(self) -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S")

    def export_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        filename = filename or f"data/results/annonces_{self._ts()}.csv"
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        logger.info(f"✓ CSV → {filename}")
        return filename

    def export_excel(self, df: pd.DataFrame, df_recommandees: pd.DataFrame = None, filename: str = None) -> str:
        filename = filename or f"data/results/annonces_{self._ts()}.xlsx"
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Toutes les annonces", index=False)
            if df_recommandees is not None and len(df_recommandees) > 0:
                df_recommandees.to_excel(writer, sheet_name="Recommandées ⭐", index=False)
            self._build_stats_sheet(df).to_excel(writer, sheet_name="Statistiques", index=False)
            self._format_excel(writer, df, "Toutes les annonces")
            if df_recommandees is not None and len(df_recommandees) > 0:
                self._format_excel(writer, df_recommandees, "Recommandées ⭐")
        logger.info(f"✓ Excel → {filename}")
        return filename

    def _format_excel(self, writer, df, sheet_name):
        try:
            from openpyxl.styles import PatternFill, Font, Alignment
            ws = writer.sheets[sheet_name]
            h_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            for cell in ws[1]:
                cell.fill = h_fill
                cell.font = Font(color="FFFFFF", bold=True)
                cell.alignment = Alignment(horizontal="center")
            col_widths = {"titre":45,"prix":15,"surface":12,"etage":12,"orientation":15,
                          "etat":15,"score_global":15,"categorie":20,"lien":50,"description":60}
            for col in df.columns:
                idx = df.columns.get_loc(col) + 1
                ws.column_dimensions[ws.cell(row=1,column=idx).column_letter].width = col_widths.get(col,18)
            if "score_global" in df.columns:
                sc_idx = list(df.columns).index("score_global") + 1
                for ri, row in enumerate(ws.iter_rows(min_row=2, max_row=ws.max_row), start=2):
                    score = ws.cell(row=ri, column=sc_idx).value
                    if score is not None:
                        color = "C6EFCE" if score >= 80 else "FFEB9C" if score >= 65 else "FFC7CE"
                        fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
                        for cell in row:
                            cell.fill = fill
        except Exception as e:
            logger.warning(f"Mise en forme Excel : {e}")

    def _build_stats_sheet(self, df):
        stats = []
        for label, col, fmt in [("Prix moyen","prix","{:,.0f} €"),("Prix médian","prix",None),
                                  ("Surface moyenne","surface","{:.0f} m²"),("Prix/m² moyen","prix_m2","{:,.0f} €/m²")]:
            if col in df.columns:
                val = df[col].mean() if "moyen" in label else df[col].median()
                stats.append({"Indicateur": label, "Valeur": (fmt or "{:,.0f} €").format(val) if val else "N/A"})
        if "score_global" in df.columns:
            stats.append({"Indicateur":"Score moyen","Valeur":f"{df['score_global'].mean():.1f}/100"})
        if "categorie" in df.columns:
            for cat, count in df["categorie"].value_counts().items():
                stats.append({"Indicateur":f"Catégorie: {cat}","Valeur":str(count)})
        stats.append({"Indicateur":"Total annonces","Valeur":str(len(df))})
        return pd.DataFrame(stats)

    def export_sqlite(self, df: pd.DataFrame, db_path: str = None) -> str:
        db_path = db_path or self.export_config.get("db_path","data/results/immobilier.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            if "date_collecte" not in df.columns:
                df["date_collecte"] = datetime.now().isoformat()
            df.to_sql("annonces", conn, if_exists="append", index=False)
            conn.execute("DELETE FROM annonces WHERE rowid NOT IN (SELECT MAX(rowid) FROM annonces GROUP BY id)")
            conn.commit()
            count = conn.execute("SELECT COUNT(*) FROM annonces").fetchone()[0]
            logger.info(f"✓ SQLite → {db_path} ({count} entrées)")
        finally:
            conn.close()
        return db_path

    def export_all(self, df: pd.DataFrame, df_recommandees: pd.DataFrame = None) -> dict:
        paths = {}
        if self.export_config.get("csv", True):
            paths["csv"] = self.export_csv(df)
        if self.export_config.get("excel", True):
            paths["excel"] = self.export_excel(df, df_recommandees)
        if self.export_config.get("sqlite", True):
            paths["sqlite"] = self.export_sqlite(df, self.export_config.get("db_path"))
        return paths


class AlertSender:
    def __init__(self, config: dict):
        self.config = config.get("alerts", {})

    def send_email(self, df_recommandees: pd.DataFrame, excel_path: str = None) -> bool:
        email_config = self.config.get("email", {})
        if not email_config.get("enabled", False):
            logger.info("Email désactivé dans la config")
            return False
        if df_recommandees is None or len(df_recommandees) == 0:
            return False
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"🏠 {len(df_recommandees)} nouvelles annonces — {datetime.now().strftime('%d/%m/%Y')}"
            msg["From"] = email_config["sender"]
            msg["To"] = ", ".join(email_config["recipients"])
            rows = ""
            for _, row in df_recommandees.iterrows():
                score = row.get("score_global", 0)
                c = "#27ae60" if score >= 80 else "#f39c12" if score >= 65 else "#e74c3c"
                rows += f"<tr><td><a href='{row.get('lien','#')}'>{str(row.get('titre',''))[:60]}</a></td><td>{row.get('prix',0):,.0f} €</td><td>{row.get('surface',0)} m²</td><td>{row.get('etage','')}</td><td>{row.get('orientation','')}</td><td style='color:{c};font-weight:bold'>{score}/100</td></tr>"
            html = f"<html><body><h2>🏠 Nouvelles annonces</h2><p>{len(df_recommandees)} annonces correspondantes</p><table border='1' cellpadding='8' style='border-collapse:collapse'><thead style='background:#1F4E79;color:white'><tr><th>Titre</th><th>Prix</th><th>Surface</th><th>Étage</th><th>Orient.</th><th>Score</th></tr></thead><tbody>{rows}</tbody></table></body></html>"
            msg.attach(MIMEText(html, "html", "utf-8"))
            if excel_path and os.path.exists(excel_path):
                with open(excel_path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(excel_path)}")
                    msg.attach(part)
            with smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"]) as server:
                server.starttls()
                server.login(email_config["sender"], email_config["password"])
                server.sendmail(email_config["sender"], email_config["recipients"], msg.as_string())
            logger.info("✓ Email envoyé")
            return True
        except Exception as e:
            logger.error(f"Erreur email : {e}")
            return False