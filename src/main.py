"""
Script principal - Orchestrateur du pipeline immobilier.
Usage :
    python src/main.py              # Mode démo (recommandé pour tester)
    python src/main.py --live       # Mode scraping réel
    python src/main.py --schedule   # Mode planification quotidienne
"""

import os
import sys
import logging
import argparse
import yaml
try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False
import time
from datetime import datetime
from pathlib import Path

# Assurer que le dossier src est dans le path
sys.path.insert(0, str(Path(__file__).parent))

from collector import IdealistaCollector
from cleaner import DataCleaner
from analyzer import TextAnalyzer
from categorizer import AnnonceCategorizer
from exporter import DataExporter, AlertSender


def setup_logging(config: dict):
    os.makedirs("logs", exist_ok=True)
    log_config = config.get("logging", {})
    level = getattr(logging, log_config.get("level", "INFO"))
    log_file = log_config.get("file", "logs/execution.log")
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ]
    )


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Charge la configuration YAML."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_pipeline(config: dict, use_demo: bool = True):
    """Exécute le pipeline complet de collecte, analyse et export."""
    logger = logging.getLogger("main")
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("🏠 DÉMARRAGE DU PIPELINE IMMOBILIER")
    logger.info(f"Mode : {'Démonstration' if use_demo else 'Scraping réel'}")
    logger.info(f"Heure : {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)

    # 1. COLLECTE
    logger.info("\n📡 ÉTAPE 1 : Collecte des annonces")
    collector = IdealistaCollector(config)
    if use_demo:
        raw_annonces = collector.load_demo_data()
    else:
        base_url = config.get("scraping", {}).get("base_url")
        max_pages = config.get("scraping", {}).get("max_pages", 3)
        raw_annonces = collector.collect(base_url, max_pages)
    logger.info(f"→ {len(raw_annonces)} annonces collectées")

    # 2. NETTOYAGE
    logger.info("\n🧹 ÉTAPE 2 : Nettoyage des données")
    cleaner = DataCleaner(config)
    df_clean = cleaner.clean(raw_annonces)
    stats = cleaner.get_stats(df_clean)
    logger.info(f"→ {stats['total_annonces']} annonces après nettoyage")
    if stats["prix_moyen"]:
        logger.info(f"→ Prix moyen : {stats['prix_moyen']:,.0f} €")
    os.makedirs("data/cleaned", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_clean.to_csv(f"data/cleaned/annonces_clean_{ts}.csv", index=False)

    # 3. ANALYSE NLP
    logger.info("\n🔍 ÉTAPE 3 : Analyse NLP des descriptions")
    analyzer = TextAnalyzer()
    df_analyzed = analyzer.analyze_dataframe(df_clean, text_col="description")

    # 4. CATÉGORISATION
    logger.info("\n🏷️  ÉTAPE 4 : Catégorisation et filtrage")
    categorizer = AnnonceCategorizer(config)
    df_final = categorizer.categorize(df_analyzed)
    summary = categorizer.get_summary(df_final)
    logger.info(f"→ Annonces recommandées : {summary.get('total_recommandees', 0)}")
    df_recommandees = categorizer.get_filtered(df_final, only_recommended=True)

    # 5. EXPORT
    logger.info("\n💾 ÉTAPE 5 : Export des résultats")
    exporter = DataExporter(config)
    paths = exporter.export_all(df_final, df_recommandees)
    for fmt, path in paths.items():
        logger.info(f"→ {fmt.upper()} : {path}")

    # 6. ALERTES
    logger.info("\n📧 ÉTAPE 6 : Envoi des alertes")
    alert_sender = AlertSender(config)
    excel_path = paths.get("excel")
    alert_sender.send_email(df_recommandees, excel_path)

    elapsed = (datetime.now() - start_time).seconds
    logger.info(f"\n✅ PIPELINE TERMINÉ en {elapsed}s")
    logger.info(f"Total : {len(df_final)} annonces | Recommandées : {len(df_recommandees)}")
    return df_final, df_recommandees, paths


def run_scheduled(config: dict):
    """Lance le pipeline en mode planifié."""
    logger = logging.getLogger("scheduler")
    if not HAS_SCHEDULE:
        logger.error("pip install schedule")
        return
    heure = config.get("scheduling", {}).get("heure_execution", "08:00")
    schedule.every().day.at(heure).do(lambda: run_pipeline(config, use_demo=False))
    logger.info(f"⏰ Planifié à {heure} — en attente...")
    while True:
        schedule.run_pending()
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Pipeline d'automatisation immobilière")
    parser.add_argument("--live", action="store_true", help="Scraping réel")
    parser.add_argument("--schedule", action="store_true", help="Planification quotidienne")
    parser.add_argument("--config", default="config/config.yaml")
    args = parser.parse_args()
    config = load_config(args.config)
    setup_logging(config)
    if args.schedule:
        run_scheduled(config)
    else:
        run_pipeline(config, use_demo=not args.live)


if __name__ == "__main__":
    main()