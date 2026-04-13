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

# ─── CONFIGURATION LOGGING ───────────────────────────────────────────────────

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


# ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

def run_pipeline(config: dict, use_demo: bool = True):
    """Exécute le pipeline complet de collecte, analyse et export."""
    logger = logging.getLogger("main")
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("🏠 DÉMARRAGE DU PIPELINE IMMOBILIER")
    logger.info(f"Mode : {'Démonstration' if use_demo else 'Scraping réel'}")
    logger.info(f"Heure : {start_time.strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)

    # ── 0. NETTOYAGE DES ANCIENS FICHIERS ────────────────────────
    logger.info("\n🗂  ÉTAPE 0 : Nettoyage des anciens exports")
    exporter_early = DataExporter(config)
    exporter_early.cleanup()

    # ── 1. COLLECTE ──────────────────────────────────────────────
    logger.info("\n📡 ÉTAPE 1 : Collecte des annonces")
    collector = IdealistaCollector(config)

    if use_demo:
        raw_annonces = collector.load_demo_data()
    else:
        base_url = config.get("scraping", {}).get("base_url")
        max_pages = config.get("scraping", {}).get("max_pages", 3)
        raw_annonces = collector.collect(base_url, max_pages)

    logger.info(f"→ {len(raw_annonces)} annonces collectées")

    # ── 2. NETTOYAGE ──────────────────────────────────────────────
    logger.info("\n🧹 ÉTAPE 2 : Nettoyage des données")
    cleaner = DataCleaner(config)
    df_clean = cleaner.clean(raw_annonces)

    stats = cleaner.get_stats(df_clean)
    logger.info(f"→ {stats['total_annonces']} annonces après nettoyage")
    if stats["prix_moyen"]:
        logger.info(f"→ Prix moyen : {stats['prix_moyen']:,.0f} €")
    if stats["surface_moyenne"]:
        logger.info(f"→ Surface moyenne : {stats['surface_moyenne']:.0f} m²")

    # Sauvegarder les données nettoyées
    os.makedirs("data/cleaned", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_clean.to_csv(f"data/cleaned/annonces_clean_{ts}.csv", index=False)

    # ── 3. ANALYSE NLP ────────────────────────────────────────────
    logger.info("\n🔍 ÉTAPE 3 : Analyse NLP des descriptions")
    analyzer = TextAnalyzer()
    df_analyzed = analyzer.analyze_dataframe(df_clean, text_col="description")

    # Statistiques NLP
    if "orientation" in df_analyzed.columns:
        orient_counts = df_analyzed["orientation"].value_counts()
        logger.info(f"→ Orientations détectées : {orient_counts.to_dict()}")
    if "etat" in df_analyzed.columns:
        etat_counts = df_analyzed["etat"].value_counts()
        logger.info(f"→ États détectés : {etat_counts.to_dict()}")

    # ── 4. CATÉGORISATION ─────────────────────────────────────────
    logger.info("\n🏷️  ÉTAPE 4 : Catégorisation et filtrage")
    categorizer = AnnonceCategorizer(config)
    df_final = categorizer.categorize(df_analyzed)

    summary = categorizer.get_summary(df_final)
    logger.info(f"→ Distribution : {summary.get('par_categorie', {})}")
    logger.info(f"→ Annonces recommandées : {summary.get('total_recommandees', 0)}")

    df_recommandees = categorizer.get_filtered(df_final, only_recommended=True)

    # ── 5. EXPORT ─────────────────────────────────────────────────
    logger.info("\n💾 ÉTAPE 5 : Export des résultats")
    exporter = DataExporter(config)
    paths = exporter.export_all(df_final, df_recommandees)

    for fmt, path in paths.items():
        logger.info(f"→ {fmt.upper()} : {path}")

    # ── 6. ALERTES ────────────────────────────────────────────────
    logger.info("\n📧 ÉTAPE 6 : Envoi des alertes")
    alert_sender = AlertSender(config)
    excel_path = paths.get("excel")
    email_sent = alert_sender.send_email(df_recommandees, excel_path)
    if not email_sent:
        logger.info("→ Alertes désactivées (configurer config/config.yaml pour les activer)")

    # ── RÉSUMÉ FINAL ──────────────────────────────────────────────
    elapsed = (datetime.now() - start_time).seconds
    logger.info("\n" + "=" * 60)
    logger.info("✅ PIPELINE TERMINÉ")
    logger.info(f"Durée : {elapsed}s")
    logger.info(f"Total annonces analysées : {len(df_final)}")
    logger.info(f"Annonces recommandées : {len(df_recommandees)}")

    # Afficher le top 5
    if len(df_recommandees) > 0:
        logger.info("\n🏆 TOP ANNONCES :")
        cols_display = ["titre", "prix", "surface", "etage", "orientation",
                        "etat", "score_global", "categorie"]
        cols_display = [c for c in cols_display if c in df_recommandees.columns]

        for i, row in df_recommandees.head(5).iterrows():
            logger.info(f"\n  #{i+1} ─ Score: {row.get('score_global', 'N/A')}/100")
            logger.info(f"  Titre     : {row.get('titre', 'N/A')[:70]}")
            logger.info(f"  Prix      : {row.get('prix', 'N/A'):,.0f} €")
            logger.info(f"  Surface   : {row.get('surface', 'N/A')} m²")
            logger.info(f"  Étage     : {row.get('etage', 'N/A')}")
            logger.info(f"  Orient.   : {row.get('orientation', 'N/A')}")
            logger.info(f"  État      : {row.get('etat', 'N/A')}")
            logger.info(f"  Catégorie : {row.get('categorie', 'N/A')}")
            logger.info(f"  Lien      : {row.get('lien', 'N/A')}")

    logger.info("=" * 60)
    return df_final, df_recommandees, paths


# ─── PLANIFICATION ───────────────────────────────────────────────────────────

def run_scheduled(config: dict):
    """Lance le pipeline en mode planifié."""
    logger = logging.getLogger("scheduler")
    if not HAS_SCHEDULE:
        logger.error("Module 'schedule' non installé. Installez-le avec : pip install schedule")
        return
    heure = config.get("scheduling", {}).get("heure_execution", "08:00")

    logger.info(f"⏰ Planification : exécution quotidienne à {heure}")

    def job():
        logger.info("⏰ Exécution planifiée déclenchée")
        try:
            run_pipeline(config, use_demo=False)
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution planifiée : {e}", exc_info=True)

    schedule.every().day.at(heure).do(job)

    logger.info("En attente... (Ctrl+C pour arrêter)")
    while True:
        schedule.run_pending()
        time.sleep(60)


# ─── POINT D'ENTRÉE ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline d'automatisation immobilière"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Mode scraping réel (par défaut : données de démonstration)"
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Mode planification quotidienne"
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Chemin vers le fichier de configuration"
    )
    args = parser.parse_args()

    # Charger la config
    config = load_config(args.config)
    setup_logging(config)

    if args.schedule:
        run_scheduled(config)
    else:
        use_demo = not args.live
        run_pipeline(config, use_demo=use_demo)


if __name__ == "__main__":
    main()
