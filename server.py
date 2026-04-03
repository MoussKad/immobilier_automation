"""
Serveur Flask — API ImmobilierBot
----------------------------------
Lance avec :  python server.py
Dashboard  :  http://localhost:5000
API docs   :  http://localhost:5000/api/status
"""

import os
import sys
import json
import time
import queue
import logging
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory

# ── Assurer que src/ est dans le path ────────────────────────────────────────
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))

from collector import IdealistaCollector
from cleaner import DataCleaner
from analyzer import TextAnalyzer
from categorizer import AnnonceCategorizer
from exporter import DataExporter, AlertSender

# ── App Flask ─────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=str(ROOT), static_url_path="")
app.config["SECRET_KEY"] = "immobilierbot-dev"

# ── État global du pipeline ───────────────────────────────────────────────────
pipeline_state = {
    "running": False,
    "last_run": None,
    "last_result": None,   # dict avec annonces, stats, etc.
    "log_queue": queue.Queue(),
    "run_count": 0,
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER : SSE event formatter
# ─────────────────────────────────────────────────────────────────────────────

def sse_event(data: dict, event: str = "message") -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE RUNNER (thread séparé)
# ─────────────────────────────────────────────────────────────────────────────

class SSELogHandler(logging.Handler):
    """Redirige les logs Python vers la queue SSE."""
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q

    def emit(self, record):
        msg = self.format(record)
        level = record.levelname
        color = {"INFO": "#6FCF97", "WARNING": "#F2C94C",
                 "ERROR": "#EB5757", "DEBUG": "#8A95A3"}.get(level, "#E0E0E0")
        self.q.put({"type": "log", "message": msg, "level": level, "color": color,
                    "ts": datetime.now().strftime("%H:%M:%S")})


def run_pipeline_thread(use_demo: bool, config: dict):
    """Exécute le pipeline dans un thread et pousse les événements dans la queue."""
    q = pipeline_state["log_queue"]
    start_time = datetime.now()

    def push(msg, level="INFO", color="#6FCF97"):
        q.put({"type": "log", "message": msg, "level": level, "color": color,
               "ts": datetime.now().strftime("%H:%M:%S")})

    def push_step(step: int, name: str, status: str):
        """status: running | done | error"""
        q.put({"type": "step", "step": step, "name": name, "status": status})

    try:
        pipeline_state["running"] = True
        pipeline_state["run_count"] += 1

        # Attacher le handler SSE au logger racine
        sse_handler = SSELogHandler(q)
        sse_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S"
        ))
        root_logger = logging.getLogger()
        root_logger.addHandler(sse_handler)
        root_logger.setLevel(logging.INFO)

        push("═" * 52, color="#C4972A")
        push("🏠  DÉMARRAGE DU PIPELINE", color="#C4972A")
        push(f"Mode : {'Démonstration' if use_demo else 'Scraping réel'}", color="#8A95A3")
        push(f"Heure : {start_time.strftime('%d/%m/%Y %H:%M:%S')}", color="#8A95A3")
        push("═" * 52, color="#C4972A")

        # ── ÉTAPE 1 : Collecte ────────────────────────────────────────────────
        push_step(1, "Collecte", "running")
        push("\n📡  ÉTAPE 1 : Collecte des annonces", color="#58A6FF")

        collector = IdealistaCollector(config)
        if use_demo:
            raw_annonces = collector.load_demo_data()
        else:
            base_url = config.get("scraping", {}).get("base_url")
            max_pages = config.get("scraping", {}).get("max_pages", 3)
            raw_annonces = collector.collect(base_url, max_pages)

        push(f"→ {len(raw_annonces)} annonces collectées", color="#6FCF97")
        push_step(1, "Collecte", "done")
        time.sleep(0.1)

        # ── ÉTAPE 2 : Nettoyage ───────────────────────────────────────────────
        push_step(2, "Nettoyage", "running")
        push("\n🧹  ÉTAPE 2 : Nettoyage des données", color="#58A6FF")

        cleaner = DataCleaner(config)
        df_clean = cleaner.clean(raw_annonces)
        stats = cleaner.get_stats(df_clean)
        push(f"→ {stats['total_annonces']} annonces valides après nettoyage", color="#6FCF97")
        if stats.get("prix_moyen"):
            push(f"→ Prix moyen : {stats['prix_moyen']:,.0f} €", color="#6FCF97")
        if stats.get("surface_moyenne"):
            push(f"→ Surface moyenne : {stats['surface_moyenne']:.0f} m²", color="#6FCF97")

        os.makedirs("data/cleaned", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_clean.to_csv(f"data/cleaned/annonces_clean_{ts}.csv", index=False)
        push_step(2, "Nettoyage", "done")
        time.sleep(0.1)

        # ── ÉTAPE 3 : NLP ─────────────────────────────────────────────────────
        push_step(3, "NLP", "running")
        push("\n🔍  ÉTAPE 3 : Analyse NLP des descriptions", color="#58A6FF")

        analyzer = TextAnalyzer()
        df_analyzed = analyzer.analyze_dataframe(df_clean, text_col="description")

        orient_counts = df_analyzed["orientation"].value_counts().to_dict() if "orientation" in df_analyzed.columns else {}
        etat_counts = df_analyzed["etat"].value_counts().to_dict() if "etat" in df_analyzed.columns else {}
        push(f"→ Orientations : {orient_counts}", color="#6FCF97")
        push(f"→ États : {etat_counts}", color="#6FCF97")
        push_step(3, "NLP", "done")
        time.sleep(0.1)

        # ── ÉTAPE 4 : Scoring ─────────────────────────────────────────────────
        push_step(4, "Scoring", "running")
        push("\n🏷️   ÉTAPE 4 : Catégorisation et scoring", color="#58A6FF")

        categorizer = AnnonceCategorizer(config)
        df_final = categorizer.categorize(df_analyzed)
        summary = categorizer.get_summary(df_final)
        push(f"→ Distribution : {summary.get('par_categorie', {})}", color="#6FCF97")
        push(f"→ Annonces recommandées : {summary.get('total_recommandees', 0)}", color="#6FCF97")

        df_recommandees = categorizer.get_filtered(df_final, only_recommended=True)
        push_step(4, "Scoring", "done")
        time.sleep(0.1)

        # ── ÉTAPE 5 : Export ──────────────────────────────────────────────────
        push_step(5, "Export", "running")
        push("\n💾  ÉTAPE 5 : Export des résultats", color="#58A6FF")

        exporter = DataExporter(config)
        paths = exporter.export_all(df_final, df_recommandees)
        for fmt, path in paths.items():
            push(f"→ {fmt.upper()} : {path}", color="#6FCF97")

        push_step(5, "Export", "done")

        # ── Résumé ────────────────────────────────────────────────────────────
        elapsed = (datetime.now() - start_time).total_seconds()
        push(f"\n{'═' * 52}", color="#C4972A")
        push(f"✅  PIPELINE TERMINÉ — {elapsed:.2f}s", color="#C4972A")
        push(f"Annonces analysées   : {len(df_final)}", color="#6FCF97")
        push(f"Annonces recommandées: {len(df_recommandees)}", color="#6FCF97")
        push(f"{'═' * 52}", color="#C4972A")

        # ── Sérialiser les résultats ──────────────────────────────────────────
        def safe_val(v):
            if hasattr(v, "item"):
                return v.item()
            return v

        annonces_list = []
        for _, row in df_final.iterrows():
            annonces_list.append({
                "id": str(row.get("id", "")),
                "titre": str(row.get("titre", "")),
                "prix": safe_val(row.get("prix")),
                "surface": safe_val(row.get("surface")),
                "etage": str(row.get("etage", "")),
                "orientation": str(row.get("orientation", "")),
                "etat": str(row.get("etat", "")),
                "score_global": int(safe_val(row.get("score_global", 0))),
                "score_attractivite": int(safe_val(row.get("score_attractivite", 0))),
                "categorie": str(row.get("categorie", "")),
                "recommande": bool(row.get("recommande", False)),
                "localisation": str(row.get("localisation", "")),
                "description": str(row.get("description", ""))[:200],
                "lien": str(row.get("lien", "")),
                "prix_m2": safe_val(row.get("prix_m2")),
                "a_parking": bool(row.get("a_parking", False)),
                "a_ascenseur": bool(row.get("a_ascenseur", False)),
                "a_terrasse": bool(row.get("a_terrasse", False)),
                "a_balcon": bool(row.get("a_balcon", False)),
                "proche_metro": bool(row.get("proche_metro", False)),
                "luminosite": str(row.get("luminosite", "")),
                "equipements": str(row.get("equipements", "")),
                "vue": str(row.get("vue", "")),
            })

        result = {
            "annonces": annonces_list,
            "stats": {
                "total": len(df_final),
                "recommandees": len(df_recommandees),
                "prix_moyen": round(float(df_final["prix"].mean()), 0) if "prix" in df_final else 0,
                "prix_median": round(float(df_final["prix"].median()), 0) if "prix" in df_final else 0,
                "surface_moyenne": round(float(df_final["surface"].mean()), 0) if "surface" in df_final else 0,
                "prix_m2_moyen": round(float(df_final["prix_m2"].mean()), 0) if "prix_m2" in df_final else 0,
                "score_moyen": round(float(df_final["score_global"].mean()), 1) if "score_global" in df_final else 0,
                "par_categorie": {str(k): int(v) for k, v in df_final["categorie"].value_counts().items()},
                "par_orientation": {str(k): int(v) for k, v in df_final["orientation"].value_counts().items()},
                "par_etat": {str(k): int(v) for k, v in df_final["etat"].value_counts().items()},
            },
            "elapsed": elapsed,
            "ran_at": datetime.now().isoformat(),
            "mode": "demo" if use_demo else "live",
            "export_paths": paths,
        }

        pipeline_state["last_result"] = result
        pipeline_state["last_run"] = datetime.now().isoformat()

        # Sauvegarder le résultat JSON pour le dashboard
        os.makedirs("data/results", exist_ok=True)
        with open("data/results/last_run.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        # Persister dans l'historique SQLite
        _save_run_history(result)

        # Signal de fin pour le client SSE
        q.put({"type": "done", "result": result})

    except Exception as e:
        import traceback
        err = traceback.format_exc()
        push(f"\n❌  ERREUR : {e}", level="ERROR", color="#EB5757")
        push(err, level="ERROR", color="#EB5757")
        q.put({"type": "error", "message": str(e)})

    finally:
        pipeline_state["running"] = False
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
            if isinstance(h, SSELogHandler):
                root_logger.removeHandler(h)


def _save_run_history(result: dict):
    """Persist a run summary to SQLite."""
    os.makedirs("data/results", exist_ok=True)
    conn = sqlite3.connect("data/results/immobilier.db")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at TEXT,
            mode TEXT,
            total INTEGER,
            recommandees INTEGER,
            elapsed REAL,
            prix_moyen REAL
        )
    """)
    conn.execute(
        "INSERT INTO run_history (ran_at, mode, total, recommandees, elapsed, prix_moyen) VALUES (?,?,?,?,?,?)",
        (result["ran_at"], result["mode"], result["stats"]["total"],
         result["stats"]["recommandees"], result["elapsed"], result["stats"]["prix_moyen"])
    )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Sert le dashboard HTML."""
    return send_from_directory(str(ROOT), "dashboard.html")


@app.route("/api/status")
def api_status():
    """Statut du serveur et du dernier run."""
    return jsonify({
        "status": "ok",
        "running": pipeline_state["running"],
        "last_run": pipeline_state["last_run"],
        "run_count": pipeline_state["run_count"],
        "has_data": pipeline_state["last_result"] is not None,
    })


@app.route("/api/run", methods=["POST"])
def api_run():
    """Lance le pipeline. Body JSON optionnel : {"demo": true/false}"""
    if pipeline_state["running"]:
        return jsonify({"error": "Pipeline déjà en cours"}), 409

    body = request.get_json(silent=True) or {}
    use_demo = body.get("demo", True)

    # Charger config
    config_path = ROOT / "config" / "config.yaml"
    import yaml
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Vider la queue
    while not pipeline_state["log_queue"].empty():
        pipeline_state["log_queue"].get_nowait()

    # Lancer dans un thread
    t = threading.Thread(target=run_pipeline_thread, args=(use_demo, config), daemon=True)
    t.start()

    return jsonify({"status": "started", "demo": use_demo})


@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events — diffuse les logs du pipeline en temps réel.
    Écouter avec : EventSource('/api/stream')
    """
    def generate():
        # Heartbeat initial
        yield sse_event({"type": "connected", "ts": datetime.now().isoformat()}, "connected")

        # Attendre les messages
        while True:
            try:
                msg = pipeline_state["log_queue"].get(timeout=25)
                yield sse_event(msg)

                # Si le pipeline est fini on envoie un dernier event puis on ferme
                if msg.get("type") in ("done", "error"):
                    yield sse_event({"type": "close"}, "close")
                    break

            except queue.Empty:
                # Heartbeat pour garder la connexion vivante
                yield sse_event({"type": "heartbeat", "ts": datetime.now().isoformat()}, "heartbeat")

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        }
    )


@app.route("/api/results")
def api_results():
    """Retourne le résultat du dernier run."""
    # Essayer d'abord depuis la mémoire
    if pipeline_state["last_result"]:
        return jsonify(pipeline_state["last_result"])

    # Sinon lire le fichier JSON
    json_path = ROOT / "data" / "results" / "last_run.json"
    if json_path.exists():
        with open(json_path, encoding="utf-8") as f:
            return jsonify(json.load(f))

    return jsonify({"error": "Aucun résultat disponible. Lancez d'abord le pipeline."}), 404


@app.route("/api/history")
def api_history():
    """Retourne les N dernières exécutions depuis SQLite."""
    db_path = ROOT / "data" / "results" / "immobilier.db"
    if not db_path.exists():
        return jsonify([])
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM run_history ORDER BY id DESC LIMIT 20"
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["GET"])
def api_config_get():
    """Retourne la configuration actuelle (sans secrets)."""
    import yaml
    config_path = ROOT / "config" / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Masquer les mots de passe
    if "alerts" in cfg:
        if "email" in cfg["alerts"]:
            cfg["alerts"]["email"]["password"] = "***"
    return jsonify(cfg)


@app.route("/api/config", methods=["POST"])
def api_config_post():
    """Met à jour les filtres de configuration."""
    import yaml
    config_path = ROOT / "config" / "config.yaml"
    body = request.get_json(silent=True) or {}

    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Mettre à jour seulement les filtres
    allowed_keys = ["prix_max", "prix_min", "surface_min",
                    "etages_acceptes", "orientations_souhaitees",
                    "mots_cles_positifs", "mots_cles_negatifs"]
    for k in allowed_keys:
        if k in body:
            cfg["filters"][k] = body[k]

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    return jsonify({"status": "ok", "updated": list(body.keys())})


# ─────────────────────────────────────────────────────────────────────────────
# DÉMARRAGE
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print()
    print("  ╔══════════════════════════════════════════╗")
    print("  ║    🏠  ImmobilierBot — Serveur Flask     ║")
    print("  ╠══════════════════════════════════════════╣")
    print("  ║  Dashboard  →  http://localhost:5000     ║")
    print("  ║  API status →  http://localhost:5000/api/status  ║")
    print("  ╚══════════════════════════════════════════╝")
    print()

    os.chdir(ROOT)  # Assurer que les chemins relatifs sont corrects
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)


# ── DOWNLOAD ROUTE (ajouté) ───────────────────────────────────────────────────

@app.route("/api/download/<fmt>")
def api_download(fmt):
    """
    Télécharge le dernier fichier exporté.
    GET /api/download/csv
    GET /api/download/excel
    """
    import glob
    from flask import send_file

    results_dir = ROOT / "data" / "results"
    if fmt == "csv":
        pattern = str(results_dir / "annonces_*.csv")
        mime = "text/csv"
        ext = "csv"
    elif fmt == "excel":
        pattern = str(results_dir / "annonces_*.xlsx")
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ext = "xlsx"
    else:
        return jsonify({"error": "Format inconnu. Utilisez 'csv' ou 'excel'."}), 400

    files = sorted(glob.glob(pattern))
    if not files:
        return jsonify({"error": f"Aucun fichier {fmt} disponible. Lancez d'abord le pipeline."}), 404

    latest = files[-1]
    filename = f"immobilierbot_annonces.{ext}"
    return send_file(latest, mimetype=mime, as_attachment=True, download_name=filename)
