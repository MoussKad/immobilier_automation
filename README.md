# 🏠 ImmobilierBot — Automatisation des annonces immobilières

Pipeline Python complet pour collecter, analyser (NLP) et filtrer les annonces d'idealista.com selon vos critères. Inclut un serveur Flask et un dashboard web interactif.

---

## 🚀 Installation

```bash
git clone https://github.com/user/immobilierbot
cd immobilierbot

python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows

pip install -r config/requirements.txt
```

---

## 🎯 Utilisation

### Mode dashboard (recommandé)

Lance le serveur Flask, puis ouvre le dashboard dans le navigateur :

```bash
python server.py
# → http://localhost:5000
```

Le dashboard permet de :
- Lancer le pipeline en **mode démo** ou **scraping réel**
- Voir les logs en **temps réel** (Server-Sent Events)
- Consulter les annonces, scores NLP, graphiques
- **Télécharger** le CSV ou Excel généré

### Mode ligne de commande

```bash
python src/main.py              # Mode démo (12 annonces fictives)
python src/main.py --live       # Scraping réel idealista.com
python src/main.py --schedule   # Planification quotidienne à 08:00
```

---

## 🌐 API Flask

| Méthode | Route | Description |
|---------|-------|-------------|
| `GET` | `/` | Dashboard web |
| `POST` | `/api/run` | Lance le pipeline `{"demo": true}` |
| `GET` | `/api/stream` | Logs en temps réel (SSE) |
| `GET` | `/api/results` | Résultats du dernier run (JSON) |
| `GET` | `/api/download/csv` | Télécharger le CSV |
| `GET` | `/api/download/excel` | Télécharger le fichier Excel |
| `GET` | `/api/history` | Historique des exécutions (SQLite) |
| `GET/POST` | `/api/config` | Lire / modifier les filtres |

---

## ⚙️ Configuration

Modifiez `config/config.yaml` :

```yaml
scraping:
  base_url: "https://www.idealista.com/venta-viviendas/valencia-valencia/"
  max_pages: 3

filters:
  prix_max: 400000
  prix_min: 100000
  surface_min: 60
  etages_acceptes: ["dernier", "5", "6", "7", "8"]
  orientations_souhaitees: ["sud", "sud-est", "est"]
  mots_cles_positifs: ["rénové", "terrasse", "parking", "ascenseur"]
  mots_cles_negatifs: ["bajo", "interior", "a reformar", "sótano"]

alerts:
  email:
    enabled: false          # Passer à true + remplir smtp_* pour activer
    smtp_server: "smtp.gmail.com"
    smtp_port: 587
    sender: "votre@gmail.com"
    password: "mot_de_passe_application"
    recipients: ["vous@email.com"]
```

---

## 🧠 Pipeline (5 étapes)

```
Collecte → Nettoyage → NLP → Scoring → Export
```

| Étape | Module | Description |
|-------|--------|-------------|
| 1 | `collector.py` | Scraping idealista + fallback démo automatique si blocage |
| 2 | `cleaner.py` | Normalisation prix/m²/étages FR+ES, déduplication |
| 3 | `analyzer.py` | NLP : orientation, état, équipements, transports, vue |
| 4 | `categorizer.py` | Score 0–100 = 40% NLP + 60% matching filtres |
| 5 | `exporter.py` | CSV, Excel coloré (3 onglets), SQLite historique, email |

### Score et catégories

| Score | Catégorie |
|-------|-----------|
| ≥ 80 | 💚 coup_de_coeur |
| ≥ 65 | 🟡 très_intéressant |
| ≥ 50 | 🟠 intéressant |
| ≥ 35 | 🔵 à_surveiller |
| < 35 | 🔴 non_prioritaire |

Les filtres durs (hors budget, surface insuffisante, mots-clés rédhibitoires) excluent l'annonce directement, quelle que soit son score NLP.

---

## 📁 Structure du projet

```
immobilierbot/
├── server.py              ← Serveur Flask + API REST
├── dashboard.html         ← Dashboard web (servi par Flask)
├── config/
│   ├── config.yaml        ← Paramètres (URL, filtres, alertes)
│   └── requirements.txt   ← Dépendances Python
├── src/
│   ├── main.py            ← Orchestrateur CLI
│   ├── collector.py       ← Scraping + données démo
│   ├── cleaner.py         ← Nettoyage et normalisation
│   ├── analyzer.py        ← Analyse NLP (FR + ES)
│   ├── categorizer.py     ← Scoring et filtrage
│   └── exporter.py        ← CSV, Excel, SQLite, email
└── data/
    ├── raw/               ← JSON brut horodaté
    ├── cleaned/           ← CSV après nettoyage
    └── results/           ← Excel, CSV final, SQLite, last_run.json
```

---

## 📧 Alertes email (Gmail)

1. Activer la validation en 2 étapes sur votre compte Google
2. Générer un [mot de passe d'application](https://myaccount.google.com/apppasswords)
3. Renseigner `sender`, `password` et `recipients` dans `config.yaml`
4. Passer `enabled: true`

---

## ⚠️ Notes légales

Le scraping peut violer les CGU de certains sites. Ce projet est fourni à des fins éducatives. Consultez le `robots.txt` du site cible avant toute utilisation. En cas de blocage (403/CAPTCHA), le pipeline bascule automatiquement sur les données de démonstration.
