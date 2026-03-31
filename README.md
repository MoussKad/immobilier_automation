# 🏠 ImmobilierBot — Automatisation des annonces immobilières

Pipeline Python complet pour collecter, analyser et filtrer les annonces d'idealista.com selon vos critères.

## 🚀 Installation rapide

```bash
git clone https://github.com/user/immobilierbot
cd immobilierbot
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate     # Windows
pip install -r config/requirements.txt
python src/main.py
```

## 🎯 Utilisation

| Commande | Description |
|----------|-------------|
| `python src/main.py` | Mode démonstration (12 annonces fictives) |
| `python src/main.py --live` | Scraping réel idealista.com |
| `python src/main.py --schedule` | Planification quotidienne |

## ⚙️ Configuration (config/config.yaml)

```yaml
filters:
  prix_max: 400000
  surface_min: 60
  etages_acceptes: ["dernier", "6", "7", "8"]
  orientations_souhaitees: ["sud", "sud-est", "est"]
```

## 🧠 Pipeline

Collecte → Nettoyage → NLP → Catégorisation → Export → Alertes

### Score global (0–100)
- 40% score NLP (orientation, état, équipements, vue)
- 60% score matching (correspondance à vos filtres)

### Catégories
| Score | Catégorie |
|-------|-----------|
| ≥ 80 | coup_de_coeur |
| ≥ 65 | très_intéressant |
| ≥ 50 | intéressant |
| ≥ 35 | à_surveiller |
| < 35 | non_prioritaire |

## 📁 Structure

```
immobilierbot/
├── config/
│   ├── config.yaml
│   └── requirements.txt
├── src/
│   ├── main.py        # Orchestrateur
│   ├── collector.py   # Scraping
│   ├── cleaner.py     # Nettoyage
│   ├── analyzer.py    # NLP
│   ├── categorizer.py # Scoring
│   └── exporter.py    # Export + Email
└── data/
    ├── raw/
    ├── cleaned/
    └── results/
```