"""
Module de collecte (scraping) des annonces immobilières.
Cible principale : idealista.com
Fallback automatique : données de démonstration réalistes.
"""

import requests
import time
import random
import logging
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)


DEMO_ANNONCES = [
    {"id": "demo_001", "titre": "Appartement 3 pièces - Vue mer - Dernier étage",
     "prix": 285000, "surface": 78, "nb_pieces": 3, "nb_chambres": 2,
     "etage": "dernier", "etage_raw": "Último piso",
     "description": "Magnifique appartement au dernier étage orienté sud avec terrasse de 15m². Entièrement rénové en 2022. Vue panoramique. Ascenseur, parking inclus. Proche métro ligne 3.",
     "lien": "https://www.idealista.com/immobile/demo001/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Valencia Centro", "source": "demo"},
    {"id": "demo_002", "titre": "Piso luminoso 2 habitaciones - Ático",
     "prix": 195000, "surface": 65, "nb_pieces": 2, "nb_chambres": 1,
     "etage": "dernier", "etage_raw": "Ático",
     "description": "Ático luminoso orientado sur-este. Terraza privada de 20m². Cocina renovada. Ascensor. Sin garaje. Muy tranquilo.",
     "lien": "https://www.idealista.com/immobile/demo002/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Ruzafa, Valencia", "source": "demo"},
    {"id": "demo_003", "titre": "Studio rez-de-chaussée - intérieur",
     "prix": 89000, "surface": 35, "nb_pieces": 1, "nb_chambres": 0,
     "etage": "0", "etage_raw": "Bajo",
     "description": "Studio en bajo interior. A reformar. Sin ascensor. Muy céntrico. Ideal inversión.",
     "lien": "https://www.idealista.com/immobile/demo003/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Valencia Centro", "source": "demo"},
    {"id": "demo_004", "titre": "Appartement 4 pièces - 5ème étage - Orienté Est",
     "prix": 320000, "surface": 95, "nb_pieces": 4, "nb_chambres": 3,
     "etage": "5", "etage_raw": "5ª planta",
     "description": "Grand appartement lumineux orienté est avec balcon. Parquet, double vitrage. Rénové cuisine et salles de bain. Immeuble avec gardien. 2 parkings en sous-sol.",
     "lien": "https://www.idealista.com/immobile/demo004/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Mestalla, Valencia", "source": "demo"},
    {"id": "demo_005", "titre": "Piso 3 habitaciones - 7ª planta - Sur",
     "prix": 245000, "surface": 82, "nb_pieces": 3, "nb_chambres": 2,
     "etage": "7", "etage_raw": "7ª planta",
     "description": "Piso en planta alta orientado al sur. Mucha luz natural. Balcón con vistas. Cocina americana. Ascensor. Comunidad tranquila. Cerca del centro comercial.",
     "lien": "https://www.idealista.com/immobile/demo005/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Benimaclet, Valencia", "source": "demo"},
    {"id": "demo_006", "titre": "Appartement 2 pièces - 3ème étage - Nord",
     "prix": 160000, "surface": 58, "nb_pieces": 2, "nb_chambres": 1,
     "etage": "3", "etage_raw": "3ª planta",
     "description": "Appartement intérieur au 3ème étage orienté nord. Cuisine séparée. Sans ascenseur. Copropriété calme.",
     "lien": "https://www.idealista.com/immobile/demo006/",
     "date_collecte": datetime.now().isoformat(), "localisation": "El Cabanyal, Valencia", "source": "demo"},
    {"id": "demo_007", "titre": "Ático dúplex con terraza - Último piso",
     "prix": 450000, "surface": 120, "nb_pieces": 5, "nb_chambres": 3,
     "etage": "dernier", "etage_raw": "Ático dúplex",
     "description": "Espectacular ático dúplex orientado sur-oeste con terraza de 40m². Completamente reformado con materiales de alta calidad. Dos baños completos. Garaje doble. Vistas al jardín.",
     "lien": "https://www.idealista.com/immobile/demo007/",
     "date_collecte": datetime.now().isoformat(), "localisation": "El Pla del Real, Valencia", "source": "demo"},
    {"id": "demo_008", "titre": "Appartement 2 pièces à rénover - 1er étage",
     "prix": 115000, "surface": 62, "nb_pieces": 2, "nb_chambres": 1,
     "etage": "1", "etage_raw": "1ª planta",
     "description": "Appartement à rénover entièrement. Premier étage sans vis-à-vis. Potentiel énorme. Prix négociable. Copropriété avec charges faibles.",
     "lien": "https://www.idealista.com/immobile/demo008/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Patraix, Valencia", "source": "demo"},
    {"id": "demo_009", "titre": "Piso exterior luminoso - 6ª planta - Este",
     "prix": 275000, "surface": 88, "nb_pieces": 4, "nb_chambres": 2,
     "etage": "6", "etage_raw": "6ª planta",
     "description": "Piso exterior orientado este en planta alta. Muy luminoso por las mañanas. Terraza de 8m². Ascensor. Parking opcional. Zona tranquila con colegios cerca.",
     "lien": "https://www.idealista.com/immobile/demo009/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Campanar, Valencia", "source": "demo"},
    {"id": "demo_010", "titre": "Studio sous-sol - sótano - très bas",
     "prix": 65000, "surface": 28, "nb_pieces": 1, "nb_chambres": 0,
     "etage": "sous-sol", "etage_raw": "Sótano",
     "description": "Studio en sótano. Sin luz natural. A reformar completamente. Precio muy reducido.",
     "lien": "https://www.idealista.com/immobile/demo010/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Valencia Centro", "source": "demo"},
    {"id": "demo_011", "titre": "Appartement 3 pièces - 8ème étage - Sud-Est",
     "prix": 310000, "surface": 90, "nb_pieces": 3, "nb_chambres": 2,
     "etage": "8", "etage_raw": "8ª planta",
     "description": "Superbe appartement orienté sud-est au 8ème étage. Vue dégagée sur la ville. Double exposition. Cuisine équipée récente. Parquet massif. Ascenseur. Gardiennage 24h.",
     "lien": "https://www.idealista.com/immobile/demo011/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Gran Vía, Valencia", "source": "demo"},
    {"id": "demo_012", "titre": "Ático orientado sur con parking - Último",
     "prix": 385000, "surface": 105, "nb_pieces": 4, "nb_chambres": 3,
     "etage": "dernier", "etage_raw": "Último piso",
     "description": "Precioso ático orientado sur con terraza de 30m² y parking incluido. Totalmente renovado. Tres habitaciones amplias. Baño en suite. Muy cerca del metro. Vista a la ciudad.",
     "lien": "https://www.idealista.com/immobile/demo012/",
     "date_collecte": datetime.now().isoformat(), "localisation": "Extramurs, Valencia", "source": "demo"},
]


class IdealistaCollector:
    """Collecteur d'annonces depuis idealista.com avec fallback démo."""

    def __init__(self, config: dict):
        self.config = config
        self.session = requests.Session()
        self.user_agents = config.get("scraping", {}).get("user_agents", [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        ])
        self.delay = config.get("scraping", {}).get("delay_between_requests", 2.5)

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,es;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }

    def scrape_page(self, url: str) -> List[Dict]:
        """Scrape une page de résultats."""
        try:
            time.sleep(self.delay + random.uniform(0, 1))
            response = self.session.get(url, headers=self._get_headers(), timeout=15)
            if response.status_code in (403, 429):
                logger.warning(f"Accès refusé ({response.status_code}) — fallback démo")
                return []
            if "captcha" in response.text.lower():
                logger.warning("CAPTCHA détecté — fallback démo")
                return []
            soup = BeautifulSoup(response.content, "lxml")
            articles = soup.select("article.item") or soup.select(".item-info-container")
            annonces = []
            for article in articles:
                a = self._parse_annonce(article)
                if a:
                    annonces.append(a)
            logger.info(f"✓ {len(annonces)} annonces collectées depuis {url}")
            return annonces
        except Exception as e:
            logger.error(f"Erreur scraping {url}: {e}")
            return []

    def _parse_annonce(self, article) -> Optional[Dict]:
        try:
            a = {}
            a["id"] = article.get("data-adid", "unknown")
            titre_el = article.select_one("a.item-link")
            a["titre"] = titre_el.get_text(strip=True) if titre_el else "N/A"
            a["lien"] = "https://www.idealista.com" + titre_el["href"] if titre_el and titre_el.get("href") else "N/A"
            prix_el = article.select_one(".item-price")
            if prix_el:
                m = re.search(r"[\d]+", prix_el.get_text().replace(".", ""))
                a["prix"] = int(m.group()) if m else None
            else:
                a["prix"] = None
            a["surface"] = None
            a["etage_raw"] = None
            a["nb_pieces"] = None
            for detail in article.select(".item-detail"):
                text = detail.get_text(strip=True).lower()
                if "m²" in text:
                    m = re.search(r"(\d+)", text)
                    if m: a["surface"] = int(m.group(1))
                elif "planta" in text or "étage" in text:
                    a["etage_raw"] = detail.get_text(strip=True)
            desc_el = article.select_one(".item-description")
            a["description"] = desc_el.get_text(strip=True) if desc_el else ""
            a["date_collecte"] = datetime.now().isoformat()
            a["source"] = "idealista"
            return a
        except Exception:
            return None

    def collect(self, base_url: str, max_pages: int = 3) -> List[Dict]:
        """Collecte les annonces sur plusieurs pages."""
        all_annonces = []
        for page_num in range(1, max_pages + 1):
            url = base_url if page_num == 1 else base_url.rstrip("/") + f"/pagina-{page_num}.htm"
            logger.info(f"Scraping page {page_num}/{max_pages}: {url}")
            annonces = self.scrape_page(url)
            all_annonces.extend(annonces)
            if not annonces:
                break
        if not all_annonces:
            logger.info("Aucune donnée réelle → données de démonstration")
            return DEMO_ANNONCES.copy()
        self._save_raw(all_annonces)
        return all_annonces

    def _save_raw(self, annonces: List[Dict]):
        import os
        os.makedirs("data/raw", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f"data/raw/annonces_raw_{ts}.json", "w", encoding="utf-8") as f:
            json.dump(annonces, f, ensure_ascii=False, indent=2)

    def load_demo_data(self) -> List[Dict]:
        logger.info(f"Chargement de {len(DEMO_ANNONCES)} annonces de démonstration")
        return DEMO_ANNONCES.copy()