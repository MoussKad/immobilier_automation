"""
Module de collecte (scraping) des annonces immobilières.
Cible principale : idealista.com
Fallback : génération de données de démonstration réalistes.
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
    {
        "id": "demo_001",
        "titre": "Appartement 3 pièces - Vue mer - Dernier étage",
        "prix": 285000,
        "surface": 78,
        "nb_pieces": 3,
        "nb_chambres": 2,
        "etage": "dernier",
        "etage_raw": "Último piso",
        "description": "Magnifique appartement au dernier étage orienté sud avec terrasse de 15m². Entièrement rénové en 2022. Vue panoramique. Ascenseur, parking inclus. Proche métro ligne 3.",
        "lien": "https://www.idealista.com/immobile/demo001/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Valencia Centro",
        "source": "demo"
    },
    {
        "id": "demo_002",
        "titre": "Piso luminoso 2 habitaciones - Ático",
        "prix": 195000,
        "surface": 65,
        "nb_pieces": 2,
        "nb_chambres": 1,
        "etage": "dernier",
        "etage_raw": "Ático",
        "description": "Ático luminoso orientado sur-este. Terraza privada de 20m². Cocina renovada. Ascensor. Sin garaje. Muy tranquilo.",
        "lien": "https://www.idealista.com/immobile/demo002/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Ruzafa, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_003",
        "titre": "Studio rez-de-chaussée - intérieur",
        "prix": 89000,
        "surface": 35,
        "nb_pieces": 1,
        "nb_chambres": 0,
        "etage": "0",
        "etage_raw": "Bajo",
        "description": "Studio en bajo interior. A reformar. Sin ascensor. Muy céntrico. Ideal inversión.",
        "lien": "https://www.idealista.com/immobile/demo003/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Valencia Centro",
        "source": "demo"
    },
    {
        "id": "demo_004",
        "titre": "Appartement 4 pièces - 5ème étage - Orienté Est",
        "prix": 320000,
        "surface": 95,
        "nb_pieces": 4,
        "nb_chambres": 3,
        "etage": "5",
        "etage_raw": "5ª planta",
        "description": "Grand appartement lumineux orienté est avec balcon. Parquet, double vitrage. Rénové cuisine et salles de bain. Immeuble avec gardien. 2 parkings en sous-sol.",
        "lien": "https://www.idealista.com/immobile/demo004/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Mestalla, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_005",
        "titre": "Piso 3 habitaciones - 7ª planta - Sur",
        "prix": 245000,
        "surface": 82,
        "nb_pieces": 3,
        "nb_chambres": 2,
        "etage": "7",
        "etage_raw": "7ª planta",
        "description": "Piso en planta alta orientado al sur. Mucha luz natural. Balcón con vistas. Cocina americana. Ascensor. Comunidad tranquila. Cerca del centro comercial.",
        "lien": "https://www.idealista.com/immobile/demo005/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Benimaclet, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_006",
        "titre": "Appartement 2 pièces - 3ème étage - Nord",
        "prix": 160000,
        "surface": 58,
        "nb_pieces": 2,
        "nb_chambres": 1,
        "etage": "3",
        "etage_raw": "3ª planta",
        "description": "Appartement intérieur au 3ème étage orienté nord. Cuisine séparée. Sans ascenseur. Copropriété calme.",
        "lien": "https://www.idealista.com/immobile/demo006/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "El Cabanyal, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_007",
        "titre": "Ático dúplex con terraza - Último piso",
        "prix": 450000,
        "surface": 120,
        "nb_pieces": 5,
        "nb_chambres": 3,
        "etage": "dernier",
        "etage_raw": "Ático dúplex",
        "description": "Espectacular ático dúplex orientado sur-oeste con terraza de 40m². Completamente reformado con materiales de alta calidad. Dos baños completos. Garaje doble. Vistas al jardín.",
        "lien": "https://www.idealista.com/immobile/demo007/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "El Pla del Real, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_008",
        "titre": "Appartement 2 pièces à rénover - 1er étage",
        "prix": 115000,
        "surface": 62,
        "nb_pieces": 2,
        "nb_chambres": 1,
        "etage": "1",
        "etage_raw": "1ª planta",
        "description": "Appartement à rénover entièrement. Premier étage sans vis-à-vis. Potentiel énorme. Prix négociable. Copropriété avec charges faibles.",
        "lien": "https://www.idealista.com/immobile/demo008/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Patraix, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_009",
        "titre": "Piso exterior luminoso - 6ª planta - Este",
        "prix": 275000,
        "surface": 88,
        "nb_pieces": 4,
        "nb_chambres": 2,
        "etage": "6",
        "etage_raw": "6ª planta",
        "description": "Piso exterior orientado este en planta alta. Muy luminoso por las mañanas. Terraza de 8m². Ascensor. Parking opcional. Zona tranquila con colegios cerca.",
        "lien": "https://www.idealista.com/immobile/demo009/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Campanar, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_010",
        "titre": "Studio sous-sol - sótano - très bas",
        "prix": 65000,
        "surface": 28,
        "nb_pieces": 1,
        "nb_chambres": 0,
        "etage": "sous-sol",
        "etage_raw": "Sótano",
        "description": "Studio en sótano. Ideal para almacén o estudio. Sin luz natural. A reformar completamente. Precio muy reducido.",
        "lien": "https://www.idealista.com/immobile/demo010/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Valencia Centro",
        "source": "demo"
    },
    {
        "id": "demo_011",
        "titre": "Appartement 3 pièces - 8ème étage - Sud-Est",
        "prix": 310000,
        "surface": 90,
        "nb_pieces": 3,
        "nb_chambres": 2,
        "etage": "8",
        "etage_raw": "8ª planta",
        "description": "Superbe appartement orienté sud-est au 8ème étage. Vue dégagée sur la ville. Double exposition. Cuisine équipée récente. Parquet massif. Ascenseur. Gardiennage 24h.",
        "lien": "https://www.idealista.com/immobile/demo011/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Gran Vía, Valencia",
        "source": "demo"
    },
    {
        "id": "demo_012",
        "titre": "Ático orientado sur con parking - Último",
        "prix": 385000,
        "surface": 105,
        "nb_pieces": 4,
        "nb_chambres": 3,
        "etage": "dernier",
        "etage_raw": "Último piso",
        "description": "Precioso ático orientado sur con terraza de 30m² y parking incluido. Totalmente renovado. Tres habitaciones amplias. Baño en suite. Muy cerca del metro. Vista a la ciudad.",
        "lien": "https://www.idealista.com/immobile/demo012/",
        "date_collecte": datetime.now().isoformat(),
        "localisation": "Extramurs, Valencia",
        "source": "demo"
    },
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
        # Nombre d'annonces démo à générer (depuis config, défaut 12)
        self.nb_demo = int(config.get("demo", {}).get("nb_annonces", 12))

    def _get_headers(self) -> dict:
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,es;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
        }

    def _parse_annonce(self, article) -> Optional[Dict]:
        """Parse une annonce HTML depuis idealista."""
        try:
            annonce = {}

            # ID
            annonce["id"] = article.get("data-adid") or article.get("data-element-id", "unknown")

            # Titre
            titre_el = article.select_one("a.item-link")
            annonce["titre"] = titre_el.get_text(strip=True) if titre_el else "N/A"
            annonce["lien"] = "https://www.idealista.com" + titre_el["href"] if titre_el and titre_el.get("href") else "N/A"

            # Prix
            prix_el = article.select_one(".item-price")
            if prix_el:
                prix_text = prix_el.get_text(strip=True)
                prix_match = re.search(r"[\d\.]+", prix_text.replace(".", "").replace(",", ""))
                annonce["prix"] = int(prix_match.group()) if prix_match else None
            else:
                annonce["prix"] = None

            # Détails (surface, pièces, étage)
            details = article.select(".item-detail")
            annonce["surface"] = None
            annonce["nb_pieces"] = None
            annonce["etage_raw"] = None

            for detail in details:
                text = detail.get_text(strip=True).lower()
                if "m²" in text or "m2" in text:
                    m = re.search(r"(\d+)", text)
                    if m:
                        annonce["surface"] = int(m.group(1))
                elif "hab" in text or "pièce" in text or "rooms" in text:
                    m = re.search(r"(\d+)", text)
                    if m:
                        annonce["nb_pieces"] = int(m.group(1))
                elif "planta" in text or "piso" in text or "étage" in text or "floor" in text:
                    annonce["etage_raw"] = detail.get_text(strip=True)

            # Description
            desc_el = article.select_one(".item-description")
            annonce["description"] = desc_el.get_text(strip=True) if desc_el else ""

            # Localisation
            loc_el = article.select_one(".item-detail-char .item-detail")
            annonce["localisation"] = loc_el.get_text(strip=True) if loc_el else "N/A"

            annonce["date_collecte"] = datetime.now().isoformat()
            annonce["source"] = "idealista"

            return annonce

        except Exception as e:
            logger.warning(f"Erreur parsing annonce: {e}")
            return None

    def scrape_page(self, url: str) -> List[Dict]:
        """Scrape une page de résultats."""
        try:
            time.sleep(self.delay + random.uniform(0, 1))
            response = self.session.get(url, headers=self._get_headers(), timeout=15)

            if response.status_code == 403:
                logger.warning(f"Accès refusé (403) pour {url} - utilisation des données démo")
                return []
            elif response.status_code != 200:
                logger.warning(f"Status {response.status_code} pour {url}")
                return []

            soup = BeautifulSoup(response.content, "lxml")

            # Vérification CAPTCHA
            if "captcha" in response.text.lower() or "robot" in response.text.lower():
                logger.warning("CAPTCHA détecté - passage en mode démo")
                return []

            articles = soup.select("article.item") or soup.select(".item-info-container")

            if not articles:
                logger.warning("Aucune annonce trouvée sur la page - vérifier le sélecteur CSS")
                return []

            annonces = []
            for article in articles:
                annonce = self._parse_annonce(article)
                if annonce:
                    annonces.append(annonce)

            logger.info(f"✓ {len(annonces)} annonces collectées depuis {url}")
            return annonces

        except requests.exceptions.ConnectionError:
            logger.warning(f"Impossible de se connecter à {url}")
            return []
        except Exception as e:
            logger.error(f"Erreur scraping {url}: {e}")
            return []

    def collect(self, base_url: str, max_pages: int = 3) -> List[Dict]:
        """Collecte les annonces sur plusieurs pages."""
        all_annonces = []

        logger.info(f"Démarrage de la collecte sur {base_url} ({max_pages} pages max)")

        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = base_url
            else:
                # idealista pagine avec /pagina-N/
                url = base_url.rstrip("/") + f"/pagina-{page_num}.htm"

            logger.info(f"Scraping page {page_num}/{max_pages}: {url}")
            annonces = self.scrape_page(url)
            all_annonces.extend(annonces)

            if not annonces:
                logger.info("Page vide ou erreur - arrêt de la pagination")
                break

        if not all_annonces:
            logger.info("Aucune donnée réelle collectée → utilisation des données de démonstration")
            all_annonces = self._build_demo_annonces(self.nb_demo)
            logger.info(f"✓ {len(all_annonces)} annonces de démonstration chargées "
                        f"(nb_annonces={self.nb_demo})")
        else:
            logger.info(f"✓ Total : {len(all_annonces)} annonces collectées")

        # Sauvegarder les données brutes
        self._save_raw(all_annonces)
        return all_annonces

    def _save_raw(self, annonces: List[Dict]):
        """Sauvegarde les données brutes en JSON."""
        import os
        os.makedirs("data/raw", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/annonces_raw_{timestamp}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(annonces, f, ensure_ascii=False, indent=2)
        logger.info(f"Données brutes sauvegardées → {filepath}")

    def load_demo_data(self) -> List[Dict]:
        """
        Retourne les données de démonstration.
        La quantité est contrôlée par demo.nb_annonces dans config.yaml.
        Si nb_annonces > len(DEMO_ANNONCES), génère des annonces supplémentaires.
        """
        annonces = self._build_demo_annonces(self.nb_demo)
        logger.info(f"Chargement de {len(annonces)} annonces de démonstration "
                    f"(configuré: {self.nb_demo})")
        return annonces

    def _build_demo_annonces(self, n: int) -> List[Dict]:
        """
        Retourne n annonces de démonstration.
        Utilise les 12 annonces de base et en génère de nouvelles si n > 12.
        """
        base = DEMO_ANNONCES.copy()
        if n <= len(base):
            return base[:n]

        # Générer des annonces synthétiques supplémentaires
        generated = base.copy()
        quartiers = [
            "Ruzafa", "El Carmen", "Benimaclet", "Campanar", "Mestalla",
            "Patraix", "Extramurs", "Gran Vía", "Cabanyal", "Nou Moles",
            "Jesús", "Malvarrosa", "Exposición", "Aiora", "Torrefiel",
        ]
        orientations_pool = ["sud", "est", "sud-est", "nord", "ouest", "non_précisé"]
        orient_weights  = [0.35, 0.20, 0.15, 0.10, 0.05, 0.15]
        etats_pool = ["rénové", "non_précisé", "à_rénover", "bon_état"]
        etat_weights = [0.30, 0.45, 0.15, 0.10]
        etages_pool = ["dernier", "dernier", "7", "8", "6", "5", "4", "3", "2", "1", "0"]

        idx = len(base) + 1
        while len(generated) < n:
            prix_base = random.randint(90, 480) * 1000
            surface = random.randint(40, 130)
            etage = random.choice(etages_pool)
            orientation = random.choices(orientations_pool, weights=orient_weights)[0]
            etat = random.choices(etats_pool, weights=etat_weights)[0]
            quartier = random.choice(quartiers)
            nb_pieces = max(1, surface // 30)

            equip_parts = []
            if random.random() > 0.45: equip_parts.append("parking inclus")
            if random.random() > 0.30: equip_parts.append("ascenseur")
            if random.random() > 0.55: equip_parts.append("terrasse")
            if random.random() > 0.40: equip_parts.append("balcon")
            if random.random() > 0.70: equip_parts.append("proche métro")

            orient_txt = {
                "sud": "orientado al sur", "est": "orientado este",
                "sud-est": "orientado sur-este", "nord": "orientado norte",
                "ouest": "orientado oeste", "non_précisé": "",
            }[orientation]

            etat_txt = {
                "rénové": "completamente reformado",
                "non_précisé": "",
                "à_rénover": "necesita reforma",
                "bon_état": "en buen estado",
            }[etat]

            desc_parts = [f"Piso de {surface}m²"]
            if orient_txt: desc_parts.append(orient_txt)
            if etat_txt: desc_parts.append(etat_txt)
            desc_parts += equip_parts
            description = ". ".join(desc_parts) + "."

            annonce = {
                "id": f"demo_{idx:03d}",
                "titre": f"Appartement {nb_pieces}P — {etage.capitalize()} étage — {quartier}",
                "prix": prix_base,
                "surface": surface,
                "nb_pieces": nb_pieces,
                "nb_chambres": max(0, nb_pieces - 1),
                "etage": etage,
                "etage_raw": etage,
                "description": description,
                "lien": f"https://www.idealista.com/immobile/demo{idx:03d}/",
                "date_collecte": datetime.now().isoformat(),
                "localisation": f"{quartier}, Valencia",
                "source": "demo_generated",
            }
            generated.append(annonce)
            idx += 1

        return generated
