"""
Module de nettoyage et normalisation des données immobilières.
Normalise : prix, surface, étage (FR + ES), supprime les doublons.
"""

import pandas as pd
import re
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

ETAGE_MAPPING = {
    "bajo": "0", "sótano": "sous-sol", "sotano": "sous-sol",
    "semisótano": "sous-sol", "semisotano": "sous-sol",
    "entresuelo": "entresol", "entreplanta": "entresol",
    "principal": "0", "primero": "1", "segundo": "2",
    "tercero": "3", "cuarto": "4", "quinto": "5",
    "sexto": "6", "séptimo": "7", "octavo": "8",
    "noveno": "9", "décimo": "10",
    "ático": "dernier", "atico": "dernier",
    "último": "dernier", "ultimo": "dernier",
    "rez-de-chaussée": "0", "rdc": "0",
    "premier": "1", "deuxième": "2", "troisième": "3",
    "quatrième": "4", "cinquième": "5",
    "dernier étage": "dernier", "dernier": "dernier",
}


def normalize_etage(etage_raw: Optional[str]) -> str:
    if not etage_raw or pd.isna(etage_raw):
        return "inconnu"
    text = str(etage_raw).lower().strip()
    for pattern, normalized in ETAGE_MAPPING.items():
        if pattern in text:
            return normalized
    m = re.search(r"(\d+)[ªº°]", text)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d+)\b", text)
    if m:
        num = int(m.group(1))
        if num <= 15:
            return str(num)
    return text[:20]


def normalize_prix(prix_raw) -> Optional[float]:
    if prix_raw is None or (isinstance(prix_raw, float) and pd.isna(prix_raw)):
        return None
    try:
        if isinstance(prix_raw, (int, float)):
            return float(prix_raw)
        text = str(prix_raw).replace(".", "").replace(",", "").replace("€", "").replace(" ", "")
        m = re.search(r"(\d+)", text)
        return float(m.group(1)) if m else None
    except Exception:
        return None


def normalize_surface(surface_raw) -> Optional[float]:
    if surface_raw is None or (isinstance(surface_raw, float) and pd.isna(surface_raw)):
        return None
    try:
        if isinstance(surface_raw, (int, float)):
            return float(surface_raw)
        text = str(surface_raw).replace(",", ".").lower()
        text = text.replace("m²", "").replace("m2", "").replace("metros", "").strip()
        m = re.search(r"(\d+(?:\.\d+)?)", text)
        return float(m.group(1)) if m else None
    except Exception:
        return None


class DataCleaner:
    def __init__(self, config: dict = None):
        self.config = config or {}

    def clean(self, annonces: List[Dict]) -> pd.DataFrame:
        logger.info(f"Nettoyage de {len(annonces)} annonces...")
        df = pd.DataFrame(annonces)
        df = self._normalize_columns(df)
        nb_avant = len(df)
        df = self._remove_duplicates(df)
        logger.info(f"Doublons supprimés : {nb_avant - len(df)}")
        df = self._handle_missing(df)
        df = self._compute_price_per_sqm(df)
        df = self._validate(df)
        logger.info(f"✓ Nettoyage terminé : {len(df)} annonces valides")
        return df

    def _normalize_columns(self, df):
        if "prix" in df.columns:
            df["prix"] = df["prix"].apply(normalize_prix)
        if "surface" in df.columns:
            df["surface"] = df["surface"].apply(normalize_surface)
        if "etage" in df.columns:
            df["etage"] = df["etage"].astype(str).str.strip()
        elif "etage_raw" in df.columns:
            df["etage"] = df["etage_raw"].apply(normalize_etage)
        else:
            df["etage"] = "inconnu"
        if "etage_raw" not in df.columns:
            df["etage_raw"] = df.get("etage", "inconnu")
        for col in ["nb_pieces", "nb_chambres"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "description" not in df.columns:
            df["description"] = ""
        df["description"] = df["description"].fillna("").astype(str)
        if "titre" not in df.columns:
            df["titre"] = "Annonce sans titre"
        df["titre"] = df["titre"].fillna("Annonce sans titre").astype(str)
        return df

    def _remove_duplicates(self, df):
        if "id" in df.columns:
            df = df.drop_duplicates(subset=["id"], keep="first")
        subset = [c for c in ["lien", "prix", "surface"] if c in df.columns]
        if subset:
            df = df.drop_duplicates(subset=subset, keep="first")
        return df.reset_index(drop=True)

    def _handle_missing(self, df):
        for col in ["prix", "surface", "nb_pieces", "nb_chambres"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["localisation", "lien"]:
            if col in df.columns:
                df[col] = df[col].fillna("N/A")
        return df

    def _compute_price_per_sqm(self, df):
        if "prix" in df.columns and "surface" in df.columns:
            mask = (df["prix"].notna()) & (df["surface"].notna()) & (df["surface"] > 0)
            df["prix_m2"] = None
            df.loc[mask, "prix_m2"] = (df.loc[mask, "prix"] / df.loc[mask, "surface"]).round(0)
        return df

    def _validate(self, df):
        if "prix" in df.columns:
            df = df[df["prix"].isna() | df["prix"].between(10000, 10000000)]
        if "surface" in df.columns:
            df = df[df["surface"].isna() | df["surface"].between(5, 2000)]
        return df.reset_index(drop=True)

    def get_stats(self, df: pd.DataFrame) -> Dict:
        stats = {
            "total_annonces": len(df),
            "prix_moyen": round(df["prix"].mean(), 0) if "prix" in df.columns else None,
            "prix_median": round(df["prix"].median(), 0) if "prix" in df.columns else None,
            "surface_moyenne": round(df["surface"].mean(), 0) if "surface" in df.columns else None,
            "prix_m2_moyen": round(df["prix_m2"].mean(), 0) if "prix_m2" in df.columns else None,
        }
        return stats