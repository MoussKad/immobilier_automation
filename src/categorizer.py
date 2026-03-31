"""
Module de catégorisation et filtrage des annonces selon les critères utilisateur.
Génère un score global (0-100) et assigne une catégorie à chaque annonce.
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

ETAGES_HAUTS = {"dernier", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}
ETAGES_DEFAVORABLES = {"sous-sol", "0"}


class AnnonceCategorizer:
    def __init__(self, config: dict):
        self.config = config
        self.filters = config.get("filters", {})

    def categorize(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Catégorisation de {len(df)} annonces...")
        df = df.copy()

        prix_max = self.filters.get("prix_max", float("inf"))
        prix_min = self.filters.get("prix_min", 0)
        surface_min = self.filters.get("surface_min", 0)
        etages_acceptes = [str(e) for e in self.filters.get("etages_acceptes", [])]
        orientations_souhaitees = self.filters.get("orientations_souhaitees", [])
        mots_positifs = [m.lower() for m in self.filters.get("mots_cles_positifs", [])]
        mots_negatifs = [m.lower() for m in self.filters.get("mots_cles_negatifs", [])]

        df["filtre_prix"] = df.get("prix", pd.Series([None]*len(df))).apply(
            lambda p: pd.isna(p) or (prix_min <= p <= prix_max))
        df["filtre_surface"] = df.get("surface", pd.Series([None]*len(df))).apply(
            lambda s: pd.isna(s) or s >= surface_min)
        df["filtre_etage"] = df.get("etage", pd.Series(["inconnu"]*len(df))).apply(
            lambda e: str(e) in etages_acceptes or str(e) == "dernier") if etages_acceptes else True
        df["filtre_orientation"] = df.get("orientation", pd.Series(["non_précisé"]*len(df))).apply(
            lambda o: o in orientations_souhaitees or o == "non_précisé") if orientations_souhaitees else True

        if mots_negatifs and "description" in df.columns:
            def check_neg(row):
                text = (str(row.get("titre","")) + " " + str(row.get("description",""))).lower()
                return not any(m in text for m in mots_negatifs)
            df["filtre_mots_negatifs"] = df.apply(check_neg, axis=1)
        else:
            df["filtre_mots_negatifs"] = True

        df["score_matching"] = self._compute_matching_score(
            df, prix_min, prix_max, surface_min, etages_acceptes, orientations_souhaitees, mots_positifs)

        if "score_attractivite" in df.columns:
            df["score_global"] = (df["score_attractivite"] * 0.4 + df["score_matching"] * 0.6).round(0).astype(int)
        else:
            df["score_global"] = df["score_matching"]

        df["categorie"] = df.apply(self._assign_category, axis=1)
        df["recommande"] = (df["filtre_prix"] & df["filtre_surface"] &
                            df["filtre_etage"] & df["filtre_mots_negatifs"] & (df["score_global"] >= 55))

        df = df.sort_values("score_global", ascending=False).reset_index(drop=True)
        logger.info(f"✓ Catégorisation terminée : {df['recommande'].sum()} recommandées")
        return df

    def _compute_matching_score(self, df, prix_min, prix_max, surface_min,
                                 etages_acceptes, orientations_souhaitees, mots_positifs) -> pd.Series:
        scores = pd.Series(50, index=df.index)
        if "prix" in df.columns and prix_max < float("inf"):
            prix_center = (prix_min + prix_max) / 2
            prix_range = max((prix_max - prix_min) / 2, 1)
            scores += df["prix"].apply(lambda p: max(0, 100 - int(abs((p or prix_center) - prix_center) / prix_range * 50)) * 0.3 - 15)
        if "surface" in df.columns:
            scores += df["surface"].apply(lambda s: 15 if s and s >= surface_min*1.3 else 8 if s and s >= surface_min else -10)
        if "etage" in df.columns and etages_acceptes:
            scores += df["etage"].apply(lambda e: 20 if str(e) in etages_acceptes or str(e)=="dernier" else -20 if str(e) in ETAGES_DEFAVORABLES else -5)
        if "orientation" in df.columns and orientations_souhaitees:
            scores += df["orientation"].apply(lambda o: 20 if o in orientations_souhaitees else 0 if o=="non_précisé" else -10)
        if mots_positifs and "description" in df.columns:
            def count_pos(row):
                text = (str(row.get("titre","")) + " " + str(row.get("description",""))).lower()
                return min(sum(1 for m in mots_positifs if m in text) * 3, 20)
            scores += df.apply(count_pos, axis=1)
        return scores.clip(0, 100).round(0).astype(int)

    def _assign_category(self, row) -> str:
        if not row.get("filtre_prix", True): return "hors_budget"
        if not row.get("filtre_surface", True): return "trop_petit"
        if not row.get("filtre_mots_negatifs", True): return "critères_rédhibitoires"
        if not row.get("filtre_etage", True): return "étage_inadapté"
        score = row.get("score_global", 50)
        if score >= 80: return "coup_de_coeur"
        if score >= 65: return "très_intéressant"
        if score >= 50: return "intéressant"
        if score >= 35: return "à_surveiller"
        return "non_prioritaire"

    def get_filtered(self, df: pd.DataFrame, only_recommended: bool = True) -> pd.DataFrame:
        return df[df["recommande"] == True].copy() if only_recommended else df.copy()

    def get_summary(self, df: pd.DataFrame) -> Dict:
        return {
            "par_categorie": df["categorie"].value_counts().to_dict() if "categorie" in df.columns else {},
            "total_recommandees": int(df["recommande"].sum()) if "recommande" in df.columns else 0,
            "score_moyen": round(df["score_global"].mean(), 1) if "score_global" in df.columns else 0,
        }