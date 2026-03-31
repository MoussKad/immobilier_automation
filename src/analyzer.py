"""
Module d'analyse NLP des descriptions d'annonces immobilières.
Extraction : orientation, état, équipements, transport, vue, luminosité.
Approche par règles (regex + mots-clés) — pas de dépendances ML lourdes.
Bilingue : français + espagnol.
"""

import re
import logging
import pandas as pd
from typing import Dict, List

logger = logging.getLogger(__name__)

ORIENTATIONS = {
    "sud": ["orienté sud", "orientado sur", "sur", "plein sud", "fachada sur", "cara sur", "exposición sur"],
    "nord": ["orienté nord", "orientado norte", "norte", "fachada norte"],
    "est": ["orienté est", "orientado este", "este", "levante", "lever du soleil"],
    "ouest": ["orienté ouest", "orientado oeste", "oeste", "poniente"],
    "sud-est": ["sud-est", "sur-este", "sureste"],
    "sud-ouest": ["sud-ouest", "sur-oeste", "suroeste"],
    "nord-est": ["nord-est", "nor-este", "noreste"],
    "nord-ouest": ["nord-ouest", "nor-oeste", "noroeste"],
}

ETATS = {
    "rénové": ["rénové", "renovado", "reformado", "réformé", "remis à neuf",
               "totalmente rénové", "completamente reformado", "como nuevo", "a estrenar"],
    "bon_état": ["bon état", "buen estado", "bien conservado", "entretenu"],
    "à_rénover": ["à rénover", "a reformar", "a rehabilitar", "travaux à prévoir",
                  "necesita reforma", "para reformar"],
    "neuf": ["neuf", "nuevo", "nouvelle construction", "obra nueva", "sin estrenar"],
}

EQUIPEMENTS = {
    "parking": ["parking", "garaje", "garage", "place de parking", "box"],
    "ascenseur": ["ascenseur", "ascensor", "lift", "elevator"],
    "terrasse": ["terrasse", "terraza", "terraza privada"],
    "balcon": ["balcon", "balcón"],
    "piscine": ["piscine", "piscina", "pool"],
    "jardin": ["jardin", "jardín", "garden"],
    "cave": ["cave", "bodega", "trastero"],
    "gardien": ["gardien", "conserje", "portero", "vigilance"],
    "climatisation": ["climatisation", "aire acondicionado", "clim", "climatisé"],
    "digicode": ["digicode", "portero automático", "videoportero", "interphone"],
}

TRANSPORTS = {
    "métro": ["métro", "metro", "subway"],
    "bus": ["bus", "autobus", "autobús"],
    "tram": ["tram", "tramway", "tranvía"],
    "train": ["train", "gare", "estación", "cercanías"],
}

VUES = {
    "mer": ["vue mer", "vistas al mar", "playa", "plage", "frente al mar"],
    "montagne": ["vue montagne", "vistas a la montaña", "sierra"],
    "ville": ["vue ville", "vistas a la ciudad", "panoramique", "panorámico"],
    "jardin": ["vue jardin", "vistas al jardín"],
    "dégagée": ["vue dégagée", "vistas despejadas", "vista libre", "sin obstáculos"],
}

LUMINOSITE = {
    "très_lumineux": ["très lumineux", "muy luminoso", "mucha luz", "muy soleado"],
    "lumineux": ["lumineux", "luminoso", "luz natural", "soleado", "ensoleillé"],
    "sombre": ["sombre", "oscuro", "sin luz", "intérieur", "interior", "poco luminoso"],
}

PROXIMITES = {
    "centre": ["centre-ville", "centro", "muy céntrico", "très central", "cœur de ville"],
    "commerces": ["commerces", "comercios", "supermercado", "tout à pied"],
    "écoles": ["écoles", "colegio", "universidad", "lycée", "escuela"],
    "parcs": ["parc", "parque", "zona verde"],
    "plage": ["plage", "playa"],
}


class TextAnalyzer:
    def __init__(self):
        self._compile_patterns()

    def _compile_patterns(self):
        self._patterns = {}
        for category, mapping in {
            "orientation": ORIENTATIONS, "etat": ETATS, "equipements": EQUIPEMENTS,
            "transport": TRANSPORTS, "vue": VUES, "luminosite": LUMINOSITE, "proximite": PROXIMITES
        }.items():
            self._patterns[category] = {
                label: re.compile("|".join(re.escape(k.lower()) for k in kws), re.IGNORECASE)
                for label, kws in mapping.items()
            }

    def _detect(self, text: str, category: str) -> List[str]:
        return [label for label, pat in self._patterns[category].items() if pat.search(text.lower())]

    def analyze_text(self, text: str) -> Dict:
        if not text or pd.isna(text):
            return self._empty()
        text = str(text)
        r = {}

        orientations = self._detect(text, "orientation")
        r["orientation"] = orientations[0] if orientations else "non_précisé"
        r["orientations_all"] = ", ".join(orientations)

        etats = self._detect(text, "etat")
        r["etat"] = etats[0] if etats else "non_précisé"

        equip = self._detect(text, "equipements")
        r["a_parking"] = "parking" in equip
        r["a_ascenseur"] = "ascenseur" in equip
        r["a_terrasse"] = "terrasse" in equip
        r["a_balcon"] = "balcon" in equip or "terrasse" in equip
        r["a_piscine"] = "piscine" in equip
        r["a_jardin"] = "jardin" in equip
        r["equipements"] = ", ".join(equip)

        transports = self._detect(text, "transport")
        r["proche_metro"] = "métro" in transports
        r["proche_transport"] = bool(transports)
        r["transports"] = ", ".join(transports)

        vues = self._detect(text, "vue")
        r["vue"] = vues[0] if vues else "non_précisé"
        r["vues_all"] = ", ".join(vues)

        lumins = self._detect(text, "luminosite")
        r["luminosite"] = "très_lumineux" if "très_lumineux" in lumins else "lumineux" if "lumineux" in lumins else "sombre" if "sombre" in lumins else "non_précisé"

        prox = self._detect(text, "proximite")
        r["proximites"] = ", ".join(prox)
        r["est_central"] = "centre" in prox

        r["score_attractivite"] = self._score(r)
        return r

    def _score(self, r: Dict) -> int:
        s = 50
        s += {"sud": 20, "sud-est": 15, "est": 10, "sud-ouest": 10, "ouest": 0,
              "nord-ouest": -5, "nord-est": -5, "nord": -15, "non_précisé": 0}.get(r["orientation"], 0)
        s += {"neuf": 20, "rénové": 15, "bon_état": 5, "non_précisé": 0, "à_rénover": -20}.get(r["etat"], 0)
        s += {"très_lumineux": 10, "lumineux": 5, "sombre": -15, "non_précisé": 0}.get(r["luminosite"], 0)
        for k, v in [("a_parking",8),("a_ascenseur",5),("a_terrasse",10),("a_balcon",5),
                     ("a_piscine",8),("proche_metro",8),("est_central",5)]:
            if r.get(k): s += v
        s += {"mer":15,"montagne":10,"dégagée":8,"ville":5,"jardin":5,"non_précisé":0}.get(r["vue"], 0)
        return max(0, min(100, s))

    def _empty(self) -> Dict:
        return {"orientation":"non_précisé","orientations_all":"","etat":"non_précisé",
                "a_parking":False,"a_ascenseur":False,"a_terrasse":False,"a_balcon":False,
                "a_piscine":False,"a_jardin":False,"equipements":"",
                "proche_metro":False,"proche_transport":False,"transports":"",
                "vue":"non_précisé","vues_all":"","luminosite":"non_précisé",
                "proximites":"","est_central":False,"score_attractivite":50}

    def analyze_dataframe(self, df: pd.DataFrame, text_col: str = "description") -> pd.DataFrame:
        logger.info(f"Analyse NLP de {len(df)} annonces...")
        combined = (df.get("titre", pd.Series([""] * len(df))).fillna("") + " " + df[text_col].fillna(""))
        results = pd.DataFrame(combined.apply(self.analyze_text).tolist())
        for col in results.columns:
            df[col] = results[col].values
        logger.info("✓ Analyse NLP terminée")
        return df