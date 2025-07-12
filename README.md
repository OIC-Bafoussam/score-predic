# Scraper La Liga - MondeFootball.fr

Ce projet contient des scripts Python pour extraire les données de La Liga depuis MondeFootball.fr et les sauvegarder au format CSV avec toutes les informations nécessaires pour l'analyse de matchs.

## 🎯 Objectif

Extraire les données des matchs de La Liga avec les colonnes suivantes :
- `id`, `season`, `matchday`, `date`, `home_team`, `away_team`
- `home_goals`, `away_goals`, `result`
- `home_position`, `away_position` (positions au classement)
- `home_scored_and_conceded_goals`, `away_scored_and_conceded_goals`

## 📋 Prérequis

- Python 3.7+
- Environnement virtuel (recommandé)

## 🚀 Installation

1. **Activer l'environnement virtuel** (déjà créé dans le projet) :
```bash
source venv/bin/activate
```

2. **Vérifier les dépendances** (déjà installées) :
```bash
pip list | grep -E "(requests|beautifulsoup4|pandas)"
```

## 📖 Utilisation

### Script principal : `fixed_scraper.py`

Ce script est **prêt à l'emploi** et extrait automatiquement les données de la **8e journée de La Liga 2024/2025**.

```bash
# Activer l'environnement virtuel
source venv/bin/activate

# Lancer le scraping
python fixed_scraper.py
```

**Sortie attendue :**
- Fichier CSV généré dans `laliga_data/`
- Affichage des résultats dans le terminal
- 10 matchs avec toutes les données du classement

### Script générique : `laliga_scraper_final.py`

Pour scraper d'autres journées (avec support extensible) :

```bash
python laliga_scraper_final.py --url "https://www.mondefootball.fr/calendrier/esp-primera-division-2024-2025-spieltag/8/" --season 2024 --matchday 8 --output "custom_name.csv"
```

**Paramètres :**
- `--url` : URL de la page MondeFootball (obligatoire)
- `--season` : Année de début de saison (défaut: 2024)
- `--matchday` : Numéro de journée (défaut: 8)
- `--output` : Nom du fichier de sortie (optionnel)

## 📊 Format des données

### Structure du CSV généré

| Colonne | Description | Exemple |
|---------|-------------|---------|
| `id` | Identifiant unique du match | 1, 2, 3... |
| `season` | Saison format YYYY/YYYY+1 | 2024/2025 |
| `matchday` | Numéro de journée | 8 |
| `date` | Date du match | 2024-09-27 |
| `home_team` | Équipe domicile | Real Valladolid |
| `away_team` | Équipe extérieur | RCD Mallorca |
| `home_goals` | Buts équipe domicile | 1 |
| `away_goals` | Buts équipe extérieur | 2 |
| `result` | Résultat (1=domicile, X=nul, 2=extérieur) | 2 |
| `home_position` | Position au classement domicile | 18 |
| `away_position` | Position au classement extérieur | 6 |
| `home_scored_and_conceded_goals` | Buts pour:contre domicile | 4:17 |
| `away_scored_and_conceded_goals` | Buts pour:contre extérieur | 8:6 |

### Exemple de sortie

```csv
id,season,matchday,date,home_team,away_team,home_goals,away_goals,result,home_position,away_position,home_scored_and_conceded_goals,away_scored_and_conceded_goals
1,2024/2025,8,2024-09-27,Real Valladolid,RCD Mallorca,1,2,2,18,6,4:17,8:6
2,2024/2025,8,2024-09-28,Getafe CF,CD Alavés,2,0,1,16,11,5:6,11:12
3,2024/2025,8,2024-09-28,Rayo Vallecano,CD Leganés,1,1,X,9,15,9:8,5:9
```

## 📁 Structure du projet

```
score-predic/
├── fixed_scraper.py           # Script principal (8e journée 2024/2025)
├── laliga_scraper_final.py    # Script générique extensible
├── laliga_data/               # Répertoire des fichiers CSV générés
│   └── *.csv                  # Fichiers de données
├── venv/                      # Environnement virtuel
└── README.md                  # Ce fichier
```

## 🎯 Données disponibles

**Actuellement supporté :**
- ✅ Journée 8 de La Liga 2024/2025 (données complètes)
- ✅ Positions au classement
- ✅ Statistiques buts pour/contre
- ✅ Tous les 10 matchs de la journée

**Extension future :**
- Le script générique peut être étendu pour d'autres journées
- Structure prête pour ajouter d'autres saisons

## 💡 Fonctionnalités

✅ **Extraction complète** : Toutes les colonnes demandées
✅ **Gestion des erreurs** : Logging détaillé
✅ **Décodage automatique** : Gestion du contenu gzip
✅ **Normalisation** : Noms d'équipes standardisés
✅ **CSV optimisé** : Format prêt pour l'analyse
✅ **Classement intégré** : Positions et statistiques

## 🔧 Exemple d'utilisation complète

```bash
# 1. Activer l'environnement
source venv/bin/activate

# 2. Scraper la journée 8
python fixed_scraper.py

# 3. Vérifier le fichier généré
ls laliga_data/

# 4. Voir le contenu
head -5 laliga_data/*.csv
```

## 📈 Utilisation des données

Le fichier CSV généré est parfait pour :
- **Analyse statistique** avec pandas
- **Modèles de prédiction** de matchs
- **Bases de données** relationnelles
- **Tableaux de bord** et visualisations

Exemple avec pandas :
```python
import pandas as pd

# Charger les données
df = pd.read_csv('laliga_data/laliga_scraping_*.csv')

# Analyser les résultats
print(df['result'].value_counts())  # Distribution des résultats
print(df.groupby('home_team')['home_goals'].mean())  # Moyenne buts domicile
```

## 🐛 Dépannage

**Problème de module non trouvé :**
```bash
source venv/bin/activate
pip install requests beautifulsoup4 pandas
```

**Problème de permissions :**
```bash
chmod +x fixed_scraper.py
```

**Problème de réseau :**
Le script gère automatiquement les timeouts et retry.

## 📝 Notes techniques

- **Headers HTTP optimisés** pour éviter la détection anti-bot
- **Session persistante** pour de meilleures performances
- **Encoding UTF-8** pour les caractères spéciaux
- **Timestamps** dans les noms de fichiers pour éviter les écrasements 