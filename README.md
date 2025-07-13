# Scraper La Liga - MondeFootball.fr

Ce projet contient des scripts Python pour extraire les données de La Liga depuis MondeFootball.fr et les sauvegarder au format CSV avec toutes les informations nécessaires pour l'analyse de matchs.

## 🎯 Objectif

Extraire les données des matchs de La Liga avec les colonnes suivantes :
- `id`, `season`, `matchday`, `date`, `home_team`, `away_team`
- `home_goals`, `away_goals`, `result`
- `home_position`, `away_position` (positions au classement)
- `home_scored_and_conceded_goals`, `away_scored_and_conceded_goals`

## 📋 Prérequis

- Python 3.7+ installé sur votre système
- Git (pour cloner le projet)
- Connexion Internet

## 🚀 Installation

### 1. Cloner le projet
 
```bash
git clone https://github.com/votre-username/score-predic.git
cd score-predic
```

### 2. Créer et activer l'environnement virtuel

#### Sur Windows :
```cmd
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement virtuel
venv\Scripts\activate

# Ou avec PowerShell
venv\Scripts\Activate.ps1
```

#### Sur macOS/Linux :
```bash
# Créer l'environnement virtuel
python3 -m venv venv

# Activer l'environnement virtuel
source venv/bin/activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Vérifier l'installation

```bash
pip list | grep -E "(requests|beautifulsoup4|pandas)"
```

#### Sur Windows (PowerShell) :
```powershell
pip list | findstr "requests beautifulsoup4 pandas"
```

## 📖 Utilisation

### Script principal : `fixed_scraper.py`

Pour scraper d'autres journées (avec support extensible) :

#### Sur macOS/Linux :
```bash
python laliga_scraper_final.py --auto-continue --season 2024 --delay 2.0
```

#### Sur Windows :
```cmd
python laliga_scraper_final.py --auto-continue --season 2024 --delay 2.0
```

**Paramètres :** 
- `--season` : Année de début de saison (défaut: 2024) 
- `--delay` : temps d'attente après chaque journée

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

### Sur macOS/Linux :
```bash
# 1. Cloner le projet
git clone https://github.com/votre-username/score-predic.git
cd score-predic

# 2. Créer et activer l'environnement virtuel
python3 -m venv venv
source venv/bin/activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Scraper la journée 8
python fixed_scraper.py

# 5. Vérifier le fichier généré
ls laliga_data/

# 6. Voir le contenu
head -5 laliga_data/*.csv
```

### Sur Windows :
```cmd
# 1. Cloner le projet
git clone https://github.com/votre-username/score-predic.git
cd score-predic

# 2. Créer et activer l'environnement virtuel
python -m venv venv
venv\Scripts\activate

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Scraper la journée 8
python fixed_scraper.py

# 5. Vérifier le fichier généré
dir laliga_data\

# 6. Voir le contenu
type laliga_data\*.csv | more
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

### Problèmes courants

**Module non trouvé :**
```bash
# Vérifier que l'environnement virtuel est activé
# Windows : venv\Scripts\activate
# macOS/Linux : source venv/bin/activate

# Réinstaller les dépendances
pip install -r requirements.txt
```

**Problème de permissions (macOS/Linux) :**
```bash
chmod +x fixed_scraper.py
```

**Problème avec Python sur Windows :**
- Assurez-vous que Python est ajouté au PATH
- Utilisez `py` au lieu de `python` si nécessaire

**Problème de réseau :**
Le script gère automatiquement les timeouts et retry.

### Commandes pour désactiver l'environnement virtuel

#### Sur tous les OS :
```bash
deactivate
```

## 🖥️ Compatibilité OS

- ✅ **Windows 10/11** (Command Prompt, PowerShell)
- ✅ **macOS** (Terminal)
- ✅ **Linux** (Ubuntu, Debian, CentOS, etc.)

## 📝 Notes techniques

- **Headers HTTP optimisés** pour éviter la détection anti-bot
- **Session persistante** pour de meilleures performances
- **Encoding UTF-8** pour les caractères spéciaux
- **Timestamps** dans les noms de fichiers pour éviter les écrasements 

## 📚 Ressources utiles

- [Documentation Python](https://docs.python.org/3/)
- [Guide des environnements virtuels](https://docs.python.org/3/tutorial/venv.html)
- [Installation de Git](https://git-scm.com/downloads) 