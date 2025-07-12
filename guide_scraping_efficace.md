# Guide du Scraping Efficace pour La Liga

## 🎯 Objectif
Ce guide présente les meilleures pratiques pour scraper et enregistrer efficacement les données de matchs de La Liga pour la construction d'un dataset de prédiction.

## 📊 Structure des Données Optimisée

### 1. Format de Données Standardisé

```python
@dataclass
class MatchData:
    season: int              # 2023
    matchday: int           # 1-38
    date: str               # "2023-08-12"
    home_team: str          # "Real Madrid"
    away_team: str          # "Barcelona"
    home_goals: int         # 2
    away_goals: int         # 1
    result: str             # 'H', 'D', 'A'
    source_url: str         # URL source
    scraped_at: str         # Timestamp
    data_hash: str          # Hash unique pour détecter doublons
```

### 2. Avantages de cette Structure

- **Cohérence** : Format uniforme pour tous les matchs
- **Intégrité** : Validation automatique des données
- **Efficacité** : Hash unique pour éviter les doublons
- **Traçabilité** : Source et timestamp pour chaque donnée

## 🗄️ Système de Stockage Multi-Format

### 1. Base de Données SQLite (Principal)

```sql
CREATE TABLE matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER NOT NULL,
    matchday INTEGER NOT NULL,
    date TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_goals INTEGER NOT NULL,
    away_goals INTEGER NOT NULL,
    result TEXT NOT NULL,
    source_url TEXT NOT NULL,
    scraped_at TEXT NOT NULL,
    data_hash TEXT UNIQUE NOT NULL
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_season_matchday ON matches(season, matchday);
CREATE INDEX idx_teams ON matches(home_team, away_team);
CREATE INDEX idx_date ON matches(date);
CREATE INDEX idx_hash ON matches(data_hash);
```

**Avantages :**
- Requêtes SQL rapides
- Contraintes d'intégrité
- Pas de doublons (data_hash UNIQUE)
- Index optimisés pour les recherches

### 2. Formats d'Export

```python
# CSV pour l'analyse
df.to_csv('laliga_dataset.csv', index=False)

# JSON pour l'interopérabilité
df.to_json('laliga_dataset.json', orient='records')

# Pickle pour la vitesse (Python)
df.to_pickle('laliga_dataset.pkl')

# Parquet pour Big Data
df.to_parquet('laliga_dataset.parquet')
```

## 🔧 Stratégies de Scraping Optimisées

### 1. Analyse de Page Préalable

```python
def analyze_page_structure(url):
    """Analyse la structure d'une page avant scraping"""
    soup = get_page(url)
    
    # Identifier les conteneurs de matchs
    potential_containers = [
        'table',
        'div[class*="match"]',
        'div[class*="fixture"]',
        'div[class*="game"]'
    ]
    
    # Analyser les patterns de données
    score_patterns = find_score_patterns(soup)
    team_patterns = find_team_patterns(soup)
    
    return {
        'containers': potential_containers,
        'score_patterns': score_patterns,
        'team_patterns': team_patterns
    }
```

### 2. Extraction Multi-Stratégies

```python
def extract_matches_robust(soup, season, matchday, url):
    """Extraction avec plusieurs stratégies de fallback"""
    matches = []
    
    # Stratégie 1: Tableaux HTML
    matches.extend(extract_from_tables(soup, season, matchday, url))
    
    # Stratégie 2: Divs avec classes spécifiques
    if not matches:
        matches.extend(extract_from_divs(soup, season, matchday, url))
    
    # Stratégie 3: Regex sur le texte brut
    if not matches:
        matches.extend(extract_from_text(soup, season, matchday, url))
    
    return matches
```

### 3. Gestion des Erreurs et Retry

```python
def scrape_with_retry(url, max_retries=3):
    """Scraping avec retry automatique"""
    for attempt in range(max_retries):
        try:
            # Délai exponentiel
            time.sleep(2 ** attempt)
            
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            return BeautifulSoup(response.content, 'html.parser')
            
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1} échouée: {e}")
            if attempt == max_retries - 1:
                raise
```

## ⚡ Optimisations de Performance

### 1. Cache Intelligent

```python
class DataCache:
    def __init__(self):
        self.cache = {}  # Hash -> Boolean
        self.cache_lock = Lock()
    
    def is_duplicate(self, match_data):
        """Vérification ultra-rapide des doublons"""
        with self.cache_lock:
            return match_data.data_hash in self.cache
    
    def add_to_cache(self, match_data):
        """Ajout au cache"""
        with self.cache_lock:
            self.cache[match_data.data_hash] = True
```

### 2. Batch Processing

```python
class BatchProcessor:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.pending_matches = []
        self.batch_lock = Lock()
    
    def add_match(self, match_data):
        """Ajout avec flush automatique"""
        with self.batch_lock:
            self.pending_matches.append(match_data)
            
            if len(self.pending_matches) >= self.batch_size:
                self.flush_batch()
    
    def flush_batch(self):
        """Insertion en lot dans la DB"""
        if not self.pending_matches:
            return
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(INSERT_QUERY, self.pending_matches)
            conn.commit()
```

### 3. Scraping Parallèle

```python
def scrape_parallel(urls, max_workers=4):
    """Scraping parallèle avec ThreadPoolExecutor"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre toutes les tâches
        future_to_url = {
            executor.submit(scrape_url, url): url 
            for url in urls
        }
        
        # Traiter les résultats au fur et à mesure
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                matches = future.result()
                process_matches(matches)
            except Exception as e:
                logger.error(f"Erreur {url}: {e}")
```

## 📈 Monitoring et Métadonnées

### 1. Suivi des Performances

```python
class ScrapingMetrics:
    def __init__(self):
        self.metrics = {
            'total_pages_scraped': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'total_matches_found': 0,
            'duplicates_avoided': 0,
            'scraping_speed': 0  # pages/minute
        }
    
    def update_metrics(self, success, matches_count=0, is_duplicate=False):
        self.metrics['total_pages_scraped'] += 1
        if success:
            self.metrics['successful_scrapes'] += 1
            self.metrics['total_matches_found'] += matches_count
        else:
            self.metrics['failed_scrapes'] += 1
            
        if is_duplicate:
            self.metrics['duplicates_avoided'] += 1
```

### 2. Métadonnées par URL

```sql
CREATE TABLE scraping_metadata (
    url TEXT PRIMARY KEY,
    last_scraped TEXT,
    success_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    avg_matches_per_page REAL
);
```

## 🛠️ Outils et Bibliothèques Recommandées

### 1. Scraping
```python
# Bibliothèques essentielles
import requests          # HTTP requests
from bs4 import BeautifulSoup  # HTML parsing
import selenium          # JavaScript rendering (si nécessaire)
from fake_useragent import UserAgent  # User agents aléatoires
```

### 2. Données
```python
import pandas as pd      # DataFrames
import sqlite3          # Base de données
import numpy as np      # Calculs numériques
from dataclasses import dataclass  # Structures de données
```

### 3. Performance
```python
import concurrent.futures  # Parallélisation
from threading import Lock  # Thread safety
import time             # Délais
import hashlib         # Hash pour doublons
```

## 📋 Checklist de Validation

### Avant le Scraping
- [ ] Analyser la structure des pages cibles
- [ ] Identifier les sélecteurs CSS optimaux
- [ ] Tester les patterns d'extraction
- [ ] Configurer les headers HTTP appropriés
- [ ] Définir les délais entre requêtes

### Pendant le Scraping
- [ ] Monitorer les taux de succès/échec
- [ ] Vérifier la qualité des données extraites
- [ ] Surveiller les doublons
- [ ] Ajuster les délais si nécessaire
- [ ] Logger les erreurs pour debug

### Après le Scraping
- [ ] Valider l'intégrité des données
- [ ] Nettoyer les données incomplètes
- [ ] Exporter vers multiple formats
- [ ] Créer des sauvegardes
- [ ] Documenter les sources et méthodes

## 🎯 Exemple d'Implémentation Complète

```python
class LaLigaScraperOptimized:
    def __init__(self):
        self.storage = OptimizedDataStorage()
        self.cache = DataCache()
        self.batch_processor = BatchProcessor()
        self.metrics = ScrapingMetrics()
        
    def scrape_season(self, season, start_matchday=1, end_matchday=38):
        """Scrape une saison complète"""
        logger.info(f"Début scraping saison {season}")
        
        for matchday in range(start_matchday, end_matchday + 1):
            # Construire l'URL
            url = self.build_url(season, matchday)
            
            # Scraper la page
            matches = self.scrape_matchday(url, season, matchday)
            
            # Traiter les matchs
            for match in matches:
                if not self.cache.is_duplicate(match):
                    self.batch_processor.add_match(match)
                    self.cache.add_to_cache(match)
                    self.metrics.update_metrics(True, 1)
                else:
                    self.metrics.update_metrics(True, 0, is_duplicate=True)
            
            # Délai entre les pages
            time.sleep(1)
        
        # Flush final
        self.batch_processor.flush_batch()
        
        logger.info(f"Saison {season} terminée: {self.metrics.metrics}")
```

## 🔍 Conseils Spécifiques pour La Liga

### 1. Sources Recommandées
- **ESPN** : Structure stable, données fiables
- **Transfermarkt** : Données détaillées, historique complet
- **FlashScore** : Temps réel, mais structure complexe
- **SofaScore** : API-friendly, données riches

### 2. Patterns Typiques
```python
# Noms d'équipes à normaliser
TEAM_ALIASES = {
    'Real Madrid CF': 'Real Madrid',
    'FC Barcelona': 'Barcelona',
    'Club Atlético de Madrid': 'Atlético Madrid',
    'Sevilla FC': 'Sevilla'
}

# Patterns de score courants
SCORE_PATTERNS = [
    r'(\d+)[-:](\d+)',      # 2-1, 2:1
    r'(\d+)\s*-\s*(\d+)',   # 2 - 1
    r'(\d+)\s+(\d+)'        # 2 1
]
```

### 3. Validation des Données
```python
def validate_match_data(match):
    """Validation des données de match"""
    checks = [
        match.season >= 2000,
        1 <= match.matchday <= 38,
        match.home_goals >= 0,
        match.away_goals >= 0,
        match.result in ['H', 'D', 'A'],
        match.home_team != match.away_team,
        len(match.home_team) > 2,
        len(match.away_team) > 2
    ]
    
    return all(checks)
```

## 🚀 Résumé des Bonnes Pratiques

1. **Structure** : Utilisez des dataclasses pour la cohérence
2. **Stockage** : SQLite + exports multiples pour la flexibilité
3. **Cache** : Hash unique pour éviter les doublons
4. **Batch** : Insertions par lots pour la performance
5. **Parallèle** : ThreadPoolExecutor pour la vitesse
6. **Monitoring** : Métriques et logs pour le debugging
7. **Validation** : Vérifications automatiques des données
8. **Backup** : Sauvegardes régulières et multiples formats

Cette approche garantit un scraping efficace, robuste et maintenable pour construire un dataset de qualité pour la prédiction de matchs de La Liga. 