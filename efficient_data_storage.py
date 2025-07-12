"""
Système d'enregistrement efficace pour les données de matchs La Liga
Optimise le stockage et la récupération des données scrapées
"""

import pandas as pd
import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
import pickle
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MatchData:
    """Structure de données pour un match"""
    season: int
    matchday: int
    date: str
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    result: str  # 'H', 'D', 'A'
    url_source: str
    scraped_at: str
    
    def __post_init__(self):
        """Validation des données après initialisation"""
        if self.home_goals < 0 or self.away_goals < 0:
            raise ValueError("Les buts ne peuvent pas être négatifs")
        if self.result not in ['H', 'D', 'A']:
            raise ValueError("Le résultat doit être 'H', 'D' ou 'A'")

class EfficientDataStorage:
    """Système de stockage efficace pour les données de matchs"""
    
    def __init__(self, base_dir: str = "laliga_data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Chemins des fichiers
        self.db_path = self.base_dir / "laliga_matches.db"
        self.csv_path = self.base_dir / "laliga_matches.csv"
        self.json_path = self.base_dir / "laliga_matches.json"
        self.pickle_path = self.base_dir / "laliga_matches.pkl"
        self.metadata_path = self.base_dir / "metadata.json"
        
        # Initialisation de la base de données
        self.init_database()
        
        # Métadonnées
        self.metadata = self.load_metadata()
    
    def init_database(self):
        """Initialise la base de données SQLite"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    season INTEGER NOT NULL,
                    matchday INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    home_team TEXT NOT NULL,
                    away_team TEXT NOT NULL,
                    home_goals INTEGER NOT NULL,
                    away_goals INTEGER NOT NULL,
                    result TEXT NOT NULL,
                    url_source TEXT,
                    scraped_at TEXT NOT NULL,
                    UNIQUE(season, matchday, home_team, away_team)
                )
            ''')
            
            # Index pour améliorer les performances
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_season ON matches(season)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_teams ON matches(home_team, away_team)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON matches(date)')
            
            conn.commit()
    
    def load_metadata(self) -> Dict:
        """Charge les métadonnées"""
        if self.metadata_path.exists():
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            'total_matches': 0,
            'seasons': [],
            'teams': [],
            'last_update': None,
            'scraping_stats': {
                'total_pages_scraped': 0,
                'successful_scrapes': 0,
                'failed_scrapes': 0
            }
        }
    
    def save_metadata(self):
        """Sauvegarde les métadonnées"""
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
    
    def add_match(self, match_data: MatchData) -> bool:
        """Ajoute un match à la base de données"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO matches 
                    (season, matchday, date, home_team, away_team, home_goals, away_goals, result, url_source, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data.season, match_data.matchday, match_data.date,
                    match_data.home_team, match_data.away_team,
                    match_data.home_goals, match_data.away_goals,
                    match_data.result, match_data.url_source, match_data.scraped_at
                ))
                conn.commit()
                
                # Mise à jour des métadonnées
                self.update_metadata(match_data)
                return True
                
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout du match: {e}")
            return False
    
    def add_matches_batch(self, matches: List[MatchData]) -> int:
        """Ajoute plusieurs matchs en lot (plus efficace)"""
        successful_adds = 0
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Préparer les données
                match_tuples = []
                for match in matches:
                    match_tuples.append((
                        match.season, match.matchday, match.date,
                        match.home_team, match.away_team,
                        match.home_goals, match.away_goals,
                        match.result, match.url_source, match.scraped_at
                    ))
                
                # Insertion en lot
                cursor.executemany('''
                    INSERT OR REPLACE INTO matches 
                    (season, matchday, date, home_team, away_team, home_goals, away_goals, result, url_source, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', match_tuples)
                
                successful_adds = cursor.rowcount
                conn.commit()
                
                # Mise à jour des métadonnées
                for match in matches:
                    self.update_metadata(match)
                
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout en lot: {e}")
        
        return successful_adds
    
    def update_metadata(self, match_data: MatchData):
        """Met à jour les métadonnées"""
        # Saison
        if match_data.season not in self.metadata['seasons']:
            self.metadata['seasons'].append(match_data.season)
            self.metadata['seasons'].sort()
        
        # Équipes
        for team in [match_data.home_team, match_data.away_team]:
            if team not in self.metadata['teams']:
                self.metadata['teams'].append(team)
        
        # Dernière mise à jour
        self.metadata['last_update'] = datetime.now().isoformat()
        
        # Sauvegarde
        self.save_metadata()
    
    def get_matches(self, season: Optional[int] = None, team: Optional[str] = None) -> List[Dict]:
        """Récupère les matchs avec filtres optionnels"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM matches WHERE 1=1"
            params = []
            
            if season:
                query += " AND season = ?"
                params.append(season)
            
            if team:
                query += " AND (home_team = ? OR away_team = ?)"
                params.extend([team, team])
            
            query += " ORDER BY season, matchday, date"
            
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_dataframe(self, season: Optional[int] = None, team: Optional[str] = None) -> pd.DataFrame:
        """Récupère les données sous forme de DataFrame"""
        matches = self.get_matches(season, team)
        return pd.DataFrame(matches)
    
    def export_to_csv(self, filename: Optional[str] = None) -> str:
        """Exporte toutes les données vers CSV"""
        if filename is None:
            filename = self.csv_path
        
        df = self.get_dataframe()
        df.to_csv(filename, index=False, encoding='utf-8')
        logger.info(f"Données exportées vers {filename}")
        return str(filename)
    
    def export_to_json(self, filename: Optional[str] = None) -> str:
        """Exporte toutes les données vers JSON"""
        if filename is None:
            filename = self.json_path
        
        matches = self.get_matches()
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(matches, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Données exportées vers {filename}")
        return str(filename)
    
    def export_to_pickle(self, filename: Optional[str] = None) -> str:
        """Exporte toutes les données vers Pickle (plus rapide)"""
        if filename is None:
            filename = self.pickle_path
        
        df = self.get_dataframe()
        df.to_pickle(filename)
        logger.info(f"Données exportées vers {filename}")
        return str(filename)
    
    def get_statistics(self) -> Dict:
        """Retourne des statistiques sur les données"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Nombre total de matchs
            cursor.execute("SELECT COUNT(*) FROM matches")
            stats['total_matches'] = cursor.fetchone()[0]
            
            # Matchs par saison
            cursor.execute("SELECT season, COUNT(*) FROM matches GROUP BY season ORDER BY season")
            stats['matches_per_season'] = dict(cursor.fetchall())
            
            # Nombre d'équipes uniques
            cursor.execute("SELECT COUNT(DISTINCT home_team) FROM matches")
            stats['unique_teams'] = cursor.fetchone()[0]
            
            # Répartition des résultats
            cursor.execute("SELECT result, COUNT(*) FROM matches GROUP BY result")
            stats['results_distribution'] = dict(cursor.fetchall())
            
            # Moyenne de buts par match
            cursor.execute("SELECT AVG(home_goals + away_goals) FROM matches")
            stats['avg_goals_per_match'] = round(cursor.fetchone()[0] or 0, 2)
            
            # Première et dernière date
            cursor.execute("SELECT MIN(date), MAX(date) FROM matches")
            first_date, last_date = cursor.fetchone()
            stats['date_range'] = {'first': first_date, 'last': last_date}
            
        return stats
    
    def backup_data(self, backup_dir: str = "backup"):
        """Crée une sauvegarde complète des données"""
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = backup_path / f"backup_{timestamp}"
        backup_subdir.mkdir(exist_ok=True)
        
        # Sauvegarde de la base de données
        import shutil
        shutil.copy2(self.db_path, backup_subdir / "laliga_matches.db")
        
        # Sauvegarde des exports
        self.export_to_csv(backup_subdir / "laliga_matches.csv")
        self.export_to_json(backup_subdir / "laliga_matches.json")
        self.export_to_pickle(backup_subdir / "laliga_matches.pkl")
        
        # Sauvegarde des métadonnées
        shutil.copy2(self.metadata_path, backup_subdir / "metadata.json")
        
        logger.info(f"Sauvegarde créée dans {backup_subdir}")
        return str(backup_subdir)
    
    def clean_duplicates(self) -> int:
        """Nettoie les doublons dans la base de données"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Supprime les doublons en gardant le plus récent
            cursor.execute('''
                DELETE FROM matches 
                WHERE id NOT IN (
                    SELECT MAX(id) 
                    FROM matches 
                    GROUP BY season, matchday, home_team, away_team
                )
            ''')
            
            deleted_count = cursor.rowcount
            conn.commit()
            
        logger.info(f"{deleted_count} doublons supprimés")
        return deleted_count
    
    def optimize_database(self):
        """Optimise la base de données"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("VACUUM")
            cursor.execute("ANALYZE")
            conn.commit()
        
        logger.info("Base de données optimisée")

# Exemple d'utilisation avec intégration au scraper
class IntegratedScraper:
    """Scraper intégré avec stockage efficace"""
    
    def __init__(self):
        self.storage = EfficientDataStorage()
        self.session = requests.Session()
        # Configuration du session...
    
    def scrape_and_store_match(self, url: str, season: int, matchday: int) -> bool:
        """Scrape une page et stocke directement les données"""
        try:
            # Scraping (code simplifié)
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraction des données (à adapter selon la structure réelle)
            matches_data = self.extract_matches_from_page(soup, season, matchday, url)
            
            # Stockage en lot
            if matches_data:
                added_count = self.storage.add_matches_batch(matches_data)
                logger.info(f"{added_count} matchs ajoutés depuis {url}")
                return True
            
        except Exception as e:
            logger.error(f"Erreur lors du scraping de {url}: {e}")
            return False
    
    def extract_matches_from_page(self, soup: BeautifulSoup, season: int, matchday: int, url: str) -> List[MatchData]:
        """Extrait les données de match d'une page (à implémenter selon la structure)"""
        matches = []
        # Logique d'extraction à adapter...
        return matches

def main():
    """Test du système de stockage"""
    storage = EfficientDataStorage()
    
    # Exemple d'ajout de données
    sample_matches = [
        MatchData(
            season=2023,
            matchday=1,
            date="2023-08-12",
            home_team="Real Madrid",
            away_team="Barcelona",
            home_goals=2,
            away_goals=1,
            result="H",
            url_source="https://example.com",
            scraped_at=datetime.now().isoformat()
        ),
        MatchData(
            season=2023,
            matchday=1,
            date="2023-08-12",
            home_team="Atletico Madrid",
            away_team="Valencia",
            home_goals=1,
            away_goals=1,
            result="D",
            url_source="https://example.com",
            scraped_at=datetime.now().isoformat()
        )
    ]
    
    # Ajout en lot
    added = storage.add_matches_batch(sample_matches)
    print(f"Matchs ajoutés: {added}")
    
    # Statistiques
    stats = storage.get_statistics()
    print("\nStatistiques:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Export
    storage.export_to_csv()
    storage.export_to_json()
    
    # Sauvegarde
    storage.backup_data()

if __name__ == "__main__":
    main() 