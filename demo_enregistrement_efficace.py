"""
Démonstration de l'enregistrement efficace des données La Liga
Montre les meilleures pratiques pour sauvegarder et gérer les données scrapées
"""

import pandas as pd
import sqlite3
import json
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class MatchData:
    """Structure optimisée pour les données de match"""
    season: int
    matchday: int
    date: str
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    result: str
    source_url: str
    scraped_at: str
    data_hash: str = ""
    
    def __post_init__(self):
        """Génère un hash unique pour détecter les doublons"""
        if not self.data_hash:
            hash_string = f"{self.season}_{self.matchday}_{self.home_team}_{self.away_team}_{self.home_goals}_{self.away_goals}"
            self.data_hash = hashlib.md5(hash_string.encode()).hexdigest()

class EfficientDataManager:
    """Gestionnaire efficace des données avec toutes les optimisations"""
    
    def __init__(self, data_dir: str = "laliga_data_demo"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Chemins des fichiers
        self.db_path = self.data_dir / "laliga_optimized.db"
        self.csv_path = self.data_dir / "laliga_optimized.csv"
        self.json_path = self.data_dir / "laliga_optimized.json"
        self.parquet_path = self.data_dir / "laliga_optimized.parquet"
        self.stats_path = self.data_dir / "statistics.json"
        
        # Cache pour éviter les doublons
        self.cache = set()
        
        # Métriques
        self.metrics = {
            'total_processed': 0,
            'duplicates_avoided': 0,
            'successful_inserts': 0,
            'processing_time': 0,
            'last_update': None
        }
        
        # Initialisation
        self.init_database()
        self.load_cache()
    
    def init_database(self):
        """Initialise la base de données avec optimisations"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Table principale optimisée
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
                    source_url TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    data_hash TEXT UNIQUE NOT NULL
                )
            ''')
            
            # Index pour performances optimales
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_season_matchday ON matches(season, matchday)',
                'CREATE INDEX IF NOT EXISTS idx_teams ON matches(home_team, away_team)',
                'CREATE INDEX IF NOT EXISTS idx_date ON matches(date)',
                'CREATE INDEX IF NOT EXISTS idx_hash ON matches(data_hash)',
                'CREATE INDEX IF NOT EXISTS idx_result ON matches(result)',
                'CREATE INDEX IF NOT EXISTS idx_season_team ON matches(season, home_team)'
            ]
            
            for index in indexes:
                cursor.execute(index)
            
            # Table de statistiques
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_name TEXT UNIQUE NOT NULL,
                    stat_value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            ''')
            
            conn.commit()
            logger.info("Base de données initialisée avec succès")
    
    def load_cache(self):
        """Charge le cache des hash existants"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data_hash FROM matches")
                self.cache = {row[0] for row in cursor.fetchall()}
            
            logger.info(f"Cache chargé avec {len(self.cache)} entrées")
        except Exception as e:
            logger.warning(f"Erreur chargement cache: {e}")
            self.cache = set()
    
    def is_duplicate(self, match_data: MatchData) -> bool:
        """Vérification ultra-rapide des doublons"""
        return match_data.data_hash in self.cache
    
    def add_match(self, match_data: MatchData) -> bool:
        """Ajoute un match avec vérification de doublon"""
        self.metrics['total_processed'] += 1
        
        if self.is_duplicate(match_data):
            self.metrics['duplicates_avoided'] += 1
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO matches 
                    (season, matchday, date, home_team, away_team, home_goals, away_goals, result, source_url, scraped_at, data_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_data.season, match_data.matchday, match_data.date,
                    match_data.home_team, match_data.away_team,
                    match_data.home_goals, match_data.away_goals,
                    match_data.result, match_data.source_url, 
                    match_data.scraped_at, match_data.data_hash
                ))
                conn.commit()
                
                # Ajouter au cache
                self.cache.add(match_data.data_hash)
                self.metrics['successful_inserts'] += 1
                
                return True
                
        except sqlite3.IntegrityError:
            # Doublon détecté au niveau DB
            self.metrics['duplicates_avoided'] += 1
            return False
        except Exception as e:
            logger.error(f"Erreur ajout match: {e}")
            return False
    
    def add_matches_batch(self, matches: List[MatchData]) -> int:
        """Ajout en lot pour de meilleures performances"""
        start_time = time.time()
        successful_adds = 0
        
        # Filtrer les doublons avant insertion
        unique_matches = [match for match in matches if not self.is_duplicate(match)]
        
        if not unique_matches:
            logger.info("Aucun nouveau match à ajouter")
            return 0
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Préparer les données
                match_tuples = []
                for match in unique_matches:
                    match_tuples.append((
                        match.season, match.matchday, match.date,
                        match.home_team, match.away_team,
                        match.home_goals, match.away_goals,
                        match.result, match.source_url,
                        match.scraped_at, match.data_hash
                    ))
                
                # Insertion en lot
                cursor.executemany('''
                    INSERT OR IGNORE INTO matches 
                    (season, matchday, date, home_team, away_team, home_goals, away_goals, result, source_url, scraped_at, data_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', match_tuples)
                
                successful_adds = cursor.rowcount
                conn.commit()
                
                # Mettre à jour le cache
                for match in unique_matches:
                    self.cache.add(match.data_hash)
                
                self.metrics['successful_inserts'] += successful_adds
                self.metrics['total_processed'] += len(matches)
                self.metrics['duplicates_avoided'] += len(matches) - len(unique_matches)
                
        except Exception as e:
            logger.error(f"Erreur ajout batch: {e}")
        
        processing_time = time.time() - start_time
        self.metrics['processing_time'] += processing_time
        
        logger.info(f"Batch traité: {successful_adds} ajouts, {len(matches) - len(unique_matches)} doublons évités en {processing_time:.2f}s")
        return successful_adds
    
    def get_dataframe(self) -> pd.DataFrame:
        """Récupère toutes les données sous forme de DataFrame"""
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT * FROM matches ORDER BY season, matchday, date"
            df = pd.read_sql_query(query, conn)
        return df
    
    def export_all_formats(self):
        """Exporte vers tous les formats supportés"""
        logger.info("Début export multi-formats")
        
        df = self.get_dataframe()
        
        if df.empty:
            logger.warning("Aucune donnée à exporter")
            return
        
        # CSV - Format universel
        df.to_csv(self.csv_path, index=False, encoding='utf-8')
        logger.info(f"Export CSV: {self.csv_path}")
        
        # JSON - Interopérabilité
        df.to_json(self.json_path, orient='records', indent=2, force_ascii=False)
        logger.info(f"Export JSON: {self.json_path}")
        
        # Parquet - Performance (si disponible)
        try:
            df.to_parquet(self.parquet_path, index=False)
            logger.info(f"Export Parquet: {self.parquet_path}")
        except Exception as e:
            logger.warning(f"Export Parquet échoué: {e}")
        
        # Statistiques détaillées
        stats = self.get_detailed_statistics()
        with open(self.stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logger.info(f"Export statistiques: {self.stats_path}")
    
    def get_detailed_statistics(self) -> Dict:
        """Calcule des statistiques détaillées"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Statistiques générales
            cursor.execute("SELECT COUNT(*) FROM matches")
            stats['total_matches'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT season) FROM matches")
            stats['total_seasons'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT home_team) FROM matches")
            stats['total_teams'] = cursor.fetchone()[0]
            
            # Répartition par saison
            cursor.execute("SELECT season, COUNT(*) FROM matches GROUP BY season ORDER BY season")
            stats['matches_per_season'] = dict(cursor.fetchall())
            
            # Répartition des résultats
            cursor.execute("SELECT result, COUNT(*) FROM matches GROUP BY result")
            results_dist = dict(cursor.fetchall())
            total = sum(results_dist.values())
            stats['results_distribution'] = {
                'counts': results_dist,
                'percentages': {k: round(v/total*100, 2) for k, v in results_dist.items()}
            }
            
            # Statistiques de buts
            cursor.execute("SELECT AVG(home_goals + away_goals) FROM matches")
            stats['avg_goals_per_match'] = round(cursor.fetchone()[0] or 0, 2)
            
            cursor.execute("SELECT MAX(home_goals + away_goals) FROM matches")
            stats['max_goals_in_match'] = cursor.fetchone()[0] or 0
            
            # Top équipes par victoires à domicile
            cursor.execute("""
                SELECT home_team, COUNT(*) as home_wins 
                FROM matches 
                WHERE result = 'H' 
                GROUP BY home_team 
                ORDER BY home_wins DESC 
                LIMIT 10
            """)
            stats['top_home_teams'] = dict(cursor.fetchall())
            
            # Métriques de performance
            stats['performance_metrics'] = self.metrics.copy()
            stats['performance_metrics']['last_update'] = datetime.now().isoformat()
            
            # Informations sur les données
            cursor.execute("SELECT MIN(date), MAX(date) FROM matches")
            first_date, last_date = cursor.fetchone()
            stats['data_range'] = {
                'first_match': first_date,
                'last_match': last_date
            }
            
            return stats
    
    def validate_data_integrity(self) -> Dict:
        """Valide l'intégrité des données"""
        logger.info("Validation de l'intégrité des données")
        
        issues = []
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Vérifier les buts négatifs
            cursor.execute("SELECT COUNT(*) FROM matches WHERE home_goals < 0 OR away_goals < 0")
            negative_goals = cursor.fetchone()[0]
            if negative_goals > 0:
                issues.append(f"{negative_goals} matchs avec buts négatifs")
            
            # Vérifier la cohérence des résultats
            cursor.execute("""
                SELECT COUNT(*) FROM matches 
                WHERE (result = 'H' AND home_goals <= away_goals) 
                   OR (result = 'A' AND home_goals >= away_goals)
                   OR (result = 'D' AND home_goals != away_goals)
            """)
            inconsistent_results = cursor.fetchone()[0]
            if inconsistent_results > 0:
                issues.append(f"{inconsistent_results} matchs avec résultats incohérents")
            
            # Vérifier les équipes identiques
            cursor.execute("SELECT COUNT(*) FROM matches WHERE home_team = away_team")
            same_teams = cursor.fetchone()[0]
            if same_teams > 0:
                issues.append(f"{same_teams} matchs avec équipes identiques")
            
            # Vérifier les journées invalides
            cursor.execute("SELECT COUNT(*) FROM matches WHERE matchday < 1 OR matchday > 38")
            invalid_matchdays = cursor.fetchone()[0]
            if invalid_matchdays > 0:
                issues.append(f"{invalid_matchdays} matchs avec journées invalides")
        
        validation_result = {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'validated_at': datetime.now().isoformat()
        }
        
        if validation_result['is_valid']:
            logger.info("✅ Données valides")
        else:
            logger.warning(f"⚠️ {len(issues)} problèmes détectés: {issues}")
        
        return validation_result
    
    def cleanup_and_optimize(self):
        """Nettoie et optimise la base de données"""
        logger.info("Nettoyage et optimisation de la base de données")
        
        deleted_duplicates = 0
        
        # Supprimer les doublons éventuels
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM matches 
                WHERE id NOT IN (
                    SELECT MIN(id) 
                    FROM matches 
                    GROUP BY data_hash
                )
            ''')
            deleted_duplicates = cursor.rowcount
            conn.commit()
        
        # Optimiser la base de données (VACUUM doit être hors transaction)
        conn = sqlite3.connect(self.db_path)
        conn.execute("VACUUM")
        conn.execute("ANALYZE")
        conn.close()
        
        logger.info(f"Nettoyage terminé: {deleted_duplicates} doublons supprimés")
    
    def create_sample_data(self, num_seasons: int = 3) -> List[MatchData]:
        """Crée des données d'exemple pour la démonstration"""
        teams = [
            'Real Madrid', 'Barcelona', 'Atlético Madrid', 'Sevilla', 'Valencia',
            'Villarreal', 'Athletic Bilbao', 'Real Sociedad', 'Real Betis', 'Celta Vigo',
            'Espanyol', 'Getafe', 'Levante', 'Osasuna', 'Mallorca', 'Alavés',
            'Cádiz', 'Elche', 'Granada', 'Huesca'
        ]
        
        matches = []
        import random
        
        for season in range(2021, 2021 + num_seasons):
            logger.info(f"Génération données saison {season}")
            
            for matchday in range(1, 39):  # 38 journées
                # Simuler 10 matchs par journée
                season_teams = teams[:20]  # 20 équipes
                random.shuffle(season_teams)
                
                for i in range(0, 20, 2):
                    home_team = season_teams[i]
                    away_team = season_teams[i + 1]
                    
                    # Générer score réaliste
                    home_goals = random.choices([0, 1, 2, 3, 4], weights=[15, 35, 30, 15, 5])[0]
                    away_goals = random.choices([0, 1, 2, 3, 4], weights=[20, 40, 25, 10, 5])[0]
                    
                    # Déterminer résultat
                    if home_goals > away_goals:
                        result = 'H'
                    elif home_goals < away_goals:
                        result = 'A'
                    else:
                        result = 'D'
                    
                    # Date simulée
                    base_date = datetime(season, 8, 15)
                    days_offset = (matchday - 1) * 7  # Une semaine entre chaque journée
                    match_date = base_date + timedelta(days=days_offset)
                    
                    match_data = MatchData(
                        season=season,
                        matchday=matchday,
                        date=match_date.strftime("%Y-%m-%d"),
                        home_team=home_team,
                        away_team=away_team,
                        home_goals=home_goals,
                        away_goals=away_goals,
                        result=result,
                        source_url=f"https://example.com/{season}/matchday/{matchday}",
                        scraped_at=datetime.now().isoformat()
                    )
                    matches.append(match_data)
        
        return matches

def main():
    """Démonstration complète de l'enregistrement efficace"""
    print("🚀 DÉMONSTRATION DE L'ENREGISTREMENT EFFICACE LA LIGA")
    print("=" * 60)
    
    # Initialisation
    manager = EfficientDataManager()
    
    # 1. Génération de données d'exemple
    print("\n📊 1. Génération de données d'exemple...")
    sample_matches = manager.create_sample_data(num_seasons=2)
    print(f"   ✅ {len(sample_matches)} matchs générés")
    
    # 2. Ajout en lot (démonstration de performance)
    print("\n💾 2. Enregistrement en lot...")
    start_time = time.time()
    added_count = manager.add_matches_batch(sample_matches)
    end_time = time.time()
    print(f"   ✅ {added_count} matchs ajoutés en {end_time - start_time:.2f}s")
    
    # 3. Test de détection de doublons
    print("\n🔍 3. Test de détection de doublons...")
    duplicate_count = manager.add_matches_batch(sample_matches[:100])  # Réessayer les 100 premiers
    print(f"   ✅ {duplicate_count} nouveaux matchs (doublons évités)")
    
    # 4. Validation des données
    print("\n✅ 4. Validation de l'intégrité...")
    validation = manager.validate_data_integrity()
    if validation['is_valid']:
        print("   ✅ Données valides")
    else:
        print(f"   ⚠️ Problèmes détectés: {validation['issues']}")
    
    # 5. Statistiques détaillées
    print("\n📈 5. Statistiques détaillées...")
    stats = manager.get_detailed_statistics()
    print(f"   📊 Total matchs: {stats['total_matches']}")
    print(f"   📊 Saisons: {stats['total_seasons']}")
    print(f"   📊 Équipes: {stats['total_teams']}")
    print(f"   📊 Moyenne buts/match: {stats['avg_goals_per_match']}")
    print(f"   📊 Répartition résultats: {stats['results_distribution']['percentages']}")
    
    # 6. Export multi-formats
    print("\n💾 6. Export multi-formats...")
    manager.export_all_formats()
    
    # 7. Nettoyage et optimisation
    print("\n🧹 7. Nettoyage et optimisation...")
    manager.cleanup_and_optimize()
    
    # 8. Métriques de performance
    print("\n⚡ 8. Métriques de performance:")
    metrics = manager.metrics
    print(f"   🔢 Matchs traités: {metrics['total_processed']}")
    print(f"   ✅ Insertions réussies: {metrics['successful_inserts']}")
    print(f"   🚫 Doublons évités: {metrics['duplicates_avoided']}")
    print(f"   ⏱️ Temps de traitement: {metrics['processing_time']:.2f}s")
    
    # 9. Affichage des fichiers créés
    print("\n📁 9. Fichiers créés:")
    for file_path in manager.data_dir.glob("*"):
        size = file_path.stat().st_size
        print(f"   📄 {file_path.name}: {size:,} bytes")
    
    # 10. Échantillon des données
    print("\n📋 10. Échantillon des données:")
    df = manager.get_dataframe()
    print(df.head(10).to_string(index=False))
    
    print("\n🎉 DÉMONSTRATION TERMINÉE!")
    print(f"💾 Données sauvegardées dans: {manager.data_dir}")
    print("📊 Consultez les fichiers CSV, JSON et statistiques pour plus de détails")

if __name__ == "__main__":
    main() 