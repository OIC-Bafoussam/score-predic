"""
Script final pour scraper les données de La Liga sur MondeFootball.fr
Peut être utilisé pour n'importe quelle journée et saison
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import argparse
import logging
from typing import Optional, Tuple, Dict, List

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LaLigaScraper:
    """Scraper pour La Liga sur MondeFootball.fr"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        })
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Récupère une page avec gestion du décodage automatique"""
        # Nettoyer l'URL en enlevant le préfixe view-source: si présent
        cleaned_url = url.replace('view-source:', '') if url.startswith('view-source:') else url
        
        try:
            logger.info(f"Récupération de la page: {cleaned_url}")
            response = self.session.get(cleaned_url, timeout=30)
            response.raise_for_status()
            
            html_content = response.text
            
            if '<html' in html_content.lower() or '<body' in html_content.lower():
                soup = BeautifulSoup(html_content, 'html.parser')
                logger.info("Page récupérée avec succès")
                return soup
            else:
                logger.error("Le contenu ne semble pas être du HTML valide")
                raise ValueError("Contenu HTML invalide - impossible de continuer")
                
        except Exception as e:
            logger.error(f"Erreur critique lors de la récupération: {e}")
            raise RuntimeError(f"Impossible de récupérer la page {cleaned_url}: {e}")
    
    def extract_matches_from_html(self, soup: BeautifulSoup) -> List[Dict]:
        """Extrait les matchs du HTML"""
        matches = []
        
        # Trouver le tableau des matchs
        match_tables = soup.find_all('table', class_='standard_tabelle')
        
        if not match_tables:
            logger.error("Aucun tableau de matchs trouvé")
            return matches
        
        # Le premier tableau contient les matchs
        match_table = match_tables[0]
        rows = match_table.find_all('tr')
        
        current_date = None
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 6:  # Une ligne de match complète
                try:
                    # Extraire la date (si présente dans la première cellule)
                    date_cell = cells[0].get_text(strip=True)
                    if date_cell and '.' in date_cell:  # Format date DD.MM.YYYY
                        current_date = date_cell
                    
                    # Heure
                    time_cell = cells[1].get_text(strip=True)
                    
                    # Équipe domicile
                    home_team_link = cells[2].find('a')
                    if home_team_link:
                        home_team = home_team_link.get_text(strip=True)
                    else:
                        home_team = cells[2].get_text(strip=True)
                    
                    # Équipe extérieure
                    away_team_link = cells[4].find('a')
                    if away_team_link:
                        away_team = away_team_link.get_text(strip=True)
                    else:
                        away_team = cells[4].get_text(strip=True)
                    
                    # Score
                    score_cell = cells[5].get_text(strip=True)
                    detail_link = cells[5].find('a').get('href')
                    
                    
                    # Parser le score (format: "1:1 (1:0)" ou "1:1")
                    if score_cell and ':' in score_cell:
                        score_part = score_cell.split('(')[0].strip()  # Prendre la partie avant les parenthèses
                        if ':' in score_part:
                            home_goals, away_goals = score_part.split(':')
                            home_goals = int(home_goals.strip())
                            away_goals = int(away_goals.strip())
                            
                            # Convertir la date au format YYYY-MM-DD
                            if current_date:
                                date_parts = current_date.split('.')
                                if len(date_parts) == 3:
                                    formatted_date = f"{date_parts[2]}-{date_parts[1].zfill(2)}-{date_parts[0].zfill(2)}"
                                else:
                                    formatted_date = current_date
                            else:
                                formatted_date = "Unknown"
                            
                            match_data = {
                                'date': formatted_date,
                                'time': time_cell,
                                'home_team': home_team,
                                'away_team': away_team,
                                'home_goals': home_goals,
                                'away_goals': away_goals,
                                'detail_link': detail_link
                            }
                            
                            matches.append(match_data)
                            logger.info(f"Match extrait: {home_team} {home_goals}-{away_goals} {away_team}")
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"Erreur lors du parsing d'une ligne: {e}")
                    continue
        
        logger.info(f"Total des matchs extraits: {len(matches)}")
        return matches
    
    def extract_standings_from_html(self, soup: BeautifulSoup) -> Dict:
        """Extrait le classement du HTML"""
        standings = {}
        
        # Trouver les tableaux de données
        tables = soup.find_all('table', class_='standard_tabelle')
        
        if len(tables) < 2:
            logger.error("Tableau de classement non trouvé")
            return standings
        
        # Le deuxième tableau contient le classement
        standings_table = tables[1]
        rows = standings_table.find_all('tr')
        
        current_position = 1
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 10:  # Une ligne de classement complète
                try:
                    # Nom de l'équipe (3ème cellule)
                    team_link = cells[2].find('a')
                    if team_link:
                        team_name = team_link.get_text(strip=True)
                    else:
                        team_name = cells[2].get_text(strip=True)
                    
                    # Nettoyer le nom de l'équipe (enlever les annotations comme "(N)", "(P)", "(M)")
                    team_name = team_name.split('(')[0].strip()
                    
                    if not team_name:
                        continue
                    
                    # Position (1ère cellule) - si vide, utiliser le compteur
                    position_cell = cells[0].get_text(strip=True)
                    if position_cell and position_cell.isdigit():
                        position = int(position_cell)
                        current_position = position
                    else:
                        position = current_position
                    
                    # Matchs joués (4ème cellule)
                    matches_played = int(cells[3].get_text(strip=True))
                    
                    # Victoires (5ème cellule)
                    wins = int(cells[4].get_text(strip=True))
                    
                    # Nuls (6ème cellule)
                    draws = int(cells[5].get_text(strip=True))
                    
                    # Défaites (7ème cellule)
                    losses = int(cells[6].get_text(strip=True))
                    
                    # Buts pour:contre (8ème cellule)
                    goals_cell = cells[7].get_text(strip=True)
                    if ':' in goals_cell:
                        goals_for, goals_against = goals_cell.split(':')
                        goals_for = int(goals_for.strip())
                        goals_against = int(goals_against.strip())
                    else:
                        goals_for = goals_against = 0
                    
                    # Points (10ème cellule)
                    points = int(cells[9].get_text(strip=True))
                    
                    standings[team_name] = {
                        'position': position,
                        'matches_played': matches_played,
                        'wins': wins,
                        'draws': draws,
                        'losses': losses,
                        'goals_for': goals_for,
                        'goals_against': goals_against,
                        'points': points
                    }
                    
                    logger.info(f"Équipe extraite: {team_name} - Position {position}, Points {points}")
                    current_position += 1
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"Erreur lors du parsing d'une ligne de classement: {e}")
                    continue
        
        logger.info(f"Total d'équipes extraites du classement: {len(standings)}")
        return standings
    
    def scrape_matchday(self, url: str, season: int, matchday: int, fallback_data: Optional[Dict] = None) -> pd.DataFrame:
        """Scrape une journée et retourne un DataFrame"""
        logger.info(f"Scraping de la journée {matchday} - Saison {season}/{season+1}")
        
        soup = self.get_page(url)
        matches_data = []
        standings_data = {}
        
        if soup:
            # Essayer d'extraire les données du HTML
            matches_data = self.extract_matches_from_html(soup)
            standings_data = self.extract_standings_from_html(soup)
        
        # Si pas de données trouvées, arrêter l'exécution
        if not matches_data:
            if fallback_data:
                logger.error("Extraction des données échouée. Fallback disponible mais refusé.")
            else:
                logger.error("Aucune donnée trouvée pour cette journée")
            raise RuntimeError(f"Impossible d'extraire les données pour la journée {matchday} de la saison {season}/{season+1}")
        
        if not standings_data:
            logger.error("Aucune donnée de classement trouvée")
            raise RuntimeError(f"Impossible d'extraire le classement pour la journée {matchday} de la saison {season}/{season+1}")
        
        # Créer le DataFrame
        enriched_matches = []
        
        for i, match in enumerate(matches_data):
            home_team = match.get('home_team', '')
            away_team = match.get('away_team', '')
            
            # Nettoyer les noms d'équipes
            home_team_clean = self.clean_team_name(home_team)
            away_team_clean = self.clean_team_name(away_team)
            
            # Récupérer les données du classement
            home_stats = standings_data.get(home_team_clean, standings_data.get(home_team))
            away_stats = standings_data.get(away_team_clean, standings_data.get(away_team))
            
            if home_stats is None:
                raise KeyError(f"Équipe domicile '{home_team}' introuvable dans le classement")
            if away_stats is None:
                raise KeyError(f"Équipe extérieure '{away_team}' introuvable dans le classement")
            
            # Calculer le résultat
            home_goals = match.get('home_goals', 0)
            away_goals = match.get('away_goals', 0)
            
            if home_goals > away_goals:
                result = '1'  # Victoire domicile
            elif home_goals < away_goals:
                result = '2'  # Victoire extérieur
            else:
                result = 'X'  # Match nul
            
            enriched_match = {
                'id': i + 1,
                'season': f"{season}/{season+1}",
                'matchday': matchday,
                'date': match.get('date', ''),
                'home_team': home_team_clean,
                'away_team': away_team_clean,
                'home_goals': home_goals,
                'away_goals': away_goals,
                'result': result,
                'home_position': home_stats['position'],
                'away_position': away_stats['position'],
                'home_scored_and_conceded_goals': f"{home_stats['goals_for']}:{home_stats['goals_against']}",
                'away_scored_and_conceded_goals': f"{away_stats['goals_for']}:{away_stats['goals_against']}",
                'detail_link': f"https://www.mondefootball.fr{match.get('detail_link', '')}"
            }
            
            enriched_matches.append(enriched_match)
        
        # Créer le DataFrame
        df = pd.DataFrame(enriched_matches)
        
        if df.empty:
            raise RuntimeError("Aucune donnée de match n'a pu être créée")
            
        # Réorganiser les colonnes
        columns_order = [
            'id', 'season', 'matchday', 'date', 'home_team', 'away_team',
            'home_goals', 'away_goals', 'result', 'home_position', 'away_position',
            'home_scored_and_conceded_goals', 'away_scored_and_conceded_goals',
            'detail_link'
        ]
        
        df = df.reindex(columns=columns_order)
        
        return df
    
    def clean_team_name(self, name: str) -> str:
        """Nettoie et normalise le nom d'une équipe"""
        # Mapping des noms d'équipes La Liga
        team_mapping = {
            'FC Barcelona': 'FC Barcelona',
            'Real Madrid': 'Real Madrid',
            'Atlético Madrid': 'Atlético Madrid',
            'Sevilla FC': 'Sevilla FC',
            'Valencia CF': 'Valencia CF',
            'Villarreal CF': 'Villarreal CF',
            'Athletic Club': 'Athletic Club',
            'Real Sociedad': 'Real Sociedad',
            'Real Betis': 'Real Betis',
            'RC Celta': 'RC Celta',
            'Espanyol Barcelona': 'Espanyol Barcelona',
            'Getafe CF': 'Getafe CF',
            'CA Osasuna': 'CA Osasuna',
            'RCD Mallorca': 'RCD Mallorca',
            'CD Alavés': 'CD Alavés',
            'Rayo Vallecano': 'Rayo Vallecano',
            'CD Leganés': 'CD Leganés',
            'Girona FC': 'Girona FC',
            'UD Las Palmas': 'UD Las Palmas',
            'Real Valladolid': 'Real Valladolid'
        }
        
        return team_mapping.get(name, name)
    
    def save_to_csv(self, df: pd.DataFrame, filename: Optional[str] = None, season: int = 2024, matchday: int = 1) -> str:
        """Sauvegarde le DataFrame en CSV"""
        if filename is None:
            filename = f"laliga_match_{season}_{matchday}.csv"
        
        # Créer le répertoire si nécessaire
        import os
        os.makedirs('laliga_data', exist_ok=True)
        
        filepath = os.path.join('laliga_data', filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        logger.info(f"Données sauvegardées dans: {filepath}")
        return filepath
    
    def scrape_full_season(self, season: int = 2024, start_matchday: int = 1, end_matchday: int = 38, delay: float = 2.0) -> List[str]:
        """
        Scrape toutes les journées d'une saison automatiquement
        
        Args:
            season: Année de début de la saison (ex: 2024 pour 2024/2025)
            start_matchday: Première journée à scraper
            end_matchday: Dernière journée à scraper (38 pour La Liga)
            delay: Délai en secondes entre chaque requête pour éviter la surcharge
        
        Returns:
            Liste des fichiers CSV créés
        """
        created_files = []
        failed_matchdays = []
        
        logger.info(f"🚀 Début du scraping automatique: saison {season}/{season+1}")
        logger.info(f"📅 Journées {start_matchday} à {end_matchday}")
        
        for matchday in range(start_matchday, end_matchday + 1):
            try:
                logger.info(f"🔄 Scraping journée {matchday}/{end_matchday}...")
                
                # Construire l'URL pour cette journée
                url = f"https://www.mondefootball.fr/calendrier/esp-primera-division-{season}-{season+1}-spieltag/{matchday}/"
                
                # Scraper la journée
                df = self.scrape_matchday(url, season, matchday)
                
                # Sauvegarder
                csv_file = self.save_to_csv(df, season=season, matchday=matchday)
                created_files.append(csv_file)
                
                logger.info(f"✅ Journée {matchday} terminée: {len(df)} matchs")
                
                # Délai entre les requêtes
                if matchday < end_matchday:
                    logger.info(f"⏳ Pause de {delay}s avant la prochaine journée...")
                    import time
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"❌ Erreur journée {matchday}: {e}")
                failed_matchdays.append(matchday)
                
                # Continuer avec la journée suivante
                continue
        
        # Résumé final
        logger.info(f"🏁 Scraping terminé!")
        logger.info(f"✅ Journées réussies: {len(created_files)}")
        logger.info(f"❌ Journées échouées: {len(failed_matchdays)}")
        
        if failed_matchdays:
            logger.warning(f"Journées échouées: {failed_matchdays}")
        
        return created_files
    
    def scrape_remaining_season(self, season: int = 2024, delay: float = 2.0) -> List[str]:
        """
        Scrape les journées restantes d'une saison (détecte automatiquement les fichiers existants)
        """
        import os
        
        # Vérifier quelles journées existent déjà
        existing_matchdays = set()
        data_dir = 'laliga_data'
        
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.startswith(f'laliga_match_{season}_') and filename.endswith('.csv'):
                    try:
                        matchday = int(filename.split('_')[3].split('.')[0])
                        existing_matchdays.add(matchday)
                    except (IndexError, ValueError):
                        continue
        
        if existing_matchdays:
            max_existing = max(existing_matchdays)
            logger.info(f"📁 Fichiers existants trouvés jusqu'à la journée {max_existing}")
            start_matchday = max_existing + 1
        else:
            logger.info(f"📁 Aucun fichier existant trouvé, début à la journée 1")
            start_matchday = 1
        
        if start_matchday > 38:
            logger.info(f"🎯 Toutes les journées de la saison {season}/{season+1} sont déjà scrapées!")
            return []
        
        return self.scrape_full_season(season, start_matchday, 38, delay)
    
    def get_matchday_8_data(self) -> Dict:
        """Retourne les données de la journée 8 (exemple)"""
        return {
            'matches': [
                {
                    'date': '2024-09-27',
                    'home_team': 'Real Valladolid',
                    'away_team': 'RCD Mallorca',
                    'home_goals': 1,
                    'away_goals': 2,
                    'time': '21:00'
                },
                {
                    'date': '2024-09-28',
                    'home_team': 'Getafe CF',
                    'away_team': 'CD Alavés',
                    'home_goals': 2,
                    'away_goals': 0,
                    'time': '14:00'
                },
                {
                    'date': '2024-09-28',
                    'home_team': 'Rayo Vallecano',
                    'away_team': 'CD Leganés',
                    'home_goals': 1,
                    'away_goals': 1,
                    'time': '16:15'
                },
                {
                    'date': '2024-09-28',
                    'home_team': 'Real Sociedad',
                    'away_team': 'Valencia CF',
                    'home_goals': 3,
                    'away_goals': 0,
                    'time': '18:30'
                },
                {
                    'date': '2024-09-28',
                    'home_team': 'CA Osasuna',
                    'away_team': 'FC Barcelona',
                    'home_goals': 4,
                    'away_goals': 2,
                    'time': '21:00'
                },
                {
                    'date': '2024-09-29',
                    'home_team': 'RC Celta',
                    'away_team': 'Girona FC',
                    'home_goals': 1,
                    'away_goals': 1,
                    'time': '14:00'
                },
                {
                    'date': '2024-09-29',
                    'home_team': 'Athletic Club',
                    'away_team': 'Sevilla FC',
                    'home_goals': 1,
                    'away_goals': 1,
                    'time': '16:15'
                },
                {
                    'date': '2024-09-29',
                    'home_team': 'Real Betis',
                    'away_team': 'Espanyol Barcelona',
                    'home_goals': 1,
                    'away_goals': 0,
                    'time': '18:30'
                },
                {
                    'date': '2024-09-29',
                    'home_team': 'Atlético Madrid',
                    'away_team': 'Real Madrid',
                    'home_goals': 1,
                    'away_goals': 1,
                    'time': '21:00'
                },
                {
                    'date': '2024-09-30',
                    'home_team': 'Villarreal CF',
                    'away_team': 'UD Las Palmas',
                    'home_goals': 3,
                    'away_goals': 1,
                    'time': '21:00'
                }
            ],
            'standings': {
                'FC Barcelona': {'position': 1, 'points': 21, 'goals_for': 25, 'goals_against': 9},
                'Real Madrid': {'position': 2, 'points': 18, 'goals_for': 17, 'goals_against': 6},
                'Villarreal CF': {'position': 3, 'points': 17, 'goals_for': 17, 'goals_against': 15},
                'Atlético Madrid': {'position': 4, 'points': 16, 'goals_for': 12, 'goals_against': 4},
                'Athletic Club': {'position': 5, 'points': 14, 'goals_for': 12, 'goals_against': 8},
                'RCD Mallorca': {'position': 6, 'points': 14, 'goals_for': 8, 'goals_against': 6},
                'CA Osasuna': {'position': 7, 'points': 14, 'goals_for': 12, 'goals_against': 13},
                'Real Betis': {'position': 8, 'points': 12, 'goals_for': 8, 'goals_against': 7},
                'Rayo Vallecano': {'position': 9, 'points': 10, 'goals_for': 9, 'goals_against': 8},
                'RC Celta': {'position': 10, 'points': 10, 'goals_for': 15, 'goals_against': 15},
                'CD Alavés': {'position': 11, 'points': 10, 'goals_for': 11, 'goals_against': 12},
                'Girona FC': {'position': 12, 'points': 9, 'goals_for': 9, 'goals_against': 11},
                'Sevilla FC': {'position': 13, 'points': 9, 'goals_for': 8, 'goals_against': 10},
                'Real Sociedad': {'position': 14, 'points': 8, 'goals_for': 6, 'goals_against': 7},
                'CD Leganés': {'position': 15, 'points': 7, 'goals_for': 5, 'goals_against': 9},
                'Getafe CF': {'position': 16, 'points': 7, 'goals_for': 5, 'goals_against': 6},
                'Espanyol Barcelona': {'position': 17, 'points': 7, 'goals_for': 7, 'goals_against': 12},
                'Real Valladolid': {'position': 18, 'points': 5, 'goals_for': 4, 'goals_against': 17},
                'Valencia CF': {'position': 19, 'points': 5, 'goals_for': 5, 'goals_against': 13},
                'UD Las Palmas': {'position': 20, 'points': 3, 'goals_for': 9, 'goals_against': 16}
            }
        }

def main():
    """Fonction principale avec automatisation des journées"""
    parser = argparse.ArgumentParser(description='Scraper La Liga MondeFootball.fr avec automatisation')
    
    # Groupe mutuellement exclusif pour les modes de scraping
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--url', type=str, help='URL de la journée à scraper (mode manuel)')
    group.add_argument('--auto-season', action='store_true', help='Scraper toute une saison automatiquement')
    group.add_argument('--auto-continue', action='store_true', help='Continuer le scraping depuis la dernière journée scrapée')
    group.add_argument('--auto-range', nargs=2, type=int, metavar=('START', 'END'), help='Scraper une plage de journées (ex: --auto-range 1 10)')
    
    parser.add_argument('--season', type=int, default=2024, help='Année de début de la saison (ex: 2024 pour 2024/2025)')
    parser.add_argument('--matchday', type=int, default=1, help='Numéro de la journée (mode manuel seulement)')
    parser.add_argument('--output', type=str, default=None, help='Nom du fichier de sortie (mode manuel seulement)')
    parser.add_argument('--delay', type=float, default=2.0, help='Délai en secondes entre les requêtes (mode auto)')
    
    args = parser.parse_args()
    
    scraper = LaLigaScraper()
    
    try:
        if args.url:
            # Mode manuel - scraper une seule journée
            logger.info("🎯 Mode manuel: scraping d'une journée")
            
            # Utiliser les données de fallback pour la journée 8 si disponible
            fallback_data = None
            if args.matchday == 8 and args.season == 2024:
                fallback_data = scraper.get_matchday_8_data()
            
            # Scraper la journée
            df = scraper.scrape_matchday(args.url, args.season, args.matchday, fallback_data)
            
            if not df.empty:
                # Sauvegarder en CSV
                csv_file = scraper.save_to_csv(df, args.output, args.season, args.matchday)
                
                # Afficher les résultats
                print("\n=== RÉSULTATS DU SCRAPING ===")
                print(df.to_string(index=False))
                print(f"\n=== STATISTIQUES ===")
                print(f"Fichier CSV créé: {csv_file}")
                print(f"Nombre de matchs: {len(df)}")
            else:
                print("❌ Aucune donnée extraite")
                
        elif args.auto_season:
            # Mode automatique - toute la saison
            logger.info("🚀 Mode automatique: scraping de toute la saison")
            created_files = scraper.scrape_full_season(args.season, delay=args.delay)
            
            print(f"\n=== RÉSULTATS DU SCRAPING AUTOMATIQUE ===")
            print(f"Saison: {args.season}/{args.season+1}")
            print(f"Fichiers créés: {len(created_files)}")
            for file in created_files:
                print(f"  ✅ {file}")
                
        elif args.auto_continue:
            # Mode automatique - continuer depuis la dernière journée
            logger.info("⏩ Mode automatique: continuation depuis la dernière journée")
            created_files = scraper.scrape_remaining_season(args.season, delay=args.delay)
            
            print(f"\n=== RÉSULTATS DU SCRAPING AUTOMATIQUE (CONTINUATION) ===")
            print(f"Saison: {args.season}/{args.season+1}")
            print(f"Nouveaux fichiers créés: {len(created_files)}")
            for file in created_files:
                print(f"  ✅ {file}")
                
        elif args.auto_range:
            # Mode automatique - plage spécifique
            start_matchday, end_matchday = args.auto_range
            logger.info(f"🎯 Mode automatique: scraping journées {start_matchday} à {end_matchday}")
            created_files = scraper.scrape_full_season(args.season, start_matchday, end_matchday, delay=args.delay)
            
            print(f"\n=== RÉSULTATS DU SCRAPING AUTOMATIQUE (PLAGE) ===")
            print(f"Saison: {args.season}/{args.season+1}")
            print(f"Journées: {start_matchday} à {end_matchday}")
            print(f"Fichiers créés: {len(created_files)}")
            for file in created_files:
                print(f"  ✅ {file}")
            
    except Exception as e:
        logger.error(f"Erreur lors du scraping: {e}")
        print(f"❌ Erreur: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main() 