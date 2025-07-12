import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

class LaLigaDatasetBuilder:
    """
    Build a comprehensive La Liga dataset for match outcome prediction
    from 2014 to 2023 using OpenLigaDB API
    """
    
    def __init__(self):
        self.base_url = "https://api.openligadb.de"
        self.league_code = "bl1"  # We'll need to find the correct code for La Liga
        self.all_matches = []
        self.teams_mapping = {}
        
    def fetch_available_leagues(self) -> List[Dict]:
        """Fetch all available leagues to find La Liga code"""
        try:
            url = f"{self.base_url}/getavailableleagues"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            print(f"Error fetching leagues: {e}")
            return []
    
    def find_laliga_code(self) -> Optional[str]:
        """Find the correct league code for La Liga"""
        leagues = self.fetch_available_leagues()
        for league in leagues:
            if 'liga' in league.get('leagueName', '').lower() and 'spain' in league.get('leagueName', '').lower():
                return league.get('leagueShortcut')
        # If not found, let's try common codes
        possible_codes = ['es1', 'laliga', 'primera', 'liga']
        return possible_codes[0]  # Default to 'es1'
    
    def fetch_season_data(self, season: int, league_code: str) -> List[Dict]:
        """Fetch all matches for a specific season"""
        season_matches = []
        
        # Try to get all matches for the season
        try:
            url = f"{self.base_url}/getmatchdata/{league_code}/{season}"
            response = requests.get(url)
            if response.status_code == 200:
                matches = response.json()
                if matches:
                    return matches
        except Exception as e:
            print(f"Error fetching season {season}: {e}")
        
        # If that fails, try matchday by matchday
        for matchday in range(1, 39):  # La Liga has 38 matchdays
            try:
                url = f"{self.base_url}/getmatchdata/{league_code}/{season}/{matchday}"
                response = requests.get(url)
                if response.status_code == 200:
                    matches = response.json()
                    if matches:
                        season_matches.extend(matches)
                time.sleep(0.1)  # Rate limiting
            except Exception as e:
                print(f"Error fetching season {season}, matchday {matchday}: {e}")
                continue
        
        return season_matches
    
    def collect_all_matches(self, start_year: int = 2014, end_year: int = 2023) -> None:
        """Collect all matches from start_year to end_year"""
        print("Finding La Liga league code...")
        league_code = self.find_laliga_code()
        print(f"Using league code: {league_code}")
        
        print(f"Collecting matches from {start_year} to {end_year}...")
        
        for season in range(start_year, end_year + 1):
            print(f"Fetching season {season}/{season+1}...")
            season_matches = self.fetch_season_data(season, league_code)
            
            if season_matches:
                self.all_matches.extend(season_matches)
                print(f"  Found {len(season_matches)} matches")
            else:
                print(f"  No matches found for season {season}")
            
            time.sleep(0.5)  # Rate limiting
    
    def process_raw_matches(self) -> pd.DataFrame:
        """Process raw match data into a clean DataFrame"""
        if not self.all_matches:
            print("No matches to process!")
            return pd.DataFrame()
        
        processed_matches = []
        
        for match in self.all_matches:
            try:
                # Extract basic match info
                match_data = {
                    'match_id': match.get('matchID'),
                    'date': match.get('matchDateTime'),
                    'matchday': match.get('group', {}).get('groupOrderID'),
                    'season': match.get('leagueSeason'),
                    'home_team': match.get('team1', {}).get('teamName'),
                    'away_team': match.get('team2', {}).get('teamName'),
                    'home_team_id': match.get('team1', {}).get('teamId'),
                    'away_team_id': match.get('team2', {}).get('teamId'),
                    'is_finished': match.get('matchIsFinished', False)
                }
                
                # Extract final result
                if match.get('matchResults'):
                    for result in match['matchResults']:
                        if result.get('resultTypeID') == 2:  # Final result
                            match_data['home_goals'] = result.get('pointsTeam1', 0)
                            match_data['away_goals'] = result.get('pointsTeam2', 0)
                            break
                
                # Only include finished matches with valid results
                if (match_data['is_finished'] and 
                    'home_goals' in match_data and 
                    'away_goals' in match_data):
                    processed_matches.append(match_data)
                    
            except Exception as e:
                print(f"Error processing match: {e}")
                continue
        
        df = pd.DataFrame(processed_matches)
        
        if not df.empty:
            # Convert date column
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # Create result column
            df['result'] = df.apply(self._get_match_result, axis=1)
            
        return df
    
    def _get_match_result(self, row) -> str:
        """Get match result from home team perspective"""
        if row['home_goals'] > row['away_goals']:
            return 'HomeWin'
        elif row['home_goals'] < row['away_goals']:
            return 'AwayWin'
        else:
            return 'Draw'
    
    def calculate_team_form(self, df: pd.DataFrame, team: str, date: datetime, n: int = 5) -> Dict:
        """Calculate team form (points, goals) from last n matches"""
        # Get matches played by the team before the given date
        team_matches = df[
            ((df['home_team'] == team) | (df['away_team'] == team)) & 
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(n)
        
        points = 0
        goals_scored = 0
        goals_conceded = 0
        wins = 0
        draws = 0
        losses = 0
        
        for _, match in team_matches.iterrows():
            if match['home_team'] == team:
                # Team played at home
                goals_scored += match['home_goals']
                goals_conceded += match['away_goals']
                
                if match['home_goals'] > match['away_goals']:
                    points += 3
                    wins += 1
                elif match['home_goals'] == match['away_goals']:
                    points += 1
                    draws += 1
                else:
                    losses += 1
            else:
                # Team played away
                goals_scored += match['away_goals']
                goals_conceded += match['home_goals']
                
                if match['away_goals'] > match['home_goals']:
                    points += 3
                    wins += 1
                elif match['away_goals'] == match['home_goals']:
                    points += 1
                    draws += 1
                else:
                    losses += 1
        
        return {
            'points': points,
            'goals_scored': goals_scored,
            'goals_conceded': goals_conceded,
            'goal_difference': goals_scored - goals_conceded,
            'wins': wins,
            'draws': draws,
            'losses': losses,
            'matches_played': len(team_matches)
        }
    
    def calculate_home_away_form(self, df: pd.DataFrame, team: str, date: datetime, 
                                venue: str, n: int = 5) -> Dict:
        """Calculate team's home or away form specifically"""
        if venue == 'home':
            team_matches = df[
                (df['home_team'] == team) & (df['date'] < date)
            ].sort_values('date', ascending=False).head(n)
        else:
            team_matches = df[
                (df['away_team'] == team) & (df['date'] < date)
            ].sort_values('date', ascending=False).head(n)
        
        points = 0
        goals_scored = 0
        goals_conceded = 0
        
        for _, match in team_matches.iterrows():
            if venue == 'home':
                goals_scored += match['home_goals']
                goals_conceded += match['away_goals']
                
                if match['home_goals'] > match['away_goals']:
                    points += 3
                elif match['home_goals'] == match['away_goals']:
                    points += 1
            else:
                goals_scored += match['away_goals']
                goals_conceded += match['home_goals']
                
                if match['away_goals'] > match['home_goals']:
                    points += 3
                elif match['away_goals'] == match['home_goals']:
                    points += 1
        
        return {
            'points': points,
            'goals_scored': goals_scored,
            'goals_conceded': goals_conceded,
            'matches_played': len(team_matches)
        }
    
    def calculate_head_to_head(self, df: pd.DataFrame, team1: str, team2: str, 
                              date: datetime, n: int = 10) -> Dict:
        """Calculate head-to-head record between two teams"""
        h2h_matches = df[
            (((df['home_team'] == team1) & (df['away_team'] == team2)) |
             ((df['home_team'] == team2) & (df['away_team'] == team1))) &
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(n)
        
        team1_wins = 0
        team2_wins = 0
        draws = 0
        team1_goals = 0
        team2_goals = 0
        
        for _, match in h2h_matches.iterrows():
            if match['home_team'] == team1:
                team1_goals += match['home_goals']
                team2_goals += match['away_goals']
                
                if match['home_goals'] > match['away_goals']:
                    team1_wins += 1
                elif match['home_goals'] == match['away_goals']:
                    draws += 1
                else:
                    team2_wins += 1
            else:
                team1_goals += match['away_goals']
                team2_goals += match['home_goals']
                
                if match['away_goals'] > match['home_goals']:
                    team1_wins += 1
                elif match['away_goals'] == match['home_goals']:
                    draws += 1
                else:
                    team2_wins += 1
        
        return {
            'team1_wins': team1_wins,
            'team2_wins': team2_wins,
            'draws': draws,
            'team1_goals': team1_goals,
            'team2_goals': team2_goals,
            'matches_played': len(h2h_matches)
        }
    
    def calculate_league_position(self, df: pd.DataFrame, team: str, date: datetime) -> Dict:
        """Calculate team's league position and points at a given date"""
        # Get all matches played before the given date in the same season
        season_matches = df[
            (df['date'] < date) & 
            (df['season'] == df[df['date'] <= date]['season'].iloc[-1] if len(df[df['date'] <= date]) > 0 else 2014)
        ]
        
        # Calculate league table
        teams = set(season_matches['home_team'].unique()) | set(season_matches['away_team'].unique())
        league_table = []
        
        for team_name in teams:
            team_stats = self.calculate_team_form(df, team_name, date, n=100)  # All matches
            league_table.append({
                'team': team_name,
                'points': team_stats['points'],
                'goal_difference': team_stats['goal_difference'],
                'goals_scored': team_stats['goals_scored']
            })
        
        # Sort by points, then goal difference, then goals scored
        league_table.sort(key=lambda x: (x['points'], x['goal_difference'], x['goals_scored']), 
                         reverse=True)
        
        # Find team position
        for i, team_data in enumerate(league_table):
            if team_data['team'] == team:
                return {
                    'position': i + 1,
                    'points': team_data['points'],
                    'goal_difference': team_data['goal_difference']
                }
        
        return {'position': 20, 'points': 0, 'goal_difference': 0}
    
    def calculate_rest_days(self, df: pd.DataFrame, team: str, date: datetime) -> int:
        """Calculate days since team's last match"""
        last_match = df[
            ((df['home_team'] == team) | (df['away_team'] == team)) & 
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(1)
        
        if not last_match.empty:
            days_diff = (date - last_match.iloc[0]['date']).days
            return days_diff
        
        return 0
    
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build all features for the dataset"""
        print("Building features...")
        
        features_list = []
        total_matches = len(df)
        
        for idx, match in df.iterrows():
            if idx % 100 == 0:
                print(f"Processing match {idx+1}/{total_matches}")
            
            date = match['date']
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Skip matches without enough historical data
            if idx < 50:  # Need some matches for form calculation
                continue
            
            try:
                # Team form features
                home_form = self.calculate_team_form(df, home_team, date, n=5)
                away_form = self.calculate_team_form(df, away_team, date, n=5)
                
                # Home/Away specific form
                home_home_form = self.calculate_home_away_form(df, home_team, date, 'home', n=5)
                away_away_form = self.calculate_home_away_form(df, away_team, date, 'away', n=5)
                
                # Head-to-head
                h2h = self.calculate_head_to_head(df, home_team, away_team, date, n=10)
                
                # League positions
                home_position = self.calculate_league_position(df, home_team, date)
                away_position = self.calculate_league_position(df, away_team, date)
                
                # Rest days
                home_rest = self.calculate_rest_days(df, home_team, date)
                away_rest = self.calculate_rest_days(df, away_team, date)
                
                # Build feature row
                feature_row = {
                    # Basic match info
                    'match_id': match['match_id'],
                    'date': date,
                    'season': match['season'],
                    'matchday': match['matchday'],
                    'home_team': home_team,
                    'away_team': away_team,
                    
                    # Target variable
                    'result': match['result'],
                    'home_goals': match['home_goals'],
                    'away_goals': match['away_goals'],
                    
                    # Home team features
                    'home_form_points': home_form['points'],
                    'home_form_goals_scored': home_form['goals_scored'],
                    'home_form_goals_conceded': home_form['goals_conceded'],
                    'home_form_goal_diff': home_form['goal_difference'],
                    'home_form_wins': home_form['wins'],
                    'home_form_draws': home_form['draws'],
                    'home_form_losses': home_form['losses'],
                    
                    # Away team features
                    'away_form_points': away_form['points'],
                    'away_form_goals_scored': away_form['goals_scored'],
                    'away_form_goals_conceded': away_form['goals_conceded'],
                    'away_form_goal_diff': away_form['goal_difference'],
                    'away_form_wins': away_form['wins'],
                    'away_form_draws': away_form['draws'],
                    'away_form_losses': away_form['losses'],
                    
                    # Home/Away specific form
                    'home_home_form_points': home_home_form['points'],
                    'home_home_form_goals_scored': home_home_form['goals_scored'],
                    'home_home_form_goals_conceded': home_home_form['goals_conceded'],
                    
                    'away_away_form_points': away_away_form['points'],
                    'away_away_form_goals_scored': away_away_form['goals_scored'],
                    'away_away_form_goals_conceded': away_away_form['goals_conceded'],
                    
                    # League position features
                    'home_position': home_position['position'],
                    'away_position': away_position['position'],
                    'home_points': home_position['points'],
                    'away_points': away_position['points'],
                    'position_difference': home_position['position'] - away_position['position'],
                    'points_difference': home_position['points'] - away_position['points'],
                    
                    # Head-to-head features
                    'h2h_home_wins': h2h['team1_wins'],
                    'h2h_away_wins': h2h['team2_wins'],
                    'h2h_draws': h2h['draws'],
                    'h2h_home_goals': h2h['team1_goals'],
                    'h2h_away_goals': h2h['team2_goals'],
                    
                    # Rest days
                    'home_rest_days': home_rest,
                    'away_rest_days': away_rest,
                    'rest_difference': home_rest - away_rest,
                    
                    # Derived features
                    'form_points_difference': home_form['points'] - away_form['points'],
                    'form_goal_diff_difference': home_form['goal_difference'] - away_form['goal_difference'],
                    'home_advantage': 1,  # Always 1 for home team
                }
                
                features_list.append(feature_row)
                
            except Exception as e:
                print(f"Error processing match {idx}: {e}")
                continue
        
        return pd.DataFrame(features_list)
    
    def build_complete_dataset(self, start_year: int = 2014, end_year: int = 2023) -> pd.DataFrame:
        """Build the complete dataset with all features"""
        print("Starting La Liga dataset construction...")
        
        # Step 1: Collect all matches
        self.collect_all_matches(start_year, end_year)
        
        # Step 2: Process raw matches
        print("Processing raw match data...")
        df_matches = self.process_raw_matches()
        
        if df_matches.empty:
            print("No matches found! Check the league code.")
            return pd.DataFrame()
        
        print(f"Found {len(df_matches)} matches")
        print(f"Date range: {df_matches['date'].min()} to {df_matches['date'].max()}")
        print(f"Teams: {sorted(df_matches['home_team'].unique())}")
        
        # Step 3: Build features
        df_features = self.build_features(df_matches)
        
        print(f"Built dataset with {len(df_features)} matches and {len(df_features.columns)} features")
        
        return df_features


def main():
    """Main function to build the dataset"""
    builder = LaLigaDatasetBuilder()
    
    # Build the dataset
    dataset = builder.build_complete_dataset(start_year=2014, end_year=2023)
    
    if not dataset.empty:
        # Save the dataset
        dataset.to_csv('laliga_dataset_2014_2023.csv', index=False)
        print(f"Dataset saved to 'laliga_dataset_2014_2023.csv'")
        
        # Print dataset info
        print("\nDataset Summary:")
        print(f"Total matches: {len(dataset)}")
        print(f"Features: {len(dataset.columns)}")
        print(f"Date range: {dataset['date'].min()} to {dataset['date'].max()}")
        print(f"Seasons: {sorted(dataset['season'].unique())}")
        
        # Result distribution
        print("\nResult distribution:")
        print(dataset['result'].value_counts())
        
        # Sample of features
        print("\nSample features:")
        print(dataset.head())
        
        # Feature list
        print("\nAll features:")
        for i, col in enumerate(dataset.columns):
            print(f"{i+1:2d}. {col}")
    
    else:
        print("Failed to build dataset. Please check the league code and API availability.")


if __name__ == "__main__":
    main() 