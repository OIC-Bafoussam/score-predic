"""
Advanced Feature Builder for La Liga Dataset
Adds sophisticated features for match outcome prediction
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
from typing import Dict, List, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

class LaLigaFeatureBuilder:
    """
    Builds advanced features for La Liga match prediction
    """
    
    def __init__(self):
        self.team_stats_cache = {}
        
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
            'matches_played': len(team_matches),
            'points_per_match': points / max(len(team_matches), 1),
            'goals_per_match': goals_scored / max(len(team_matches), 1),
            'goals_conceded_per_match': goals_conceded / max(len(team_matches), 1)
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
        wins = 0
        draws = 0
        losses = 0
        
        for _, match in team_matches.iterrows():
            if venue == 'home':
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
            'matches_played': len(team_matches),
            'points_per_match': points / max(len(team_matches), 1),
            'win_rate': wins / max(len(team_matches), 1)
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
        
        total_matches = len(h2h_matches)
        
        return {
            'team1_wins': team1_wins,
            'team2_wins': team2_wins,
            'draws': draws,
            'team1_goals': team1_goals,
            'team2_goals': team2_goals,
            'matches_played': total_matches,
            'team1_win_rate': team1_wins / max(total_matches, 1),
            'team2_win_rate': team2_wins / max(total_matches, 1),
            'draw_rate': draws / max(total_matches, 1),
            'goals_per_match': (team1_goals + team2_goals) / max(total_matches, 1)
        }
    
    def calculate_league_position(self, df: pd.DataFrame, team: str, date: datetime) -> Dict:
        """Calculate team's league position and points at a given date"""
        # Get all matches played before the given date in the same season
        season_year = date.year if date.month >= 8 else date.year - 1
        season_matches = df[
            (df['date'] < date) & 
            (df['season'] == season_year)
        ]
        
        if season_matches.empty:
            return {'position': 20, 'points': 0, 'goal_difference': 0, 'matches_played': 0}
        
        # Calculate league table
        teams = set(season_matches['home_team'].unique()) | set(season_matches['away_team'].unique())
        league_table = []
        
        for team_name in teams:
            team_stats = self.calculate_team_form(df, team_name, date, n=100)  # All matches in season
            league_table.append({
                'team': team_name,
                'points': team_stats['points'],
                'goal_difference': team_stats['goal_difference'],
                'goals_scored': team_stats['goals_scored'],
                'matches_played': team_stats['matches_played']
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
                    'goal_difference': team_data['goal_difference'],
                    'matches_played': team_data['matches_played']
                }
        
        return {'position': 20, 'points': 0, 'goal_difference': 0, 'matches_played': 0}
    
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
    
    def calculate_momentum(self, df: pd.DataFrame, team: str, date: datetime, n: int = 3) -> Dict:
        """Calculate team momentum from last n matches"""
        recent_matches = df[
            ((df['home_team'] == team) | (df['away_team'] == team)) & 
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(n)
        
        if recent_matches.empty:
            return {'momentum_score': 0, 'recent_wins': 0, 'recent_form': 'Unknown'}
        
        momentum_score = 0
        wins = 0
        
        for _, match in recent_matches.iterrows():
            if match['home_team'] == team:
                if match['home_goals'] > match['away_goals']:
                    momentum_score += 3
                    wins += 1
                elif match['home_goals'] == match['away_goals']:
                    momentum_score += 1
            else:
                if match['away_goals'] > match['home_goals']:
                    momentum_score += 3
                    wins += 1
                elif match['away_goals'] == match['home_goals']:
                    momentum_score += 1
        
        # Categorize form
        if momentum_score >= 7:
            form = 'Excellent'
        elif momentum_score >= 5:
            form = 'Good'
        elif momentum_score >= 3:
            form = 'Average'
        else:
            form = 'Poor'
        
        return {
            'momentum_score': momentum_score,
            'recent_wins': wins,
            'recent_form': form,
            'momentum_per_match': momentum_score / len(recent_matches)
        }
    
    def calculate_scoring_patterns(self, df: pd.DataFrame, team: str, date: datetime, n: int = 10) -> Dict:
        """Calculate team's scoring patterns"""
        recent_matches = df[
            ((df['home_team'] == team) | (df['away_team'] == team)) & 
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(n)
        
        if recent_matches.empty:
            return {
                'avg_goals_scored': 0,
                'avg_goals_conceded': 0,
                'clean_sheets': 0,
                'failed_to_score': 0,
                'high_scoring_games': 0
            }
        
        goals_scored = []
        goals_conceded = []
        clean_sheets = 0
        failed_to_score = 0
        high_scoring_games = 0
        
        for _, match in recent_matches.iterrows():
            if match['home_team'] == team:
                gs = match['home_goals']
                gc = match['away_goals']
            else:
                gs = match['away_goals']
                gc = match['home_goals']
            
            goals_scored.append(gs)
            goals_conceded.append(gc)
            
            if gc == 0:
                clean_sheets += 1
            if gs == 0:
                failed_to_score += 1
            if gs + gc >= 3:
                high_scoring_games += 1
        
        return {
            'avg_goals_scored': np.mean(goals_scored),
            'avg_goals_conceded': np.mean(goals_conceded),
            'clean_sheets': clean_sheets,
            'failed_to_score': failed_to_score,
            'high_scoring_games': high_scoring_games,
            'clean_sheet_rate': clean_sheets / len(recent_matches),
            'btts_rate': (len(recent_matches) - failed_to_score) / len(recent_matches)
        }
    
    def calculate_strength_of_schedule(self, df: pd.DataFrame, team: str, date: datetime, n: int = 5) -> Dict:
        """Calculate strength of recent opponents"""
        recent_matches = df[
            ((df['home_team'] == team) | (df['away_team'] == team)) & 
            (df['date'] < date)
        ].sort_values('date', ascending=False).head(n)
        
        if recent_matches.empty:
            return {'avg_opponent_position': 10, 'avg_opponent_points': 0}
        
        opponent_positions = []
        opponent_points = []
        
        for _, match in recent_matches.iterrows():
            opponent = match['away_team'] if match['home_team'] == team else match['home_team']
            
            # Get opponent's position at the time of the match
            opp_stats = self.calculate_league_position(df, opponent, match['date'])
            opponent_positions.append(opp_stats['position'])
            opponent_points.append(opp_stats['points'])
        
        return {
            'avg_opponent_position': np.mean(opponent_positions),
            'avg_opponent_points': np.mean(opponent_points),
            'strength_of_schedule': 21 - np.mean(opponent_positions)  # Lower position = stronger opponent
        }
    
    def build_advanced_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build all advanced features for the dataset"""
        logger.info("Building advanced features...")
        
        features_list = []
        total_matches = len(df)
        
        for idx, match in df.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processing match {idx+1}/{total_matches}")
            
            date = match['date']
            home_team = match['home_team']
            away_team = match['away_team']
            
            # Skip matches without enough historical data
            if idx < 20:  # Need some matches for form calculation
                continue
            
            try:
                # Basic match info
                features = {
                    'match_id': match.get('match_id', idx),
                    'date': date,
                    'season': match['season'],
                    'matchday': match.get('matchday', 1),
                    'home_team': home_team,
                    'away_team': away_team,
                    'result': match['result'],
                    'home_goals': match['home_goals'],
                    'away_goals': match['away_goals'],
                    'goal_difference': match['home_goals'] - match['away_goals'],
                    'total_goals': match['home_goals'] + match['away_goals']
                }
                
                # Team form features (last 5 matches)
                home_form = self.calculate_team_form(df, home_team, date, n=5)
                away_form = self.calculate_team_form(df, away_team, date, n=5)
                
                for key, value in home_form.items():
                    features[f'home_form_{key}'] = value
                for key, value in away_form.items():
                    features[f'away_form_{key}'] = value
                
                # Home/Away specific form
                home_home_form = self.calculate_home_away_form(df, home_team, date, 'home', n=5)
                away_away_form = self.calculate_home_away_form(df, away_team, date, 'away', n=5)
                
                for key, value in home_home_form.items():
                    features[f'home_home_{key}'] = value
                for key, value in away_away_form.items():
                    features[f'away_away_{key}'] = value
                
                # Head-to-head
                h2h = self.calculate_head_to_head(df, home_team, away_team, date, n=10)
                for key, value in h2h.items():
                    features[f'h2h_{key}'] = value
                
                # League positions
                home_position = self.calculate_league_position(df, home_team, date)
                away_position = self.calculate_league_position(df, away_team, date)
                
                for key, value in home_position.items():
                    features[f'home_league_{key}'] = value
                for key, value in away_position.items():
                    features[f'away_league_{key}'] = value
                
                # Rest days
                features['home_rest_days'] = self.calculate_rest_days(df, home_team, date)
                features['away_rest_days'] = self.calculate_rest_days(df, away_team, date)
                features['rest_difference'] = features['home_rest_days'] - features['away_rest_days']
                
                # Momentum
                home_momentum = self.calculate_momentum(df, home_team, date, n=3)
                away_momentum = self.calculate_momentum(df, away_team, date, n=3)
                
                for key, value in home_momentum.items():
                    if key != 'recent_form':  # Skip non-numeric
                        features[f'home_momentum_{key}'] = value
                for key, value in away_momentum.items():
                    if key != 'recent_form':  # Skip non-numeric
                        features[f'away_momentum_{key}'] = value
                
                # Scoring patterns
                home_scoring = self.calculate_scoring_patterns(df, home_team, date, n=10)
                away_scoring = self.calculate_scoring_patterns(df, away_team, date, n=10)
                
                for key, value in home_scoring.items():
                    features[f'home_scoring_{key}'] = value
                for key, value in away_scoring.items():
                    features[f'away_scoring_{key}'] = value
                
                # Strength of schedule
                home_sos = self.calculate_strength_of_schedule(df, home_team, date, n=5)
                away_sos = self.calculate_strength_of_schedule(df, away_team, date, n=5)
                
                for key, value in home_sos.items():
                    features[f'home_sos_{key}'] = value
                for key, value in away_sos.items():
                    features[f'away_sos_{key}'] = value
                
                # Derived features
                features['position_difference'] = home_position['position'] - away_position['position']
                features['points_difference'] = home_position['points'] - away_position['points']
                features['form_points_difference'] = home_form['points'] - away_form['points']
                features['form_goal_diff_difference'] = home_form['goal_difference'] - away_form['goal_difference']
                features['momentum_difference'] = home_momentum['momentum_score'] - away_momentum['momentum_score']
                features['home_advantage'] = 1  # Always 1 for home team
                
                # Time-based features
                features['month'] = date.month
                features['day_of_week'] = date.weekday()
                features['is_weekend'] = 1 if date.weekday() >= 5 else 0
                
                features_list.append(features)
                
            except Exception as e:
                logger.warning(f"Error processing match {idx}: {e}")
                continue
        
        return pd.DataFrame(features_list)
    
    def save_features_info(self, df: pd.DataFrame, filename: str = 'features_info.txt'):
        """Save information about all features"""
        with open(filename, 'w') as f:
            f.write("LA LIGA DATASET FEATURES\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Total Features: {len(df.columns)}\n")
            f.write(f"Total Matches: {len(df)}\n\n")
            
            f.write("FEATURE CATEGORIES:\n")
            f.write("-" * 30 + "\n")
            
            categories = {
                'Basic Info': ['match_id', 'date', 'season', 'matchday', 'home_team', 'away_team', 'result'],
                'Match Stats': ['home_goals', 'away_goals', 'goal_difference', 'total_goals'],
                'Team Form': [col for col in df.columns if 'form_' in col],
                'Home/Away Form': [col for col in df.columns if ('home_home_' in col or 'away_away_' in col)],
                'Head-to-Head': [col for col in df.columns if 'h2h_' in col],
                'League Position': [col for col in df.columns if 'league_' in col],
                'Rest/Recovery': [col for col in df.columns if 'rest' in col],
                'Momentum': [col for col in df.columns if 'momentum_' in col],
                'Scoring Patterns': [col for col in df.columns if 'scoring_' in col],
                'Strength of Schedule': [col for col in df.columns if 'sos_' in col],
                'Derived Features': ['position_difference', 'points_difference', 'form_points_difference', 
                                   'form_goal_diff_difference', 'momentum_difference', 'home_advantage'],
                'Time Features': ['month', 'day_of_week', 'is_weekend']
            }
            
            for category, features in categories.items():
                f.write(f"\n{category} ({len(features)} features):\n")
                for feature in features:
                    if feature in df.columns:
                        f.write(f"  - {feature}\n")
            
            f.write(f"\nAll Features ({len(df.columns)}):\n")
            f.write("-" * 30 + "\n")
            for i, col in enumerate(df.columns, 1):
                f.write(f"{i:3d}. {col}\n")


def main():
    """Main function to build advanced features"""
    # Load the scraped data
    try:
        df = pd.read_csv('laliga_dataset_mondefootball_2014_2023.csv')
        logger.info(f"Loaded {len(df)} matches from scraped data")
    except FileNotFoundError:
        logger.error("Scraped data file not found. Please run the scraper first.")
        return
    
    # Convert date column
    df['date'] = pd.to_datetime(df['date'])
    
    # Build features
    feature_builder = LaLigaFeatureBuilder()
    features_df = feature_builder.build_advanced_features(df)
    
    # Save dataset with features
    output_file = 'laliga_dataset_with_features_2014_2023.csv'
    features_df.to_csv(output_file, index=False)
    logger.info(f"Dataset with features saved to {output_file}")
    
    # Save features info
    feature_builder.save_features_info(features_df)
    logger.info("Features information saved to features_info.txt")
    
    # Print summary
    print("\n" + "="*60)
    print("LA LIGA DATASET WITH ADVANCED FEATURES")
    print("="*60)
    print(f"Total matches: {len(features_df)}")
    print(f"Total features: {len(features_df.columns)}")
    print(f"Date range: {features_df['date'].min()} to {features_df['date'].max()}")
    
    print("\nResult distribution:")
    print(features_df['result'].value_counts())
    
    print("\nSample features:")
    print(features_df.head())
    
    return features_df


if __name__ == "__main__":
    dataset = main() 