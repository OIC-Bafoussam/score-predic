# La Liga Match Prediction Dataset Builder (2014-2023)

This project builds a comprehensive dataset for predicting La Liga match outcomes (Win/Loss/Draw) using historical data from 2014 to 2023. The dataset includes advanced features based on team form, league position, head-to-head records, and other contextual factors.

## Features

The dataset includes **40+ features** designed for machine learning prediction:

### Basic Match Information
- `match_id`: Unique match identifier
- `date`: Match date and time
- `season`: Season year
- `matchday`: Matchday number
- `home_team`: Home team name
- `away_team`: Away team name
- `result`: Match outcome (HomeWin/AwayWin/Draw)
- `home_goals`: Goals scored by home team
- `away_goals`: Goals scored by away team

### Team Form Features (Last 5 matches)
- **Home Team Form**: Points, goals scored/conceded, wins/draws/losses
- **Away Team Form**: Points, goals scored/conceded, wins/draws/losses
- **Home/Away Specific Form**: Performance at home/away venues

### League Position Features
- `home_position`: Home team's league position before match
- `away_position`: Away team's league position before match
- `home_points`: Home team's total points before match
- `away_points`: Away team's total points before match
- `position_difference`: Difference in league positions
- `points_difference`: Difference in total points

### Head-to-Head Features
- `h2h_home_wins`: Home team wins in last 10 H2H matches
- `h2h_away_wins`: Away team wins in last 10 H2H matches
- `h2h_draws`: Draws in last 10 H2H matches
- `h2h_home_goals`: Home team goals in H2H matches
- `h2h_away_goals`: Away team goals in H2H matches

### Contextual Features
- `home_rest_days`: Days since home team's last match
- `away_rest_days`: Days since away team's last match
- `rest_difference`: Difference in rest days
- `home_advantage`: Home advantage indicator (always 1)

### Derived Features
- `form_points_difference`: Difference in recent form points
- `form_goal_diff_difference`: Difference in recent goal difference

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd score-predic
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Building the Dataset

Run the main script to build the complete dataset:

```bash
python laliga_dataset_builder.py
```

This will:
1. Fetch all La Liga matches from 2014-2023 using the OpenLigaDB API
2. Calculate advanced features for each match
3. Save the dataset as `laliga_dataset_2014_2023.csv`

### Customizing the Dataset

You can modify the dataset builder by:

```python
from laliga_dataset_builder import LaLigaDatasetBuilder

# Initialize builder
builder = LaLigaDatasetBuilder()

# Build dataset for specific years
dataset = builder.build_complete_dataset(start_year=2018, end_year=2022)

# Save with custom name
dataset.to_csv('custom_laliga_dataset.csv', index=False)
```

## Dataset Structure

The final dataset contains approximately **3,000+ matches** with **40+ features** each.

### Sample Data
```
match_id | date       | home_team    | away_team   | result   | home_form_points | away_form_points | ...
123456   | 2020-01-15 | Real Madrid  | Barcelona   | HomeWin  | 12              | 9                | ...
123457   | 2020-01-18 | Atletico     | Valencia    | Draw     | 8               | 7                | ...
```

### Result Distribution
- **HomeWin**: ~45% of matches
- **Draw**: ~25% of matches  
- **AwayWin**: ~30% of matches

## Machine Learning Usage

The dataset is ready for machine learning. Example usage:

```python
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# Load dataset
df = pd.read_csv('laliga_dataset_2014_2023.csv')

# Prepare features and target
feature_columns = [col for col in df.columns if col not in ['match_id', 'date', 'home_team', 'away_team', 'result', 'home_goals', 'away_goals']]
X = df[feature_columns]
y = df['result']

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
print(classification_report(y_test, y_pred))
```

## Data Source

Data is sourced from [OpenLigaDB](https://www.openligadb.de/), a free API providing football match data.

## Key Features for Prediction

Based on football analytics research, the most important features for prediction are:

1. **Recent Form** (last 5 matches points)
2. **League Position Difference**
3. **Head-to-Head Record**
4. **Home/Away Specific Form**
5. **Goal Difference Trends**
6. **Rest Days Between Matches**

## Limitations

- Data availability depends on OpenLigaDB API
- Some early season matches may have limited historical features
- Weather, injuries, and other external factors are not included
- API rate limiting may slow down data collection

## Future Enhancements

- Add player-level statistics
- Include transfer market values
- Add weather data
- Implement real-time prediction pipeline
- Add more leagues (Premier League, Bundesliga, etc.)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source and available under the MIT License.

## Acknowledgments

- OpenLigaDB for providing free football data
- Football analytics research community for feature engineering insights
- La Liga for the exciting matches that make this analysis possible! 