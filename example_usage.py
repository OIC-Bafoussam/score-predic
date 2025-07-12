"""
Example usage of the La Liga dataset for machine learning prediction
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_explore_dataset(filename='laliga_dataset_2014_2023.csv'):
    """Load and explore the La Liga dataset"""
    print("Loading dataset...")
    df = pd.read_csv(filename)
    
    print(f"\nDataset shape: {df.shape}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    # Result distribution
    print("\nResult distribution:")
    result_counts = df['result'].value_counts()
    print(result_counts)
    print(f"Percentages: {result_counts / len(df) * 100}")
    
    # Teams in dataset
    teams = sorted(set(df['home_team'].unique()) | set(df['away_team'].unique()))
    print(f"\nTeams in dataset ({len(teams)}): {teams}")
    
    # Missing values
    print(f"\nMissing values: {df.isnull().sum().sum()}")
    
    return df

def prepare_features(df):
    """Prepare features for machine learning"""
    print("Preparing features...")
    
    # Define feature columns (exclude non-predictive columns)
    exclude_columns = ['match_id', 'date', 'home_team', 'away_team', 'result', 
                      'home_goals', 'away_goals', 'season']
    
    feature_columns = [col for col in df.columns if col not in exclude_columns]
    
    print(f"Using {len(feature_columns)} features:")
    for i, col in enumerate(feature_columns, 1):
        print(f"{i:2d}. {col}")
    
    X = df[feature_columns]
    y = df['result']
    
    # Handle any missing values
    X = X.fillna(0)
    
    return X, y, feature_columns

def train_and_evaluate_models(X, y):
    """Train and evaluate multiple models"""
    print("\nTraining and evaluating models...")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Scale features for logistic regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Define models
    models = {
        'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
        'Gradient Boosting': GradientBoostingClassifier(n_estimators=100, random_state=42),
        'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000)
    }
    
    results = {}
    
    for name, model in models.items():
        print(f"\n--- {name} ---")
        
        # Use scaled data for logistic regression
        if name == 'Logistic Regression':
            X_train_model = X_train_scaled
            X_test_model = X_test_scaled
        else:
            X_train_model = X_train
            X_test_model = X_test
        
        # Train model
        model.fit(X_train_model, y_train)
        
        # Cross-validation score
        cv_scores = cross_val_score(model, X_train_model, y_train, cv=5, scoring='accuracy')
        print(f"Cross-validation accuracy: {cv_scores.mean():.3f} (+/- {cv_scores.std() * 2:.3f})")
        
        # Test set predictions
        y_pred = model.predict(X_test_model)
        
        # Classification report
        print(f"\nClassification Report:")
        print(classification_report(y_test, y_pred))
        
        # Store results
        results[name] = {
            'model': model,
            'cv_score': cv_scores.mean(),
            'y_pred': y_pred,
            'scaler': scaler if name == 'Logistic Regression' else None
        }
    
    return results, X_test, y_test

def analyze_feature_importance(model, feature_columns):
    """Analyze feature importance for tree-based models"""
    if hasattr(model, 'feature_importances_'):
        print("\nTop 15 Most Important Features:")
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(feature_importance.head(15))
        
        # Plot feature importance
        plt.figure(figsize=(10, 8))
        sns.barplot(data=feature_importance.head(15), x='importance', y='feature')
        plt.title('Top 15 Feature Importances')
        plt.xlabel('Importance')
        plt.tight_layout()
        plt.show()
        
        return feature_importance
    else:
        print("Model doesn't have feature_importances_ attribute")
        return None

def plot_confusion_matrix(y_true, y_pred, title):
    """Plot confusion matrix"""
    cm = confusion_matrix(y_true, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=['AwayWin', 'Draw', 'HomeWin'],
                yticklabels=['AwayWin', 'Draw', 'HomeWin'])
    plt.title(f'Confusion Matrix - {title}')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.tight_layout()
    plt.show()

def predict_match_outcome(model, scaler, feature_columns, match_features):
    """Predict outcome for a single match"""
    # Convert to DataFrame
    match_df = pd.DataFrame([match_features], columns=feature_columns)
    
    # Scale if needed
    if scaler:
        match_df_scaled = scaler.transform(match_df)
        prediction = model.predict(match_df_scaled)[0]
        probabilities = model.predict_proba(match_df_scaled)[0]
    else:
        prediction = model.predict(match_df)[0]
        probabilities = model.predict_proba(match_df)[0]
    
    # Get class names
    classes = model.classes_
    
    print(f"Predicted outcome: {prediction}")
    print("Probabilities:")
    for cls, prob in zip(classes, probabilities):
        print(f"  {cls}: {prob:.3f}")
    
    return prediction, probabilities

def main():
    """Main function to demonstrate the complete workflow"""
    
    # Load and explore dataset
    df = load_and_explore_dataset()
    
    # Prepare features
    X, y, feature_columns = prepare_features(df)
    
    # Train and evaluate models
    results, X_test, y_test = train_and_evaluate_models(X, y)
    
    # Find best model
    best_model_name = max(results, key=lambda x: results[x]['cv_score'])
    best_model = results[best_model_name]['model']
    best_scaler = results[best_model_name]['scaler']
    
    print(f"\nBest model: {best_model_name} (CV Score: {results[best_model_name]['cv_score']:.3f})")
    
    # Analyze feature importance
    feature_importance = analyze_feature_importance(best_model, feature_columns)
    
    # Plot confusion matrix for best model
    plot_confusion_matrix(y_test, results[best_model_name]['y_pred'], best_model_name)
    
    # Example prediction
    print("\n" + "="*50)
    print("EXAMPLE PREDICTION")
    print("="*50)
    
    # Use the last match from test set as example
    example_match = X_test.iloc[-1].values
    actual_result = y_test.iloc[-1]
    
    print(f"Actual result: {actual_result}")
    prediction, probabilities = predict_match_outcome(
        best_model, best_scaler, feature_columns, example_match
    )
    
    print(f"\nPrediction correct: {prediction == actual_result}")

if __name__ == "__main__":
    # Set style for plots
    plt.style.use('seaborn-v0_8')
    
    main() 