"""
Analyseur de page MondeFootball.fr pour optimiser le scraping
Examine la structure HTML et identifie les meilleurs sélecteurs
"""

import requests
from bs4 import BeautifulSoup
import json
import re
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MondeFootballPageAnalyzer:
    """Analyse la structure des pages MondeFootball.fr pour optimiser le scraping"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
    
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Récupère une page web"""
        try:
            logger.info(f"Récupération de: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Erreur lors de la récupération: {e}")
            return None
    
    def analyze_page_structure(self, soup: BeautifulSoup) -> Dict:
        """Analyse la structure générale de la page"""
        structure = {
            'title': soup.title.text if soup.title else None,
            'total_elements': len(soup.find_all()),
            'tables': len(soup.find_all('table')),
            'divs': len(soup.find_all('div')),
            'classes_found': [],
            'ids_found': [],
            'potential_match_containers': []
        }
        
        # Collecte toutes les classes et IDs
        for element in soup.find_all(class_=True):
            for class_name in element.get('class', []):
                if class_name not in structure['classes_found']:
                    structure['classes_found'].append(class_name)
        
        for element in soup.find_all(id=True):
            id_name = element.get('id')
            if id_name not in structure['ids_found']:
                structure['ids_found'].append(id_name)
        
        # Recherche de conteneurs de matchs potentiels
        match_keywords = ['match', 'game', 'fixture', 'rencontre', 'calendrier', 'resultat']
        for keyword in match_keywords:
            # Classes contenant le mot-clé
            for class_name in structure['classes_found']:
                if keyword.lower() in class_name.lower():
                    structure['potential_match_containers'].append(f".{class_name}")
            
            # IDs contenant le mot-clé
            for id_name in structure['ids_found']:
                if keyword.lower() in id_name.lower():
                    structure['potential_match_containers'].append(f"#{id_name}")
        
        return structure
    
    def find_match_data_patterns(self, soup: BeautifulSoup) -> Dict:
        """Identifie les patterns de données de match"""
        patterns = {
            'score_patterns': [],
            'team_name_patterns': [],
            'date_patterns': [],
            'potential_selectors': []
        }
        
        # Recherche de patterns de score
        score_regex = re.compile(r'\b\d+[-:]\d+\b')
        for element in soup.find_all(text=score_regex):
            parent = element.parent
            if parent:
                patterns['score_patterns'].append({
                    'text': element.strip(),
                    'parent_tag': parent.name,
                    'parent_class': parent.get('class', []),
                    'parent_id': parent.get('id')
                })
        
        # Recherche de noms d'équipes (liens vers équipes)
        team_links = soup.find_all('a', href=re.compile(r'equipe|team|club'))
        for link in team_links[:10]:  # Limite à 10 pour l'analyse
            patterns['team_name_patterns'].append({
                'text': link.get_text(strip=True),
                'href': link.get('href'),
                'parent_class': link.parent.get('class', []) if link.parent else [],
                'parent_tag': link.parent.name if link.parent else None
            })
        
        # Recherche de patterns de date
        date_regex = re.compile(r'\b\d{1,2}[./]\d{1,2}[./]\d{2,4}\b|\b\d{1,2}\s+\w+\s+\d{4}\b')
        for element in soup.find_all(text=date_regex):
            parent = element.parent
            if parent:
                patterns['date_patterns'].append({
                    'text': element.strip(),
                    'parent_tag': parent.name,
                    'parent_class': parent.get('class', []),
                    'parent_id': parent.get('id')
                })
        
        return patterns
    
    def analyze_table_structure(self, soup: BeautifulSoup) -> List[Dict]:
        """Analyse la structure des tableaux"""
        tables_info = []
        
        for i, table in enumerate(soup.find_all('table')):
            table_info = {
                'table_index': i,
                'rows': len(table.find_all('tr')),
                'columns': 0,
                'headers': [],
                'sample_data': [],
                'classes': table.get('class', []),
                'id': table.get('id')
            }
            
            # Analyse des en-têtes
            headers = table.find_all('th')
            table_info['headers'] = [th.get_text(strip=True) for th in headers]
            
            # Analyse des lignes de données
            rows = table.find_all('tr')
            if rows:
                # Nombre de colonnes basé sur la première ligne
                first_row_cells = rows[0].find_all(['td', 'th'])
                table_info['columns'] = len(first_row_cells)
                
                # Échantillon de données (3 premières lignes)
                for row in rows[:3]:
                    cells = row.find_all(['td', 'th'])
                    row_data = [cell.get_text(strip=True) for cell in cells]
                    table_info['sample_data'].append(row_data)
            
            tables_info.append(table_info)
        
        return tables_info
    
    def generate_optimal_selectors(self, soup: BeautifulSoup) -> Dict:
        """Génère les sélecteurs CSS optimaux pour extraire les données"""
        selectors = {
            'match_containers': [],
            'team_names': [],
            'scores': [],
            'dates': [],
            'recommendations': []
        }
        
        # Analyse des conteneurs de matchs potentiels
        for table in soup.find_all('table'):
            if table.find_all('tr'):
                score_found = False
                team_found = False
                
                # Vérifier si le tableau contient des scores
                table_text = table.get_text()
                if re.search(r'\d+[-:]\d+', table_text):
                    score_found = True
                
                # Vérifier si le tableau contient des noms d'équipes
                if table.find_all('a'):
                    team_found = True
                
                if score_found and team_found:
                    selector = 'table'
                    if table.get('class'):
                        selector = f"table.{'.'.join(table['class'])}"
                    elif table.get('id'):
                        selector = f"table#{table['id']}"
                    
                    selectors['match_containers'].append(selector)
        
        # Recherche de sélecteurs pour les noms d'équipes
        team_links = soup.find_all('a', href=re.compile(r'equipe|team|club'))
        for link in team_links:
            if link.get('class'):
                selector = f"a.{'.'.join(link['class'])}"
                if selector not in selectors['team_names']:
                    selectors['team_names'].append(selector)
        
        # Recommandations basées sur l'analyse
        if selectors['match_containers']:
            selectors['recommendations'].append("Utiliser les tableaux comme conteneurs principaux")
        
        if selectors['team_names']:
            selectors['recommendations'].append("Extraire les noms d'équipes via les liens")
        
        return selectors
    
    def save_analysis(self, analysis: Dict, filename: str = 'page_analysis.json'):
        """Sauvegarde l'analyse dans un fichier JSON"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, indent=2, ensure_ascii=False)
        logger.info(f"Analyse sauvegardée dans {filename}")
    
    def analyze_url(self, url: str) -> Dict:
        """Analyse complète d'une URL"""
        soup = self.get_page(url)
        if not soup:
            return {}
        
        analysis = {
            'url': url,
            'page_structure': self.analyze_page_structure(soup),
            'match_patterns': self.find_match_data_patterns(soup),
            'table_structure': self.analyze_table_structure(soup),
            'optimal_selectors': self.generate_optimal_selectors(soup)
        }
        
        return analysis

def main():
    """Test de l'analyseur avec une page La Liga"""
    analyzer = MondeFootballPageAnalyzer()
    
    # URLs de test pour La Liga
    test_urls = [
        "https://www.mondefootball.fr/calendrier/esp-primera-division-2024-2025-spieltag/1/",
        "https://www.mondefootball.fr/calendrier/esp-primera-division-2023-2024-spieltag/1/",
        "https://www.mondefootball.fr/calendrier/esp-primera-division/"
    ]
    
    for url in test_urls:
        print(f"\n{'='*60}")
        print(f"ANALYSE DE: {url}")
        print('='*60)
        
        analysis = analyzer.analyze_url(url)
        
        if analysis:
            # Affichage des résultats clés
            print(f"\nTitre de la page: {analysis['page_structure']['title']}")
            print(f"Nombre de tableaux: {analysis['page_structure']['tables']}")
            print(f"Classes potentielles: {analysis['page_structure']['potential_match_containers'][:5]}")
            
            print(f"\nPatterns de score trouvés: {len(analysis['match_patterns']['score_patterns'])}")
            for pattern in analysis['match_patterns']['score_patterns'][:3]:
                print(f"  - {pattern['text']} (dans {pattern['parent_tag']})")
            
            print(f"\nNoms d'équipes trouvés: {len(analysis['match_patterns']['team_name_patterns'])}")
            for pattern in analysis['match_patterns']['team_name_patterns'][:5]:
                print(f"  - {pattern['text']}")
            
            print(f"\nTableaux analysés: {len(analysis['table_structure'])}")
            for table in analysis['table_structure'][:2]:
                print(f"  - Tableau {table['table_index']}: {table['rows']} lignes, {table['columns']} colonnes")
                if table['headers']:
                    print(f"    En-têtes: {table['headers']}")
            
            print(f"\nSélecteurs recommandés:")
            for selector in analysis['optimal_selectors']['match_containers']:
                print(f"  - {selector}")
            
            # Sauvegarde de l'analyse
            filename = f"analysis_{url.split('/')[-2]}.json"
            analyzer.save_analysis(analysis, filename)
        else:
            print("Échec de l'analyse")

if __name__ == "__main__":
    main() 