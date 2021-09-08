# Qualitätsberichte der Krankenhäuser des Gemeinsamen Bundesausschuss (GBA)

Dieses Repo stellt eine Grundlage bereit, um die XML-Daten der Qualitätsberichte des GBA mit Python zu analyiseren.

[Die XML-Dateien der Qualitätsberichte des Gemeinsamen Bundesausschuss können hier bestellt werden](https://www.g-ba.de/themen/qualitaetssicherung/datenerhebung-zur-qualitaetssicherung/datenerhebung-qualitaetsbericht/) und sollten dann im Ordner `data/` wie dort beschrieben abgelegt werden.

[Das Notebook zeigt zwei Beispiele der Nutzung.](gba_qualitaetsberichte.ipynb)

[Genauere Infos zu den XML-Daten finden sich hier.](https://www.g-ba.de/institution/themenschwerpunkte/qualitaetssicherung/qualitaetsdaten/qualitaetsbericht/servicedateien/)

## Installation

Es wird Python 3 benötigt.

```bash
# Erstelle virtualenv
python3 -m venv env

# Aktiviere environment
source env/bin/activate

# Installiere dependencies
pip install -r requirements.txt

# Starte Jupyter
jupyter notebook

```
