# ReadMe  Cloud Survey

## Architektur
Das Tool besteht aus 3 Komponenten
- Jobschätzung
- Dauerberechnung
- Kostenfunktion (CloudSurvey_Package)

Zudem greift es auf mehrere Datenbanken zu:
Kostenfunktion:
- Kosten der Computing Instanzen
- Kosten der Storage Instanzen

## Set-Up
Alle Pakete der requirements.txt müssen installiert sein.

## Usage

#todo: Beschreibung aufsetzten von Jobschätzung

Erstellen Sie eine json file für die Inputparameter. 
Die Datei input_parameter.json kann dabei als Vorlage oder Demo dienen.

In der Datei app.py beachten Sie, dass der richtige Pfad für die Inputparameter genutzt wird.

Führen Sie die python datei aus.
