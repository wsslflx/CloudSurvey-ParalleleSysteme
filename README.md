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

## Daten
Die Preisdaten der Storageinstazen wurden mithilfe der Skripte storage_prices_fetch_aws.py und storage_prices_fetch_azure.py beschaffen.
Die Preisdaten der Computinginstanzen wurden mithilfe der Skripte AWS_fetch_spot_prices.py und Azure_fetch_spot_prices.py beschaffen.

Beide können die Skripte beispielsweise über Github-Actions regelmäßig ausgeführt werden.

Test Daten sind in den Zip Dateien enthalten.

## Set-Up
Alle Pakete der requirements.txt müssen installiert sein.

Zusätzlich müssen in der .env dateien url´s für die MongoDB Datenbanken gesetzt werden.
Wenn Sie eine andere Datenbank verwenden wollen, müssen Sie dies in der Datei db_operations.py angepen

Die Datenbanken sind folgendermaßen aufgebaut:

DB:  
aws_storage_pricing_db \
    Collection: \
    aws_data_transfer_prices\
    aws_ebs_prices\
    aws_efs_prices

azure_storage_pricing_db\
    Collection:\
    StoragePrices\
    TransferPrices

AWSInstancesDB\
    Collection:\
    AWSInstancesCollection

AzureInstancesDB\
    Collection:\
    AzureInstancesCollection

AzureSpotPricesDB\
    Collection:\
    SpotPrices

aws_spot_prices_db \
    Collection:\
    aws_spot_prices

        
Dies kann nach belieben geändert werden, muss aber in db_operations.py dementsprechend angepasst werden.
    

## Usage


Erstellen Sie eine json file für die Inputparameter.
Die Datei input_parameter.json kann dabei als Vorlage oder Demo dienen.

Beachten Sie nun, dass in der Python Datei main.py, die richtige Json Datei angegeben ist.

Zusätzlich müssen die richtigen IP Adressen, für die HTTP request eingetragen werden.
In main.py muss der Parameter url angepasst werden, zu der IP Adresse in der app.py gestartet werden soll.
In app.py muss der Parameter url_simulate angepasst werden, zu der IP Adresse, in der CloudSim gestartet wurde.

Nun führen Sie zuerst CloudSim aus(siehe Abhängigkeiten), dann app.py.
Jetzt können Sie einen request senden, indem Sie main.py laufen lassen.
Das Ergebnis wird sowohl geprintet als auch als eigene Json Datei gepspeichert.


## Abhängigkeiten
Für die Simulation greift dieses Projekt auf das CloudSim-Modul unter folgendem Repository zu:

[https://github.com/Houou101/cloudsim-hpc](https://github.com/Houou101/cloudsim-hpc)

Stellen Sie sicher, dass die CloudSim-Simulation sowie die zugehörige REST-API dort gestartet wurden, bevor Sie dieses Tool verwenden.

