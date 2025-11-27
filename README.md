## 🚀 Instructions d'Exécution

### Prérequis
- Docker installé sur votre machine
- Architecture du projet respectée 

### Étapes d'exécution

1. **Démarrer l'environnement Docker**
   ```bash
   docker-compose up
   
2. **Exécuter un script Spark**

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/script/nom_du_script.py
