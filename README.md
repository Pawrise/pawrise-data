# Pawrise Care - Data et IA

Ce dépôt regroupe le traitement des données du collier et la logique IA pour le projet Pawrise Care.

---

## Contexte et principes

Le système repose sur trois règles simples :

- **Anonymisation** : aucune donnée personnelle n'est manipulée ici, seulement un identifiant de collier unique (`anonymous_collar_id`).
- **Garde-fou déterministe** : repérer les urgences vitales dès l'entrée pour bloquer l'IA si l'animal est en danger.
- **IA en cage** : l'IA n'a pas le droit de poser un diagnostic médical (Code rural, article L243-1). Son rôle se limite à contextualiser la donnée et orienter le propriétaire.

---

## Architecture du flux

![Architecture du flux](https://mermaid.ink/img/CmdyYXBoIFRECiAgICBDb2xsaWVyKCJDb2xsaWVyIGNvbm5lY3TDqSIpIC0tPiBCYWNrZW5kKCJCYWNrZW5kIENvcmUgUnVzdCIpCiAgICBCYWNrZW5kIC0tPiBLYWZrYSgiS2Fma2EgVG9waWMgZG9nX21ldHJpY3MiKQogICAgCiAgICBLYWZrYSAtLT4gQ29uc3VtZXIoIkNvbnN1bWVyIEthZmthIC0gVGVtcHMgcsOpZWwiKQogICAgQ29uc3VtZXIgLS0-IFB5ZGFudGljKCJWYWxpZGF0aW9uIFB5ZGFudGljIikKICAgIFB5ZGFudGljIC0tPiBEZXRlY3RvcigiQW5vbWFseSBEZXRlY3RvciAtIExlIFZpZGV1ciIpCiAgICAKICAgIERldGVjdG9yIC0tIFVyZ2VuY2UgY3JpdGlxdWUgLS0-IENyaXRpY2FsKCJBbGVydGUgZXQgSUEgYmxvcXXDqWUiKQogICAgRGV0ZWN0b3IgLS0gRG9ubsOpZSBub3JtYWxlIC0tPiBSZXBvKCJEYXRhYmFzZSBSZXBvc2l0b3J5IikKICAgIAogICAgUmVwbyAtLT4gREJbKCJQb3N0Z3JlU1FMIGV0IFRpbWVzY2FsZURCIildCiAgICAKICAgIERCIC0uIFByb2NoYWluZW1lbnQgLi0-IExhbmdHcmFwaCgiQXNzaXN0YW50IElBIExhbmdHcmFwaCIpCg==)

---

## Fonctionnement du pipeline

Le traitement s'organise comme une chaîne étape par étape :

1. **Écoute** : le consumer Kafka écoute le topic en direct et attrape les métriques envoyées par le backend.
2. **Validation** : Pydantic vérifie que la donnée est bien formée et que les constantes physiologiques sont cohérentes (rythme cardiaque, température, fréquence respiratoire).
3. **Garde-fou** : l'intercepteur teste la donnée avec des règles strictes (seuils critiques). Aucun appel IA n'est fait à ce stade.
4. **Aiguillage** :
   - Si la situation est critique, l'IA est bloquée et une procédure d'urgence est lancée.
   - Si la donnée est normale, elle est enregistrée dans TimescaleDB pour alimenter l'historique de santé du chien.

---

## État du projet

### Ce qui est fait (MVP)

- **Setup infra** : configuration locale de Kafka et de PostgreSQL avec les extensions TimescaleDB et pgvector sous Docker.
- **Validation des données** : contrôle des métriques brutes avec Pydantic pour filtrer les paquets invalides.
- **Écoute Kafka** : worker qui tourne en tâche de fond pour consommer les messages en temps réel.
- **Le garde-fou (sécurité)** : script qui repère les urgences (tachycardie, forte fièvre, détresse respiratoire) pour bloquer l'IA.
- **Base de données** : sauvegarde de l'historique de santé et requêtage des 14 derniers jours.
- **Scripts de test** : simulateurs pour injecter de la donnée saine et tester le déclenchement des urgences.

### Ce qui reste à faire

- **Tenir la charge (Scale)** : le consumer traite actuellement les messages un par un. Il faut ajouter du traitement par lots (batching) et un pool de connexions pour absorber le flux de plusieurs milliers de colliers sans saturer la base.
- **Garde-fou V2** : fiabiliser la détection en prenant en compte le niveau d'activité du chien (pour ne pas lever d'alerte s'il vient de courir) et ses constantes de base habituelles.
- **Vraies alertes** : relier la sortie du garde-fou à un vrai canal d'alerte (topic Kafka dédié ou webhook vers le backend) pour notifier le propriétaire et le vétérinaire.
- **Assistant IA (LangGraph)** : développer l'assistant avec LangGraph et du RAG sur un corpus vétérinaire, pour lire l'historique en base et proposer des conseils bien-être sans jamais poser de diagnostic.

---

## Lancement en local

### Prérequis
- Python 3.11 ou supérieur
- uv
- Docker et Docker Compose

### Commandes

Installer les dépendances :
```bash
uv sync
```

Démarrer l'infrastructure :
```bash
docker compose up -d
```

Lancer l'écoute Kafka :
```bash
uv run python -m pawrise_data.infrastructure.kafka_consumer
```

Générer des données de test :
```bash
uv run python -m pawrise_data.scripts.mock_generator
uv run python -m pawrise_data.scripts.mock_producer
```
