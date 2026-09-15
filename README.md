# Pawrise Care - Data/IA Component

Ce dépôt contient la logique de traitement de la donnée et d'IA (LangGraph, RAG) pour le projet Pawrise Care.

## Sécurité et Conformité
- **Anonymisation** : Aucune donnée nominative n'est traitée (on utilise des `anonymous_collar_id` UUID).
- **Red Flags** : Détection d'anomalies en amont pour by-passer l'IA si besoin.
- **IA en Cage** : L'IA ne pose aucun diagnostic médical (Code rural L243-1).

## Architecture du Pipeline de Données (Data Pipeline)

Notre système agit comme une ligne d'assemblage (ou pipeline). Le point d'entrée est notre **Kafka Consumer**, qui orchestre le flux selon les étapes suivantes :

1. **L'Ingestion (Streaming)** : Le backend (Rust) envoie les métriques brutes. Notre Consumer Kafka "écoute" et attrape cette donnée au vol dès qu'elle arrive sur le réseau.
2. **Le Filtrage (Validation & Parsing)** : Le Consumer vérifie que la donnée est valide et la transforme en un objet Python (nos modèles **Pydantic**).
3. **Le Garde-fou (Routing & Interceptor)** : Le Consumer donne cet objet à notre `AnomalyDetector`. C'est notre videur de boîte de nuit : il applique un algorithme **déterministe** (des règles strictes if/else, aucun appel à l'IA).
4. **L'Aiguillage (Sink / Trigger)** :
   - **Si c'est critique** : L'IA est bloquée. Le Consumer déclenche un Handoff vétérinaire immédiat (**Critical Handoff**).
   - **Si c'est normal** : Le Consumer confie la donnée au magasinier (le **Database Repository**) pour la stocker (**Persistence** dans PostgreSQL/TimescaleDB), puis réveille le module IA (**LangGraph** / RAG) pour générer des conseils bien-être.

## Démarrage rapide (Local)

1. `uv sync` (Installation des dépendances)
2. `docker-compose up -d` (Lancement de la BDD PostgreSQL + TimescaleDB + pgvector)
