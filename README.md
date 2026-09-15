# Pawrise Care - Data/IA Component

Ce dépôt contient la logique de traitement de la donnée et d'IA (LangGraph, RAG) pour le projet Pawrise Care.

## Sécurité et Conformité
- **Anonymisation** : Aucune donnée nominative n'est traitée.
- **Red Flags** : Détection d'anomalies en amont pour by-passer l'IA si besoin.
- **IA en Cage** : L'IA ne pose aucun diagnostic médical (Code rural L243-1).

## Démarrage rapide (Local)

1. `uv sync` (Installation des dépendances)
2. `docker-compose up -d` (Lancement de la BDD PostgreSQL + TimescaleDB + pgvector)
