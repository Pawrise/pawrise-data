# Pawrise Care - Data

Ce dépôt regroupe l'ingestion, la validation, le garde-fou et le stockage des données du collier pour le projet Pawrise Care (EIP Epitech). Le périmètre est strictement Data Engineering : le pipeline s'arrête au stockage dans TimescaleDB/pgvector, qui servira de source de vérité à un futur module IA externe.

---

## Contexte et principes

Le système repose sur trois règles simples :

- **Anonymisation** : aucune donnée personnelle n'est manipulée ici, seulement un identifiant de collier unique (`anonymous_collar_id`).
- **Garde-fou déterministe** : repérer les urgences vitales dès l'entrée pour bloquer l'IA si l'animal est en danger.
- **IA en cage (futur module externe)** : l'IA n'a pas le droit de poser un diagnostic médical (Code rural, article L243-1). Son rôle se limitera à contextualiser la donnée et orienter le propriétaire. Aucune logique d'IA n'existe dans ce dépôt.

---

## Architecture

Vue d'ensemble du flux, du capteur jusqu'au stockage et aux alertes. Le service Data ne consomme que `dog_metrics_topic` et reste stateless : l'`AnomalyDetector` ne décide qu'à partir du payload.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1060 580" width="100%" role="img" aria-labelledby="arch-title arch-desc" font-family="-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif">
<title id="arch-title">Architecture Pawrise Care : du capteur IoT à TimescaleDB</title>
<desc id="arch-desc">Le capteur IoT envoie en MQTT au backend Rust, qui enrichit les données et publie sur Kafka. Le service Data Python consomme dog_metrics_topic, valide avec Pydantic, applique l'AnomalyDetector puis écrit dans TimescaleDB ou publie sur critical_alerts_topic.</desc>
<defs>
<marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#475569"/></marker>
<marker id="arr-red" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#D64545"/></marker>
<marker id="arr-green" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#2E9E6B"/></marker>
<marker id="arr-grey" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#94A3B8"/></marker>
</defs>
<rect x="0" y="0" width="1060" height="580" rx="14" fill="#FFFFFF"/>
<!-- ZONE 1 -->
<rect x="20" y="20" width="280" height="540" rx="12" fill="#EEF5FD" stroke="#4C8DD6" stroke-width="1.5"/>
<text x="160" y="48" text-anchor="middle" font-size="14" font-weight="700" fill="#1D4E89">ZONE 1 · Edge</text>
<text x="160" y="66" text-anchor="middle" font-size="12" fill="#4B6A8F">Collier connecté</text>
<rect x="40" y="100" width="240" height="90" rx="8" fill="#FFFFFF" stroke="#4C8DD6" stroke-width="1.5"/>
<text x="160" y="126" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">Capteur IoT</text>
<text x="160" y="148" text-anchor="middle" font-size="12" fill="#475569">Fréquence cardiaque · respiratoire</text>
<text x="160" y="166" text-anchor="middle" font-size="12" fill="#475569">Température · accéléromètre</text>
<rect x="40" y="215" width="240" height="80" rx="8" fill="#FFFFFF" stroke="#94A3B8" stroke-width="1.2" stroke-dasharray="5 4"/>
<text x="160" y="240" text-anchor="middle" font-size="12" font-weight="700" fill="#1F2937">Mesures brutes</text>
<text x="160" y="260" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">anonymous_collar_id</text>
<text x="160" y="278" text-anchor="middle" font-size="11" fill="#475569">UUID v4 · aucune donnée nominative</text>
<!-- ZONE 2 -->
<rect x="350" y="20" width="280" height="540" rx="12" fill="#FFF4EA" stroke="#E0782B" stroke-width="1.5"/>
<text x="490" y="48" text-anchor="middle" font-size="14" font-weight="700" fill="#9A4A0F">ZONE 2 · Backend Rust</text>
<text x="490" y="66" text-anchor="middle" font-size="12" fill="#9A6A42">Ingestion, enrichissement, publication</text>
<rect x="370" y="100" width="240" height="60" rx="8" fill="#FFFFFF" stroke="#E0782B" stroke-width="1.5"/>
<text x="490" y="126" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">Ingestion MQTT</text>
<text x="490" y="145" text-anchor="middle" font-size="12" fill="#475569">Réception des mesures</text>
<rect x="370" y="190" width="240" height="95" rx="8" fill="#FFFFFF" stroke="#E0782B" stroke-width="1.5"/>
<text x="490" y="214" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">Enrichissement</text>
<text x="490" y="236" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">weight_kg · age_months</text>
<text x="490" y="254" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">breed_size</text>
<text x="490" y="272" text-anchor="middle" font-size="11" fill="#475569">poids · âge · gabarit</text>
<rect x="365" y="315" width="250" height="160" rx="10" fill="#FFFFFF" stroke="#475569" stroke-width="1.5"/>
<text x="490" y="336" text-anchor="middle" font-size="12" font-weight="700" fill="#1F2937">Apache Kafka</text>
<rect x="378" y="348" width="224" height="42" rx="8" fill="#FFF7ED" stroke="#E0782B" stroke-width="1.5"/>
<text x="490" y="366" text-anchor="middle" font-size="12" font-weight="700" fill="#1F2937" font-family="ui-monospace, Menlo, Consolas, monospace">dog_metrics_topic</text>
<text x="490" y="381" text-anchor="middle" font-size="10.5" fill="#6B7280">santé</text>
<rect x="378" y="406" width="224" height="42" rx="8" fill="#F8FAFC" stroke="#94A3B8" stroke-width="1.5"/>
<text x="490" y="424" text-anchor="middle" font-size="12" font-weight="700" fill="#1F2937" font-family="ui-monospace, Menlo, Consolas, monospace">dog_telemetry_topic</text>
<text x="490" y="439" text-anchor="middle" font-size="10.5" fill="#6B7280">GPS · batterie</text>
<!-- ZONE 3 -->
<rect x="680" y="20" width="360" height="540" rx="12" fill="#EEF9F3" stroke="#2E9E6B" stroke-width="1.5"/>
<text x="860" y="48" text-anchor="middle" font-size="14" font-weight="700" fill="#17694A">ZONE 3 · Service Data (Python)</text>
<text x="860" y="66" text-anchor="middle" font-size="12" fill="#4A8068">Pawrise Care Data</text>
<rect x="700" y="90" width="320" height="50" rx="8" fill="#FFFFFF" stroke="#2E9E6B" stroke-width="1.5"/>
<text x="860" y="112" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">MetricsConsumer</text>
<text x="860" y="129" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">consomme dog_metrics_topic</text>
<rect x="700" y="165" width="320" height="50" rx="8" fill="#FFFFFF" stroke="#2E9E6B" stroke-width="1.5"/>
<text x="860" y="187" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">Validateur Pydantic</text>
<text x="860" y="204" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">DogMetric.model_validate()</text>
<rect x="700" y="240" width="320" height="70" rx="8" fill="#FFFFFF" stroke="#2E9E6B" stroke-width="2"/>
<text x="860" y="263" text-anchor="middle" font-size="14" font-weight="700" fill="#1F2937">AnomalyDetector</text>
<text x="860" y="282" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">evaluate()</text>
<text x="860" y="299" text-anchor="middle" font-size="11" fill="#475569">Garde-fou déterministe · stateless</text>
<rect x="700" y="350" width="155" height="70" rx="8" fill="#E3F5EC" stroke="#2E9E6B" stroke-width="1.5"/>
<text x="777" y="374" text-anchor="middle" font-size="13" font-weight="700" fill="#1F2937">TimescaleDB</text>
<text x="777" y="392" text-anchor="middle" font-size="11" fill="#475569">hypertable</text>
<text x="777" y="408" text-anchor="middle" font-size="11" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">dog_metrics</text>
<rect x="865" y="350" width="155" height="70" rx="8" fill="#FDECEC" stroke="#D64545" stroke-width="1.5"/>
<text x="942" y="374" text-anchor="middle" font-size="13" font-weight="700" fill="#1F2937">Critical Handoff</text>
<text x="942" y="394" text-anchor="middle" font-size="10.5" fill="#475569" font-family="ui-monospace, Menlo, Consolas, monospace">critical_alerts_topic</text>
<rect x="700" y="480" width="320" height="50" rx="8" fill="#FFFFFF" stroke="#94A3B8" stroke-width="1.2" stroke-dasharray="5 4"/>
<text x="860" y="502" text-anchor="middle" font-size="11" font-weight="700" fill="#64748B" font-family="ui-monospace, Menlo, Consolas, monospace">dog_telemetry_topic</text>
<text x="860" y="518" text-anchor="middle" font-size="11" fill="#64748B">ignoré par la Data</text>
<!-- FLUX -->
<path d="M280,130 H370" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<text x="325" y="122" text-anchor="middle" font-size="11" font-weight="700" fill="#475569">MQTT</text>
<path d="M490,160 V190" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<path d="M490,285 V315" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<path d="M602,369 H645 V115 H700" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<path d="M602,427 H665 V505 H700" fill="none" stroke="#94A3B8" stroke-width="1.5" stroke-dasharray="5 4" marker-end="url(#arr-grey)"/>
<path d="M860,140 V165" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<path d="M860,215 V240" fill="none" stroke="#475569" stroke-width="1.8" marker-end="url(#arr)"/>
<path d="M860,310 V330 H777 V350" fill="none" stroke="#2E9E6B" stroke-width="1.8" marker-end="url(#arr-green)"/>
<path d="M860,330 H942 V350" fill="none" stroke="#D64545" stroke-width="1.8" marker-end="url(#arr-red)"/>
<text x="815" y="324" text-anchor="middle" font-size="10.5" font-weight="700" fill="#17694A">NORMAL</text>
<text x="905" y="324" text-anchor="middle" font-size="10.5" font-weight="700" fill="#B83232">URGENCE</text>
</svg>

Version autonome du schéma : [`docs/architecture.svg`](docs/architecture.svg).

---

## Format des données (Le Payload)

Le collier publie en MQTT sur `pawrise/+/telemetry` (`device_id`, `temperature`, `activity`, `fsm_state`). Le script `mqtt_ingestion.py` (pôle Data) enrichit le message, le valide avec Pydantic et le publie sur Kafka `dog_metrics_topic`. Le payload Kafka est purement numérique et anonyme (aucun texte libre) :

- `anonymous_collar_id` (UUID v4) : l'ID unique du collier.
- `recorded_at` (Datetime) : l'heure exacte de la mesure.
- `bpm` (Int, 20-400) : le rythme cardiaque.
- `respiratory_rate` (Int, 1-150) : la fréquence respiratoire.
- `temperature` (Float, 30-45) : la température corporelle en °C.
- `activity` : `REST` | `WALKING` | `RUNNING` | `AGITATED`.
- `fsm_state` : état de la machine à états du collier (`ESCAPE`, `LOW POWER`, etc.).
- `weight_kg` (Float, > 0), `age_months` (Int, >= 0), `breed_size` (`small` | `medium` | `large`) : enrichissement du profil du chien.
- `bpm_moving_avg` (Float, optionnel) : bpm lissé, calculé à l'ingestion.

Pydantic (`DogMetric`) rejette les valeurs corrompues (comme un BPM négatif) ou hors bornes.

## Lien avec le Backend

Puisqu'on ne stocke aucun nom de chien ni d'utilisateur (Anonymisation), comment ça marche côté client ?

Le backend (Rust) reste la source des métadonnées du chien (poids, âge, taille). Le backend et notre pipeline Data ne se parlent pas directement, ils passent par Kafka et par la base. Le backend gère la vraie base utilisateurs (ex: Adam a un chien Rex qui porte le collier XYZ). 
Quand l'application s'ouvre, le backend lit l'historique (dont les agrégats journaliers `daily_dog_metrics`) et écoute les alertes `critical_alerts_topic` du collier XYZ. Il récupère nos données, et c'est le backend qui se charge de recoller le nom "Rex" sur les graphiques avant de l'afficher.

---

## Fonctionnement du pipeline

Le traitement s'organise comme une chaîne étape par étape :

0. **Ingestion IoT** : `mqtt_ingestion.py` reçoit la télémétrie MQTT (Mosquitto), l'enrichit (poids, âge, taille, moyenne mobile du bpm), la valide et la publie sur `dog_metrics_topic`.
1. **Écoute** : le `MetricsConsumer` lit `dog_metrics_topic` en direct, par micro-lots, avec commit manuel des offsets.
2. **Validation** : Pydantic vérifie que la donnée est bien formée et cohérente. Les messages invalides partent en DLQ (`dog_metrics_dlq`).
3. **Garde-fou** : `AnomalyDetector.evaluate()` applique des seuils déterministes selon la taille du chien, son activité et son état (les seuils sont relevés en effort). Il est stateless : aucune requête BDD.
4. **Aiguillage** :
   - Critique : alerte publiée sur `critical_alerts_topic` (Critical Handoff).
   - Normale : insertion en lot dans l'hypertable TimescaleDB `dog_metrics` (si l'insertion échoue, le lot part en DLQ).
5. **Observabilité** : chaque flush de lot journalise le volume inséré, le temps d'exécution et le nombre de messages envoyés en DLQ.

---

## État du projet

### Ce qui est fait

- **Infra** : Docker Compose avec PostgreSQL (TimescaleDB + pgvector), Kafka 3.7 (KRaft) et Mosquitto (MQTT).
- **Ingestion MQTT → Kafka** avec enrichissement du payload et moyenne mobile du bpm.
- **Validation** : modèle `DogMetric` (Pydantic v2) avec bornes strictes.
- **Garde-fou V2** : seuils contextuels (taille de race), prise en compte de l'activité, de l'état du collier et du bpm lissé.
- **Alertes** : Critical Handoff publié sur `critical_alerts_topic`.
- **Consumer Kafka** : batching (taille/intervalle), commit manuel, DLQ, logs structurés.
- **Stockage TimescaleDB** : hypertable, compression (7 jours), rétention (90 jours), agrégat continu journalier `daily_dog_metrics` avec sa policy de rafraîchissement.
- **pgvector préparé** : table `ai_insights` (`embedding VECTOR(1536)`, index HNSW), schéma seulement, peuplée plus tard par le futur microservice IA.
- **Tests et mocks** : `mock_generator`, `mock_producer`, `test_interceptor` et une suite de 77 tests (dont un smoke test e2e ignoré tant que la stack Docker n'est pas démarrée).

### Hors périmètre (décisions d'architecture)

- `executemany()` conservé plutôt que `COPY` : le micro-batching avec DLQ exige une granularité transactionnelle fine.
- Pas d'assistant IA / LangGraph / RAG dans ce dépôt : réservé à un futur microservice externe qui lira les tables.

---

## Lancement en local

### Prérequis

- [uv](https://docs.astral.sh/uv/) : gère l'environnement virtuel et les dépendances. Il télécharge lui-même Python 3.11+ s'il est absent de la machine.
- [Docker](https://docs.docker.com/get-docker/) et Docker Compose : font tourner PostgreSQL (TimescaleDB + pgvector), Kafka et Mosquitto.

### 1. Installation

Depuis la racine du dépôt :

```bash
uv sync
```

Cette commande crée le `.venv` du projet et installe toutes les dépendances.

### 2. Démarrer l'infrastructure

```bash
docker compose up -d
```

Lance PostgreSQL (`5432`), Kafka (`9092`) et Mosquitto (`1883`). Au premier démarrage, `docker/postgres/init.sql` crée les tables, l'hypertable, les politiques de compression et de rétention, et l'agrégat continu `daily_dog_metrics`.

La configuration par défaut convient en local. Pour la modifier, copier `.env.example` en `.env` et ajuster les variables.

### 3. Lancer le pipeline

Chaque commande tourne en continu : l'ouvrir dans son propre terminal, dans cet ordre.

Terminal 1, pont MQTT vers Kafka (réception, enrichissement, validation, publication sur `dog_metrics_topic`) :

```bash
uv run python -m pawrise_data.infrastructure.mqtt_ingestion
```

Terminal 2, consumer Kafka (garde-fou, alertes sur `critical_alerts_topic`, archivage en base) :

```bash
uv run python -m pawrise_data.infrastructure.kafka_consumer
```

### 4. Injecter des données de test

Terminal 3, simulateur de collier : publie des mesures fictives sur Mosquitto.

```bash
uv run python -m pawrise_data.scripts.mock_producer
```

Par défaut, tous les scénarios sont joués dans l'ordre. Options :

```bash
uv run python -m pawrise_data.scripts.mock_producer --scenario rest escape_critical --interval 0.5
```

Scénarios disponibles : `rest`, `walk`, `escape`, `escape_critical`, `critical_rest`, ainsi que leurs variantes `_medium` et `_large`. `--interval` règle le délai entre deux messages, en secondes (1.0 par défaut).

### 5. Vérifier le résultat

Rejouer le garde-fou sur les dernières mesures stockées en base :

```bash
uv run python -m pawrise_data.scripts.test_interceptor
```

Vérifier l'agrégat journalier :

```bash
docker exec pawrise-postgres psql -U pawrise -d pawrise_care -c "CALL refresh_continuous_aggregate('daily_dog_metrics', NULL, NULL); SELECT * FROM daily_dog_metrics LIMIT 5;"
```

### 6. Monitoring (Prometheus + Grafana)

Le consumer expose ses métriques de santé sur `http://localhost:8000/metrics` (variable `METRICS_PORT`). Prometheus les collecte toutes les 15 s, Grafana les affiche.

| Service | URL | Identifiants par défaut |
|---|---|---|
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | `admin` / `admin` (`GRAFANA_USER`, `GRAFANA_PASSWORD`) |

Métriques : `pawrise_messages_inserted_total`, `pawrise_messages_dlq_total`, `pawrise_critical_alerts_total`, `pawrise_batch_flush_seconds`, `pawrise_consumer_lag`.

Grafana est provisionné automatiquement avec deux sources (Prometheus et TimescaleDB) et le dashboard « Pawrise - Santé du pipeline ». Le consumer tourne sur l'hôte : Prometheus le joint via `host.docker.internal:8000`.

### Tests

```bash
uv run pytest
```

Le smoke test e2e est ignoré tant que la stack Docker n'est pas démarrée (`docker compose up -d`).

### Arrêter

```bash
docker compose down
```

Ajouter `-v` pour supprimer aussi les données de la base.
