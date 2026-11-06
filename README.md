# Pawrise Care - Data et IA

Ce dépôt regroupe le traitement des données du collier et la logique IA pour le projet Pawrise Care.

---

## Contexte et principes

Le système repose sur trois règles simples :

- **Anonymisation** : aucune donnée personnelle n'est manipulée ici, seulement un identifiant de collier unique (`anonymous_collar_id`).
- **Garde-fou déterministe** : repérer les urgences vitales dès l'entrée pour bloquer l'IA si l'animal est en danger.
- **IA en cage** : l'IA n'a pas le droit de poser un diagnostic médical (Code rural, article L243-1). Son rôle se limite à contextualiser la donnée et orienter le propriétaire.

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

Concrètement, voici ce qu'on reçoit depuis Kafka en temps réel. C'est purement mathématique et anonyme (aucun texte libre, donc pas de nettoyage de texte classique à faire de notre côté) :

- `anonymous_collar_id` (UUID) : L'ID unique du collier.
- `recorded_at` (Datetime) : L'heure exacte de la mesure.
- `bpm` (Int) : Le rythme cardiaque (battements/min).
- `respiratory_rate` (Int) : La fréquence respiratoire.
- `temperature` (Float) : La température corporelle en degrés.

Le seul "nettoyage" qu'on fait à l'entrée, c'est avec Pydantic : on rejette les valeurs corrompues (comme un BPM négatif).

## Lien avec le Backend

Puisqu'on ne stocke aucun nom de chien ni d'utilisateur (Anonymisation), comment ça marche côté client ?

Le backend et notre pipeline Data ne se parlent pas directement, ils passent par Kafka. Le backend gère la vraie base utilisateurs (ex: Nino a un chien Rex qui porte le collier XYZ). 
Quand l'application s'ouvre, le backend vient nous demander l'historique et les alertes du collier XYZ. On lui renvoie nos calculs, et c'est le backend qui se charge de recoller le nom "Rex" sur les graphiques avant de l'afficher.

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
