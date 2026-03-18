# Scénario de Démonstration : Architecture Sécurisée des Microservices

Ce document présente le protocole de validation de la sécurité de l'architecture backend, avec un focus particulier sur la gestion des secrets via HashiCorp Vault.

## Objectif de la Démonstration
Prouver le découplage total entre le code source applicatif (FastAPI) et les secrets d'infrastructure (PostgreSQL), en démontrant :
1. Le déploiement conteneurisé intégrant un coffre-fort numérique dynamique.
2. L'absence de mots de passe en clair dans les conteneurs des APIs.
3. Le mécanisme d'injection statique et de récupération asynchrone ("Zero-Trust") des identifiants au démarrage des services.

---

## Étape 1 : Initialisation de l'Infrastructure Docker (SecOps)
L'environnement isole volontairement la base de données et le routeur d'accès via des réseaux fermés gérés par Docker Compose.

**Action :**
Démarrage simultané des services.

```bash
docker-compose up -d --build
```

**Observation attendue :**
- Le service `scmd_vault` démarre et devient sain (`Healthy`).
- Le conteneur éphémère `scmd_vault_init` s'exécute pour inscrire les accès BDD (User, Password, Host, Database) via la commande `vault kv put` directement dans le coffre.
- Les Microservices (`finance_api` et `prescription_api`) démarrent **uniquement** après initialisation sécurisée du Vault.

---

## Étape 2 : L'Appel HTTP vers le Vault (Preuve d'Authentification)
Le code applicatif dépend du composant tiers HashiCorp Vault pour initialiser ses moteurs JDBC/SQLAlchemy. 

**Action :**
Vérification des sondes de vitalité (Healthchecks) de l'API.

```bash
curl -s http://localhost:8001/health
```

**Résultat attendu :**
```json
{"status": "ok", "service": "finance_api", "db_initialized": true}
```
**Interprétation :** 
L'application a utilisé son jeton d'accès (`VAULT_TOKEN`) transmis de façon éphémère à l'exécution, a requêté le chemin racine `secret/data/scmd/postgres` en HTTP, et a consolidé les chaînes de connexion en mémoire vive. L'indicateur `db_initialized: true` fait office de preuve de bon fonctionnement de cette chaîne de sécurité.

---

## Étape 3 : Scénario d'Échec (Robustesse SecOps)
*(Optionnel / Pour discussion)*
Si le secret est modifié (rotation manuelle par équipe Ops) via la CLI Vault, ou si le conteneur Vault est inaccessible, le Microservice échouera dès le démarrage (Fail-Fast) en retournant une erreur `HTTPException(500)` bloquant ainsi toute fuite potentiel de tentatives de connexion non sécurisées vers la base de données cible.

## Conclusion
L'architecture démontre l'application stricte du principe de moindre privilège (Least-Privilege). Les Microservices ne connaissent pas la topologie de la base de données avant leur exécution, neutralisant les risques de compromissions liés aux dépôts de code partagés.
