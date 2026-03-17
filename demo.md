# 🎓 Script de Démo : Microservices & HashiCorp Vault

Ce guide vous aidera à présenter à votre professeur comment fonctionnent les microservices (Finance et Prescription) et comment ils s'intègrent de manière sécurisée avec HashiCorp Vault et PostgreSQL.

---

## 🕒 Étape 1 : Introduction du Concept (2 minutes)
**Ce qu'il faut dire :**
> "Bonjour. Pour cette partie de l'architecture, j'ai mis en place une approche orientée **Microservices**. L'objectif est de séparer l'exposition des données Gold (calculées par Airflow et Spark) en deux APIs distinctes : l'une pour la **Finance** et l'autre pour les **Prescriptions**.
>
> Le point critique ici est la **Sécurité**. Plutôt que de coder en dur les mots de passe de la base de données PostgreSQL dans le code ou dans des variables d'environnement en clair, j'utilise **HashiCorp Vault** comme gestionnaire de secrets centralisé."

---

## 🛠️ Étape 2 : L'Infrastructure Docker Compose (3 minutes)
**Ce qu'il faut montrer :** Ouvrez le fichier `docker-compose.yml`.

**Ce qu'il faut dire :**
> "Toute cette infrastructure est orchestrée par Docker Compose. Voici comment ça fonctionne au démarrage :
> 1. Le conteneur **PostgreSQL** démarre en premier.
> 2. Ensuite, le conteneur **Vault** démarre en mode développement.
> 3. J'ai créé un conteneur éphémère appelé **`vault-init`**. Son seul rôle est d'attendre que Vault soit prêt, puis d'y injecter automatiquement les identifiants de la base de données PostgreSQL (user, password, host, port) dans un chemin secret (`secret/scmd/postgres`). Une fois terminé, ce conteneur s'éteint.
> 4. Enfin, les deux APIs (**finance_api** et **prescription_api**) démarrent."

---

## 🔒 Étape 3 : Comment les APIs récupèrent les secrets (3 minutes)
**Ce qu'il faut montrer :** Ouvrez le fichier `finance_api/main.py` (ou prescription_api) et montrez la fonction `get_db_credentials_from_vault()` et `init_db()`.

**Ce qu'il faut dire :**
> "Voyons maintenant comment ça marche côté code avec FastAPI.
> Au démarrage de l'API (grâce à l'événement `@app.on_event("startup")`), l'application ne connait pas le mot de passe de la base de données.
> 
> Elle utilise la librairie Python `hvac` pour se connecter à Vault en utilisant un Token d'authentification (`VAULT_TOKEN`).
> Elle fait une requête HTTP au serveur Vault sur le chemin `secret/data/scmd/postgres`.
> Vault lui renvoie alors le dictionnaire contenant les credentials en toute sécurité en mémoire.
> 
> Ce n'est qu'à ce moment-là que l'API construit son URL de connexion (le `db_url`) et initialise le moteur SQLAlchemy (`create_engine`) pour discuter avec PostgreSQL."

*Argument fort pour le prof : "Si le mot de passe de la base de données change demain, je n'ai pas besoin de modifier le code source des APIs ni de reconstruire les images Docker. Je change juste le secret dans Vault, et je redémarre les conteneurs."*

---

## 🚀 Étape 4 : Démonstration Live (2 minutes)
**Ce qu'il faut montrer :** Le terminal et votre navigateur.

1. **Montrer que ça tourne :** Tapez `docker-compose ps` dans le terminal pour montrer que les conteneurs sont `Up`.
2. **Tester les Healthchecks :**
   Dans le terminal, tapez :
   - `curl http://localhost:8001/health` (ou via le navigateur)
   - `curl http://localhost:8002/health`

**Ce qu'il faut dire :**
> "Comme vous pouvez le voir, l'attribut `db_initialized` est à `true`. Cela prouve de bout-en-bout que l'API a réussi à s'authentifier auprès de Vault, à récupérer le mot de passe, et à établir la connexion avec PostgreSQL."

3. **Tester un endpoint de données :**
   Allez sur `http://localhost:8001/docs` (le Swagger généré automatiquement par FastAPI).
   Testez l'endpoint `/kpis/f1`.

**Ce qu'il faut dire :**
> *(Si la table n'existe pas encore car Airflow n'a pas tourné)* : "Ici on obtient une erreur SQL indiquant que la table n'existe pas. C'est le comportement attendu, car cela prouve que l'API discute bien avec la base de données cible. Il ne reste plus qu'à faire tourner le pipeline Big Data (Spark) pour la remplir."
> 
> *(Si les données sont présentes)* : "Et voilà, les données Gold calculées par Spark sont servies par l'API de manière sécurisée."

---

## 🎯 Conclusion pour le professeur
> "En résumé, cette architecture est **Robuste** (orchestration conteneurisée), **Scallable** (microservices indépendants pour Finance et Prescriptions), et surtout prête pour la production en termes de **Sécurité** (Zéro confiance, gestion des secrets par Vault Vault)."
