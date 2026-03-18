# Scénario de Démonstration : Architecture Microservices Sécurisée

Ce document décrit comment montrer que l'architecture fonctionne correctement, avec un focus sur la gestion des secrets via HashiCorp Vault.

## Objectif

Montrer que le code source ne contient aucun mot de passe en clair, et que les credentials PostgreSQL sont injectés dynamiquement au démarrage via Vault.

---

## Étape 1 : Démarrer l'infrastructure

```bash
docker-compose up -d --build
```

**Ce qu'on doit voir :**
- `scmd_vault` démarre et passe à l'état `Healthy`
- `scmd_vault_init` s'exécute une seule fois pour stocker les credentials PostgreSQL dans Vault (`vault kv put`)
- `finance_api` et `prescription_api` démarrent seulement après que Vault soit prêt

---

## Étape 2 : Vérifier que l'API récupère bien ses secrets

```bash
curl -s http://localhost:8001/health
```

**Réponse attendue :**
```json
{"status": "ok", "service": "finance_api", "db_initialized": true}
```

`db_initialized: true` confirme que l'API a bien contacté Vault au démarrage, récupéré les credentials depuis `secret/data/scmd/postgres`, et initialisé la connexion à la base de données. Aucun mot de passe n'est dans le code.

---

## Étape 3 : Tester la robustesse

Si on modifie ou supprime le secret dans Vault (pour simuler une rotation), l'API échoue au démarrage avec une `HTTPException(500)` au lieu d'essayer de se connecter avec de mauvais credentials. C'est volontaire — le service préfère ne pas démarrer plutôt que d'exposer une connexion non sécurisée.

---

## Conclusion

Les microservices ne connaissent pas les identifiants de la base de données avant leur exécution. Ça élimine le risque de fuite de credentials via le dépôt Git ou les images Docker.
