# HAI708I - Big Data

## TP Hadoop

Ce TP a pour objectif de vous familiariser avec les plateformes de Big Data afin de comprendre les principes fondamentaux du fonctionnement de ces systèmes reposant sur une parallélisation massive.

En réalisant ce TP, vous serez mieux préparés à tirer pleinement parti des séminaires industriels sur le Big Data, dans lesquels ces concepts sont évoqués.


## Ressources

- [MR1] MapReduce: Simplified Data Processing on Large Clusters - Jeffrey Dean and Sanjay Ghemawat
- [MR2] Apache Hadoop http://hadoop.apache.org/
- [MR3] Hadoop: the definitive guide (http://grut-computing.com/HadoopBook.pdf)

## Avant commencer

La programmation en Map-Reduce utilise le langage Java - avec ses avantages et inconvénients. Il est ainsi indispensable d'effectuer ce travail en binômes, de rester bien concentrés pour bien comprendre la cause des bogues (souvent ce seront des problèmes de typage ou nommage des ressources).  

## Exercices de préparation - Partie 1

### Exercice 0 - WordCount
Tester le programme WordCount.

### Exercice 1 - WordCount + Filter
Modifier la fonction reduce du programme WordCount.java pour afficher uniquement les mots ayant un nombre d’occurrences supérieur ou égal à deux.

### Exercice 2 - Group-By
Implémenter un opérateur de regroupement sur l'attribut `Customer-ID` dans GroupBy.java.  
Les données sont dans `input-groupBy` et doivent calculer le total des profits (`Profit`) par client.

### Exercice 3 - Group-By
Modifier le programme précédent :
1. Calculer les ventes par `Date` et `State`.
2. Calculer les ventes par `Date` et `Category`.
3. Calculer par commande :
   - Le nombre de produits distincts achetés.
   - Le nombre total d'exemplaires.

### Exercice 4 - Join
Créer une classe Join.java pour joindre les informations des clients et commandes dans `input-join`.  
Restituer les couples `(CUSTOMERS.name, ORDERS.comment)`.

**Note :** Copier les valeurs de l'itérateur dans un tableau temporaire et utiliser deux boucles imbriquées pour effectuer la jointure.


## Exercices - Partie 2  

À l'aide de map/reduce, implementer trois (3) requêtes analytiques proposées pour le premier datamart (aspect principal) de votre projet et deux (2) requêtes analytiques proposées pour le deuxième datamart (aspect secondaire).

Vous pouvez rapidement extraire vos données de votre instance Oracle avec les commandes suivantes que vous pouvez adapter pour vos tables. 

```sql
-- ouvrir la connexion
SET MARKUP CSV ON;

-- répeter pour chaque table à exporter
SPOOL change_this_table_name.csv;
SELECT * FROM change_this_table_name;

-- dernière commande avant de fermer la connexion
SPOOL OFF;
```

### Exercice 5 - GroupBy + Join

Pour le fichier `superstore.csv`, calculer le montant total des achats faits par chaque client.  
**Le programme doit restituer des couples** `(CUSTOMERS.name, SUM(totalprice))`.

---

### Exercice 6 - Suppression des doublons (DISTINCT)

Donner la liste des clients (sans doublons) présents dans le dataset du répertoire `input-groupBy`.

---

### Exercice 7 - MR <-> SQL

Donner le code SQL équivalent aux traitements Map/Reduce implémentés pour les questions 4, 5 et 6.

### Exercice 9 - Tri

Hadoop trie les clés des groupes en ordre lexicographique ascendant pendant la phase de shuffling. Modifier la méthode de tri.  
1. Trier les commandes clients du fichier `superstore.csv` par date d’expédition en ordre croissant, puis décroissant.  
2. Trier les clients (identifiant + nom) par profit généré.

---

### Exercice 10 - Requêtes Top-k

Modifier la classe `TopkWordCount.java` pour répondre aux requêtes suivantes :  
1. Les k premières lignes triées par profit (ordre décroissant).  
2. Les k premiers clients en termes de profit réalisé (ordre décroissant).

