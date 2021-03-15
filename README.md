# BusinessRules - Kaedin Schouten

Voor deze opdracht heb ik een regel gemaakt die een bepaalde product matched met andere. Alle producten die bij elkaar in een bestelling hebben gezeten worden met elkaar vergeleken en gekeken of de `sub_sub_categorieen` overeenkomen. Deze producten die overeenkomen worden in een lijst gestopt en vervolges in een nieuwe table in de database gezet. Dit process wordt gedaan in 5 stappen:

### Stap 1:
  Eerst wordt een table gemaakt voor onze database zodat we daar alle data naar heen kunnen pushen. De table heeft 2 kolommen:
  1. Product_id (string. Omdat jammer genoeg sommige producten tekst in hun id hebben)
  2. Recommendations (lijst met strings, wat dus ids zijn van de recommended producten)
  
  Hierbij is product id een primary key én een foreign key van de table `products -> (id)`

### Stap 2
  Database connectie openen.

### Stap 3:
  Nu moeten we beginnen met de data ophalen. We beginnen met van alle producten hun ids ophalen ALS zei een `sub_sub_categorie` hebben. Dit wordt gedaan met deze query: 
  `select p.product_id, pc.sub_sub_category from products p inner join product_categories pc on pc.product_id = p.product_id where sub_sub_category is not null;` 
  
  Deze van deze producten worden hun ids in een lijst gestopt.

### Stap :
  Hier loopen we door elke id in de lijst van stap 2. En roepen we de functie `retrieve_recommendations` aan met als argument(en) de `product_id` en evenueel een `list_limit` van recommendations (die staat standaard op 4).
  
  In deze functie wordt deze query aangeroepen: `select pio.product_id , count(pio.product_id) as p_count from product_in_order pio inner join product_categories pc on pc.product_id = pio.product_id where session_id in (select session_id from product_in_order pio2 where product_id like '{product_id}') and pio.product_id not like '{product_id}' and pc.sub_sub_category not in ( select sub_sub_category from product_categories pc2 where product_id like '{product_id}') and sub_sub_category is not null group by pio.product_id, pc.sub_sub_category order by p_count desc limit {list_limit};`
  
  wat in essentie een lijst retourneerd met 4 producten, hun ids en het aantal keer voorgekomen (`count`) in dezelfde bestelling(en) (hieruit weten we welke producten het meest voorkomt met het opgegeven product).
  
### Stap 5:
  De lijst die we van stap 3 krijgen inserten wij nu in de database m.b.v de functie `fill_recommendations` die de argumenten `product_id`(iteratie van de for-loop) en `recommendations`(resultaat stap 3).
  
### Stap 6:
  Database connectie sluiten.
