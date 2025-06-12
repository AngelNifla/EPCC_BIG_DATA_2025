-- Crear tabla de visitas
CREATE TABLE visits (
    `usuario` STRING,
    `page` STRING,
    `time` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

-- Crear tabla de páginas con su ranking
CREATE TABLE pages (
    `page` STRING,
    `rank` FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

-- Cargar archivos
LOAD DATA LOCAL INPATH 'visits.txt' OVERWRITE INTO TABLE visits;
LOAD DATA LOCAL INPATH 'pages.txt' OVERWRITE INTO TABLE pages;

-- Páginas con ranking mayor a 0.5
CREATE TABLE high_ranked_pages AS
SELECT * FROM pages WHERE rank > 0.5;

-- Visitas buenas por usuario
CREATE TABLE visitas_buenas AS
SELECT v.usuario, COUNT(*) AS total_visitas_buenas
FROM visits v
JOIN high_ranked_pages p ON v.page = p.page
GROUP BY v.usuario;

-- Promedio de visitas buenas
SELECT AVG(total_visitas_buenas) AS promedio_buenas
FROM visitas_buenas;

