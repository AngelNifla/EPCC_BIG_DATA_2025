-- Crear tabla del log con nombres escapados
CREATE TABLE log_usuarios (
    `user` STRING,
    `time` STRING,
    query STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

-- Cargar los datos
LOAD DATA LOCAL INPATH './log.txt' OVERWRITE INTO TABLE log_usuarios;

-- Crear tabla con total de visitas por usuario
CREATE TABLE entradas_por_usuario AS
SELECT `user`, COUNT(*) AS total_visitas
FROM log_usuarios
GROUP BY `user`;

-- Calcular el promedio de visitas por usuario
SELECT AVG(total_visitas) AS promedio_visitas
FROM entradas_por_usuario;

