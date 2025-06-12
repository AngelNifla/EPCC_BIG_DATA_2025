-- Crear tabla de entrada
CREATE EXTERNAL TABLE  docs (line STRING);

-- Cargar el archivo local
LOAD DATA LOCAL INPATH './words.txt' OVERWRITE INTO TABLE docs;

-- Crear tabla para el conteo de palabras
CREATE TABLE  word_counts AS
SELECT w.word, COUNT(1) AS count
FROM (
    SELECT explode(split(line, ' ')) AS word
    FROM docs
) w
GROUP BY w.word
ORDER BY w.word;
