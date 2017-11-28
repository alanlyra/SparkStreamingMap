select codigo, datahora, linha, velocidade
from StreamingLoop
where
sqrt( pow(latitude - -22.876830,2) + pow(longitude - -43.337424,2) ) <= 500
and velocidade > 0
