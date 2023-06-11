# Projeto com dados do Dota2
## Engenharia de dados

### Data raw

Dados brutos historicos de partidas e detalhes coletados a partir da API - https://www.opendota.com/

Armazenados em um banco nao relacional - Mongodb

### Dados estruturados
Conversao dos dados armazenados no mongodb para uma estrutura tabelar e estruturada.

### Comandos
docker exec -it dota-mongo python /root/dotaScience/hook/get_match_history.py --how newest