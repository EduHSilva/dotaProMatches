# Projeto com dados do Dota2
## Engenharia de dados

Apresentação: https://drive.google.com/drive/folders/18CYcSgyMmJ72Yd1VUnte41dp70VE-p62

### Data raw

Dados brutos historicos de partidas e detalhes coletados a partir da API - https://www.opendota.com/

Armazenados em um banco nao relacional - Mongodb

### Comandos
docker exec -it dota-mongo python /root/dotaScience/hook/get_match_history.py --how newest