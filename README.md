# 2024M09-P2

# Objetivo

Conectar ao kafka, salvar a informação em algum lugar e realizar testes.

## Como rodar

Na pasta raiz, rode os comandos:

1. `cd src && docker compose up -d` e o docker compose ira subir. Lembre-se o conduktor deve ter sido inicado e cadastrado o cluster para facilitar a visualização.
2. Abra um novo terminal, para rodar os teste, um script foi criado, sendo assim, rodo os comandos: `chmod +x ./test.sh && ./test.sh`. Os teste serão iniciados e todos passaram.
3. Agora, para rodar o projeto, no terminal, rode os comanos: `chmod +x ./start.sh && ./start.sh`. Dessa forma, o script irá pegar as informações do json e pública-las, seguindo o fluxo proposto.

## Vídeo

https://drive.google.com/file/d/1IKrTL3ts4Ml_I8B9TVDd8eEqc67gnQTJ/view?usp=sharing