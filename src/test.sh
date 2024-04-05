#!/bin/bash

# Inicializa um novo módulo Go
go mod init paho-go

# Limpa o módulo baixando as dependências necessárias
go mod tidy

go test
