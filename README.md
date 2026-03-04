# Laboratório de Sistemas Distribuídos

Este repositório contém implementações práticas desenvolvidas para o estudo de Sistemas Distribuídos. O projeto está dividido em três módulos independentes, cada um focado num conceito fundamental da computação distribuída: comunicação via Sockets, Bases de Dados Distribuídas e Blockchain P2P.

## Estrutura do Repositório

### Sockets: Comunicação Cliente-Servidor desenvolvida em C.

### Database: Sistema de base de dados e nós distribuídos em Python (integrado com MySQL via XAMPP).

### Blockchain: Implementação de uma rede Peer-to-Peer e estrutura de Blockchain em Python.

## Pré-requisitos Gerais

Antes de executar os projetos, certifique-se de que tem as seguintes ferramentas instaladas no seu ambiente:

- Python 3.x (Para os módulos Database e Blockchain)

- XAMPP (Para o módulo Database)

- GCC / MinGW ou um ambiente Windows padrão (Para executar ou recompilar os binários do módulo Sockets)

## Módulo 1: Sockets

Este módulo demonstra a comunicação em rede utilizando a arquitetura clássica Cliente-Servidor desenvolvida na linguagem C.

### Instruções de Execução:

Os ficheiros executáveis (.exe) já se encontram compilados para o sistema operativo Windows.

Abra um terminal na pasta Sockets.

### Inicie o Servidor primeiro:

    ./server.exe

(O servidor ficará a aguardar conexões).

### Inicie o Cliente:

Abra um novo terminal (ou vários, caso pretenda testar a concorrência) e execute:

    ./client.exe


Nota para programadores: Caso pretenda alterar o código-fonte em C (server_main.c, client_gui.c, etc.) e recompilar, utilize o comando make no terminal (é estritamente necessário ter o utilitário make e um compilador C devidamente configurados no seu PATH).

## Módulo 2: Base de Dados Distribuída (com XAMPP)

Este módulo simula um ambiente de base de dados distribuída utilizando a linguagem Python. Recorre ao XAMPP para gerir a base de dados MySQL localmente e dispõe de uma interface gráfica (app_gui.py).

Procedimento de Configuração e Execução:

1. Configuração do XAMPP (MySQL):

- Abra o painel de controlo do XAMPP.

- Inicie os serviços Apache e MySQL.

- Aceda ao PHPMyAdmin no seu navegador através da ligação: http://localhost/phpmyadmin/.

- Crie uma base de dados conforme as especificações no ficheiro utils_config.py (verifique o código-fonte para obter o nome exato da base de dados, utilizador e palavra-passe requeridos).

2. Instalação das dependências (Python):
Abra o terminal na diretoria Database e instale as bibliotecas necessárias. Em regra, projetos desta natureza exigem conectores MySQL e bibliotecas de interface gráfica:

    pip install mysql-connector-python 


3. Execução do sistema:

Para iniciar a Interface Principal e o motor da base de dados:

    python app_gui.py


Para iniciar Nós Distribuídos adicionais (simulando outras máquinas na rede):
Abra novos terminais e execute:

    python distributed_node.py


Os registos de operação (logs) serão guardados de forma automática na pasta logs/.

## Módulo 3: Blockchain P2P

Este módulo apresenta uma implementação de raiz em Python de uma Blockchain a operar sobre uma rede Peer-to-Peer (P2P).

### Instruções de Execução:

O propósito deste módulo é executar múltiplos nós simultaneamente para que comuniquem entre si e sincronizem a cadeia de blocos.

Abra um terminal na pasta Blockchain.

### Inicie o primeiro nó da rede P2P:

    python p2p_node.py


Para simular uma rede distribuída real, abra outros terminais e execute o mesmo comando para iniciar novos nós. Estes deverão descobrir-se mutuamente e iniciar a partilha da estrutura de dados definida em blockchain_core.py.

## Licença

Este projeto é disponibilizado sob a licença MIT. O código pode ser utilizado, modificado e distribuído livremente para fins educacionais e de investigação.