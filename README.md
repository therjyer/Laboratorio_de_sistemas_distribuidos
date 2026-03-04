Passo 1: Instalar o MSYS2

    Vá a msys2.org e descarregue o instalador (msys2-x86_64-xxxxxxxx.exe).

    Instale com as opções por defeito (geralmente em C:\msys64).

Passo 2: Instalar as ferramentas (GCC e GTK3)

    No menu Iniciar do Windows, procure por "MSYS2 UCRT64" (ou "MSYS2 MinGW 64-bit") e abra este terminal específico.

    Atualize o sistema de pacotes colando este comando (e pressionando Y quando for pedido):
    Bash

    pacman -Syu
       *(Pode pedir para fechar a janela, feche, abra de novo o UCRT64 e repita o comando).*

    Agora, instale o GCC, o GTK3 e o Make correndo este comando:
    Bash

    pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-gtk3 mingw-w64-ucrt-x86_64-pkg-config make

Passo 3: Compilar e Executar

    Navegue no terminal do MSYS2 para a pasta onde guardou estes 6 ficheiros que gerei (exemplo: cd /c/Users/SeuNome/Desktop/TrabalhoSockets).

    Execute o comando de compilação:
    Bash

    make
       *(Isto vai gerar dois ficheiros no Windows: `server.exe` e `client.exe`)*.

    Num terminal, execute o servidor:
    Bash

    ./server.exe
    4. Numa nova aba de terminal MSYS2 UCRT64, execute o seu cliente gráfico:
    ```bash
    ./client.exe

O código utilizará agora a API nativa do Winsock2 e de Threads do Windows em vez de POSIX, permitindo estabilidade de execução num ambiente nativo Windows.