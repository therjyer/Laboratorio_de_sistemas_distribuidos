import socket
import threading
import json
import sys
import time
from typing import Set, Tuple
from blockchain_core import Blockchain, Bloco, Transacao

class P2PNode:
    """
    Gerencia a comunicação via Sockets (Semana 1) e sincronização (Semana 5).
    """
    def __init__(self, host: str, port: int, carteira: str):
        self.host = host
        self.port = port
        self.carteira = carteira # Identificador deste nó na rede
        self.blockchain = Blockchain()
        self.peers: Set[Tuple[str, int]] = set() # Conjunto de (ip, porta)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def iniciar(self):
        """Inicia a thread do servidor TCP."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        print(f"[*] Nó iniciado em {self.host}:{self.port} | Carteira: {self.carteira}")
        
        thread_servidor = threading.Thread(target=self._aceitar_conexoes)
        thread_servidor.daemon = True
        thread_servidor.start()

    def _aceitar_conexoes(self):
        while True:
            try:
                client_socket, addr = self.server_socket.accept()
                thread_cliente = threading.Thread(target=self._lidar_cliente, args=(client_socket, addr))
                thread_cliente.daemon = True
                thread_cliente.start()
            except Exception as e:
                print(f"Erro ao aceitar conexão: {e}")

    def _lidar_cliente(self, client_socket: socket.socket, addr: Tuple[str, int]):
        """Processa as mensagens recebidas de outros nós."""
        try:
            dados_brutos = client_socket.recv(1024 * 1024).decode('utf-8') # Buffer de 1MB
            if not dados_brutos: return
            
            mensagem = json.loads(dados_brutos)
            tipo = mensagem.get('tipo')

            if tipo == 'HELLO':
                peer_port = mensagem.get('porta')
                self.peers.add((addr[0], peer_port))
                print(f"\n[Rede] Novo peer conectado: {addr[0]}:{peer_port}")
                # Envia a cadeia atual para o novo peer (Semana 5: Entrada tardia)
                self.enviar_mensagem(client_socket, {'tipo': 'SYNC_CADEIA', 'cadeia': self.blockchain.to_dict()['cadeia']})

            elif tipo == 'SYNC_CADEIA':
                # Recebe uma blockchain de um peer e tenta resolver conflitos (Semana 5)
                cadeia_recebida = [Bloco.from_dict(b) for b in mensagem.get('cadeia')]
                if self.blockchain.substituir_cadeia(cadeia_recebida):
                    print("\n[Rede] Blockchain sincronizada com sucesso!")

            elif tipo == 'NOVA_TRANSACAO':
                # Semana 3: Propagação de transações
                tx = Transacao.from_dict(mensagem.get('transacao'))
                # Verifica se já temos essa transação para evitar loop infinito
                if not any(t.hash == tx.hash for t in self.blockchain.transacoes_pendentes):
                    print(f"\n[Rede] Recebida nova transação de {tx.valor} de {tx.remetente}")
                    self.blockchain.adicionar_transacao(tx)
                    self.transmitir(mensagem) # Repassa para a rede (gossip)

            elif tipo == 'NOVO_BLOCO':
                # Semana 4: Propagação de blocos
                bloco = Bloco.from_dict(mensagem.get('bloco'))
                # Verifica se não temos o bloco
                if bloco.indice > self.blockchain.obter_ultimo_bloco().indice:
                    if self.blockchain.adicionar_bloco_externo(bloco):
                        print(f"\n[Rede] Novo bloco válido recebido e adicionado! Índice: {bloco.indice}")
                        self.transmitir(mensagem) # Repassa para a rede
                    else:
                        # Se não encaixou, possivelmente estamos desatualizados. Pedimos a cadeia inteira.
                        print("\n[Rede] Bloco recebido não encaixa. Solicitando sincronização completa...")
                        # Apenas como simplificação, pediremos pro próximo bloco que chegar

        except Exception as e:
            pass # Ignora erros de parse silenciosamente no lab
        finally:
            client_socket.close()

    def enviar_mensagem(self, sock: socket.socket, mensagem: dict):
        try:
            sock.sendall(json.dumps(mensagem).encode('utf-8'))
        except Exception:
            pass

    def conectar_peer(self, host: str, port: int):
        """Conecta a um novo nó da rede."""
        if (host, port) == (self.host, self.port): return
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            self.enviar_mensagem(sock, {'tipo': 'HELLO', 'porta': self.port})
            
            # Aguarda a resposta (sincronização inicial)
            resposta = sock.recv(1024 * 1024).decode('utf-8')
            if resposta:
                msg = json.loads(resposta)
                if msg.get('tipo') == 'SYNC_CADEIA':
                    cadeia_recebida = [Bloco.from_dict(b) for b in msg.get('cadeia')]
                    self.blockchain.substituir_cadeia(cadeia_recebida)

            self.peers.add((host, port))
            print(f"Conectado com sucesso ao peer {host}:{port}")
            sock.close()
        except Exception as e:
            print(f"Falha ao conectar ao peer {host}:{port} - {e}")

    def transmitir(self, mensagem: dict):
        """Envia uma mensagem para todos os peers conhecidos (Gossip Protocol)."""
        peers_inativos = set()
        for ip, porta in self.peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                sock.connect((ip, porta))
                self.enviar_mensagem(sock, mensagem)
                sock.close()
            except Exception:
                peers_inativos.add((ip, porta))
        
        # Remove peers que não responderam
        self.peers -= peers_inativos

# ==========================================
# INTERFACE DE LINHA DE COMANDO (MENU)
# ==========================================
def exibir_menu():
    print("\n" + "="*40)
    print("      SISTEMA DISTRIBUÍDO - CRIPTO")
    print("="*40)
    print("1. Conectar a um Peer (IP:Porta)")
    print("2. Criar Transação")
    print("3. Minerar Bloco Pendente")
    print("4. Ver Meu Saldo")
    print("5. Ver Blockchain Completa")
    print("6. Ver Peers Conectados")
    print("7. Receber Moedas Iniciais (Airdrop)")
    print("0. Sair")
    print("="*40)

def iniciar_app():
    if len(sys.argv) < 3:
        print("Uso: python p2p_node.py <PORTA> <NOME_CARTEIRA>")
        sys.exit()

    porta_local = int(sys.argv[1])
    carteira = sys.argv[2]
    
    no = P2PNode("127.0.0.1", porta_local, carteira)
    no.iniciar()

    while True:
        exibir_menu()
        opcao = input("Escolha uma opção: ")

        if opcao == '1':
            ip_peer = input("IP do Peer (ex: 127.0.0.1): ") or "127.0.0.1"
            porta_peer = int(input("Porta do Peer: "))
            no.conectar_peer(ip_peer, porta_peer)

        elif opcao == '2':
            dest = input("Destinatário (Nome da Carteira): ")
            valor = float(input("Valor: "))
            
            saldo_atual = no.blockchain.calcular_saldo(no.carteira)
            if saldo_atual >= valor: 
                tx = Transacao(no.carteira, dest, valor)
                no.blockchain.adicionar_transacao(tx)
                print(f"Transação criada! Hash: {tx.hash[:10]}...")
                # Propaga a transação na rede
                no.transmitir({'tipo': 'NOVA_TRANSACAO', 'transacao': tx.to_dict()})
            else:
                print("Saldo insuficiente para esta transação!")

        elif opcao == '3':
            if not no.blockchain.transacoes_pendentes:
                print("Não há transações no pool. Crie uma transação primeiro.")
            else:
                novo_bloco = no.blockchain.minerar_bloco(no.carteira)
                if novo_bloco:
                    # Propaga o novo bloco na rede
                    no.transmitir({'tipo': 'NOVO_BLOCO', 'bloco': novo_bloco.to_dict()})

        elif opcao == '4':
            saldo = no.blockchain.calcular_saldo(no.carteira)
            print(f"Saldo atual da carteira '{no.carteira}': {saldo} moedas")

        elif opcao == '5':
            print("\n--- ESTADO DA BLOCKCHAIN ---")
            for bloco in no.blockchain.cadeia:
                print(f"Bloco {bloco.indice} | Hash: {bloco.hash[:15]}... | Hash Ant: {bloco.hash_anterior[:15]}... | TXs: {len(bloco.transacoes)}")
            print(f"Tamanho da Cadeia: {len(no.blockchain.cadeia)}")

        elif opcao == '6':
            print(f"Peers Conectados ({len(no.peers)}):")
            for p in no.peers: print(f"- {p[0]}:{p[1]}")

        elif opcao == '7':
            print("\nSolicitando airdrop do sistema...")
            tx = Transacao("SISTEMA", no.carteira, 100.0)
            no.blockchain.adicionar_transacao(tx)
            print(f"Transação de Airdrop criada! Hash: {tx.hash[:10]}...")
            print("IMPORTANTE: Agora escolha a opção '3' para minerar e efetivar seu saldo!")
            no.transmitir({'tipo': 'NOVA_TRANSACAO', 'transacao': tx.to_dict()})

        elif opcao == '0':
            print("Encerrando nó...")
            sys.exit()
        else:
            print("Opção inválida.")

        time.sleep(0.5)

if __name__ == "__main__":
    iniciar_app()