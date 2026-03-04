import socket
import threading
import json
import sys
import time
from typing import Set, Tuple
from blockchain_core import Blockchain, Bloco, Transacao

class P2PNode:
    """
    Gere a comunicação via Sockets (Semana 1) e sincronização (Semana 5).
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
        # 0.0.0.0 permite receber ligações de outras máquinas na mesma rede Wi-Fi/LAN
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        print(f"[*] Nó iniciado na porta {self.port} (A aceitar ligações externas) | Carteira: {self.carteira}")
        
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
                print(f"Erro ao aceitar ligação: {e}")

    def _lidar_cliente(self, client_socket: socket.socket, addr: Tuple[str, int]):
        """Processa as mensagens recebidas de outros nós."""
        try:
            dados_brutos = client_socket.recv(1024 * 1024).decode('utf-8') # Buffer de 1MB
            if not dados_brutos: return
            
            mensagem = json.loads(dados_brutos)
            tipo = mensagem.get('tipo')

            if tipo == 'HELLO':
                peer_port = mensagem.get('porta')
                if peer_port:
                    self.peers.add((addr[0], peer_port))
                    print(f"\n[Rede] Novo peer ligado: {addr[0]}:{peer_port}")
                
                # Envia a cadeia atual para o novo peer (Semana 5: Entrada tardia)
                self.enviar_mensagem(client_socket, {'tipo': 'SYNC_CADEIA', 'cadeia': self.blockchain.to_dict()['cadeia']})

            elif tipo == 'GET_PEERS':
                # Alguém está a pedir a nossa lista de contactos da rede
                lista_peers = [{'ip': p[0], 'porta': p[1]} for p in self.peers]
                self.enviar_mensagem(client_socket, {'tipo': 'PEER_LIST', 'peers': lista_peers})

            elif tipo == 'PEER_LIST':
                # Recebemos uma lista de novos nós da central
                peers_recebidos = mensagem.get('peers', [])
                novos_descobertos = 0
                for p in peers_recebidos:
                    ip_recebido = p.get('ip')
                    porta_recebida = p.get('porta')
                    if ip_recebido and porta_recebida:
                        # Evita adicionar a si mesmo
                        if (ip_recebido, porta_recebida) != (self.host, self.port) and (ip_recebido, porta_recebida) not in self.peers:
                            self.peers.add((ip_recebido, porta_recebida))
                            novos_descobertos += 1
                
                if novos_descobertos > 0:
                    print(f"\n[Rede] Descoberta: {novos_descobertos} novos nós adicionados à sua lista a partir da central!")

            elif tipo == 'SYNC_CADEIA':
                # Recebe uma blockchain de um peer e tenta resolver conflitos (Semana 5)
                cadeia_recebida = [Bloco.from_dict(b) for b in mensagem.get('cadeia')]
                if self.blockchain.substituir_cadeia(cadeia_recebida):
                    print("\n[Rede] Blockchain sincronizada com sucesso!")

            elif tipo == 'NOVA_TRANSACAO':
                # Semana 3: Propagação de transações
                tx = Transacao.from_dict(mensagem.get('transacao'))
                if not any(t.hash == tx.hash for t in self.blockchain.transacoes_pendentes):
                    print(f"\n[Rede] Recebida nova transação de {tx.valor} de {tx.remetente}")
                    self.blockchain.adicionar_transacao(tx)
                    self.transmitir(mensagem) # Repassa para a rede (gossip)

            elif tipo == 'NOVO_BLOCO':
                # Semana 4: Propagação de blocos
                bloco = Bloco.from_dict(mensagem.get('bloco'))
                if bloco.indice > self.blockchain.obter_ultimo_bloco().indice:
                    if self.blockchain.adicionar_bloco_externo(bloco):
                        print(f"\n[Rede] Novo bloco válido recebido e adicionado! Índice: {bloco.indice}")
                        self.transmitir(mensagem) # Repassa para a rede
                    else:
                        print("\n[Rede] Bloco recebido não encaixa. A solicitar sincronização completa...")

        except Exception as e:
            pass # Ignora erros silenciosamente
        finally:
            client_socket.close()

    def enviar_mensagem(self, sock: socket.socket, mensagem: dict):
        try:
            sock.sendall(json.dumps(mensagem).encode('utf-8'))
        except Exception:
            pass

    def conectar_peer(self, host: str, port: int):
        """Liga a um novo nó da rede lidando com códigos de outros grupos."""
        # Se for o próprio IP (0.0.0.0 ou 127.0.0.1) e a própria porta, ignora
        if host in ("127.0.0.1", "0.0.0.0", "localhost") and port == self.port: 
            return
        
        print(f"A tentar estabelecer ligação TCP com {host}:{port}...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0) 
            sock.connect((host, port))
            print(f"[OK] Ligação TCP estabelecida com {host}! A enviar handshake (HELLO)...")
            
            self.enviar_mensagem(sock, {'tipo': 'HELLO', 'porta': self.port})
            
            # A partir daqui, tenta aguardar a resposta (Timeout de Recv)
            try:
                resposta = sock.recv(1024 * 1024).decode('utf-8')
                if resposta:
                    # Em alguns casos a central pode enviar dois JSONs colados, tentamos contornar
                    try:
                        msg = json.loads(resposta)
                        if msg.get('tipo') == 'SYNC_CADEIA':
                            cadeia_recebida = [Bloco.from_dict(b) for b in msg.get('cadeia')]
                            self.blockchain.substituir_cadeia(cadeia_recebida)
                    except json.JSONDecodeError:
                        print(f"[Aviso] O peer {host} enviou uma resposta que não conseguiu ser lida à primeira (Múltiplos JSONs ou formato diferente).")
            except socket.timeout:
                print(f"[Aviso] O peer {host} ligou-se, mas não respondeu ao HELLO (Pode ser a Central a processar).")

            self.peers.add((host, port))
            print(f"Peer (Central/Nó) {host}:{port} adicionado à sua lista com sucesso!")
            sock.close()
            
            # Logo após ligar, tenta pedir a lista de peers conhecidos da rede
            self.pedir_peers_para_rede(host, port)
            
        except socket.timeout:
            print(f"Erro TCP: Falha ao ligar. O PC {host} está inacessível ou a Firewall está a bloquear.")
        except ConnectionRefusedError:
            print(f"Erro TCP: Ligação recusada. O PC {host} está online, mas a porta {port} está fechada.")
        except Exception as e:
            print(f"Falha ao ligar ao peer {host}:{port} - Detalhe: {e}")

    def pedir_peers_para_rede(self, host: str, port: int):
        """Pede a um nó específico (ou à central) a lista de quem mais está ligado"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            sock.connect((host, port))
            self.enviar_mensagem(sock, {'tipo': 'GET_PEERS'})
            
            resposta = sock.recv(1024 * 1024).decode('utf-8')
            if resposta:
                msg = json.loads(resposta)
                if msg.get('tipo') == 'PEER_LIST':
                    peers_recebidos = msg.get('peers', [])
                    novos = 0
                    for p in peers_recebidos:
                        ip = p.get('ip')
                        pt = p.get('porta')
                        if ip and pt and (ip, pt) != (self.host, self.port) and (ip, pt) not in self.peers:
                            self.peers.add((ip, pt))
                            novos += 1
                    if novos > 0:
                        print(f"\n[Descoberta] {novos} novos nós encontrados via {host}:{port}!")
            sock.close()
        except Exception:
            pass # Ignora se a central não suportar este comando

    def transmitir(self, mensagem: dict):
        """Envia uma mensagem para todos os peers conhecidos (Gossip Protocol)."""
        peers_inativos = set()
        for ip, porta in self.peers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((ip, porta))
                self.enviar_mensagem(sock, mensagem)
                sock.close()
            except Exception:
                peers_inativos.add((ip, porta))
        
        # Remove peers que não responderam
        self.peers -= peers_inativos

# ==========================================
# INTERFACE DE LINHA DE COMANDOS (MENU)
# ==========================================
def exibir_menu():
    print("\n" + "="*50)
    print("      SISTEMA DISTRIBUÍDO - CRIPTO")
    print("="*50)
    print("1. Ligar a um Peer / Central (IP:Porta)")
    print("2. Criar Transação")
    print("3. Minerar Bloco Pendente")
    print("4. Ver o Meu Saldo")
    print("5. Ver Blockchain Completa (Com Hashes)")
    print("6. Ver Peers Ligados")
    print("7. Receber Moedas Iniciais (Airdrop)")
    print("8. Atualizar Rede (Pedir lista de nós)")
    print("9. Ver Transações Pendentes (Pool)")
    print("0. Sair")
    print("="*50)

def iniciar_app():
    if len(sys.argv) < 3:
        print("Uso: python p2p_node.py <PORTA> <NOME_CARTEIRA>")
        sys.exit()

    porta_local = int(sys.argv[1])
    carteira = sys.argv[2]
    
    no = P2PNode("0.0.0.0", porta_local, carteira)
    no.iniciar()

    while True:
        exibir_menu()
        opcao = input("Escolha uma opção: ")

        if opcao == '1':
            ip_peer = input("IP do Peer / Central (ex: 10.25.149.238): ")
            if not ip_peer:
                print("IP inválido!")
                continue
            porta_peer = int(input("Porta: "))
            no.conectar_peer(ip_peer, porta_peer)

        elif opcao == '2':
            dest = input("Destinatário (Nome da Carteira): ")
            valor = float(input("Valor: "))
            
            saldo_atual = no.blockchain.calcular_saldo(no.carteira)
            if saldo_atual >= valor: 
                tx = Transacao(no.carteira, dest, valor)
                no.blockchain.adicionar_transacao(tx)
                print(f"Transação criada!\n -> Hash da TX: {tx.hash}")
                no.transmitir({'tipo': 'NOVA_TRANSACAO', 'transacao': tx.to_dict()})
            else:
                print("Saldo insuficiente para esta transação!")

        elif opcao == '3':
            if not no.blockchain.transacoes_pendentes:
                print("Não há transações na pool. Crie uma transação primeiro.")
            else:
                novo_bloco = no.blockchain.minerar_bloco(no.carteira)
                if novo_bloco:
                    no.transmitir({'tipo': 'NOVO_BLOCO', 'bloco': novo_bloco.to_dict()})

        elif opcao == '4':
            saldo = no.blockchain.calcular_saldo(no.carteira)
            print(f"Saldo atual da carteira '{no.carteira}': {saldo} moedas")

        elif opcao == '5':
            print("\n--- ESTADO DA BLOCKCHAIN ---")
            for bloco in no.blockchain.cadeia:
                print(f"\n[ Bloco {bloco.indice} ]")
                print(f"Hash Atual   : {bloco.hash}")
                print(f"Hash Anterior: {bloco.hash_anterior}")
                print(f"Nonce (PoW)  : {bloco.nonce}")
                print(f"Transações ({len(bloco.transacoes)}):")
                for tx in bloco.transacoes:
                    print(f"  -> TX Hash: {tx.hash}")
                    print(f"     De: {tx.remetente} | Para: {tx.destinatario} | Valor: {tx.valor}")
            print(f"\nTamanho Total da Cadeia: {len(no.blockchain.cadeia)}")

        elif opcao == '6':
            print(f"Peers Ligados ({len(no.peers)}):")
            for p in no.peers: print(f"- {p[0]}:{p[1]}")

        elif opcao == '7':
            print("\nA solicitar airdrop do sistema...")
            tx = Transacao("SISTEMA", no.carteira, 100.0)
            no.blockchain.adicionar_transacao(tx)
            print(f"Transação de Airdrop criada!\n -> Hash da TX: {tx.hash}")
            print("IMPORTANTE: Agora escolha a opção '3' para minerar e efetivar o seu saldo!")
            no.transmitir({'tipo': 'NOVA_TRANSACAO', 'transacao': tx.to_dict()})
            
        elif opcao == '8':
            if not no.peers:
                print("Ainda não está ligado a nenhum nó/central.")
            else:
                print("A sondar nós conhecidos em busca de novos peers...")
                peers_atuais = list(no.peers)
                for ip, porta in peers_atuais:
                    no.pedir_peers_para_rede(ip, porta)

        elif opcao == '9':
            print("\n--- TRANSAÇÕES PENDENTES (POOL) ---")
            if not no.blockchain.transacoes_pendentes:
                print("Nenhuma transação a aguardar mineração no momento.")
            else:
                for tx in no.blockchain.transacoes_pendentes:
                    print(f"-> TX Hash: {tx.hash}")
                    print(f"   De: {tx.remetente} | Para: {tx.destinatario} | Valor: {tx.valor}\n")

        elif opcao == '0':
            print("A encerrar o nó...")
            sys.exit()
        else:
            print("Opção inválida.")

        time.sleep(0.5)

if __name__ == "__main__":
    iniciar_app()