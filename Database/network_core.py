"""
Módulo: network_core.py
Descrição: Gerencia toda a comunicação via sockets TCP entre os nós do DDB.
           Implementa o protocolo de mensagens com verificação de checksum,
           comunicação Unicast (para um nó) e Broadcast (para todos).
"""

import socket
import threading
import json
import time
from typing import Dict, Any, Callable, Optional, Tuple
from utils_config import logger, IntegrityManager, ChecksumMismatchError

class NetworkProtocol:
    """
    Define o formato da mensagem transmitida na rede.
    Toda mensagem contém cabeçalhos (tipo, remetente), payload (dados) e checksum.
    """
    
    @staticmethod
    def build_message(msg_type: str, sender_ip: str, payload: Any) -> bytes:
        """
        Constrói uma mensagem segura com checksum pronta para envio via socket.
        """
        checksum = IntegrityManager.generate_checksum(payload)
        
        message_dict = {
            "header": {
                "type": msg_type,
                "sender": sender_ip,
                "timestamp": time.time()
            },
            "payload": payload,
            "checksum": checksum
        }
        
        # Converte para string JSON e depois para bytes
        json_str = json.dumps(message_dict, ensure_ascii=False)
        # Prefixa com o tamanho da mensagem (para lidar com mensagens longas no socket)
        msg_bytes = json_str.encode('utf-8')
        length_prefix = f"{len(msg_bytes):08d}".encode('utf-8')
        
        return length_prefix + msg_bytes

    @staticmethod
    def parse_message(conn: socket.socket) -> Optional[Dict[str, Any]]:
        """
        Lê e decodifica uma mensagem do socket de forma segura.
        Resolve problemas de fragmentação de pacotes TCP.
        """
        try:
            # Lê os primeiros 8 bytes que contêm o tamanho da mensagem
            length_prefix = conn.recv(8)
            if not length_prefix:
                return None
                
            msg_length = int(length_prefix.decode('utf-8'))
            
            # Lê o restante da mensagem baseado no tamanho
            chunks = []
            bytes_recd = 0
            while bytes_recd < msg_length:
                chunk = conn.recv(min(msg_length - bytes_recd, 4096))
                if chunk == b'':
                    raise RuntimeError("Conexão com socket quebrada prematuramente")
                chunks.append(chunk)
                bytes_recd = bytes_recd + len(chunk)
                
            full_msg = b''.join(chunks).decode('utf-8')
            message_dict = json.loads(full_msg)
            
            # Verificação do Checksum (Garante integridade pedida nos requisitos)
            payload = message_dict.get('payload', {})
            received_checksum = message_dict.get('checksum', '')
            IntegrityManager.verify_checksum(payload, received_checksum)
            
            return message_dict
            
        except ChecksumMismatchError as ce:
            logger.error(str(ce))
            return None
        except Exception as e:
            logger.debug(f"Erro ao parsear mensagem recebida: {e}")
            return None

class NetworkServer:
    """
    Servidor TCP que escuta conexões de outros nós.
    Rodando em uma thread separada para não bloquear a aplicação.
    """
    
    def __init__(self, host: str, port: int, message_handler: Callable):
        self.host = host
        self.port = port
        self.message_handler = message_handler # Callback para processar a msg
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.is_running = False
        self.thread = None

    def start(self):
        """Inicia o servidor em uma thread em background."""
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.is_running = True
            logger.info(f"Servidor de rede iniciado em {self.host}:{self.port}")
            
            self.thread = threading.Thread(target=self._listen_loop, daemon=True)
            self.thread.start()
        except Exception as e:
            logger.error(f"Falha ao iniciar NetworkServer: {e}")

    def _listen_loop(self):
        """Loop infinito aceitando conexões."""
        while self.is_running:
            try:
                # Timeout curto para permitir verificar self.is_running
                self.server_socket.settimeout(1.0) 
                try:
                    conn, addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                    
                # Inicia uma thread para lidar com o cliente específico
                client_thread = threading.Thread(
                    target=self._handle_client, 
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
            except Exception as e:
                if self.is_running:
                    logger.error(f"Erro no loop do servidor: {e}")

    def _handle_client(self, conn: socket.socket, addr: Tuple[str, int]):
        """Lê a mensagem do cliente e envia para o callback."""
        # logger.debug(f"Conexão recebida de {addr[0]}:{addr[1]}")
        try:
            message = NetworkProtocol.parse_message(conn)
            if message:
                # Executa a função do Middleware para tratar a mensagem
                response_payload = self.message_handler(message)
                
                # Se o handler retornar algo, envia de volta (para operações síncronas)
                if response_payload is not None:
                    response_bytes = NetworkProtocol.build_message(
                        msg_type="RESPONSE",
                        sender_ip=self.host,
                        payload=response_payload
                    )
                    conn.sendall(response_bytes)
        except Exception as e:
            logger.error(f"Erro ao tratar cliente {addr[0]}: {e}")
        finally:
            conn.close()

    def stop(self):
        """Encerra o servidor de forma limpa."""
        self.is_running = False
        self.server_socket.close()
        if self.thread:
            self.thread.join(timeout=2)
        logger.info("Servidor de rede desligado.")

class NetworkClient:
    """
    Responsável por enviar mensagens para outros nós (Unicast ou Broadcast).
    """
    
    def __init__(self, my_ip: str):
        self.my_ip = my_ip

    def send_unicast(self, target_ip: str, target_port: int, msg_type: str, payload: Any, wait_response: bool = False) -> Optional[Any]:
        """
        Envia uma mensagem para um único nó específico.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3.0) # 3 segundos de timeout
        
        try:
            sock.connect((target_ip, target_port))
            msg_bytes = NetworkProtocol.build_message(msg_type, self.my_ip, payload)
            
            # Requisito: Log do tamanho e conteúdo transmitido
            logger.debug(f"[TX] Unicast para {target_ip}:{target_port} | Tipo: {msg_type} | Tamanho: {len(msg_bytes)} bytes")
            
            sock.sendall(msg_bytes)
            
            if wait_response:
                response = NetworkProtocol.parse_message(sock)
                if response:
                    return response.get('payload')
            return True
            
        except socket.timeout:
            logger.warning(f"Timeout ao conectar com {target_ip}:{target_port}")
            return False
        except ConnectionRefusedError:
            logger.warning(f"Conexão recusada por {target_ip}:{target_port} (Nó offline?)")
            return False
        except Exception as e:
            logger.error(f"Erro de envio unicast para {target_ip}: {e}")
            return False
        finally:
            sock.close()

    def send_broadcast(self, peers: list, msg_type: str, payload: Any):
        """
        Simula um envio Broadcast iterando sobre a lista de peers conhecidos.
        Executa em threads para não bloquear e simular simultaneidade.
        """
        logger.debug(f"[TX] Broadcast de {msg_type} para {len(peers)} nós.")
        
        def _send_task(peer):
            self.send_unicast(peer['ip'], peer['port'], msg_type, payload, wait_response=False)

        threads = []
        for peer in peers:
            t = threading.Thread(target=_send_task, args=(peer,))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()