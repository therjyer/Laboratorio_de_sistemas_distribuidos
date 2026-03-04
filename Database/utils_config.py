"""
Módulo: utils_config.py
Descrição: Responsável por gerenciar configurações globais, logs do sistema,
           exceções personalizadas e funções utilitárias como geração de Checksum.
           Este arquivo garante que todos os nós operem com os mesmos parâmetros base.
"""

import logging
import hashlib
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

# =====================================================================
# 1. EXCEÇÕES PERSONALIZADAS DO MIDDLEWARE
# =====================================================================

class DDBBaseException(Exception):
    """Classe base para todas as exceções do Banco de Dados Distribuído."""
    def __init__(self, message: str, code: int = 500):
        super().__init__(message)
        self.code = code
        self.message = message

class ChecksumMismatchError(DDBBaseException):
    """Lançada quando a integridade dos dados recebidos via socket é violada."""
    def __init__(self, expected: str, received: str):
        msg = f"Falha de Integridade: Checksum esperado {expected}, recebido {received}."
        super().__init__(msg, 400)

class NodeOfflineError(DDBBaseException):
    """Lançada quando há tentativa de comunicação com um nó inativo."""
    def __init__(self, node_ip: str):
        msg = f"Nó Inacessível: O nó {node_ip} não está respondendo."
        super().__init__(msg, 404)

class ACIDTransactionError(DDBBaseException):
    """Lançada quando ocorre falha nas propriedades ACID (ex: falha no commit)."""
    def __init__(self, details: str):
        msg = f"Falha de Transação ACID: {details}"
        super().__init__(msg, 503)

class CoordinatorElectionError(DDBBaseException):
    """Lançada durante anomalias na eleição do coordenador."""
    def __init__(self, details: str):
        msg = f"Erro na Eleição: {details}"
        super().__init__(msg, 502)

# =====================================================================
# 2. GERENCIADOR DE LOGS (LOGGING ENGINE)
# =====================================================================

class DDBLogger:
    """
    Sistema avançado de logging para rastrear todas as queries,
    erros, heartbeats e informações transmitidas na rede.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DDBLogger, cls).__new__(cls)
            cls._instance._setup_logger()
        return cls._instance

    def _setup_logger(self):
        """Configura a saída de log para console e arquivo rotativo."""
        self.logger = logging.getLogger("DDB_Middleware")
        self.logger.setLevel(logging.DEBUG)
        
        # Formatação do Log
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | [%(filename)s:%(lineno)d] | %(message)s'
        )
        
        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO) # No console mostra INFO pra cima
        
        # File Handler (cria pasta de logs se não existir)
        if not os.path.exists("logs"):
            os.makedirs("logs")
            
        file_handler = logging.FileHandler(
            f"logs/ddb_node_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG) # No arquivo salva tudo
        
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    @classmethod
    def get_logger(cls) -> logging.Logger:
        """Retorna a instância do logger."""
        return cls().logger

# Instância global do logger
logger = DDBLogger.get_logger()

# =====================================================================
# 3. ALGORITMOS DE INTEGRIDADE (CHECKSUM)
# =====================================================================

class IntegrityManager:
    """
    Gerencia a criação e verificação de checksums para garantir que pacotes
    enviados via socket não sofram alterações na rede.
    """
    
    @staticmethod
    def generate_checksum(data: Any) -> str:
        """
        Gera um hash SHA-256 a partir dos dados fornecidos.
        
        Args:
            data (Any): Dados a serem convertidos (string, dict, list).
            
        Returns:
            str: Hash hexadecimal SHA-256.
        """
        try:
            if isinstance(data, (dict, list, tuple)):
                # Ordena as chaves para garantir que o hash seja consistente
                data_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
            else:
                data_str = str(data)
                
            hash_object = hashlib.sha256(data_str.encode('utf-8'))
            return hash_object.hexdigest()
        except Exception as e:
            logger.error(f"Erro ao gerar checksum: {e}")
            raise

    @staticmethod
    def verify_checksum(data: Any, received_checksum: str) -> bool:
        """
        Verifica se os dados correspondem ao checksum recebido.
        
        Args:
            data (Any): O payload dos dados.
            received_checksum (str): O checksum informado pelo remetente.
            
        Raises:
            ChecksumMismatchError: Se a integridade estiver comprometida.
            
        Returns:
            bool: True se for válido.
        """
        calculated_checksum = IntegrityManager.generate_checksum(data)
        if calculated_checksum != received_checksum:
            logger.warning(f"Aviso de integridade! Calc: {calculated_checksum} | Recv: {received_checksum}")
            raise ChecksumMismatchError(calculated_checksum, received_checksum)
        return True

# =====================================================================
# 4. GERENCIADOR DE CONFIGURAÇÃO (NODE CONFIG)
# =====================================================================

class NodeConfig:
    """
    Mantém as configurações em memória do nó atual. 
    Permite configuração dinâmica via IPs.
    """
    
    def __init__(self):
        # Identificação do nó
        self.my_ip: str = "127.0.0.1"
        self.my_port: int = 5000
        self.node_id: int = 1  # Usado para eleição (Bully Algorithm)
        
        # Lista de IPs dos outros nós (DDB exige pelo menos 3)
        self.peer_nodes: list[Dict[str, Any]] = []
        
        # Configuração MySQL
        self.db_host: str = "127.0.0.1"
        self.db_user: str = "root"
        self.db_pass: str = ""
        self.db_name: str = "ddb_test"
        
        # Tempos e Timeouts
        self.heartbeat_interval: int = 5 # segundos
        self.socket_timeout: int = 3 # segundos
        self.election_timeout: int = 7 # segundos
        
    def load_from_file(self, filepath: str = "config.json"):
        """Carrega configurações de um arquivo JSON (Opcional)."""
        if not os.path.exists(filepath):
            logger.warning(f"Arquivo de configuração {filepath} não encontrado. Usando defaults.")
            return
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            self.my_ip = data.get("my_ip", self.my_ip)
            self.my_port = data.get("my_port", self.my_port)
            self.node_id = data.get("node_id", self.node_id)
            self.peer_nodes = data.get("peer_nodes", self.peer_nodes)
            self.db_host = data.get("db_host", self.db_host)
            self.db_user = data.get("db_user", self.db_user)
            self.db_pass = data.get("db_pass", self.db_pass)
            self.db_name = data.get("db_name", self.db_name)
            
            logger.info("Configurações carregadas com sucesso.")
            
        except Exception as e:
            logger.error(f"Erro ao ler {filepath}: {e}")

    def add_peer(self, ip: str, port: int, node_id: int):
        """Adiciona um nó à rede DDB."""
        for peer in self.peer_nodes:
            if peer['ip'] == ip and peer['port'] == port:
                return # Já existe
        self.peer_nodes.append({"ip": ip, "port": port, "id": node_id})
        logger.info(f"Peer adicionado: {ip}:{port} (ID: {node_id})")

    def remove_peer(self, ip: str):
        """Remove um nó em caso de falha definitiva."""
        self.peer_nodes = [p for p in self.peer_nodes if p['ip'] != ip]
        logger.warning(f"Peer removido da lista de comunicação: {ip}")

    def get_all_peers(self) -> list:
        """Retorna lista de peers."""
        return self.peer_nodes

    def print_config(self):
        """Exibe no log a configuração atual."""
        logger.info("--- Configuração Atual do Nó ---")
        logger.info(f"Meu IP/Porta : {self.my_ip}:{self.my_port}")
        logger.info(f"Meu Node ID  : {self.node_id}")
        logger.info(f"MySQL DB     : {self.db_name}@{self.db_host}")
        logger.info(f"Total Peers  : {len(self.peer_nodes)}")
        for p in self.peer_nodes:
            logger.info(f"  -> Peer ID {p['id']} em {p['ip']}:{p['port']}")
        logger.info("--------------------------------")

# Fim do arquivo utils_config.py