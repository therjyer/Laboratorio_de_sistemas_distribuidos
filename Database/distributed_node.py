"""
Módulo: distributed_node.py
Descrição: O cérebro do middleware. Integra Redes e Banco de Dados.
           Implementa DDM homogêneo autônomo.
           Lida com Heartbeats, Eleição (Bully), Replicação (2PC) e Balanceamento de Carga.
"""

import threading
import time
import uuid
import random
from typing import Dict, Any
from utils_config import logger, NodeConfig
from network_core import NetworkServer, NetworkClient
from database_engine import DatabaseEngine

class NodeState:
    INIT = "INITIALIZING"
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    COORDINATOR = "COORDINATOR"

class DistributedNode:
    """
    Classe principal que orquestra todo o nó da rede distribuída.
    """
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.state = NodeState.INIT
        
        # Componentes
        self.db = DatabaseEngine(
            self.config.db_host, self.config.db_user,
            self.config.db_pass, self.config.db_name
        )
        self.net_server = NetworkServer(
            self.config.my_ip, self.config.my_port, self._message_router
        )
        self.net_client = NetworkClient(self.config.my_ip)
        
        # Estado de Rede
        self.coordinator_ip = None
        self.active_peers = {} # Dicionário: IP -> timestamp do último heartbeat
        self.election_in_progress = False
        
        # Callback para a GUI (Para atualizar painel)
        self.gui_update_callback = None

    def start_node(self):
        """Inicializa os serviços do nó."""
        self.net_server.start()
        self.state = NodeState.FOLLOWER
        
        # Threads de manutenção da rede
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_peers_loop, daemon=True).start()
        
        logger.info(f"Nó iniciado com sucesso. Estado: {self.state}")
        # Inicia uma eleição logo que liga para achar o coordenador
        self._start_election()

    def register_gui_callback(self, callback):
        """Associa uma função para atualizar a interface gráfica."""
        self.gui_update_callback = callback

    def _update_gui(self):
        """Chama o callback da GUI com os dados atuais do nó."""
        if self.gui_update_callback:
            status = {
                "ip": f"{self.config.my_ip}:{self.config.my_port}",
                "state": self.state,
                "coordinator": self.coordinator_ip,
                "active_peers": len(self.active_peers)
            }
            self.gui_update_callback(status)

    # =====================================================================
    # ROTEADOR DE MENSAGENS E PROTOCOLOS
    # =====================================================================

    def _message_router(self, message: Dict[str, Any]) -> Any:
        """
        Recebe requisições do NetworkServer e direciona para a função correta.
        """
        header = message.get('header', {})
        msg_type = header.get('type')
        sender = header.get('sender')
        payload = message.get('payload', {})
        
        # Atualiza tempo de vida do peer que enviou a mensagem
        self.active_peers[sender] = time.time()
        self._update_gui()

        # REQUISITO: Nó deve informar que recebeu e vai transmitir (via Log)
        if msg_type != "HEARTBEAT": # Ocultando heartbeat do log pra não poluir
            logger.info(f"[NETWORK] Recebido {msg_type} de {sender}")

        # Roteamento
        if msg_type == "HEARTBEAT":
            return self._handle_heartbeat(sender, payload)
        elif msg_type == "ELECTION":
            return self._handle_election(sender, payload)
        elif msg_type == "COORDINATOR_ANNOUNCE":
            return self._handle_coordinator_announce(sender, payload)
        elif msg_type == "EXECUTE_QUERY":
            return self._handle_client_query(sender, payload)
        elif msg_type == "2PC_PREPARE":
            return self._handle_2pc_prepare(payload)
        elif msg_type == "2PC_COMMIT":
            return self._handle_2pc_commit(payload)
            
        return None

    # =====================================================================
    # ALGORITMO DE ELEIÇÃO (BULLY ALGORITHM) E HEARTBEATS
    # =====================================================================

    def _heartbeat_loop(self):
        """Requisito: Informar periodicamente que estão ativos no DDB."""
        while True:
            try:
                if self.state != NodeState.INIT:
                    payload = {"id": self.config.node_id, "state": self.state}
                    self.net_client.send_broadcast(self.config.get_all_peers(), "HEARTBEAT", payload)
            except Exception as e:
                logger.error(f"Erro no loop de heartbeat: {e}")
            time.sleep(self.config.heartbeat_interval)

    def _handle_heartbeat(self, sender: str, payload: dict):
        """Processa heartbeat recebido."""
        # Se um nó disser que é o coordenador, e eu não tiver um, eu aceito
        if payload.get("state") == NodeState.COORDINATOR and not self.election_in_progress:
            if self.coordinator_ip != sender:
                self.coordinator_ip = sender
                logger.info(f"Coordenador atualizado via heartbeat: {sender}")
                self._update_gui()

    def _monitor_peers_loop(self):
        """Verifica se algum nó (especialmente o coordenador) parou de mandar heartbeat."""
        while True:
            time.sleep(self.config.heartbeat_interval + 2)
            current_time = time.time()
            dead_nodes = []
            
            for ip, last_seen in list(self.active_peers.items()):
                if current_time - last_seen > (self.config.heartbeat_interval * 2.5):
                    logger.warning(f"Peer {ip} considerado MORTO por timeout.")
                    dead_nodes.append(ip)
            
            for dead_ip in dead_nodes:
                if dead_ip in self.active_peers:
                    del self.active_peers[dead_ip]
                # Se o coordenador morreu, requisita nova eleição!
                if dead_ip == self.coordinator_ip:
                    logger.critical("O Coordenador falhou! Iniciando algoritmo de eleição.")
                    self.coordinator_ip = None
                    self._start_election()
                    
            self._update_gui()

    def _start_election(self):
        """
        Implementa o Algoritmo Bully. 
        Nó envia ELECTION para todos com ID MAIOR que o seu.
        Se ninguém responder, ele vira o Coordenador.
        """
        if self.election_in_progress:
            return
            
        self.election_in_progress = True
        self.state = NodeState.CANDIDATE
        logger.info(f"Iniciando eleição. Meu ID: {self.config.node_id}")
        
        higher_nodes = [p for p in self.config.get_all_peers() if p['id'] > self.config.node_id]
        
        if not higher_nodes:
            # Eu sou o maior ID, eu ganho a eleição
            self._become_coordinator()
        else:
            # Envia ELECTION para nós maiores. Se algum responder (retornar True), eu perco e viro FOLLOWER.
            election_won = True
            for peer in higher_nodes:
                responded = self.net_client.send_unicast(peer['ip'], peer['port'], "ELECTION", {"id": self.config.node_id}, wait_response=True)
                if responded:
                    logger.info(f"Nó maior ({peer['ip']}) respondeu. Eu me rendo a ser FOLLOWER.")
                    election_won = False
                    self.state = NodeState.FOLLOWER
                    break
                    
            if election_won:
                # Ninguém maior respondeu
                self._become_coordinator()
                
        self.election_in_progress = False
        self._update_gui()

    def _become_coordinator(self):
        """Assume o cargo de Coordenador da rede."""
        self.state = NodeState.COORDINATOR
        self.coordinator_ip = self.config.my_ip
        logger.info("Fui eleito o novo COORDENADOR da rede DDB!")
        # Avisa todos (Broadcast)
        self.net_client.send_broadcast(
            self.config.get_all_peers(), 
            "COORDINATOR_ANNOUNCE", 
            {"coordinator_ip": self.config.my_ip}
        )
        self._update_gui()

    def _handle_election(self, sender: str, payload: dict):
        """Recebeu mensagem de eleição de outro nó."""
        sender_id = payload.get("id", 0)
        # Bully: Se meu ID é maior, eu digo "Pode parar, eu sou maior" e inicio minha eleição
        if self.config.node_id > sender_id:
            # Responde OK (bloqueia a vitória do menor) e starta a sua eleição em thread
            threading.Thread(target=self._start_election).start()
            return {"status": "OK_I_AM_BIGGER"}
        return None

    def _handle_coordinator_announce(self, sender: str, payload: dict):
        """Aceita o novo coordenador eleito."""
        self.coordinator_ip = payload.get("coordinator_ip")
        self.state = NodeState.FOLLOWER
        self.election_in_progress = False
        logger.info(f"Novo coordenador aceito: {self.coordinator_ip}")
        self._update_gui()

    # =====================================================================
    # EXECUÇÃO E REPLICAÇÃO DE QUERIES (BALANCEAMENTO / ACID)
    # =====================================================================

    def submit_query(self, query: str) -> Dict[str, Any]:
        """
        Função chamada pela interface do usuário local.
        Manda a query para o coordenador ou executa direto se for o coordenador.
        """
        query_type = self.db.determine_query_type(query)
        
        # Log obrigatório: informar queries requisitadas
        logger.info(f"Nova requisição de Query submetida: {query[:50]}...")
        
        if self.state == NodeState.COORDINATOR:
            # Eu sou coordenador, eu gerencio
            return self._handle_client_query(self.config.my_ip, {"query": query, "type": query_type})
        else:
            # Sou seguidor, repasso pro coordenador
            if not self.coordinator_ip:
                return {"success": False, "error": "Nenhum coordenador ativo na rede no momento."}
                
            # Acha a porta do coordenador
            coord_port = next((p['port'] for p in self.config.get_all_peers() if p['ip'] == self.coordinator_ip), self.config.my_port)
            
            response = self.net_client.send_unicast(
                self.coordinator_ip, coord_port, 
                "EXECUTE_QUERY", {"query": query, "type": query_type}, wait_response=True
            )
            return response if response else {"success": False, "error": "Falha ao contactar coordenador."}

    def _handle_client_query(self, sender: str, payload: dict) -> Dict[str, Any]:
        """
        (Apenas no Coordenador) Decide o que fazer com a query.
        """
        query = payload.get("query")
        q_type = payload.get("type")
        
        if q_type == "READ":
            # REQUISITO: Garantir que não haja sobrecarga (Balanceamento de Carga)
            # Roteia SELECTS para um nó aleatório ativo, ou executa localmente
            available_nodes = list(self.active_peers.keys())
            available_nodes.append(self.config.my_ip) # Inclui a si mesmo
            target_ip = random.choice(available_nodes)
            
            if target_ip == self.config.my_ip:
                logger.info(f"[LOAD BALANCE] Executando LEITURA localmente.")
                res = self.db.execute_read(query)
                res["exec_node"] = self.config.my_ip
                return res
            else:
                logger.info(f"[LOAD BALANCE] Encaminhando LEITURA para {target_ip}.")
                port = next((p['port'] for p in self.config.get_all_peers() if p['ip'] == target_ip), 5000)
                res = self.net_client.send_unicast(target_ip, port, "EXECUTE_QUERY", {"query": query, "type": "READ_LOCAL"}, wait_response=True)
                return res

        elif q_type == "READ_LOCAL":
            # Recebeu do coordenador para executar
            res = self.db.execute_read(query)
            res["exec_node"] = self.config.my_ip
            return res
            
        elif q_type == "WRITE":
            # REQUISITO: Todas as alterações devem ser replicadas e usar ACID (Two-Phase Commit)
            return self._execute_2pc_write(query)

    def _execute_2pc_write(self, query: str) -> Dict[str, Any]:
        """Executa protocolo Two-Phase Commit para Replicação."""
        transaction_id = str(uuid.uuid4())
        logger.info(f"[2PC] Iniciando fase 1 para escrita: TxID {transaction_id}")
        
        peers = self.config.get_all_peers()
        prepare_payload = {"tx_id": transaction_id, "query": query}
        
        # Fase 1: Prepare
        prepare_oks = 0
        for peer in peers:
            res = self.net_client.send_unicast(peer['ip'], peer['port'], "2PC_PREPARE", prepare_payload, wait_response=True)
            if res and res.get("status") == "READY":
                prepare_oks += 1
                
        # Conta a si mesmo
        local_ready = self.db.prepare_transaction(transaction_id, query)
        if local_ready: prepare_oks += 1
        
        total_nodes = len(peers) + 1
        
        if prepare_oks == total_nodes:
            # Fase 2: Commit (Todos prontos)
            logger.info(f"[2PC] Todos prontos! Iniciando Fase 2 (COMMIT).")
            self.net_client.send_broadcast(peers, "2PC_COMMIT", {"tx_id": transaction_id, "query": query})
            local_res = self.db.commit_transaction(transaction_id, query)
            local_res["exec_node"] = "ALL_NODES (Replicated)"
            return local_res
        else:
            # Abort (Falha ACID)
            logger.warning(f"[2PC] Abortando! Apenas {prepare_oks}/{total_nodes} prontos.")
            return {"success": False, "error": "Falha na transação distribuída (ACID Abort). Nem todos os nós responderam prontos.", "exec_node": "NONE"}

    def _handle_2pc_prepare(self, payload: dict) -> dict:
        """Recebe ordem de preparação."""
        ready = self.db.prepare_transaction(payload["tx_id"], payload["query"])
        return {"status": "READY" if ready else "ABORT"}

    def _handle_2pc_commit(self, payload: dict) -> dict:
        """Recebe ordem de commit definitivo."""
        self.db.commit_transaction(payload["tx_id"], payload["query"])
        return {"status": "COMMITTED"}