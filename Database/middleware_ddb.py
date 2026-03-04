import socket
import threading
import json
import hashlib
import time
import sys
import argparse
import mysql.connector
from mysql.connector import Error

# ==========================================
# CONFIGURAÇÕES GERAIS E PROTOCOLO
# ==========================================
def gerar_checksum(payload):
    """Gera um MD5 checksum baseado no dicionário de payload."""
    payload_str = json.dumps(payload, sort_keys=True)
    return hashlib.md5(payload_str.encode('utf-8')).hexdigest()

def enviar_mensagem(sock, tipo_msg, remetente_id, payload=None):
    """Formata e envia uma mensagem através do socket."""
    if payload is None:
        payload = {}
    
    msg = {
        "tipo": tipo_msg,
        "remetente_id": remetente_id,
        "payload": payload,
        "checksum": gerar_checksum(payload)
    }
    try:
        data = json.dumps(msg) + "\n"
        sock.sendall(data.encode('utf-8'))
    except Exception as e:
        pass # Ignora erros de rede silenciosamente para não floodar logs

def receber_mensagem(sock):
    """Lê uma mensagem do socket e valida o checksum."""
    try:
        data = sock.makefile('r').readline()
        if not data:
            return None
            
        msg = json.loads(data)
        
        checksum_recebido = msg.get("checksum")
        checksum_calculado = gerar_checksum(msg.get("payload", {}))
        
        if checksum_recebido != checksum_calculado:
            print("[Aviso] Checksum inválido recebido! Mensagem descartada.")
            return None
            
        return msg
    except Exception:
        return None

# ==========================================
# CLASSES DE BASE DE DADOS (COM PROTEÇÃO)
# ==========================================

class DBNodo:
    """Gere a ligação local ao MySQL do nó."""
    def __init__(self, host, user, password, database):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = None
        self.conectar()

    def conectar(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.conn.autocommit = False # Necessário para o Two-Phase Commit
            print("[DB] Ligação ao MySQL estabelecida com sucesso.")
        except Error as e:
            print(f"[DB] Erro ao ligar ao MySQL: {e}")
            self.conn = None

    def reconectar_se_necessario(self):
        if self.conn is None or not self.conn.is_connected():
            self.conectar()

    def executar_leitura(self, query):
        """Executa queries de leitura (SELECT)."""
        self.reconectar_se_necessario()
        # Validação extra para evitar AttributeError
        if self.conn is None or not self.conn.is_connected():
            return False, "Falha na ligação à Base de Dados local do Nó."
            
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(query)
            resultado = cursor.fetchall()
            cursor.close()
            return True, resultado
        except Error as e:
            return False, str(e)

    def iniciar_transacao(self, query):
        """Fase 1 do 2PC: Prepara a transação localmente."""
        self.reconectar_se_necessario()
        # Validação extra
        if self.conn is None or not self.conn.is_connected():
            return False, "Falha na ligação à Base de Dados local do Nó."
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            cursor.close()
            return True, "Preparado"
        except Error as e:
            if self.conn and self.conn.is_connected():
                self.conn.rollback()
            return False, str(e)

    def comitar_transacao(self):
        """Fase 2 do 2PC: Efetivação (Commit)."""
        try:
            if self.conn and self.conn.is_connected():
                self.conn.commit()
                return True
        except Error:
            pass
        return False

    def abortar_transacao(self):
        """Fase 2 do 2PC: Reversão (Rollback)."""
        try:
            if self.conn and self.conn.is_connected():
                self.conn.rollback()
                return True
        except Error:
            pass
        return False

# ==========================================
# CLASSE DO NÓ DO MIDDLEWARE (SERVIDOR/CLIENTE)
# ==========================================

class MiddlewareNodo:
    def __init__(self, id_nodo, ip, porto, lista_nodos, db_config):
        self.id_nodo = id_nodo
        self.ip = ip
        self.porto = porto
        self.lista_nodos = lista_nodos
        self.db = DBNodo(**db_config)
        
        self.coordenador_id = max(self.lista_nodos.keys())
        self.nodos_ativos = set(self.lista_nodos.keys())
        self.eleicao_em_curso = False
        self.round_robin_idx = 0
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.porto))
        self.server_socket.listen(10)
        
        self.lock = threading.Lock()

    def log(self, mensagem):
        print(f"[Nó {self.id_nodo}] {mensagem}")

    def iniciar(self):
        self.log(f"Iniciado no porto {self.porto}. Coordenador atual: Nó {self.coordenador_id}")
        threading.Thread(target=self._aceitar_ligacoes, daemon=True).start()
        threading.Thread(target=self._rotina_heartbeat, daemon=True).start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log("A encerrar...")
            self.server_socket.close()

    def enviar_para_nodo(self, destino_id, tipo_msg, payload=None, esperar_resposta=False):
        if destino_id not in self.lista_nodos:
            return None
        
        ip_dest, porto_dest = self.lista_nodos[destino_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((ip_dest, porto_dest))
            enviar_mensagem(s, tipo_msg, self.id_nodo, payload)
            
            resposta = None
            if esperar_resposta:
                resposta = receber_mensagem(s)
            s.close()
            return resposta
        except Exception:
            return None

    def enviar_multicast(self, tipo_msg, payload=None, nodos_destino=None, esperar_respostas=False):
        if nodos_destino is None:
            nodos_destino = [n for n in self.lista_nodos.keys() if n != self.id_nodo]
            
        respostas = {}
        for nid in nodos_destino:
            resp = self.enviar_para_nodo(nid, tipo_msg, payload, esperar_resposta=esperar_respostas)
            if esperar_respostas:
                respostas[nid] = resp
        return respostas

    def _aceitar_ligacoes(self):
        while True:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._tratar_cliente, args=(conn,), daemon=True).start()
            except Exception as e:
                self.log(f"Erro ao aceitar ligação: {e}")
                break

    def _tratar_cliente(self, conn):
        msg = receber_mensagem(conn)
        if not msg:
            conn.close()
            return
            
        tipo = msg.get("tipo")
        remetente_id = msg.get("remetente_id")
        payload = msg.get("payload", {})
        
        if tipo == "HEARTBEAT":
            self.nodos_ativos.add(remetente_id)
            enviar_mensagem(conn, "ACK", self.id_nodo)
            
        elif tipo == "ELEICAO":
            enviar_mensagem(conn, "OK", self.id_nodo)
            self.iniciar_eleicao()
            
        elif tipo == "COORDENADOR":
            self.coordenador_id = remetente_id
            self.eleicao_em_curso = False
            self.log(f"Novo coordenador aceite: Nó {remetente_id}")

        elif tipo == "QUERY_CLIENTE":
            self.log(f"Recebeu pedido do cliente: {payload.get('query')}")
            if self.id_nodo != self.coordenador_id:
                resp = self.enviar_para_nodo(self.coordenador_id, "QUERY_CLIENTE", payload, esperar_resposta=True)
                enviar_mensagem(conn, "RESULTADO", self.id_nodo, resp.get("payload") if resp else {"erro": "Falha na comunicação com o coordenador."})
            else:
                self._processar_query_coordenador(conn, payload.get('query'))

        elif tipo == "DB_LEITURA":
            self.log(f"[DB] Executar leitura localmente: {payload.get('query')}")
            sucesso, resultado = self.db.executar_leitura(payload.get('query'))
            enviar_mensagem(conn, "RESULTADO", self.id_nodo, {"sucesso": sucesso, "dados": resultado})
            
        elif tipo == "2PC_PREPARE":
            self.log(f"[2PC] Recebeu PREPARE para query: {payload.get('query')}")
            sucesso, msg_erro = self.db.iniciar_transacao(payload.get('query'))
            if sucesso:
                enviar_mensagem(conn, "2PC_READY", self.id_nodo)
            else:
                enviar_mensagem(conn, "2PC_ABORT", self.id_nodo, {"erro": msg_erro})
                
        elif tipo == "2PC_COMMIT":
            self.log("[2PC] Recebeu instrução de COMMIT.")
            self.db.comitar_transacao()
            enviar_mensagem(conn, "ACK", self.id_nodo)
            
        elif tipo == "2PC_ROLLBACK":
            self.log("[2PC] Recebeu instrução de ROLLBACK.")
            self.db.abortar_transacao()
            enviar_mensagem(conn, "ACK", self.id_nodo)

        conn.close()

    def _processar_query_coordenador(self, conn_cliente, query):
        q_upper = query.strip().upper()
        
        if q_upper.startswith("SELECT"):
            ativos_lista = list(self.nodos_ativos)
            if self.id_nodo not in ativos_lista: ativos_lista.append(self.id_nodo)
            ativos_lista.sort()
            
            nodo_escolhido = ativos_lista[self.round_robin_idx % len(ativos_lista)]
            self.round_robin_idx += 1
            
            self.log(f"Balanceamento de carga: Encaminhar SELECT para Nó {nodo_escolhido}")
            
            if nodo_escolhido == self.id_nodo:
                sucesso, resultado = self.db.executar_leitura(query)
                enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, {"sucesso": sucesso, "dados": resultado, "nodo_executor": self.id_nodo})
            else:
                resp = self.enviar_para_nodo(nodo_escolhido, "DB_LEITURA", {"query": query}, esperar_resposta=True)
                if resp:
                    resp_payload = resp.get("payload", {})
                    resp_payload["nodo_executor"] = nodo_escolhido
                    enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, resp_payload)
                else:
                    enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, {"sucesso": False, "dados": f"Nó remoto {nodo_escolhido} falhou ou rejeitou a query."})
                    
        elif q_upper.startswith(("INSERT", "UPDATE", "DELETE")):
            self.log(f"Iniciando 2PC para query: {query}")
            sucesso, msg = self._two_phase_commit(query)
            if sucesso:
                enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, {"sucesso": True, "dados": "Escrita replicada com sucesso.", "nodo_executor": "Coordenador"})
            else:
                enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, {"sucesso": False, "dados": f"Transação falhou (Rollback). Motivo: {msg}", "nodo_executor": "Coordenador"})
        else:
             enviar_mensagem(conn_cliente, "RESULTADO", self.id_nodo, {"sucesso": False, "dados": "Comando SQL não suportado ou em branco."})

    def _two_phase_commit(self, query):
        nodos_para_replicar = list(self.nodos_ativos)
        if self.id_nodo not in nodos_para_replicar: nodos_para_replicar.append(self.id_nodo)
        
        self.log("[2PC - Fase 1] Enviar PREPARE...")
        prontos = 0
        falhas = []
        msg_erro_global = ""
        
        # Local
        local_ok, msg_local = self.db.iniciar_transacao(query)
        if local_ok:
            prontos += 1
        else:
            falhas.append(self.id_nodo)
            msg_erro_global = msg_local
        
        # Remoto
        nodos_remotos = [n for n in nodos_para_replicar if n != self.id_nodo]
        respostas = self.enviar_multicast("2PC_PREPARE", {"query": query}, nodos_remotos, esperar_respostas=True)
        
        for nid, resp in respostas.items():
            if resp and resp.get("tipo") == "2PC_READY":
                prontos += 1
            else:
                falhas.append(nid)
                if resp and resp.get("payload", {}).get("erro"):
                    msg_erro_global = resp.get("payload").get("erro")
                
        # FASE 2
        if prontos == len(nodos_para_replicar) and len(nodos_para_replicar) > 0:
            self.log("[2PC - Fase 2] Todos prontos. Enviar COMMIT...")
            self.db.comitar_transacao()
            self.enviar_multicast("2PC_COMMIT", None, nodos_remotos, esperar_respostas=False)
            return True, ""
        else:
            self.log(f"[2PC - Fase 2] Falha detectada em {falhas}. Enviar ROLLBACK para garantir ACID...")
            self.db.abortar_transacao()
            self.enviar_multicast("2PC_ROLLBACK", None, nodos_remotos, esperar_respostas=False)
            return False, msg_erro_global

    def _rotina_heartbeat(self):
        while True:
            time.sleep(3)
            nodos_remotos = [n for n in self.lista_nodos.keys() if n != self.id_nodo]
            respostas = self.enviar_multicast("HEARTBEAT", None, nodos_remotos, esperar_respostas=True)
            
            with self.lock:
                self.nodos_ativos = {self.id_nodo}
                for nid, resp in respostas.items():
                    if resp and resp.get("tipo") == "ACK":
                        self.nodos_ativos.add(nid)
                        
                if self.coordenador_id not in self.nodos_ativos and self.coordenador_id != self.id_nodo:
                    if not self.eleicao_em_curso:
                        self.log(f"Coordenador {self.coordenador_id} inativo detectado! Iniciar eleição.")
                        self.iniciar_eleicao()

    def iniciar_eleicao(self):
        self.eleicao_em_curso = True
        nodos_maiores = [n for n in self.lista_nodos.keys() if n > self.id_nodo]
        
        alguem_respondeu = False
        if nodos_maiores:
            self.log(f"A enviar mensagem de eleição para nós com ID superior: {nodos_maiores}")
            respostas = self.enviar_multicast("ELEICAO", None, nodos_maiores, esperar_respostas=True)
            for nid, resp in respostas.items():
                if resp and resp.get("tipo") == "OK":
                    alguem_respondeu = True
                    break
                    
        if not alguem_respondeu:
            self.coordenador_id = self.id_nodo
            self.eleicao_em_curso = False
            self.log(f"Assumi a posição de Coordenador (Sou o maior ID ativo).")
            nodos_menores = [n for n in self.lista_nodos.keys() if n < self.id_nodo]
            self.enviar_multicast("COORDENADOR", None, nodos_menores, esperar_respostas=False)
        else:
            self.log("Nó com ID superior respondeu. A aguardar que ele assuma a coordenação.")

# ==========================================
# APLICAÇÃO CLIENTE INTERATIVA
# ==========================================

class ClienteDDB:
    def __init__(self, ip_nodo_contato, porto_nodo_contato):
        self.ip_contato = ip_nodo_contato
        self.porto_contato = porto_nodo_contato
        
    def executar(self):
        print("\n=== Cliente DDB MySQL (Middleware) ===")
        print("Digite a sua query SQL (ex: SELECT * FROM utilizadores). Digite 'sair' para terminar.")
        
        while True:
            query = input("\nSQL> ")
            if query.lower() == 'sair':
                break
            if not query.strip():
                continue
                
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)
                s.connect((self.ip_contato, self.porto_contato))
                
                enviar_mensagem(s, "QUERY_CLIENTE", 0, {"query": query})
                resp = receber_mensagem(s)
                s.close()
                
                if resp and resp.get("tipo") == "RESULTADO":
                    dados = resp.get("payload", {})
                    status = "SUCESSO" if dados.get("sucesso") else "ERRO"
                    print(f"[{status}] Executado pelo Nó: {dados.get('nodo_executor', 'Desconhecido')}")
                    
                    if type(dados.get("dados")) == list:
                        for row in dados.get("dados"):
                            print("  ->", row)
                    else:
                        print("  ->", dados.get("dados"))
                else:
                    print("[Erro] Resposta inválida ou nula do middleware.")
                    
            except Exception as e:
                print(f"[Erro de Ligação] Não foi possível conectar ao middleware em {self.ip_contato}:{self.porto_contato} ({e})")

# ==========================================
# PARSER DE ARGUMENTOS E MAIN
# ==========================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Middleware para Banco de Dados Distribuído")
    parser.add_argument("--modo", choices=["node", "client"], required=True, help="Executar como nó do DDB ou cliente interativo")
    
    parser.add_argument("--id", type=int, help="ID único do nó (1, 2, 3...)")
    parser.add_argument("--porto", type=int, help="Porto onde este nó vai escutar")
    parser.add_argument("--dbuser", default="root", help="Utilizador do MySQL")
    
    # Adicionado nargs='?' e const="" para tratar a omissão do argumento graciosamente no PowerShell
    parser.add_argument("--dbpass", nargs='?', const="", default="", help="Palavra-passe do MySQL (pode omitir se for vazia)")
    
    parser.add_argument("--host", default="127.0.0.1", help="IP do nó a contactar (Cliente)")
    
    args = parser.parse_args()
    
    TOPOLOGIA_NODOS = {
        1: ("127.0.0.1", 5001),
        2: ("127.0.0.1", 5002),
        3: ("127.0.0.1", 5003)
    }
    
    if args.modo == "node":
        if not args.id or not args.porto:
            print("Erro: O modo 'node' requer as flags --id e --porto.")
            sys.exit(1)
            
        db_config = {
            "host": "127.0.0.1",
            "user": args.dbuser,
            "password": args.dbpass if args.dbpass is not None else "",
            # Se for testar na mesma máquina, descomente a linha abaixo e comente a "ddb_test"
            "database": f"ddb_node{args.id}" 
            # "database": "ddb_test"
        }
        
        nodo = MiddlewareNodo(args.id, "0.0.0.0", args.porto, TOPOLOGIA_NODOS, db_config)
        nodo.iniciar()
        
    elif args.modo == "client":
        porto_contato = args.porto if args.porto else 5001
        cliente = ClienteDDB(args.host, porto_contato)
        cliente.executar()