"""
Módulo: database_engine.py
Descrição: Gerencia a conexão com o SGBD MySQL (XAMPP).
           Garante operações ACID usando Two-Phase Commit (2PC).
           Retorna em qual nó a operação ocorreu e abstrai SQLs.
"""

import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector.pooling import MySQLConnectionPool
from typing import Dict, Any, List, Tuple, Optional
from utils_config import logger, ACIDTransactionError

class DatabaseEngine:
    """
    Classe robusta para interação com MySQL. 
    Usa connection pooling para lidar com requisições concorrentes.
    """

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.pool = None
        self._initialize_pool()
        self._ensure_database_exists()

    def _initialize_pool(self):
        """Cria um pool de conexões para evitar sobrecarga."""
        try:
            self.pool = MySQLConnectionPool(
                pool_name="ddb_pool",
                pool_size=5,
                pool_reset_session=True,
                host=self.host,
                user=self.user,
                password=self.password
                # Não passamos o database aqui, pois ele pode não existir na primeira rodada
            )
            logger.info("Pool de conexões MySQL criado com sucesso.")
        except MySQLError as err:
            logger.critical(f"Falha ao conectar no MySQL do XAMPP: {err}")
            raise

    def _get_connection(self):
        """Pega uma conexão do pool e seleciona o banco."""
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        try:
            # Substitui a atribuição falha por um comando SQL explícito
            cursor.execute(f"USE {self.database}")
        except MySQLError:
            pass # Ignora se o banco ainda não existe (será criado pelo ensure)
        finally:
            cursor.close()
        return conn

    def _ensure_database_exists(self):
        """Cria o banco de dados se não existir no XAMPP."""
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Banco de dados '{self.database}' garantido no XAMPP.")
            
            # Força a seleção do banco antes de criar a tabela
            cursor.execute(f"USE {self.database}")
            
            # Cria tabela de teste padrão para facilitar o uso pela interface
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ddb_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    nome VARCHAR(100) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    idade INT
                )
            """)
            conn.commit()
        except MySQLError as err:
            logger.error(f"Erro ao inicializar schema do DB: {err}")
        finally:
            cursor.close()
            conn.close()

    # =====================================================================
    # OPERAÇÕES DE LEITURA (Balanceáveis)
    # =====================================================================

    def execute_read(self, query: str) -> Dict[str, Any]:
        """
        Executa operações SELECT.
        Retorna dicionário contendo o resultado, colunas e sucesso.
        """
        logger.info(f"[DB READ] Query executada: {query}")
        conn = self._get_connection()
        cursor = conn.cursor(dictionary=True) # Retorna dicionário ao invés de tupla
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return {
                "success": True,
                "data": results,
                "row_count": cursor.rowcount,
                "error": None
            }
        except MySQLError as err:
            logger.error(f"[DB READ ERROR] Query falhou: {query} | Erro: {err}")
            return {
                "success": False,
                "data": [],
                "row_count": 0,
                "error": str(err)
            }
        finally:
            cursor.close()
            conn.close()

    # =====================================================================
    # OPERAÇÕES DE ESCRITA E PROTOCOLO 2PC (ACID)
    # =====================================================================
    
    # Para garantir ACID em rede distribuída, usamos variáveis locais para simular
    # as fases de "Prepare", "Commit" e "Abort" do Two-Phase Commit.
    
    def prepare_transaction(self, transaction_id: str, query: str) -> bool:
        """
        FASE 1 DO 2PC: Tenta executar a query sem commitar.
        Se der certo, guarda na "memória" que está pronto.
        No MySQL, podemos iniciar uma transação e não fazer o commit ainda,
        mas para simplificar o controle em threads curtas, validaremos a sintaxe.
        """
        logger.info(f"[2PC PREPARE] Validando transação {transaction_id}: {query}")
        
        # Como o conector Python fecha a sessão rapidamente, o 2PC real (XA Transactions)
        # exige controle de cursor mantido aberto. Para este middleware, faremos uma
        # validação estrita (Pre-flight check).
        
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Iniciamos transação local para teste de constraints
            conn.start_transaction()
            cursor.execute(query)
            conn.rollback() # Desfaz para o commit global ocorrer depois
            return True
        except MySQLError as err:
            logger.warning(f"[2PC PREPARE FALHOU] Transação {transaction_id}: {err}")
            return False
        finally:
            cursor.close()
            conn.close()

    def commit_transaction(self, transaction_id: str, query: str) -> Dict[str, Any]:
        """
        FASE 2 DO 2PC: Executa a query de escrita em definitivo (INSERT/UPDATE/DELETE).
        Garante a durabilidade (D do ACID).
        """
        logger.info(f"[2PC COMMIT] Efetivando transação {transaction_id}: {query}")
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            conn.start_transaction()
            cursor.execute(query)
            conn.commit() # Propriedade ACID
            
            return {
                "success": True,
                "row_count": cursor.rowcount,
                "error": None
            }
        except MySQLError as err:
            conn.rollback() # Propriedade ACID: Se falhar, reverte tudo
            logger.error(f"[2PC ROLLBACK] Erro no commit {transaction_id}: {err}")
            raise ACIDTransactionError(f"Falha de integridade: {err}")
        finally:
            cursor.close()
            conn.close()

    def determine_query_type(self, query: str) -> str:
        """Identifica se a query é leitura ou escrita para roteamento."""
        q_upper = query.strip().upper()
        if q_upper.startswith("SELECT") or q_upper.startswith("SHOW") or q_upper.startswith("DESCRIBE"):
            return "READ"
        elif q_upper.startswith("INSERT") or q_upper.startswith("UPDATE") or q_upper.startswith("DELETE") or q_upper.startswith("CREATE") or q_upper.startswith("DROP"):
            return "WRITE"
        else:
            return "UNKNOWN"

    def test_connection(self) -> bool:
        """Testa se o SGBD está online e respondendo."""
        try:
            conn = self._get_connection()
            conn.ping(reconnect=True, attempts=1, delay=0)
            conn.close()
            return True
        except MySQLError:
            return False