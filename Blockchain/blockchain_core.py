import hashlib
import json
import time
from typing import List, Dict, Any

class Transacao:
    """
    Representa uma transação entre dois nós na rede.
    (Atende aos requisitos da Semana 3: Implementação de transações)
    """
    def __init__(self, remetente: str, destinatario: str, valor: float):
        self.remetente = remetente
        self.destinatario = destinatario
        self.valor = valor
        self.timestamp = time.time()
        self.hash = self.calcular_hash()

    def calcular_hash(self) -> str:
        tx_string = f"{self.remetente}{self.destinatario}{self.valor}{self.timestamp}"
        return hashlib.sha256(tx_string.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'remetente': self.remetente,
            'destinatario': self.destinatario,
            'valor': self.valor,
            'timestamp': self.timestamp,
            'hash': self.hash
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Transacao':
        tx = cls(data['remetente'], data['destinatario'], data['valor'])
        tx.timestamp = data['timestamp']
        tx.hash = data['hash']
        return tx

class Bloco:
    """
    Representa um bloco individual na blockchain.
    (Atende aos requisitos da Semana 2: Estrutura de bloco)
    """
    def __init__(self, indice: int, transacoes: List[Transacao], timestamp: float, hash_anterior: str, nonce: int = 0):
        self.indice = indice
        self.transacoes = transacoes
        self.timestamp = timestamp
        self.hash_anterior = hash_anterior
        self.nonce = nonce
        self.hash = self.calcular_hash()

    def calcular_hash(self) -> str:
        """Calcula o hash SHA-256 do bloco."""
        bloco_string = json.dumps({
            'indice': self.indice,
            'transacoes': [tx.to_dict() for tx in self.transacoes],
            'timestamp': self.timestamp,
            'hash_anterior': self.hash_anterior,
            'nonce': self.nonce
        }, sort_keys=True)
        return hashlib.sha256(bloco_string.encode()).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'indice': self.indice,
            'transacoes': [tx.to_dict() for tx in self.transacoes],
            'timestamp': self.timestamp,
            'hash_anterior': self.hash_anterior,
            'nonce': self.nonce,
            'hash': self.hash
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Bloco':
        transacoes = [Transacao.from_dict(tx_data) for tx_data in data['transacoes']]
        bloco = cls(data['indice'], transacoes, data['timestamp'], data['hash_anterior'], data['nonce'])
        bloco.hash = data['hash']
        return bloco

class Blockchain:
    """
    Gerencia a cadeia de blocos, o pool de transações e o consenso.
    """
    def __init__(self):
        self.cadeia: List[Bloco] = []
        self.transacoes_pendentes: List[Transacao] = [] # Pool de transações (Semana 3)
        self.dificuldade = 4 # Número de zeros à esquerda exigidos no hash (Proof of Work)
        self.recompensa_mineracao = 50.0 # Recompensa por criar um bloco
        
        # Cria o bloco Gênesis (primeiro bloco)
        self.criar_bloco_genesis()

    def criar_bloco_genesis(self):
        """Cria o primeiro bloco da cadeia."""
        bloco_genesis = Bloco(0, [], time.time(), "0")
        bloco_genesis.hash = bloco_genesis.calcular_hash()
        self.cadeia.append(bloco_genesis)

    def obter_ultimo_bloco(self) -> Bloco:
        return self.cadeia[-1]

    def adicionar_transacao(self, transacao: Transacao) -> bool:
        """Adiciona uma transacao ao pool, se for válida."""
        # Em um sistema real, aqui verificaríamos assinaturas e saldo
        self.transacoes_pendentes.append(transacao)
        return True

    def minerar_bloco(self, endereco_minerador: str) -> Bloco:
        """
        Executa o Proof of Work para criar um novo bloco.
        (Atende aos requisitos da Semana 4: Proof of Work)
        """
        if not self.transacoes_pendentes:
            return None # Não mina blocos vazios para economizar processamento neste lab

        # Adiciona a transação de recompensa (coinbase)
        tx_recompensa = Transacao("SISTEMA", endereco_minerador, self.recompensa_mineracao)
        transacoes_bloco = self.transacoes_pendentes.copy()
        transacoes_bloco.insert(0, tx_recompensa)

        ultimo_bloco = self.obter_ultimo_bloco()
        novo_bloco = Bloco(
            indice=ultimo_bloco.indice + 1,
            transacoes=transacoes_bloco,
            timestamp=time.time(),
            hash_anterior=ultimo_bloco.hash
        )

        # Processo de Proof of Work
        print(f"Minerando bloco {novo_bloco.indice}...")
        novo_bloco.hash = novo_bloco.calcular_hash()
        while not novo_bloco.hash.startswith('0' * self.dificuldade):
            novo_bloco.nonce += 1
            novo_bloco.hash = novo_bloco.calcular_hash()

        print(f"Bloco minerado! Hash: {novo_bloco.hash}")
        
        self.cadeia.append(novo_bloco)
        self.transacoes_pendentes = [] # Limpa o pool de transações
        return novo_bloco

    def adicionar_bloco_externo(self, bloco: Bloco) -> bool:
        """
        Tenta adicionar um bloco recebido de outro nó.
        (Atende aos requisitos da Semana 4: Aceitação de blocos remotos)
        """
        ultimo_bloco = self.obter_ultimo_bloco()
        
        # Verifica se o bloco se encaixa na nossa cadeia
        if bloco.hash_anterior != ultimo_bloco.hash:
            return False
            
        # Verifica se o Proof of Work é válido
        if not bloco.hash.startswith('0' * self.dificuldade):
            return False
            
        # Verifica se o hash declarado confere com o cálculo
        if bloco.hash != bloco.calcular_hash():
            return False

        self.cadeia.append(bloco)
        
        # Remove do nosso pool as transações que já entraram neste bloco externo
        hashes_tx_bloco = [tx.hash for tx in bloco.transacoes]
        self.transacoes_pendentes = [tx for tx in self.transacoes_pendentes if tx.hash not in hashes_tx_bloco]
        
        return True

    def is_cadeia_valida(self, cadeia: List[Bloco]) -> bool:
        """
        Verifica a integridade de uma blockchain inteira.
        (Atende aos requisitos da Semana 2 e 5: Validação da cadeia)
        """
        for i in range(1, len(cadeia)):
            bloco_atual = cadeia[i]
            bloco_anterior = cadeia[i-1]

            if bloco_atual.hash != bloco_atual.calcular_hash():
                return False

            if bloco_atual.hash_anterior != bloco_anterior.hash:
                return False

            if not bloco_atual.hash.startswith('0' * self.dificuldade):
                return False

        return True

    def substituir_cadeia(self, nova_cadeia: List[Bloco]) -> bool:
        """
        Resolução de conflitos baseada na regra da 'cadeia mais longa'.
        (Atende aos requisitos da Semana 5: Resolução simples de conflitos)
        """
        if len(nova_cadeia) > len(self.cadeia) and self.is_cadeia_valida(nova_cadeia):
            print("Cadeia local substituída por uma cadeia mais longa e válida da rede.")
            self.cadeia = nova_cadeia
            # Em uma implementação perfeita, também reavaliaríamos o pool de transações aqui
            return True
        return False

    def calcular_saldo(self, endereco: str) -> float:
        """Calcula o saldo iterando sobre todo o histórico da blockchain."""
        saldo = 0.0
        for bloco in self.cadeia:
            for tx in bloco.transacoes:
                if tx.remetente == endereco:
                    saldo -= tx.valor
                if tx.destinatario == endereco:
                    saldo += tx.valor
                    
        # Considera também as transações pendentes para evitar gasto duplo
        for tx in self.transacoes_pendentes:
             if tx.remetente == endereco:
                    saldo -= tx.valor
                    
        return saldo

    def to_dict(self) -> Dict[str, Any]:
        return {
            'cadeia': [bloco.to_dict() for bloco in self.cadeia],
            'dificuldade': self.dificuldade
        }