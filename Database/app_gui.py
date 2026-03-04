"""
Módulo: app_gui.py
Descrição: Interface gráfica do utilizador (GUI) responsiva e elegante.
           Requisito: Aplicação simples para executar queries mostrando retorno 
           e o nó que executou.
           Desenvolvido com tkinter e ttk para compatibilidade nativa (não precisa pip install).
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import json
import socket
from utils_config import NodeConfig, logger
from distributed_node import DistributedNode

class DDBAppGUI:
    """Interface principal do Middleware de Base de Dados Distribuída."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("DDB Middleware - Sistema Distribuído")
        self.root.geometry("1000x700")
        self.root.minsize(800, 600)
        
        # Tema e Estilização
        self._apply_styles()
        
        # Variáveis de sistema
        self.config = NodeConfig()
        self.node = None
        
        # Construção da Interface
        self._build_ui()
        
        # Captura de log para a UI
        self._setup_log_capture()
        
        # Inicialização do nó automaticamente (usando portas simuladas)
        self._auto_start_node()

    def _apply_styles(self):
        """Define cores, fontes e aparência responsiva usando ttk.Style."""
        style = ttk.Style()
        style.theme_use('clam') # Tema mais limpo e moderno
        
        # Cores modernas (Dark Mode adaptado)
        bg_color = "#2E3440"
        fg_color = "#D8DEE9"
        accent_color = "#5E81AC"
        success_color = "#A3BE8C"
        
        self.root.configure(bg=bg_color)
        
        style.configure("TFrame", background=bg_color)
        style.configure("TLabel", background=bg_color, foreground=fg_color, font=("Segoe UI", 10))
        style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"), foreground=accent_color)
        style.configure("Status.TLabel", font=("Segoe UI", 11, "bold"), foreground=success_color)
        
        style.configure("TButton", 
                        font=("Segoe UI", 10, "bold"), 
                        background=accent_color, 
                        foreground="white",
                        padding=6)
        style.map("TButton", background=[('active', '#81A1C1')])

        style.configure("Treeview", 
                        background="#3B4252", 
                        foreground="white", 
                        rowheight=25,
                        fieldbackground="#3B4252")
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"), background="#4C566A", foreground="white")

    def _build_ui(self):
        """Constrói o grid principal e adiciona os painéis."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Layout principal: Topo (Status), Esquerda (Queries), Direita (Logs)
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=(0, 10))
        
        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        # --- TOP FRAME: STATUS DO NÓ ---
        ttk.Label(top_frame, text="Painel de Controlo DDB", style="Header.TLabel").pack(side=tk.LEFT)
        
        self.lbl_node_status = ttk.Label(top_frame, text="Status: A iniciar...", style="Status.TLabel")
        self.lbl_node_status.pack(side=tk.RIGHT)
        
        self.lbl_coordinator = ttk.Label(top_frame, text="Coordenador: A aguardar", foreground="#EBCB8B", font=("Segoe UI", 10, "bold"))
        self.lbl_coordinator.pack(side=tk.RIGHT, padx=20)
        
        # --- LEFT FRAME: SQL e Resultados ---
        left_frame = ttk.Frame(content_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        ttk.Label(left_frame, text="Editor de Query SQL:", style="Header.TLabel").pack(anchor=tk.W)
        
        self.txt_query = scrolledtext.ScrolledText(left_frame, height=5, font=("Consolas", 11), bg="#ECEFF4", fg="#2E3440")
        self.txt_query.pack(fill=tk.X, pady=(5, 10))
        self.txt_query.insert(tk.END, "SELECT * FROM ddb_users;")
        
        btn_run = ttk.Button(left_frame, text="Executar Query", command=self.execute_query)
        btn_run.pack(anchor=tk.E, pady=(0, 10))
        
        ttk.Label(left_frame, text="Resultados:", style="Header.TLabel").pack(anchor=tk.W)
        
        # Tabela de Resultados
        self.tree_results = ttk.Treeview(left_frame, show="headings", height=8)
        self.tree_results.pack(fill=tk.BOTH, expand=True, pady=(5,0))
        
        self.lbl_exec_node = ttk.Label(left_frame, text="Nó Executor: N/A", foreground="#B48EAD", font=("Segoe UI", 10, "italic"))
        self.lbl_exec_node.pack(anchor=tk.W, pady=5)

        # --- RIGHT FRAME: Logs do Sistema ---
        right_frame = ttk.Frame(content_frame, width=350)
        right_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=(5, 0))
        right_frame.pack_propagate(False) # Mantém tamanho fixo da largura
        
        ttk.Label(right_frame, text="Registo de Atividades da Rede:", style="Header.TLabel").pack(anchor=tk.W)
        
        self.txt_log = scrolledtext.ScrolledText(right_frame, font=("Consolas", 9), bg="#434C5E", fg="#D8DEE9")
        self.txt_log.pack(fill=tk.BOTH, expand=True, pady=(5,0))
        self.txt_log.config(state=tk.DISABLED)

    def _setup_log_capture(self):
        """Redireciona os logs do logger global para a interface."""
        import logging
        
        class UIHandler(logging.Handler):
            def __init__(self, text_widget):
                super().__init__()
                self.text_widget = text_widget
                self.setFormatter(logging.Formatter('%(message)s'))

            def emit(self, record):
                msg = self.format(record)
                def append():
                    self.text_widget.config(state=tk.NORMAL)
                    self.text_widget.insert(tk.END, msg + "\n")
                    self.text_widget.see(tk.END)
                    self.text_widget.config(state=tk.DISABLED)
                self.text_widget.after(0, append)
                
        ui_handler = UIHandler(self.txt_log)
        logger.addHandler(ui_handler)

    def _auto_start_node(self):
        """Configura dinamicamente portas para poder rodar múltiplos nós no mesmo PC."""
        def find_free_port():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', 0))
                return s.getsockname()[1]
                
        my_port = find_free_port()
        self.config.my_port = my_port
        # Gera ID aleatório para a eleição Bully
        import random
        self.config.node_id = random.randint(1000, 9999) 
        
        # Simulação: Como estamos no mesmo IP (localhost), precisamos descobrir outros nós
        # Na prática, leríamos de um arquivo ou usaríamos IPs diferentes.
        # Aqui, vamos adicionar manualmente portas base para simular os "3 nós diferentes" exigidos.
        base_ports = [5001, 5002, 5003]
        for p in base_ports:
            if p != my_port:
                self.config.add_peer("127.0.0.1", p, p) # Usando porta como ID temporário no peer
                
        logger.info(f"Interface iniciada. Nó configurado para porta {my_port}")
        
        # Inicializa o Nó em thread separada
        self.node = DistributedNode(self.config)
        self.node.register_gui_callback(self.update_status_ui)
        
        # Inicia a thread do nó
        threading.Thread(target=self.node.start_node, daemon=True).start()

    def update_status_ui(self, status_dict):
        """Atualiza os labels de status com base no callback do nó."""
        # Usa after para garantir segurança de thread no Tkinter
        def _update():
            state = status_dict.get('state')
            coord = status_dict.get('coordinator')
            peers = status_dict.get('active_peers')
            
            self.lbl_node_status.config(text=f"Status: {state} | Peers: {peers}")
            
            if state == "COORDINATOR":
                self.lbl_coordinator.config(text="Coordenador: EU MESMO", foreground="#A3BE8C")
            else:
                self.lbl_coordinator.config(text=f"Coordenador: {coord if coord else 'Nenhum'}", foreground="#EBCB8B")
                
        self.root.after(0, _update)

    def execute_query(self):
        """Envia a query da UI para o nó processar."""
        query = self.txt_query.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Aviso", "Digite uma query SQL válida.")
            return
            
        # Executa em thread para não travar a interface
        threading.Thread(target=self._process_query_thread, args=(query,), daemon=True).start()

    def _process_query_thread(self, query):
        """Comunica com o nó e renderiza a resposta."""
        try:
            # Requisito: retornar o resultado e o nó do DDB que executou
            response = self.node.submit_query(query)
            
            # Atualiza UI de forma segura
            self.root.after(0, lambda: self._render_results(response))
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Erro de Execução", str(e)))

    def _render_results(self, response):
        """Popula a Treeview com os resultados do DDB."""
        # Limpa tabela atual
        for item in self.tree_results.get_children():
            self.tree_results.delete(item)
            
        # Validação crucial: Verifica se a resposta é nula (ex: query com sintaxe não suportada)
        if response is None or not isinstance(response, dict):
            messagebox.showerror(
                "Erro na Query", 
                "A resposta devolvida pelo nó é inválida ou nula.\n\n"
                "Isto ocorre normalmente se o comando SQL não for suportado pelo middleware "
                "(ex: comandos desconhecidos em vez de SELECT/INSERT/UPDATE/DELETE) "
                "ou se ocorreu uma falha de rede sem retorno do coordenador."
            )
            self.lbl_exec_node.config(text="Nó Executor: Falhou")
            return
            
        if not response.get("success"):
            messagebox.showerror("Erro na Query", response.get("error", "Erro Desconhecido"))
            self.lbl_exec_node.config(text="Nó Executor: Falhou")
            return
            
        self.lbl_exec_node.config(text=f"Nó Executor: {response.get('exec_node', 'Local')}")
        
        data = response.get("data", [])
        
        # Tratamento para escritas (INSERT/UPDATE/DELETE) que não retornam linhas, mas row_count
        if not data and response.get("row_count") is not None:
            self.tree_results["columns"] = ("Ação",)
            self.tree_results.heading("Ação", text="Resultado da Operação")
            self.tree_results.insert("", tk.END, values=(f"{response.get('row_count')} linhas afetadas.",))
            return

        if data and isinstance(data, list):
            # Extrai nomes das colunas da primeira linha (dicionário)
            columns = list(data[0].keys())
            self.tree_results["columns"] = columns
            
            for col in columns:
                self.tree_results.heading(col, text=col.upper())
                self.tree_results.column(col, width=100, anchor=tk.W)
                
            for row in data:
                values = [row[col] for col in columns]
                self.tree_results.insert("", tk.END, values=values)
        else:
            self.tree_results["columns"] = ("Info",)
            self.tree_results.heading("Info", text="Info")
            self.tree_results.insert("", tk.END, values=("Consulta concluída. Sem dados a exibir.",))

if __name__ == "__main__":
    # Ponto de entrada principal
    root = tk.Tk()
    app = DDBAppGUI(root)
    root.mainloop()