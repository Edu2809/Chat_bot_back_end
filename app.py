from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import gspread
import google.generativeai as genai
import os
import time
import threading
import traceback
import json
import pytz # Necessário para lidar com fusos horários no log

# ======================== INICIALIZAÇÃO E CONFIGURAÇÃO DA API =========================

# Inicializa o Flask
app = Flask(__name__)
# Configura o CORS para aceitar requisições de qualquer origem na rota /api/*
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Variáveis globais para armazenamento de dados e sincronização
# O DataFrame combinado será armazenado aqui, acessível por todas as rotas
df_data = pd.DataFrame()
# O lock garante que apenas um thread acesse ou modifique df_data por vez
data_lock = threading.Lock()
# Objeto de cliente do Google Sheets
gc = None

# Constantes de Configuração
SHEET_IDS = [
    # IDs de planilhas de exemplo
    "1oeKc3Z2O1ChhrM_SYnaJ21qg21xOqAigoFAxy9z-Hn4",
    "1HIOKU5lODnLpjLbtCo7nowrkl6sOv4Hnat6Mt6mnEPY",
    "13r_ZwpyBbdtxb7e9_EhbXiIj1ezHvdL2XsQ-PdHHL70",
    "1Jupk4ZW_wun3W8eCG6rLh8BpIBj6xqROhsLWOxW8eNQ",
    "10zWcZWDAappBSqusteHPbpOqwHNjFEn0Qil0YGuvjuQ",
    "1KsQoSXt33wwh3OSbJuRBgYVPSOSEXk3Y4vhJqhl9yc4",
    "1c49j2cIEiGHaOc35ZbJkeRXp5gw2jHcVjMOKfgQJQTw",
    "1bCWUUDuQWDCnqV2EFCWctrZH_NMZBXydT0qrmv7Xe0U",
    "1OM3Vcg_lIMlXxKvqaes90PCUUwn-RThflgewSahu4zg",
    "1N6_YgXpqRXLj5k9zRD4SWQy-KtYvmSWBy0h4rFZJ43s",
    "19Ry0WTLla1D262QxjmQ8WNVbPwMnzBKo2S1FyEEZ8WA",
    "1Sfs3bjIsNDOTmeWCwgveI61asAYyQNZPvuGFyDA7xeo"
]

# Intervalo de atualização (10 minutos)
UPDATE_INTERVAL_SECONDS = 10 * 60 

# ======================== SETUP DAS CHAVES E SERVIÇOS =========================

def setup_services():
    """
    Configura o Gemini API e as credenciais do Google Sheets (gspread) 
    a partir de variáveis de ambiente.
    """
    global gc
    
    # 1. Configuração do Gemini API
    # Tenta ler GOOGLE_GENAI_API_KEY (padrão) ou GEMINI_API_KEY (variável definida pelo usuário no Render)
    gemini_key = os.environ.get("GOOGLE_GENAI_API_KEY") or os.environ.get("GEMINI_API_KEY")
    
    if gemini_key:
        try:
            genai.configure(api_key=gemini_key)
            print("✅ Gemini API configurada com sucesso.")
        except Exception as e:
            print(f"⚠️ Falha ao configurar a API do Gemini: {e}")
    else:
        print("❌ Nenhuma variável de chave Gemini válida (GOOGLE_GENAI_API_KEY ou GEMINI_API_KEY) encontrada. A API do Gemini não funcionará.")

    # 2. Configuração do gspread (Google Sheets)
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if creds_json:
        try:
            # Render exige que as credenciais sejam lidas de um arquivo. 
            # Criamos um arquivo temporário a partir da string JSON.
            creds_dict = json.loads(creds_json)
            temp_file = "service_account_creds.json"
            
            with open(temp_file, "w") as f:
                json.dump(creds_dict, f)
            
            # Autoriza o gspread usando o arquivo temporário
            gc = gspread.service_account(filename=temp_file)
            
            # Remove o arquivo temporário
            if os.path.exists(temp_file):
                os.remove(temp_file)
                
            print("✅ Credenciais do Google Sheets (gspread) carregadas com sucesso.")

        except Exception as e:
            print(f"⚠️ Falha ao carregar credenciais do Google Sheets (gspread). Verifique GOOGLE_CREDS_JSON. Erro: {e}")
            traceback.print_exc()
    else:
        print("❌ Variável GOOGLE_CREDS_JSON não encontrada. O carregamento de Sheets falhará.")

# ======================== FUNÇÕES DE DADOS E CACHE =========================

def carregar_e_atualizar_dados():
    """
    Busca os dados atualizados das planilhas do Google Sheets e atualiza 
    a variável global df_data de forma segura.
    """
    global df_data
    
    if not gc:
        print("⚠️ gspread não configurado. Impossível buscar dados.")
        return

    dfs = []
    print("🔄 Buscando dados atualizados no Google Sheets...")
    
    for sheet_id in SHEET_IDS:
        try:
            sh = gc.open_by_key(sheet_id)
            # Lê todos os registros da primeira aba para um DataFrame
            df = pd.DataFrame(sh.sheet1.get_all_records())
            
            if not df.empty:
                df["Origem"] = sh.title
                dfs.append(df)
            
            # Pequeno delay para respeitar limites de taxa do Google Sheets
            time.sleep(1)  
        except Exception as e:
            print(f"⚠️ Falha ao ler o Sheet ID {sheet_id}: {e}")
            continue

    if dfs:
        # Combina todos os DataFrames em um único
        combined = pd.concat(dfs, ignore_index=True)
        
        # Uso do lock para garantir a escrita segura na variável global
        with data_lock:
            df_data = combined.copy()
            
        print(f"✅ Dados combinados e globais atualizados. Total de linhas: {df_data.shape[0]}.")
    else:
        print("❌ Nenhuma planilha pôde ser carregada com sucesso.")


def data_refresher():
    """
    Função de thread em segundo plano que carrega os dados periodicamente.
    Isso substitui a lógica de cache baseada em arquivo, pois a memória é mais rápida.
    """
    # Carrega os dados na inicialização
    carregar_e_atualizar_dados()
    
    # Inicia o loop de atualização periódica
    while True:
        # Espera o intervalo definido
        time.sleep(UPDATE_INTERVAL_SECONDS)
        
        # Loga o tempo do próximo carregamento
        agora = pd.Timestamp.now(tz='America/Sao_Paulo')
        print(f"\n[{agora.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando atualização periódica de dados.")
        
        # Tenta carregar e atualizar
        try:
            carregar_e_atualizar_dados()
        except Exception as e:
            print(f"ERRO CRÍTICO NA ATUALIZAÇÃO DE DADOS: {e}")
            traceback.print_exc()

# ======================== FUNÇÕES AUXILIARES DO GEMINI =========================

def gerar_resposta_gemini(pergunta, df):
    """
    Gera uma resposta analítica usando o modelo Gemini 2.0 Flash.
    Envia uma amostra dos dados e estatísticas descritivas como contexto.
    """
    if df.empty:
        return "❌ O DataFrame está vazio. Não há dados para analisar."
        
    # Limita o DataFrame para o contexto (máximo de 150 linhas) 
    # para economizar tokens e respeitar o limite de contexto.
    # Evite enviar DataFrames muito grandes, pois o JSON ou CSV consome muitos tokens.
    sample = df.head(150).to_csv(index=False)
    # Gera estatísticas descritivas
    stats = df.describe(include='all').to_csv() if not df.empty else ""

    prompt = f"""
Você é um analista de dados experiente em vendas e finanças. Você tem acesso a dados combinados de 12 planilhas de vendas.
Responda de forma objetiva, profissional e amigável à pergunta do usuário abaixo, baseando-se estritamente nos dados que você vê.
Mantenha a resposta concisa e utilize emoji quando for apropriado.

Pergunta do usuário: "{pergunta}"

Resumo estatístico das colunas (incluindo contagens para não numéricas):
{stats}

Amostra dos dados (150 linhas):
{sample}

Instruções de análise:
1. Se a pergunta for sobre contagem, média, máximo/mínimo, ou análise de tendências (e os dados permitirem), forneça o valor ou a conclusão diretamente.
2. Mencione quais colunas você utilizou se a análise for complexa.
3. Se não houver dados suficientes ou as colunas necessárias não estiverem na amostra/estatísticas, diga isso claramente.
    """

    result = {}
    
    # Worker para fazer a chamada da API com timeout
    def worker():
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            # Tenta pegar o texto, ou a representação string da resposta se for complexa
            result["text"] = getattr(response, "text", str(response)) 
        except Exception as e:
            result["error"] = str(e)
            traceback.print_exc()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=60) # Tempo limite de 60 segundos

    if t.is_alive():
        return "⏱️ O modelo demorou demais para responder. Tente novamente ou formule uma pergunta mais simples."
    
    return result.get("text", result.get("error", "❌ Erro interno no serviço de análise de dados."))


# ======================== ROTAS DA API =========================

@app.route("/", methods=["GET"])
def home():
    """Rota de saúde (Health Check) da API."""
    with data_lock:
        data_status = "carregados" if not df_data.empty else "carregando ou falhou"
        rows = df_data.shape[0] if not df_data.empty else 0
        
    return jsonify({
        "status": "API online",
        "data_source": "Google Sheets via gspread/Gemini",
        "dados_status": data_status,
        "total_linhas_carregadas": rows
    })


@app.route("/api/chat", methods=["POST"])
def chat():
    """
    Recebe a pergunta do usuário e a envia para o Gemini para análise, 
    usando os dados carregados globalmente.
    """
    data = request.get_json()
    pergunta = data.get("message", "")

    if not pergunta.strip():
        return jsonify({"reply": "Por favor, envie uma mensagem."}), 400

    # 1. Acesso seguro aos dados globais (sem fazer o download dos sheets!)
    with data_lock:
        current_df = df_data.copy() # Cria uma cópia para o thread Gemini
    
    if current_df.empty:
        # Se falhar ao carregar tanto do cache quanto do Sheets
        return jsonify({
            "reply": "❌ Os dados ainda não foram carregados ou o carregamento inicial falhou. Por favor, aguarde alguns segundos e tente novamente."
        })

    # 2. Gera a resposta usando o modelo Gemini
    resposta = gerar_resposta_gemini(pergunta, current_df)
    
    return jsonify({"reply": resposta})


# ======================== INICIALIZAÇÃO DA APLICAÇÃO =========================
if __name__ == "__main__":
    # Configura os serviços (Gemini, Sheets)
    setup_services()
    
    # Inicia o thread de atualização em segundo plano
    threading.Thread(target=data_refresher, daemon=True).start()
    
    # Define a porta, usando a variável de ambiente PORT (padrão Render) ou 10000
    port = int(os.environ.get("PORT", 10000))
    print(f"🚀 Iniciando servidor Flask na porta {port}...")
    # O comando use_reloader=False é crucial quando se usa threads customizados
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
