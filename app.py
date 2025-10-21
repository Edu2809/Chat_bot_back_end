from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import gspread
import google.generativeai as genai
import time
import threading
import traceback
import json
import logging
import os
print(f"DEBUG_API_KEY_CHECK: {os.getenv('GOOGLE_GEMAI_API_KEY')}")
# Configuração de Logging para debug
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
# Permitir CORS para todas as origens (ajustar se precisar de mais segurança)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ======================== CONFIG =========================
# Configurar chave do Gemini (vem do Render → GOOGLE_GENAI_API_KEY)
# O ambiente de deploy (Render) usa 'GOOGLE_API_KEY' ou 'GEMINI_API_KEY' por padrão.
# Mantive a sua variável, mas adicionei a versão da biblioteca Gemini para ser seguro.
API_KEY = os.environ.get("GOOGLE_GENAI_API_KEY")
if API_KEY:
    genai.configure(api_key=API_KEY)
    logging.info("Chave Gemini configurada.")
else:
    logging.error("Variável GOOGLE_GENAI_API_KEY não encontrada.")


# Carregar credenciais da Service Account (Render → GOOGLE_CREDS_JSON)
GC_CREDS_PATH = "google_creds_for_gspread.json"
gc = None

def setup_gspread():
    global gc
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    
    if creds_json:
        try:
            # 1. Tenta carregar o JSON
            creds_dict = json.loads(creds_json)
            
            # 2. Salva o arquivo temporário que o gspread.service_account precisa
            with open(GC_CREDS_PATH, "w") as f:
                json.dump(creds_dict, f)
            
            # 3. Inicializa o GSpread
            gc = gspread.service_account(filename=GC_CREDS_PATH)
            logging.info("GSpread inicializado com sucesso.")

        except Exception as e:
            logging.error(f"⚠️ Falha ao configurar GSpread: {e}")
            # Certifica-se de que a credencial não está disponível
            if os.path.exists(GC_CREDS_PATH):
                os.remove(GC_CREDS_PATH)
    else:
        logging.warning("Variável GOOGLE_CREDS_JSON não encontrada. GSpread desativado.")

# Inicializa o GSpread
setup_gspread()


# Lista das 12 planilhas
SHEET_IDS = [
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

CACHE_FILE = "cache_combined.csv"
CACHE_AGE = 10 * 60  # 10 minutos


# ======================== FUNÇÕES AUXILIARES =========================
def cache_valido():
    """Verifica se o arquivo de cache existe e se não expirou."""
    return os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_AGE

def carregar_dados():
    """Carrega dados combinados das planilhas, usando cache se válido."""
    
    # 1. Tenta carregar do cache
    if cache_valido():
        logging.info("Carregando dados do cache.")
        return pd.read_csv(CACHE_FILE)

    # 2. Se o gspread não estiver configurado, retorna vazio
    if not gc:
        logging.warning("GSpread não configurado. Não é possível carregar dados.")
        return pd.DataFrame()

    logging.info("Cache inválido. Carregando dados do Google Sheets.")
    dfs = []
    for sheet_id in SHEET_IDS:
        try:
            sh = gc.open_by_key(sheet_id)
            # Lê todos os registros da primeira aba
            df = pd.DataFrame(sh.sheet1.get_all_records())
            
            if not df.empty:
                df["Origem"] = sh.title
                dfs.append(df)
            
            # Pequeno delay para evitar hitting rate limits do Google Sheets
            time.sleep(1) 
            
        except Exception as e:
            # Imprime o erro no console do Render
            logging.error(f"⚠️ Falha ao ler {sheet_id} ({sh.title if 'sh' in locals() else 'N/A'}): {e}")
            continue

    # 3. Combina e salva o novo cache
    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        # Tenta remover colunas vazias (Unnamed) se houverem
        combined = combined.loc[:, ~combined.columns.str.contains('^Unnamed')]
        combined.to_csv(CACHE_FILE, index=False)
        logging.info(f"Dados de {len(dfs)} planilhas combinados e cache atualizado. Total de linhas: {len(combined)}")
        return combined
    
    logging.warning("Nenhuma planilha carregada com sucesso.")
    return pd.DataFrame()


def gerar_resposta_gemini(pergunta, df):
    """Gera a resposta do Gemini usando um thread para aplicar timeout."""
    
    # Limita o dataframe a 150 linhas e converte para CSV para economizar tokens
    sample = df.head(150).to_csv(index=False)
    stats = df.describe().to_csv() if not df.empty else ""

    prompt = f"""
Você é um analista de vendas experiente. Sua função é responder de forma objetiva e profissional à pergunta do usuário, utilizando exclusivamente os dados fornecidos abaixo como contexto. Sua resposta deve ser focada em insights de vendas e resultados numéricos.

Pergunta do usuário: "{pergunta}"

Resumo estatístico (para análise rápida de dados):
{stats}

Amostra dos dados (estrutura e primeiras linhas):
{sample}

Se não houver dados suficientes ou relevantes para responder, diga isso claramente.
    """

    result = {}
    
    def worker():
        """Função para ser executada na thread, contendo a chamada de API."""
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            # Tenta pegar o texto e garante que a resposta é sempre uma string
            result["text"] = getattr(response, "text", str(response))
        except Exception as e:
            result["error"] = str(e)
            # Imprime o stack trace completo para ajudar no debug do Render
            traceback.print_exc()

    t = threading.Thread(target=worker)
    t.start()
    
    # Aguarda a thread terminar com um timeout de 180 segundos (3 minutos)
    t.join(timeout=180) 

    if t.is_alive():
        logging.warning("Gemini timeout: A thread ainda está viva após 180 segundos.")
        return "⏱️ O modelo demorou demais. Tente novamente em alguns minutos."
    
    return result.get("text", result.get("error", "❌ Erro interno no modelo."))


# ======================== ROTAS =========================
@app.route("/", methods=["GET"])
def home():
    """Rota de saúde da API."""
    return jsonify({"status": "API online", "message": "Backend para Análise Gemini/GSheets."})

@app.route("/api/chat", methods=["POST"])
def chat():
    """Rota principal para receber perguntas do chat e retornar a análise do Gemini."""
    try:
        data = request.get_json()
        pergunta = data.get("message", "")
    except Exception:
        return jsonify({"reply": "Erro ao processar o JSON da requisição."}), 400

    if not pergunta.strip():
        return jsonify({"reply": "Mensagem vazia."}), 400

    # 1. Carrega os dados (usa cache se disponível)
    df = carregar_dados()
    
    if df.empty:
        # Verifica o motivo da falha para dar um feedback melhor
        if not gc:
             return jsonify({"reply": "❌ Não consegui carregar as planilhas. Verifique a variável GOOGLE_CREDS_JSON e o compartilhamento das planilhas."})
        else:
             return jsonify({"reply": "❌ Não consegui carregar nenhuma das planilhas. Verifique os IDs ou o conteúdo das planilhas."})

    # 2. Gera a resposta com o modelo Gemini
    resposta = gerar_resposta_gemini(pergunta, df)
    
    return jsonify({"reply": resposta})

# O Gunicorn (servidor do Render) irá importar a instância 'app' diretamente.
# Não precisamos do if __name__ == "__main__":
# Para rodar localmente, use: gunicorn app:app -b 0.0.0.0:5000


