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
from io import StringIO # Necessário para simular o arquivo de credenciais se estiver em string

# Inicializa o Flask e configura o CORS para permitir requisições de diferentes origens
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ======================== CONFIGURAÇÃO =========================

# 🎯 PASSO 1: CHAVE API DO GEMINI
# Insira sua chave API do Gemini COMPLETA (começa com 'AIzaSy...') aqui:
GEMINI_API_KEY = "AIzaSyA-dwwt0-wPQglT7KaO8cPGtL5cIsL2Q-4" 
# NOTA DE SEGURANÇA: Em produção, o ideal é usar 'os.environ.get("GOOGLE_GENAI_API_KEY")'

try:
    if GEMINI_API_KEY != "AIzaSyA-dwwt0-wPQglT7KaO8cPGtL5cIsL2Q-4":
        genai.configure(api_key=GEMINI_API_KEY)
    elif os.environ.get("GOOGLE_GENAI_API_KEY"):
         genai.configure(api_key=os.environ.get("GOOGLE_GENAI_API_KEY"))
    else:
        print("ATENÇÃO: Nenhuma chave Gemini válida foi encontrada. A API não funcionará.")

except Exception as e:
    print(f"ATENÇÃO: Falha ao configurar a API do Gemini: {e}")


# Carregar credenciais da Service Account para o Google Sheets (lidas de GOOGLE_CREDS_JSON)
creds_json = os.environ.get("GOOGLE_CREDS_JSON")
gc = None
if creds_json:
    try:
        # Cria um arquivo temporário em memória (ou disco) a partir da string JSON
        creds_dict = json.loads(creds_json)
        
        # Para ambientes como Render, onde salvar em disco é melhor, usamos um arquivo temporário
        temp_file = "temp_creds.json"
        with open(temp_file, "w") as f:
            json.dump(creds_dict, f)
        
        # Autoriza o gspread usando o arquivo de credenciais
        gc = gspread.service_account(filename=temp_file)
        
        # Remove o arquivo temporário após o uso (opcional, mas bom para limpeza)
        if os.path.exists(temp_file):
            os.remove(temp_file)

    except Exception as e:
        print(f"⚠️ Falha ao carregar credenciais do Google Sheets: {e}")
else:
    print("❌ Variável GOOGLE_CREDS_JSON não encontrada. O carregamento de Sheets falhará.")


# Lista dos IDs das 12 planilhas que serão combinadas
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
CACHE_AGE = 10 * 60  # Cache de 10 minutos para evitar excesso de requisições ao Google Sheets


# ======================== FUNÇÕES AUXILIARES DE DADOS =========================

def cache_valido():
    """Verifica se o arquivo de cache existe e se não expirou (10 minutos)."""
    return os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_AGE

def carregar_dados():
    """
    Carrega os dados das planilhas do Google Sheets ou do cache local.
    Se o cache for válido, usa o cache. Caso contrário, busca no Sheets e atualiza o cache.
    """
    if cache_valido():
        print("✅ Dados carregados do cache.")
        return pd.read_csv(CACHE_FILE)

    if not gc:
        print("❌ gspread não configurado. Retornando DataFrame vazio.")
        return pd.DataFrame()

    print("🔄 Buscando dados atualizados no Google Sheets...")
    dfs = []
    for sheet_id in SHEET_IDS:
        try:
            # Abre a planilha pelo ID
            sh = gc.open_by_key(sheet_id)
            # Lê todos os registros da primeira aba para um DataFrame
            df = pd.DataFrame(sh.sheet1.get_all_records())
            
            if not df.empty:
                # Adiciona uma coluna para saber de qual planilha veio o dado
                df["Origem"] = sh.title
                dfs.append(df)
            
            # Adiciona um pequeno delay para evitar limites de taxa do Google Sheets
            time.sleep(1) 
        except Exception as e:
            print(f"⚠️ Falha ao ler o Sheet ID {sheet_id}: {e}")
            continue

    if dfs:
        # Combina todos os DataFrames em um único
        combined = pd.concat(dfs, ignore_index=True)
        # Salva no cache
        combined.to_csv(CACHE_FILE, index=False)
        print(f"✅ Dados combinados e salvos no cache. Total de linhas: {combined.shape[0]}.")
        return combined
    
    print("❌ Nenhuma planilha pôde ser carregada com sucesso.")
    return pd.DataFrame()


# ======================== FUNÇÕES AUXILIARES DO GEMINI =========================

def gerar_resposta_gemini(pergunta, df):
    """
    Gera uma resposta analítica usando o modelo Gemini 2.0 Flash.
    Envia uma amostra dos dados e estatísticas descritivas como contexto.
    """
    # Limita o DataFrame para o contexto (150 primeiras linhas) para economizar tokens
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

Instruções:
1. Se a pergunta for sobre contagem, média, máximo/mínimo, ou análise de tendências (e os dados permitirem), forneça o valor ou a conclusão diretamente.
2. Mencione quais colunas você utilizou se a análise for complexa.
3. Se não houver dados suficientes ou as colunas necessárias não estiverem na amostra/estatísticas, diga isso claramente.
    """

    result = {}
    
    # Usa threading para impor um limite de tempo (timeout) na chamada da API
    def worker():
        try:
            # Usa o modelo Gemini 2.0 Flash, ideal para análise rápida de texto e dados
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            result["text"] = getattr(response, "text", str(response))
        except Exception as e:
            result["error"] = str(e)
            traceback.print_exc()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=60) # Tempo limite de 60 segundos (pode ser ajustado)

    if t.is_alive():
        return "⏱️ O modelo demorou demais para responder. Tente novamente ou formule uma pergunta mais simples."
    
    # Retorna o texto gerado ou uma mensagem de erro
    return result.get("text", result.get("error", "❌ Erro interno no serviço de análise de dados."))


# ======================== ROTAS DA API =========================

@app.route("/", methods=["GET"])
def home():
    """Rota de saúde da API."""
    return jsonify({"status": "API online. Fonte de dados: Google Sheets via gspread/Gemini."})


@app.route("/api/chat", methods=["POST"])
def chat():
    """
    Recebe a pergunta do usuário, carrega os dados e envia para o Gemini para análise.
    Esta rota substitui a lógica anterior de análise de arquivos locais.
    """
    data = request.get_json()
    pergunta = data.get("message", "")

    if not pergunta.strip():
        return jsonify({"reply": "Por favor, envie uma mensagem."}), 400

    # 1. Carrega os dados (usa cache se disponível, ou baixa do Sheets)
    df = carregar_dados()
    
    if df.empty:
        # Se falhar ao carregar tanto do cache quanto do Sheets
        return jsonify({
            "reply": "❌ Não consegui carregar os dados. Verifique as credenciais do Google Sheets, os IDs das planilhas, ou se o cache expirou e a conexão falhou."
        })

    # 2. Gera a resposta usando o modelo Gemini
    resposta = gerar_resposta_gemini(pergunta, df)
    
    return jsonify({"reply": resposta})


# REMOVIDAS as rotas /api/upload, /api/ping e /api/reset pois não são mais necessárias.


# ======================== INICIALIZAÇÃO =========================
if __name__ == "__main__":
    # Define a porta, usando a variável de ambiente PORT se existir, caso contrário, usa 10000
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

