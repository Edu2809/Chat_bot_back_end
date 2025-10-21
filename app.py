from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import gspread
import google.generativeai as genai
import os
import threading
import time
import math
import json
import traceback

# ==========================================================
# CONFIGURAÇÃO BÁSICA
# ==========================================================
app = Flask(__name__, template_folder="templates")
CORS(app)

# ==========================================================
# CONFIGURAÇÃO DAS CREDENCIAIS GOOGLE
# ==========================================================
# No Render, crie uma variável chamada GOOGLE_CREDS_JSON com o conteúdo do seu JSON de service account.
if os.environ.get("GOOGLE_CREDS_JSON"):
    creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
    with open("service_account.json", "w") as f:
        json.dump(creds_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"
else:
    raise Exception("❌ Variável de ambiente GOOGLE_CREDS_JSON não encontrada!")

# Conecta ao Google Sheets
gc = gspread.service_account(filename="service_account.json")

# ==========================================================
# CONFIGURAÇÃO DA GEMINI API
# ==========================================================
if not os.environ.get("GOOGLE_GENAI_API_KEY"):
    raise Exception("❌ Variável de ambiente GOOGLE_GENAI_API_KEY não encontrada!")
genai.configure(api_key=os.environ["GOOGLE_GENAI_API_KEY"])

# ==========================================================
# LISTA DE PLANILHAS
# ==========================================================
SHEET_IDS = [
    "1oeKc3Z2O1ChhrM_SYnaJ21qg21xOqAigoFAxy9z-Hn4",  # Abril
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

# ==========================================================
# CACHE LOCAL (PARA RENDER)
# ==========================================================
CACHE_FILE = "/tmp/cache_combined.csv"  # Render permite gravação em /tmp
CACHE_MAX_AGE_SECONDS = 15 * 60  # 15 minutos

def is_cache_valid():
    if not os.path.exists(CACHE_FILE):
        return False
    mtime = os.path.getmtime(CACHE_FILE)
    return (time.time() - mtime) <= CACHE_MAX_AGE_SECONDS

def save_cache(df):
    df.to_csv(CACHE_FILE, index=False)

def load_cache():
    if os.path.exists(CACHE_FILE):
        return pd.read_csv(CACHE_FILE)
    return pd.DataFrame()

# ==========================================================
# LEITURA DAS PLANILHAS (COM RETRY E CACHE)
# ==========================================================
def load_data(max_rows=8000):
    if is_cache_valid():
        print("📤 Usando cache existente.")
        return load_cache()

    dfs = []
    total_rows = 0

    for sheet_id in SHEET_IDS:
        for tentativa in range(3):
            try:
                sh = gc.open_by_key(sheet_id)
                ws = sh.sheet1
                nome = sh.title
                print(f"📄 Lendo planilha: {nome}")

                df = pd.DataFrame(ws.get_all_records())
                if df.empty:
                    print(f"⚠️ {nome} vazia — ignorada.")
                    break

                df["Planilha"] = nome
                dfs.append(df)
                total_rows += len(df)

                if total_rows >= max_rows:
                    print("🔸 Limite de linhas atingido.")
                    break

                time.sleep(1.2)  # reduz chance de rate limit
                break
            except Exception as e:
                print(f"⚠️ Erro na {sheet_id} (tentativa {tentativa+1}): {e}")
                time.sleep(2 ** tentativa)
        else:
            print(f"❌ Falha ao ler {sheet_id} — ignorando.")

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        save_cache(combined)
        print(f"✅ Total consolidado: {len(combined)} linhas em {len(dfs)} planilhas.")
        return combined
    else:
        return pd.DataFrame()

# ==========================================================
# FUNÇÕES DE ANÁLISE
# ==========================================================
def build_compact_sample(df, max_rows=200, max_cols=20):
    cols = list(df.columns)[:max_cols]
    sample = df[cols].head(max_rows)
    sample_csv = sample.to_csv(index=False)

    numeric = df.select_dtypes(include="number")
    summary_csv = ""
    if not numeric.empty:
        summary_csv = numeric.describe().transpose().reset_index().to_csv(index=False)
    return sample_csv, summary_csv

def gerar_resposta(pergunta, timeout=240):
    df = load_data()
    if df.empty:
        return "❌ Nenhum dado foi carregado."

    sample_csv, summary_csv = build_compact_sample(df)

    prompt = f"""
Você é um analista de vendas experiente.
Baseando-se nos dados abaixo, responda de forma clara e objetiva:

Pergunta: "{pergunta}"

Resumo estatístico (colunas numéricas):
{summary_csv}

Amostra dos dados (limite 200 linhas):
{sample_csv}

Diga se sua resposta foi inferida com base na amostra.
"""

    result = {}
    def worker():
        try:
            model = genai.GenerativeModel("gemini-1.5-flash")
            resp = model.generate_content(prompt)
            result["text"] = getattr(resp, "text", str(resp)).strip()
        except Exception as e:
            traceback.print_exc()
            result["error"] = str(e)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=timeout)

    if t.is_alive():
        return "⏱️ Tempo excedido. Pergunta muito complexa."
    if "error" in result:
        return f"❌ Erro: {result['error']}"
    return result.get("text", "❌ Nenhuma resposta recebida.")

# ==========================================================
# ROTAS
# ==========================================================
@app.route("/")
def home():
    return "✅ Alpha Analyst API rodando."

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json() or {}
    user_msg = data.get("message", "").strip()
    if not user_msg:
        return jsonify({"response": "Mensagem vazia."}), 400
    resposta = gerar_resposta(user_msg)
    return jsonify({"response": resposta})

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
