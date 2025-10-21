from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import gspread
import google.generativeai as genai
import os
import time
import threading
import traceback

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ======================== CONFIG =========================
# Configurar chave do Gemini (vem do Render)
genai.configure(api_key=os.environ.get("GOOGLE_GENAI_API_KEY"))

# Carregar credenciais da Service Account (Render → GOOGLE_CREDS_JSON)
creds_json = os.environ.get("GOOGLE_CREDS_JSON")
if creds_json:
    import json
    from io import StringIO
    creds_dict = json.loads(creds_json)
    temp_file = "temp_creds.json"
    with open(temp_file, "w") as f:
        json.dump(creds_dict, f)
    gc = gspread.service_account(filename=temp_file)
else:
    gc = None

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
    return os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_AGE

def carregar_dados():
    if cache_valido():
        return pd.read_csv(CACHE_FILE)

    if not gc:
        return pd.DataFrame()

    dfs = []
    for sheet_id in SHEET_IDS:
        try:
            sh = gc.open_by_key(sheet_id)
            df = pd.DataFrame(sh.sheet1.get_all_records())
            if not df.empty:
                df["Origem"] = sh.title
                dfs.append(df)
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Falha ao ler {sheet_id}: {e}")
            continue

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        combined.to_csv(CACHE_FILE, index=False)
        return combined
    return pd.DataFrame()


def gerar_resposta_gemini(pergunta, df):
    sample = df.head(150).to_csv(index=False)
    stats = df.describe().to_csv() if not df.empty else ""

    prompt = f"""
Você é um analista de vendas experiente. 
Responda de forma objetiva e profissional à pergunta abaixo, usando os dados disponíveis.

Pergunta do usuário: "{pergunta}"

Resumo estatístico:
{stats}

Amostra dos dados:
{sample}

Se não houver dados suficientes, diga isso claramente.
    """

    result = {}
    def worker():
        try:
            model = genai.GenerativeModel("gemini-2.0-flash")
            response = model.generate_content(prompt)
            result["text"] = getattr(response, "text", str(response))
        except Exception as e:
            result["error"] = str(e)
            traceback.print_exc()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=180)

    if t.is_alive():
        return "⏱️ O modelo demorou demais. Tente novamente em alguns minutos."
    return result.get("text", result.get("error", "❌ Erro interno no modelo."))


# ======================== ROTAS =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "API online"})

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json()
    pergunta = data.get("message", "")

    if not pergunta.strip():
        return jsonify({"reply": "Mensagem vazia."}), 400

    df = carregar_dados()
    if df.empty:
        return jsonify({"reply": "❌ Não consegui carregar as planilhas. Verifique as credenciais ou IDs."})

    resposta = gerar_resposta_gemini(pergunta, df)
    return jsonify({"reply": resposta})


# ======================== INICIALIZAÇÃO =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
