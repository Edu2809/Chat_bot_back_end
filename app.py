# backend_app.py
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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# ============================
# Configurações globais
# ============================

df_data = pd.DataFrame()
data_lock = threading.Lock()
gc = None

# ✅ IDs das planilhas do Google Sheets
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

UPDATE_INTERVAL_SECONDS = 10 * 60
GEMINI_AVAILABLE = False

# ============================
# Inicialização dos serviços
# ============================

def setup_services():
    """
    Configura Gemini e Google Sheets.
    """
    global gc, GEMINI_AVAILABLE

    # Configuração do Gemini
    gemini_key = os.environ.get("GOOGLE_GENAI_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if gemini_key:
        try:
            genai.configure(api_key=gemini_key)
            GEMINI_AVAILABLE = True
            print("✅ Gemini configurado.")
        except Exception as e:
            GEMINI_AVAILABLE = False
            print("⚠️ Erro ao configurar Gemini:", e)
    else:
        print("⚠️ Gemini não configurado (nenhuma chave encontrada).")

    # Configuração do Google Sheets via gspread
    creds_json = os.environ.get("GOOGLE_CREDS_JSON")
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            temp_file = "service_account_creds.json"
            with open(temp_file, "w") as f:
                json.dump(creds_dict, f)
            gc = gspread.service_account(filename=temp_file)
            if os.path.exists(temp_file):
                os.remove(temp_file)
            print("✅ gspread credenciais carregadas.")
        except Exception as e:
            print("⚠️ Falha ao carregar credenciais Google Sheets:", e)
            traceback.print_exc()
    else:
        print("⚠️ GOOGLE_CREDS_JSON não encontrado. Pulando Google Sheets.")

# ============================
# Funções de dados
# ============================

def carregar_e_atualizar_dados_from_sheets():
    """
    Lê todas as planilhas listadas em SHEET_IDS e atualiza o df_data global.
    """
    global df_data, gc
    if not gc or not SHEET_IDS:
        print("ℹ️ gspread não configurado ou SHEET_IDS vazio. Pulando carregamento de Sheets.")
        return

    dfs = []
    for sheet_id in SHEET_IDS:
        try:
            sh = gc.open_by_key(sheet_id)
            worksheet = sh.sheet1
            df = pd.DataFrame(worksheet.get_all_records())
            if not df.empty:
                df["Origem"] = sh.title
                dfs.append(df)
                print(f"✅ Carregado {sh.title} ({len(df)} linhas).")
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ Falha ao ler sheet {sheet_id}: {e}")
            continue

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        with data_lock:
            df_data = combined.copy()
        print(f"✅ Dados combinados atualizados. Linhas totais: {len(df_data)}")
    else:
        print("ℹ️ Nenhuma sheet carregada com sucesso.")

def safe_set_df(new_df):
    global df_data
    with data_lock:
        df_data = new_df.copy()

def get_df_copy():
    with data_lock:
        return df_data.copy()

# ============================
# Função de resposta Gemini
# ============================

def gerar_resposta_gemini(pergunta, df):
    if df.empty:
        return "❌ O DataFrame está vazio. Faça upload de um arquivo ou carregue as planilhas."

    if not GEMINI_AVAILABLE:
        summary = [
            "ℹ️ Gemini não está configurado — resposta gerada localmente.",
            f"Linhas: {df.shape[0]}, Colunas: {df.shape[1]}",
            "Colunas: " + ", ".join(df.columns.astype(str).tolist()[:50]),
        ]
        q = pergunta.lower()
        if "maior" in q or "máximo" in q or "maior venda" in q:
            candidates = [c for c in df.columns if any(k in c.lower() for k in ("valor", "preço", "venda", "total"))]
            if candidates:
                col = candidates[0]
                try:
                    max_row = df.loc[df[col].astype(float).idxmax()]
                    summary.append(f"→ Coluna usada: {col}, maior valor: {max_row[col]}")
                except Exception:
                    summary.append("→ Não foi possível calcular o máximo localmente.")
        return "\n".join(summary)

    try:
        sample = df.head(150).to_csv(index=False)
        stats = df.describe(include='all').to_csv()
        prompt = f"""
Você é um analista de dados. Responda apenas com base nos dados fornecidos.
Pergunta: {pergunta}

Resumo estatístico:
{stats}

Amostra:
{sample}
"""
        model = genai.GenerativeModel("gemini-2.5-flash")
        resp = model.generate_content(prompt)
        return getattr(resp, "text", str(resp))
    except Exception as e:
        traceback.print_exc()
        return f"⚠️ Erro ao gerar resposta com Gemini: {e}"

# ============================
# Rotas da API Flask
# ============================

@app.route("/", methods=["GET"])
def home():
    d = get_df_copy()
    return jsonify({
        "status": "API online",
        "dados_status": "carregados" if not d.empty else "vazio",
        "total_linhas": d.shape[0]
    })

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)
    pergunta = data.get("message", "")
    if not pergunta.strip():
        return jsonify({"reply": "Por favor, envie uma mensagem válida."}), 400
    df = get_df_copy()
    if df.empty:
        return jsonify({"reply": "❌ Nenhum dado carregado. Aguarde o carregamento das planilhas."})
    resposta = gerar_resposta_gemini(pergunta, df)
    return jsonify({"reply": resposta}), 200

@app.route("/api/upload", methods=["POST"])
def upload():
    if 'file' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400
    f = request.files['file']
    filename = f.filename
    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(f)
        else:
            df = pd.read_excel(f)
        safe_set_df(df)
        return jsonify({"reply": f"Arquivo '{filename}' carregado com sucesso.", "rows": df.shape[0]}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Falha ao ler o arquivo: {e}"}), 400

@app.route("/api/reset", methods=["POST"])
def reset():
    safe_set_df(pd.DataFrame())
    return jsonify({"reply": "Contexto do servidor resetado."}), 200

# ============================
# Inicialização principal
# ============================

if __name__ == "__main__":
    setup_services()

    # Thread para atualização automática das Sheets
    def data_refresher():
        carregar_e_atualizar_dados_from_sheets()
        while True:
            time.sleep(UPDATE_INTERVAL_SECONDS)
            try:
                carregar_e_atualizar_dados_from_sheets()
            except Exception as e:
                print("Erro no refresher:", e)
                traceback.print_exc()

    if SHEET_IDS and gc:
        threading.Thread(target=data_refresher, daemon=True).start()

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
