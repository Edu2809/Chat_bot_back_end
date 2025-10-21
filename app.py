import threading
import time
import math
import json
import traceback

# ==========================================================
# CONFIGURAÇÃO
# CONFIGURAÇÃO BÁSICA
# ==========================================================
app = Flask(__name__, template_folder="templates")
CORS(app)

# Caminho do JSON de credenciais da service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bot-qualquer-70634b3faced.json"

# Configuração da Gemini API (mantenha sua chave segura)
genai.configure(api_key=os.environ.get("GOOGLE_GENAI_API_KEY", "AIzaSyA-dwwt0-wPQglT7KaO8cPGtL5cIsL2Q-4"))
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

# Cliente Google Sheets
gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
# ==========================================================
# CONFIGURAÇÃO DA GEMINI API
# ==========================================================
if not os.environ.get("GOOGLE_GENAI_API_KEY"):
    raise Exception("❌ Variável de ambiente GOOGLE_GENAI_API_KEY não encontrada!")
genai.configure(api_key=os.environ["GOOGLE_GENAI_API_KEY"])

# ==========================================================
# CONFIGURAÇÃO DAS PLANILHAS
# LISTA DE PLANILHAS
# ==========================================================
SHEET_IDS = [
    "1oeKc3Z2O1ChhrM_SYnaJ21qg21xOqAigoFAxy9z-Hn4",  # Abril
@@ -42,222 +56,148 @@
    "1Sfs3bjIsNDOTmeWCwgveI61asAYyQNZPvuGFyDA7xeo"
]

# Cache local (evita sobrecarga na API)
CACHE_FILE = "cache_combined.csv"
CACHE_MAX_AGE_SECONDS = 10 * 60  # 10 minutos

# ==========================================================
# FUNÇÕES AUXILIARES
# CACHE LOCAL (PARA RENDER)
# ==========================================================
CACHE_FILE = "/tmp/cache_combined.csv"  # Render permite gravação em /tmp
CACHE_MAX_AGE_SECONDS = 15 * 60  # 15 minutos

def is_cache_valid():
    if not os.path.exists(CACHE_FILE):
        return False
    mtime = os.path.getmtime(CACHE_FILE)
    age = time.time() - mtime
    return age <= CACHE_MAX_AGE_SECONDS
    return (time.time() - mtime) <= CACHE_MAX_AGE_SECONDS

def save_cache(df):
    try:
        df.to_csv(CACHE_FILE, index=False)
        print(f"📥 Cache salvo em {CACHE_FILE}")
    except Exception as e:
        print("❌ Falha ao salvar cache:", e)
    df.to_csv(CACHE_FILE, index=False)

def load_cache():
    try:
        df = pd.read_csv(CACHE_FILE)
        print(f"📤 Cache carregado ({len(df)} linhas).")
        return df
    except Exception as e:
        print("❌ Falha ao carregar cache:", e)
        return pd.DataFrame()
    if os.path.exists(CACHE_FILE):
        return pd.read_csv(CACHE_FILE)
    return pd.DataFrame()

def load_data(max_rows=5000):
    # Usa cache quando possível
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
        tentativa = 0
        while tentativa < 4:  # até 4 tentativas com backoff
        for tentativa in range(3):
            try:
                sh = gc.open_by_key(sheet_id)
                sheet = sh.sheet1
                nome_planilha = sh.title
                print(f"📄 Lendo planilha: {nome_planilha} ({sheet_id})")
                ws = sh.sheet1
                nome = sh.title
                print(f"📄 Lendo planilha: {nome}")

                df = pd.DataFrame(sheet.get_all_records())
                df = pd.DataFrame(ws.get_all_records())
                if df.empty:
                    print(f"⚠️ Planilha {nome_planilha} vazia — ignorada.")
                    print(f"⚠️ {nome} vazia — ignorada.")
                    break

                df["Planilha"] = nome_planilha
                remaining = max_rows - total_rows
                if remaining <= 0:
                    print("🔸 Limite máximo de linhas atingido, parando leitura.")
                    # salva cache parcial
                    if dfs:
                        combined = pd.concat(dfs, ignore_index=True)
                        save_cache(combined)
                        return combined
                    else:
                        return pd.DataFrame()

                if len(df) > remaining:
                    df = df.head(remaining)

                total_rows += len(df)
                df["Planilha"] = nome
                dfs.append(df)
                print(f"✅ {len(df)} linhas carregadas de {nome_planilha} (total até agora: {total_rows})")
                break  # sucesso -> sai do while
                total_rows += len(df)

            except Exception as e:
                tentativa += 1
                texto_erro = str(e)
                print(f"⚠️ Erro ao carregar planilha {sheet_id} (tentativa {tentativa}): {texto_erro}")

                # Detecta quota/read limit (heurística)
                if "Quota exceeded" in texto_erro or "429" in texto_erro or "Rate Limit" in texto_erro:
                    espera = 2 ** tentativa  # 2,4,8...
                    print(f"⏳ Quota/Rate limit detectado. Aguardando {espera}s e tentando novamente...")
                    time.sleep(espera)
                    continue
                else:
                    # erro não recuperável para essa planilha -> pula
                    traceback.print_exc()
                if total_rows >= max_rows:
                    print("🔸 Limite de linhas atingido.")
                    break

        # pequena pausa entre planilhas para reduzir chance de throttling
        time.sleep(1.5)
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
        print(f"🔹 Total consolidado: {len(combined)} linhas em {len(dfs)} planilhas.")
        print(f"✅ Total consolidado: {len(combined)} linhas em {len(dfs)} planilhas.")
        return combined
    else:
        print("⚠️ Nenhuma planilha foi carregada.")
        return pd.DataFrame()

def build_compact_sample(df, max_rows=200, max_columns=20):
    """
    Prepara uma amostra compacta: mantém colunas mais relevantes e no máximo `max_rows`.
    Também inclui resumo estatístico para colunas numéricas.
    """
    # Seleciona primeiras colunas (limitando a quantidade para evitar prompt gigantesco)
    cols = list(df.columns)[:max_columns]
    sample = df[cols].head(max_rows).copy()
# ==========================================================
# FUNÇÕES DE ANÁLISE
# ==========================================================
def build_compact_sample(df, max_rows=200, max_cols=20):
    cols = list(df.columns)[:max_cols]
    sample = df[cols].head(max_rows)
    sample_csv = sample.to_csv(index=False)

    # Gerar resumo numérico reduzido
    numeric = df.select_dtypes(include="number")
    summary = None
    summary_csv = ""
    if not numeric.empty:
        summary = numeric.describe().transpose().round(3)
        # converte para CSV compacto
        summary_csv = summary.reset_index().to_csv(index=False)
    else:
        summary_csv = ""

    sample_csv = sample.to_csv(index=False)
        summary_csv = numeric.describe().transpose().reset_index().to_csv(index=False)
    return sample_csv, summary_csv

def gerar_insights_sync(prompt_text, result_container):
    """Chamada ao modelo — colocada numa thread para podermos aplicar timeout e retries."""
    # Retry exponencial simples para o genai
    max_tries = 3
    for attempt in range(1, max_tries + 1):
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt_text)
            # Tenta extrair texto da resposta
            texto = getattr(response, "text", None)
            if texto is None:
                # tenta com str()
                texto = str(response)
            result_container["text"] = texto.strip()
            return
        except Exception as e:
            result_container.setdefault("errors", []).append(str(e))
            print(f"⚠️ Erro na chamada ao genai (tentativa {attempt}): {e}")
            if attempt < max_tries:
                wait = 2 ** attempt
                print(f"⏳ Aguardo {wait}s antes de nova tentativa ao genai...")
                time.sleep(wait)
            else:
                result_container["error"] = f"Erro interno na geração (genai): {str(e)}"
                traceback.print_exc()

def gerar_insights(pergunta, timeout_seconds=300):
    # carrega dados (usando cache quando possível)
    df = load_data(max_rows=5000)
def gerar_resposta(pergunta, timeout=240):
    df = load_data()
    if df.empty:
        return "❌ Não encontrei dados nas planilhas."
        return "❌ Nenhum dado foi carregado."

    # Prepara amostra compacta + resumo estatístico para evitar overlength do prompt
    sample_csv, summary_csv = build_compact_sample(df, max_rows=200, max_columns=20)
    sample_csv, summary_csv = build_compact_sample(df)

    prompt = f"""
Você é um analista de vendas. Responda de forma analítica, clara e curta à pergunta:
"{pergunta}"
Você é um analista de vendas experiente.
Baseando-se nos dados abaixo, responda de forma clara e objetiva:
Pergunta: "{pergunta}"
Resumo estatístico (colunas numéricas):
{summary_csv}
Aqui está uma amostra limitada dos dados (CSV):
Amostra dos dados (limite 200 linhas):
{sample_csv}
Responda objetivamente e cite se a resposta se baseia na amostra (não nos dados completos).
Diga se sua resposta foi inferida com base na amostra.
"""

    print("🧾 Prompt preparado — enviando ao modelo (tamanho aproximado:", len(prompt), "bytes )")

    result = {}
    worker = threading.Thread(target=gerar_insights_sync, args=(prompt, result))
    worker.start()
    worker.join(timeout=timeout_seconds)
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

    if worker.is_alive():
        print("❌ Timeout na geração de insights pelo modelo.")
        return "⏱️ A análise demorou demais e foi cancelada. Tente uma pergunta menor ou aumente o timeout."
    if t.is_alive():
        return "⏱️ Tempo excedido. Pergunta muito complexa."
    if "error" in result:
        print("❌ Erro do modelo:", result.get("error"))
        return result["error"]
    if "text" in result:
        resposta = result["text"]
        if not resposta.strip():
            print("⚠️ Resposta vazia do modelo; erros coletados:", result.get("errors"))
            return "❌ O modelo retornou resposta vazia. Veja logs do servidor para detalhes."
        return resposta
    print("⚠️ Sem resposta do modelo — conteúdo de result:", result)
    return "❌ Sem resposta do modelo."
        return f"❌ Erro: {result['error']}"
    return result.get("text", "❌ Nenhuma resposta recebida.")

# ==========================================================
# ROTAS
# ==========================================================
@app.route("/")
def index():
    return render_template("index.html")
def home():
    return "✅ Alpha Analyst API rodando."

@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json() or {}
    user_input = data.get("message", "").strip()
    if not user_input:
    user_msg = data.get("message", "").strip()
    if not user_msg:
        return jsonify({"response": "Mensagem vazia."}), 400

    try:
        # Timeout aumentado: 300 segundos (5 minutos)
        resposta = gerar_insights(user_input, timeout_seconds=300)
        return jsonify({"response": resposta})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"response": f"Erro: {str(e)}"}), 500
    resposta = gerar_resposta(user_msg)
    return jsonify({"response": resposta})

# ==========================================================
# INICIALIZAÇÃO
# MAIN
# ==========================================================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
