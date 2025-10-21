from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})  # 🔓 libera CORS pra frontend

# Armazena temporariamente os dados do upload
uploaded_data = None


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "API online"})


@app.route("/api/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok"})


@app.route("/api/upload", methods=["POST"])
def upload_file():
    global uploaded_data

    if "file" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado."}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "Nome de arquivo inválido."}), 400

    try:
        # Aceita .csv e .xlsx
        if file.filename.endswith(".csv"):
            df = pd.read_csv(file)
        elif file.filename.endswith(".xlsx"):
            df = pd.read_excel(file)
        else:
            return jsonify({"error": "Formato não suportado. Envie CSV ou XLSX."}), 400

        uploaded_data = df  # guarda em memória

        # Cria um resumo simples
        summary = f"Arquivo '{file.filename}' carregado com sucesso! {df.shape[0]} linhas e {df.shape[1]} colunas detectadas."
        preview = df.head(3).to_dict(orient="records")
        summary += f"\n\nExemplo de dados:\n{preview}"

        return jsonify({"data_summary": summary})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat", methods=["POST"])
def chat():
    global uploaded_data
    data = request.get_json()
    message = data.get("message", "").lower()
    has_file = data.get("is_file_loaded", False)

    if not message:
        return jsonify({"error": "Mensagem vazia."}), 400

    # Respostas básicas
    if not has_file or uploaded_data is None:
        reply = "Nenhum arquivo foi carregado ainda. Envie um CSV ou XLSX para análise."
        return jsonify({"reply": reply})

    df = uploaded_data

    # Exemplos de interpretação simples
    if "colunas" in message:
        reply = f"As colunas do arquivo são: {', '.join(df.columns)}."
    elif "linhas" in message or "quantidade" in message:
        reply = f"O arquivo possui {df.shape[0]} linhas e {df.shape[1]} colunas."
    elif "exemplo" in message or "amostra" in message:
        reply = f"Veja as 3 primeiras linhas:\n{df.head(3).to_dict(orient='records')}"
    else:
        reply = "Análise básica concluída. Faça perguntas sobre colunas, exemplos ou totais!"

    return jsonify({"reply": reply})


@app.route("/api/reset", methods=["POST"])
def reset():
    global uploaded_data
    uploaded_data = None
    return jsonify({"status": "reset"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
