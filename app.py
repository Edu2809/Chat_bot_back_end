from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

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
        if file.filename.endswith(".csv"):
            df = pd.read_csv(file)
        elif file.filename.endswith(".xlsx"):
            df = pd.read_excel(file)
        else:
            return jsonify({"error": "Formato não suportado. Envie CSV ou XLSX."}), 400

        uploaded_data = df

        summary = f"✅ Arquivo '{file.filename}' carregado com sucesso!\n"
        summary += f"Linhas: {df.shape[0]}, Colunas: {df.shape[1]}.\n"
        summary += f"Colunas detectadas: {', '.join(df.columns)}.\n"
        preview = df.head(3).to_dict(orient="records")

        return jsonify({"data_summary": summary, "preview": preview})

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

    if not has_file or uploaded_data is None:
        return jsonify({
            "reply": "📂 Nenhum arquivo foi carregado ainda. Envie um CSV ou XLSX para eu analisar!"
        })

    df = uploaded_data

    # ---- Lógica de respostas inteligentes ----
    reply = None

    try:
        # Linhas / colunas
        if "linha" in message or "quantidade" in message or "tamanho" in message:
            reply = f"O arquivo possui {df.shape[0]} linhas e {df.shape[1]} colunas."

        # Nome das colunas
        elif "coluna" in message:
            reply = f"As colunas do arquivo são: {', '.join(df.columns)}."

        # Exemplo / amostra
        elif "exemplo" in message or "amostra" in message:
            reply = f"Aqui estão as 3 primeiras linhas:\n{df.head(3).to_dict(orient='records')}"

        # Produto mais vendido
        elif "produto mais vendido" in message or "item mais vendido" in message:
            colunas = [c.lower() for c in df.columns]
            possiveis_produtos = [c for c in df.columns if "produto" in c.lower() or "item" in c.lower()]
            possiveis_qtd = [c for c in df.columns if "quant" in c.lower() or "venda" in c.lower()]

            if possiveis_produtos and possiveis_qtd:
                produto_col = possiveis_produtos[0]
                qtd_col = possiveis_qtd[0]
                resumo = df.groupby(produto_col)[qtd_col].sum().sort_values(ascending=False)
                top_produto = resumo.index[0]
                top_qtd = resumo.iloc[0]
                reply = f"🏆 O produto mais vendido foi **{top_produto}**, com um total de {top_qtd} vendas."
            else:
                reply = "Não encontrei colunas relacionadas a produtos e quantidades no arquivo."

        # Média de uma coluna
        elif "média" in message:
            for col in df.columns:
                if col.lower() in message and pd.api.types.is_numeric_dtype(df[col]):
                    reply = f"A média da coluna '{col}' é {df[col].mean():.2f}."
                    break
            if reply is None:
                reply = "Não consegui identificar qual coluna calcular a média. Tente 'média da coluna X'."

        # Valor máximo ou mínimo
        elif "maior" in message or "máximo" in message:
            for col in df.columns:
                if col.lower() in message and pd.api.types.is_numeric_dtype(df[col]):
                    reply = f"O maior valor da coluna '{col}' é {df[col].max():.2f}."
                    break
            if reply is None:
                reply = "Não identifiquei a coluna para calcular o valor máximo."

        elif "menor" in message or "mínimo" in message:
            for col in df.columns:
                if col.lower() in message and pd.api.types.is_numeric_dtype(df[col]):
                    reply = f"O menor valor da coluna '{col}' é {df[col].min():.2f}."
                    break
            if reply is None:
                reply = "Não identifiquei a coluna para calcular o valor mínimo."

        # Padrão
        else:
            reply = "Posso responder perguntas sobre colunas, médias, máximos, produtos mais vendidos, etc!"

    except Exception as e:
        reply = f"Ocorreu um erro ao analisar: {str(e)}"

    return jsonify({"reply": reply})


@app.route("/api/reset", methods=["POST"])
def reset():
    global uploaded_data
    uploaded_data = None
    return jsonify({"status": "reset"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
