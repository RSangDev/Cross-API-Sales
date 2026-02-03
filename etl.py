import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Configurações via env vars (sem defaults sensíveis)
DB_HOST = os.getenv('DB_HOST', 'postgres')  # Nome do container no Docker
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecommerce_db')
DB_USER = os.getenv('DB_USER')  # Sem default – force via env ou yml
DB_PASS = os.getenv('DB_PASS')  # Sem default

MOCK_API_URL = os.getenv('MOCK_API_URL', 'http://mock-api:5000')  # Default para Docker

def fetch_data(marketplace, num_items=50):
    """Chama o endpoint mock e retorna JSON."""
    endpoint = f'/{marketplace}/produtos?num={num_items}'
    response = requests.get(MOCK_API_URL + endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erro ao chamar API {marketplace}: {response.status_code}")

def clean_and_transform(df):
    """Limpa erros: nulos, preços inválidos, duplicatas."""
    # Converte preço para numérico (strings viram NaN)
    df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
    # Preenche nulos
    df['nome'] = df['nome'].fillna('Desconhecido')
    df['preco'] = df['preco'].fillna(0)  # Ou média, dependendo da lógica
    # Remove duplicatas por ID
    df = df.drop_duplicates(subset=['id'])
    # Normaliza categoria (ex: lowercase)
    df['categoria'] = df['categoria'].str.lower()
    # Converte moeda se preciso (ex: USD para BRL, assumindo taxa fixa)
    if 'moeda' in df.columns and df['moeda'].iloc[0] == 'USD':
        df['preco'] *= 5.0  # Taxa exemplo USD->BRL
        df['moeda'] = 'BRL'
    return df

def load_to_db(df, table_name):
    if not DB_USER or not DB_PASS:
        raise ValueError("DB_USER e DB_PASS devem ser definidos via env vars!")

    """Carrega DF no PostgreSQL com SQLAlchemy."""
    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Cria a tabela se não existir — IF NOT EXISTS evita race condition
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT PRIMARY KEY,
                nome TEXT,
                preco NUMERIC,
                categoria TEXT,
                moeda TEXT
            )
        """))

    # Insere dados com upserts via loop e execute (ON CONFLICT) - usando begin() para transação
    with engine.begin() as conn:
        for _, row in df.iterrows():
            insert_query = text(f"""
                INSERT INTO {table_name} (id, nome, preco, categoria, moeda)
                VALUES (:id, :nome, :preco, :categoria, :moeda)
                ON CONFLICT (id) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    preco = EXCLUDED.preco,
                    categoria = EXCLUDED.categoria,
                    moeda = EXCLUDED.moeda
            """)
            conn.execute(insert_query, {
                'id': row['id'],
                'nome': row['nome'],
                'preco': row['preco'],
                'categoria': row['categoria'],
                'moeda': row['moeda']
            })
        # commit automático ao sair do bloco begin()

    print(f"Dados carregados na tabela {table_name}")

def generate_dashboard():
    """Gera dashboard completo com Plotly e salva como HTML."""
    import plotly.graph_objects as go
    import plotly.io as pio
    from datetime import datetime

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Busca dados separados por marketplace
    with engine.connect() as conn:
        result_ebay = conn.execute(text("SELECT * FROM produtos_ebay"))
        df_ebay = pd.DataFrame(result_ebay.fetchall(), columns=result_ebay.keys())

        result_ml = conn.execute(text("SELECT * FROM produtos_ml"))
        df_ml = pd.DataFrame(result_ml.fetchall(), columns=result_ml.keys())

    # Adiciona coluna marketplace para diferenciar
    df_ebay['marketplace'] = 'eBay'
    df_ml['marketplace'] = 'Mercado Livre'
    df_all = pd.concat([df_ebay, df_ml], ignore_index=True)

    # --- Dados agregados ---
    categorias = sorted(df_all['categoria'].unique())
    pivot_media = df_all.groupby(['categoria', 'marketplace'])['preco'].mean().unstack(fill_value=0)
    pivot_count = df_all.groupby(['categoria', 'marketplace'])['id'].count().unstack(fill_value=0)

    # --- Cores ---
    COR_EBAY = '#e53e3e'
    COR_ML = '#38a169'
    COR_BG = '#0f1923'
    COR_CARD = '#1a2733'
    COR_TEXTO = '#e2e8f0'
    COR_TEXTO_DIM = '#718096'

    # Estilo base compartilhado entre todos os gráficos
    layout_base = dict(
        paper_bgcolor=COR_BG,
        plot_bgcolor=COR_CARD,
        font=dict(family='Segoe UI, sans-serif', color=COR_TEXTO, size=13),
        xaxis=dict(showgrid=False, tickfont=dict(color=COR_TEXTO), zerolinecolor=COR_CARD),
        yaxis=dict(showgrid=True, gridcolor='#2d3a4a', tickfont=dict(color=COR_TEXTO), zerolinecolor=COR_CARD),
        legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color=COR_TEXTO)),
        height=340,
        margin=dict(t=50, b=40, l=40, r=30),
    )

    def get_col(pivot, marketplace):
        """Pega valores de uma coluna do pivot de forma segura."""
        if marketplace in pivot.columns:
            return [float(pivot.loc[c, marketplace]) if c in pivot.index else 0 for c in categorias]
        return [0] * len(categorias)

    # --- Fig 1: Preço médio por categoria ---
    fig1 = go.Figure(data=[
        go.Bar(name='eBay', x=categorias, y=get_col(pivot_media, 'eBay'),
               marker_color=COR_EBAY, width=0.35),
        go.Bar(name='Mercado Livre', x=categorias, y=get_col(pivot_media, 'Mercado Livre'),
               marker_color=COR_ML, width=0.35),
    ])
    fig1.update_layout(**layout_base, barmode='group',
                       title_text='Preço Médio por Categoria', title_font=dict(size=18, color=COR_TEXTO))

    # --- Fig 2: Quantidade de produtos por categoria ---
    fig2 = go.Figure(data=[
        go.Bar(name='eBay', x=categorias, y=get_col(pivot_count, 'eBay'),
               marker_color=COR_EBAY),
        go.Bar(name='Mercado Livre', x=categorias, y=get_col(pivot_count, 'Mercado Livre'),
               marker_color=COR_ML),
    ])
    fig2.update_layout(**layout_base, barmode='stack',
                       title_text='Quantidade de Produtos por Categoria', title_font=dict(size=18, color=COR_TEXTO))

    # --- Fig 3: Distribuição de preços ---
    fig3 = go.Figure(data=[
        go.Histogram(name='eBay', x=df_ebay['preco'], nbinsx=20, marker_color=COR_EBAY, opacity=0.7),
        go.Histogram(name='Mercado Livre', x=df_ml['preco'], nbinsx=20, marker_color=COR_ML, opacity=0.7),
    ])
    fig3.update_layout(**layout_base, barmode='overlay',
                       title_text='Distribuição de Preços', title_font=dict(size=18, color=COR_TEXTO),
                       xaxis_title='Preço (R$)', yaxis_title='Quantidade')

    # --- Fig 4: Proporção de produtos (donut) ---
    fig4 = go.Figure(data=[go.Pie(
        labels=['eBay', 'Mercado Livre'],
        values=[len(df_ebay), len(df_ml)],
        marker=dict(colors=[COR_EBAY, COR_ML]),
        textinfo='label+percent',
        textfont=dict(size=14, color=COR_TEXTO),
        hole=0.55,
    )])
    fig4.update_layout(paper_bgcolor=COR_BG, plot_bgcolor=COR_CARD,
                       font=dict(family='Segoe UI, sans-serif', color=COR_TEXTO, size=13),
                       height=340, margin=dict(t=50, b=40, l=40, r=30), showlegend=False,
                       title_text='Proporção de Produtos', title_font=dict(size=18, color=COR_TEXTO))

    # --- KPIs ---
    total_produtos = len(df_all)
    media_geral = df_all['preco'].mean()
    preco_max = df_all['preco'].max()
    preco_min = df_all[df_all['preco'] > 0]['preco'].min()

    # --- Gráficos como HTML ---
    chart1_html = pio.to_html(fig1, include_plotlyjs=False, full_html=False, config={"responsive": True})
    chart2_html = pio.to_html(fig2, include_plotlyjs=False, full_html=False, config={"responsive": True})
    chart3_html = pio.to_html(fig3, include_plotlyjs=False, full_html=False, config={"responsive": True})
    chart4_html = pio.to_html(fig4, include_plotlyjs=False, full_html=False, config={"responsive": True})

    agora = datetime.now().strftime('%d/%m/%Y %H:%M')

    html = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <title>E-Commerce Pipeline — Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{
                background: {COR_BG};
                color: {COR_TEXTO};
                font-family: 'Segoe UI', sans-serif;
                min-height: 100vh;
                padding: 32px;
            }}
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-end;
                margin-bottom: 28px;
                border-bottom: 1px solid #2d3a4a;
                padding-bottom: 16px;
            }}
            .header h1 {{ font-size: 26px; font-weight: 600; letter-spacing: -0.5px; }}
            .header h1 span {{ color: {COR_EBAY}; }}
            .header .timestamp {{ color: {COR_TEXTO_DIM}; font-size: 13px; }}
            .kpis {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 16px;
                margin-bottom: 28px;
            }}
            .kpi-card {{
                background: {COR_CARD};
                border-radius: 12px;
                padding: 20px 22px;
                border: 1px solid #2d3a4a;
            }}
            .kpi-card .label {{
                color: {COR_TEXTO_DIM};
                font-size: 12px;
                text-transform: uppercase;
                letter-spacing: 0.8px;
                margin-bottom: 8px;
            }}
            .kpi-card .value {{ font-size: 26px; font-weight: 700; }}
            .kpi-card .value.red {{ color: {COR_EBAY}; }}
            .kpi-card .value.green {{ color: {COR_ML}; }}
            .kpi-card .value.white {{ color: {COR_TEXTO}; }}
            .charts-grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
            }}
            .chart-card {{
                background: {COR_CARD};
                border-radius: 12px;
                border: 1px solid #2d3a4a;
                padding: 8px;
                overflow: hidden;
            }}
            @media (max-width: 900px) {{
                .charts-grid {{ grid-template-columns: 1fr; }}
                .kpis {{ grid-template-columns: repeat(2, 1fr); }}
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏪 E-Commerce <span>Pipeline</span> Dashboard</h1>
            <div class="timestamp">Atualizado em {agora}</div>
        </div>
        <div class="kpis">
            <div class="kpi-card">
                <div class="label">Total de Produtos</div>
                <div class="value white">{total_produtos}</div>
            </div>
            <div class="kpi-card">
                <div class="label">Preço Médio Geral</div>
                <div class="value green">R$ {media_geral:,.2f}</div>
            </div>
            <div class="kpi-card">
                <div class="label">Maior Preço</div>
                <div class="value red">R$ {preco_max:,.2f}</div>
            </div>
            <div class="kpi-card">
                <div class="label">Menor Preço</div>
                <div class="value green">R$ {preco_min:,.2f}</div>
            </div>
        </div>
        <div class="charts-grid">
            <div class="chart-card">{chart1_html}</div>
            <div class="chart-card">{chart2_html}</div>
            <div class="chart-card">{chart3_html}</div>
            <div class="chart-card">{chart4_html}</div>
        </div>
    </body>
    </html>
    """

    with open('dashboard.html', 'w', encoding='utf-8') as f:
        f.write(html)

    print("Dashboard gerado: dashboard.html")

def run_etl(num_items=50):
    """Fluxo completo ETL."""
    # Extract
    ebay_data = fetch_data('ebay', num_items)
    ml_data = fetch_data('mercadolivre', num_items)
    
    # Transform
    df_ebay = pd.DataFrame(ebay_data)
    df_ml = pd.DataFrame(ml_data)
    df_ebay_clean = clean_and_transform(df_ebay)
    df_ml_clean = clean_and_transform(df_ml)
    
    # Load
    load_to_db(df_ebay_clean, 'produtos_ebay')
    load_to_db(df_ml_clean, 'produtos_ml')
    
    # Dashboard
    generate_dashboard()

if __name__ == '__main__':
    run_etl()