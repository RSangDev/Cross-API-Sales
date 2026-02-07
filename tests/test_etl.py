import sys
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock

# CRITICAL: Setar env vars ANTES de importar etl
# Porque DB_USER e DB_PASS são lidos no momento do import
os.environ["DB_USER"] = "test_user"
os.environ["DB_PASS"] = "test_pass"

# Ajusta path para import do etl.py (da raiz)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from etl import (  # noqa: E402
    fetch_data,
    clean_and_transform,
    load_to_db,
    generate_dashboard,
    run_etl,
)


@pytest.fixture
def mock_df():
    """Fixture para DF mock com erros."""
    data = {
        "id": ["1", "1", "3"],
        "nome": [None, "Produto", "Outro"],
        "preco": ["invalido", 100, 200],
        "categoria": ["Eletronicos", "ROUPAS", "casa"],
        "moeda": ["USD", "BRL", "USD"],
    }
    return pd.DataFrame(data)


def test_fetch_data_success(mocker):
    """Testa fetch_data com resposta 200."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": "test"}]
    mocker.patch("requests.get", return_value=mock_response)

    result = fetch_data("ebay", 1)
    assert result == [{"id": "test"}]


def test_fetch_data_failure(mocker):
    """Testa fetch_data com erro 404."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mocker.patch("requests.get", return_value=mock_response)

    with pytest.raises(Exception, match="Erro ao chamar API ebay: 404"):
        fetch_data("ebay")


def test_clean_and_transform(mock_df):
    """Testa clean_and_transform."""
    cleaned = clean_and_transform(mock_df)

    assert len(cleaned) == 2  # Duplicatas removidas
    assert cleaned["nome"].tolist() == ["Desconhecido", "Outro"]
    assert cleaned["preco"].tolist() == [
        0.0,
        1000.0,
    ]  # Invalido -> 0, 200 USD *5 = 1000
    assert cleaned["categoria"].tolist() == ["eletronicos", "casa"]
    assert cleaned["moeda"].tolist() == ["BRL", "BRL"]  # USD -> BRL


def test_load_to_db(mocker, mock_df):
    """Testa load_to_db com mocks."""
    # Env vars já foram setadas no topo do arquivo antes do import

    mock_engine = MagicMock()
    mock_conn = MagicMock()

    # Mock do context manager begin()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    mocker.patch("etl.create_engine", return_value=mock_engine)

    load_to_db(mock_df, "test_table")

    # Verifica que create_engine foi chamado
    assert mock_engine.begin.call_count >= 1


def test_generate_dashboard(mocker):
    """Testa generate_dashboard com mock."""
    # Env vars já foram setadas no topo do arquivo antes do import

    mock_engine = MagicMock()
    mock_conn = MagicMock()

    # Cria DFs com as colunas corretas que generate_dashboard espera
    mock_df_ebay = pd.DataFrame(
        {
            "id": ["1", "2"],
            "nome": ["Produto A", "Produto B"],
            "preco": [100.0, 200.0],
            "categoria": ["eletronicos", "casa"],
            "moeda": ["BRL", "BRL"],
        }
    )

    mock_df_ml = pd.DataFrame(
        {
            "id": ["3", "4"],
            "nome": ["Produto C", "Produto D"],
            "preco": [150.0, 250.0],
            "categoria": ["roupas", "eletronicos"],
            "moeda": ["BRL", "BRL"],
        }
    )

    # Mock result que será retornado por conn.execute()
    mock_result_ebay = MagicMock()
    mock_result_ebay.fetchall.return_value = mock_df_ebay.to_records(
        index=False
    ).tolist()
    mock_result_ebay.keys.return_value = mock_df_ebay.columns.tolist()

    mock_result_ml = MagicMock()
    mock_result_ml.fetchall.return_value = mock_df_ml.to_records(index=False).tolist()
    mock_result_ml.keys.return_value = mock_df_ml.columns.tolist()

    # Mock execute para retornar os resultados corretos
    mock_conn.execute.side_effect = [mock_result_ebay, mock_result_ml]

    # Mock context manager
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    mocker.patch("etl.create_engine", return_value=mock_engine)

    # Mock da função open para evitar criação de arquivo
    mock_open = mocker.patch("builtins.open", mocker.mock_open())

    generate_dashboard()

    # Verifica que o arquivo foi "escrito"
    mock_open.assert_called_once_with("dashboard.html", "w", encoding="utf-8")


def test_run_etl(mocker):
    """Testa run_etl end-to-end com mocks."""
    # BUG FIX #3: Captura o mock retornado por mocker.patch para verificar call_count
    mock_fetch = mocker.patch(
        "etl.fetch_data", side_effect=[[{"id": "1"}], [{"id": "2"}]]
    )
    mocker.patch("etl.clean_and_transform", side_effect=lambda df: df)
    mock_load = mocker.patch("etl.load_to_db")
    mock_dashboard = mocker.patch("etl.generate_dashboard")  # Captura o mock

    run_etl(1)

    # Verifica chamadas (2 fetch, 2 load, 1 dashboard)
    assert mock_fetch.call_count == 2
    assert mock_load.call_count == 2
    assert (
        mock_dashboard.call_count == 1
    )  # Agora usa o mock_dashboard que foi retornado
