from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import text, event
from sqlalchemy.exc import SQLAlchemyError

from models import db
from services.job_status import read_cancel_flag, update_status_fields

BATCH_SIZE = 1000
INPUT_DIR = Path("upload/emp")
OUTPUT_DIR = Path("outputs/td_emp")
BASE_DIR = Path(__file__).resolve().parents[1]
JSON_CHAVES_PATH = BASE_DIR / "static" / "js" / "chaves_planejamento.json"
JSON_CASOS_PATH = BASE_DIR / "static" / "js" / "chave_arrumar.json"
JSON_FORCAR_PATH = BASE_DIR / "static" / "js" / "forcar_chave.json"

_FAST_EXEC_ENABLED = False

COL_MAP = {
    "chave": "chave",
    "chave_de_planejamento": "chave_planejamento",
    "regiao": "regiao",
    "subfuncao_ug": "subfuncao_ug",
    "adj": "adj",
    "macropolitica": "macropolitica",
    "pilar": "pilar",
    "eixo": "eixo",
    "politica_decreto": "politica_decreto",
    "exercicio": "exercicio",
    "situacao": "situacao",
    "historico": "historico",
    "no_emp": "numero_emp",
    "numero_emp": "numero_emp",
    "no_ped": "numero_ped",
    "numero_ped": "numero_ped",
    "no_contrato": "numero_contrato",
    "no_convenio": "numero_convenio",
    "dotacao_orcamentaria": "dotacao_orcamentaria",
    "funcao": "funcao",
    "subfuncao": "subfuncao",
    "programa_de_governo": "programa_governo",
    "paoe": "paoe",
    "natureza_de_despesa": "natureza_despesa",
    "cat_econ": "cat_econ",
    "grupo": "grupo",
    "modalidade": "modalidade",
    "fonte": "fonte",
    "iduso": "iduso",
    "elemento": "elemento",
    "uo": "uo",
    "nome_da_unidade_orcamentaria": "nome_unidade_orcamentaria",
    "nome_unidade_orcamentaria": "nome_unidade_orcamentaria",
    "ug": "ug",
    "nome_da_unidade_gestora": "nome_unidade_gestora",
    "nome_unidade_gestora": "nome_unidade_gestora",
    "data_emissao": "data_emissao",
    "data_criacao": "data_criacao",
    "valor_emp": "valor_emp",
    "devolucao_gcv": "devolucao_gcv",
    "valor_emp_devolucao_gcv": "valor_emp_devolucao_gcv",
    "tipo_empenho": "tipo_empenho",
    "tipo_de_despesa": "tipo_despesa",
    "credor": "credor",
    "nome_do_credor": "nome_credor",
    "nome_credor": "nome_credor",
    "cpf_cnpj_do_credor": "cpf_cnpj_credor",
    "cpf_cnpj_credor": "cpf_cnpj_credor",
    "categoria_do_credor": "categoria_credor",
    "categoria_credor": "categoria_credor",
}

# Mapeamento de cabecalhos corrompidos/acentuados para nomes canonicos (com acento correto)
COLUNAS_NORMALIZACAO = {
    "ExercÇðcio": "Exercício",
    "Exercício": "Exercício",
    "Exercicio": "Exercício",
    "SituaÇõÇœo": "Situação",
    "Situação": "Situação",
    "Situacao": "Situação",
    "HistÇürico": "Histórico",
    "Histórico": "Histórico",
    "Historico": "Histórico",
    "N¶§ EMP": "Nº EMP",
    "N¶§_EMP": "Nº EMP",
    "Numero EMP": "Nº EMP",
    "N¶§ PED": "Nº PED",
    "Numero PED": "Nº PED",
    "N¶§ Contrato": "Nº Contrato",
    "Numero Contrato": "Nº Contrato",
    "N¶§ ConvÇ¦nio": "Nº Convênio",
    "Numero Convenio": "Nº Convênio",
    "DotaÇõÇœo OrÇõamentÇ­ria": "Dotação Orçamentária",
    "Dotacao Orcamentaria": "Dotação Orçamentária",
    "Data emissÇœo": "Data emissão",
    "Data emissão": "Data emissão",
    "Data emissao": "Data emissão",
    "Data criaÇõÇœo": "Data criação",
    "Data criação": "Data criação",
    "Data criacao": "Data criação",
    "DevoluÇõÇœo GCV": "Devolução GCV",
    "Devolucao GCV": "Devolução GCV",
    "Valor EMP-DevoluÇõÇœo GCV": "Valor EMP-Devolução GCV",
    "Valor EMP-Devolucao GCV": "Valor EMP-Devolução GCV",
    "RegiÇœo": "Região",
    "Regiao": "Região",
    "SubfunÇõÇœo + UG": "Subfunção + UG",
    "Subfuncao + UG": "Subfunção + UG",
    "MacropolÇðtica": "Macropolítica",
    "Macropolitica": "Macropolítica",
    "PolÇðtica_Decreto": "Política_Decreto",
    "Politica_Decreto": "Política_Decreto",
    "SubfunÇõÇœo": "Subfunção",
    "Subfuncao": "Subfunção",
    "FunÇõÇœo": "Função",
    "Funcao": "Função",
    "Tipo Conta BancÇ­ria": "Tipo Conta Bancária",
    "Tipo Conta Bancaria": "Tipo Conta Bancária",
    "N¶§ Processo OrÇõamentÇ­rio de Pagamento": "Nº Processo Orçamentário de Pagamento",
    "Numero Processo Orcamentario de Pagamento": "Nº Processo Orçamentário de Pagamento",
    "N¶§ NOBLIST": "Nº NOBLIST",
    "N¶§ DOTLIST": "Nº DOTLIST",
    "N¶§ OS": "Nº OS",
    "N¶§ Emenda (EP)": "Nº Emenda (EP)",
    "N¶§ ABJ": "Nº ABJ",
    "N¶§ Processo do Sequestro Judicial": "Nº Processo do Sequestro Judicial",
    "N¶§ LicitaÇõÇœo": "Nº Licitação",
    "Numero Licitacao": "Nº Licitação",
    "Ano LicitaÇõÇœo": "Ano Licitação",
    "Ano Licitacao": "Ano Licitação",
    "Finalidade de AplicaÇõÇœo FUNDEB (EMP)": "Finalidade de Aplicação FUNDEB (EMP)",
    "Finalidade de Aplicacao FUNDEB (EMP)": "Finalidade de Aplicação FUNDEB (EMP)",
    "Modalidade de AplicaÇõÇœo": "Modalidade de Aplicação",
    "Modalidade de Aplicacao": "Modalidade de Aplicação",
    "Nome da Modalidade de AplicaÇõÇœo": "Nome da Modalidade de Aplicação",
    "Nome da Modalidade de Aplicacao": "Nome da Modalidade de Aplicação",
    "UsuÇ­rio ResponsÇ­vel": "Usuário Responsável",
    "Usuario Responsavel": "Usuário Responsável",
    "NÇ§mero da LicitaÇõÇœo": "Número da Licitação",
    "Numero da Licitacao": "Número da Licitação",
    "Ano da LicitaÇõÇœo": "Ano da Licitação",
    "Ano da Licitacao": "Ano da Licitação",
    "Justificativa para despesa sem contrato(Sim/NÇœo)": "Justificativa para despesa sem contrato(Sim/Não)",
    "Justificativa para despesa sem contrato(Sim/Nao)": "Justificativa para despesa sem contrato(Sim/Não)",
    "SituaÇõÇœo NEX": "Situação NEX",
    "Situacao NEX": "Situação NEX",
    "N¶§ NEX": "Nº NEX",
    "Numero NEX": "Nº NEX",
    "N¶§ RPV": "Nº RPV",
    "Numero RPV": "Nº RPV",
    "N¶§ CAD": "Nº CAD",
    "Numero CAD": "Nº CAD",
    "N¶§ NLA": "Nº NLA",
    "Numero NLA": "Nº NLA",
}

# Correcao de termos corrompidos nas chaves forcadas (forcar_chave.json)
CORRECOES_FORCAR = {
    "GESTÇŸO_INOVAÇÎÇŸO": "GESTÃO_INOVAÇÃO",
    "P_GESTÇŸO_": "P_GESTÃO_",
    "E_GESTÇŸO_ESCOLAR": "E_GESTÃO_ESCOLAR",
    "E_GESTÇŸO_DO_PATRIM": "E_GESTÃO_DO_PATRIM",
    "E_VALORIZAÇÎÇŸO_PROF": "E_VALORIZAÇÃO_PROF",
    "VALORIZAÇÎÇŸO_PRO": "VALORIZAÇÃO_PRO",
    "_GESTÇŸO_ESCOLAR": "_GESTÃO_ESCOLAR",
    "_GESTÇŸO_PATRIM": "_GESTÃO_PATRIM",
    "_ALFABETIZAÇÎÇŸO": "_ALFABETIZAÇÃO",
    "E_ENSINO_MÇ%DIO": "E_ENSINO_MÉDIO",
    "_NOVO_ENSINO_MÇ%D": "_NOVO_ENSINO_MÉD",
    "CURRÇ?CULO": "CURRÍCULO",
    "CURRÇ¸CULO": "CURRÍCULO",
    "INFRAESTUTURA": "INFRAESTRUTURA",
}


def _canonicalizar_nome_coluna(coluna: str) -> str:
    texto = re.sub(r"\s+", " ", str(coluna)).strip()
    return COLUNAS_NORMALIZACAO.get(texto, texto)


def normalizar_nomes_colunas(df: pd.DataFrame) -> pd.DataFrame:
    renomear = {col: _canonicalizar_nome_coluna(col) for col in df.columns}
    df = df.rename(columns=renomear)
    df.columns = df.columns.str.strip()
    return df


def limpar_historico(texto: Any) -> str:
    if not isinstance(texto, str):
        return "NÃO INFORMADO"

    texto = texto.replace("_x000D_", " ").replace("\n", " ").replace("\r", " ")
    texto = re.sub(r"\s+\*\s+", " * ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto if texto else "NÃO INFORMADO"


def corrigir_caracteres(texto: Any) -> str:
    if isinstance(texto, str):
        texto = re.sub(r"[^\w\s,./\-|*]", "", texto)
        texto = re.sub(r"\s+", " ", texto).strip()
        return texto if texto else "NÃO INFORMADO"
    return "NÃO INFORMADO"


def normalizar_simples(texto: Any) -> str:
    if pd.isna(texto):
        return ""
    if not isinstance(texto, str):
        texto = str(texto)
    texto = texto.replace("\u00a0", " ")
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip().casefold()


def remover_empenhos_estornados(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    coluna_situacao = None
    for col in df.columns:
        if normalizar_simples(col) == "situacao":
            coluna_situacao = col
            break

    if coluna_situacao is None:
        return df, 0

    situacao_normalizada = df[coluna_situacao].apply(normalizar_simples)
    mascara_manter = ~situacao_normalizada.str.contains(
        "empenho emp com estorno total", na=False, regex=False
    )
    removidos = len(df) - mascara_manter.sum()
    df = df.loc[mascara_manter].reset_index(drop=True)
    return df, removidos


def obter_exercicio(df: pd.DataFrame) -> int | None:
    for col in df.columns:
        if normalizar_simples(col) == "exercicio":
            serie = pd.to_numeric(df[col], errors="coerce").dropna()
            if not serie.empty:
                return int(serie.mode().iloc[0])
    return None


def normalizar_para_comparacao(texto: Any) -> str:
    if not isinstance(texto, str):
        return ""
    texto = limpar_historico(texto)
    texto = corrigir_caracteres(texto)
    texto = re.sub(r"\s+\*\s+", " * ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto.lower()


def limpar_para_aba_emp(valor: Any) -> Any:
    if not isinstance(valor, str):
        return valor
    valor = re.sub(r"[\u0000-\u001f\u007f]", "", valor)
    valor = re.sub(r"\s+", " ", valor).strip()
    return valor


def _corrigir_termos_corrompidos(texto: str) -> str:
    if not isinstance(texto, str):
        return texto
    corrigido = texto
    for errado, correto in CORRECOES_FORCAR.items():
        corrigido = corrigido.replace(errado, correto)
    return corrigido


def canonicalizar_chave(chave: Any) -> Any:
    if not isinstance(chave, str):
        return chave
    partes = [p.strip() for p in chave.split("*") if p.strip()]
    if not partes:
        return chave
    return "* " + " * ".join(partes) + " *"


def carregar_chaves_planejamento(json_path: Path) -> list[str]:
    try:
        print(f" Lendo chaves de planejamento em: {json_path}")
        with open(json_path, "r", encoding="utf-8-sig") as file:
            chaves = json.load(file)
            chaves_norm = []
            for chave in chaves:
                bruto = _corrigir_termos_corrompidos(str(chave))
                bruto = bruto.replace("*", " * ")
                bruto = re.sub(r"\s+", " ", bruto)
                partes = [p.strip() for p in bruto.split("*") if p.strip()]
                if partes:
                    chaves_norm.append("* " + " * ".join(partes) + " *")
        print(f" Chaves carregadas (normalizadas): {len(chaves_norm)}")
        return chaves_norm
    except Exception as e:
        print(f"Erro ao carregar as chaves de planejamento: {e}")
        return []


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    colunas_monetarias = ["Valor EMP", "Devolução GCV", "Valor EMP-Devolução GCV"]
    colunas_datas = ["Data emissão", "Data criação"]
    colunas_numericas = ["Exercício", "UO", "UG", "Elemento"]

    df.replace({"": "NÃO INFORMADO", None: "NÃO INFORMADO"}, inplace=True)

    for col in colunas_monetarias:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].apply(lambda x: re.sub(r"\.(?=\d{3})", "", x))
            df[col] = df[col].str.replace(",", ".", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in colunas_monetarias:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: "{:.2f}".format(x))

    for col in colunas_datas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            if df[col].str.match(r"\d{4}-\d{2}-\d{2}").all():
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else "0000-00-00")

    for col in colunas_numericas:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


def formatar_para_saida_ptbr(df: pd.DataFrame) -> pd.DataFrame:
    df_saida = df.copy()

    colunas_monetarias = ["Valor EMP", "Devolução GCV", "Valor EMP-Devolução GCV"]
    for col in colunas_monetarias:
        if col in df_saida.columns:
            df_saida[col] = pd.to_numeric(df_saida[col], errors="coerce").fillna(0)
            df_saida[col] = df_saida[col].apply(
                lambda x: format(x, ",.2f").replace(",", "X").replace(".", ",").replace("X", ".")
            )

    colunas_datas = ["Data emissão", "Data criação"]
    for col in colunas_datas:
        if col in df_saida.columns:
            df_saida[col] = pd.to_datetime(df_saida[col], errors="coerce", dayfirst=False)
            df_saida[col] = df_saida[col].apply(lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "00/00/0000")

    return df_saida


def ensure_dirs() -> None:
    for base in (INPUT_DIR, OUTPUT_DIR, INPUT_DIR / "tmp", OUTPUT_DIR / "tmp"):
        base.mkdir(parents=True, exist_ok=True)


def move_existing_to_tmp(base_dir: Path) -> None:
    tmp = base_dir / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    for f in base_dir.glob("*.xlsx"):
        dest = tmp / f"{f.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{f.suffix}"
        try:
            f.rename(dest)
        except OSError:
            pass


def _normalize_col(name: Any) -> str:
    texto = str(name or "")
    texto = texto.replace("º", "o").replace("ª", "a")
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = re.sub(r"[^a-zA-Z0-9]+", "_", texto).strip("_").lower()
    return texto


def _clean_val(val: Any) -> Any:
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, str) and val.strip() == "-":
        return None
    return val


def _parse_valor_db(valor: Any) -> float | None:
    if valor is None:
        return None
    s = str(valor).strip()
    if s in (
        "",
        "-",
        "NÃO INFORMADO",
        "NAO INFORMADO",
        "NÃO IDENTIFICADO",
        "NAO IDENTIFICADO",
    ):
        return None
    s_num = re.sub(r"[^\d,.-]", "", s)
    if "," in s_num:
        s_num = s_num.replace(".", "").replace(",", ".")
    try:
        return float(s_num)
    except ValueError:
        return None


def _parse_data_db(valor: Any) -> datetime | None:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor
    s = str(valor).strip()
    if not s or s in ("-", "00/00/0000", "00/00/0000 00:00:00"):
        return None
    s = s.replace("-", "/")
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_ano(valor: Any) -> int | None:
    if not valor:
        return None
    match = re.search(r"(\d{4})", str(valor))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _enable_fast_executemany() -> None:
    global _FAST_EXEC_ENABLED
    if _FAST_EXEC_ENABLED:
        return

    @event.listens_for(db.engine, "before_cursor_execute")
    def _set_fast_exec(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-redef]
        if executemany:
            try:
                cursor.fast_executemany = True
            except Exception:
                pass

    _FAST_EXEC_ENABLED = True


def carregar_casos_especificos(json_path: Path) -> dict[str, str]:
    try:
        print(f" Lendo casos especificos em: {json_path}")
        with open(json_path, "r", encoding="utf-8-sig") as file:
            casos_especificos = json.load(file)
        casos_norm: dict[str, str] = {}
        for k, v in casos_especificos.items():
            chave_saida = canonicalizar_chave(_corrigir_termos_corrompidos(v))
            k_corrigido = _corrigir_termos_corrompidos(k)
            k_norm = normalizar_para_comparacao(k_corrigido)
            casos_norm[k_norm] = chave_saida
        print(f" Casos especificos carregados do arquivo: {json_path} ({len(casos_norm)})")
        return casos_norm
    except Exception as e:
        print(f" Erro ao carregar casos especificos: {e}")
        return {}


def extrair_chave_valida_do_historico(hist_limpo: str, chaves_planejamento: list[str]) -> str | None:
    for chave in chaves_planejamento:
        if chave in hist_limpo:
            return chave
    return None


def identificar_chave_planejamento(
    df: pd.DataFrame,
    chaves_planejamento: list[str],
    json_casos_path: Path,
    key_col_name: str = "Chave de Planejamento",
    partes_chave: int = 7,
) -> pd.DataFrame:
    casos_especificos = carregar_casos_especificos(json_casos_path)
    chaves_planejamento = [canonicalizar_chave(c) for c in chaves_planejamento]
    chaves_set = set(chaves_planejamento)

    def _para_pipe(chave: Any) -> str:
        if not isinstance(chave, str):
            return ""
        texto = chave.replace("*", "|")
        texto = re.sub(r"\s+", " ", texto).strip()
        if not texto.startswith("|"):
            texto = "| " + texto
        if not texto.endswith("|"):
            texto = texto + " |"
        return re.sub(r"\s+", " ", texto)

    chaves_pipe_set = set(_para_pipe(c) for c in chaves_planejamento if isinstance(c, str))

    debug_limite = 10
    debug_coletados: list[str] = []
    resultados: list[str] = []
    tamanho_janela = max(1, partes_chave)

    usar_fuzzy = False

    total = len(df)
    print(f"Identificando chaves em {total} registros...")

    for idx, row in enumerate(df.iterrows(), start=1):
        _, dados = row
        hist = dados.get("Histórico", "")
        num_emp = str(dados.get("Nº EMP", "")).upper()

        if hist == "NÃO INFORMADO":
            resultados.append("NÃO IDENTIFICADO")
            continue

        hist_limpo = hist.strip()
        hist_limpo = hist_limpo.replace("*", " * ")
        hist_limpo = re.sub(r"\s+\*\s+", " * ", hist_limpo)
        hist_limpo = re.sub(r"\s+", " ", hist_limpo).strip()
        hist_limpo = _corrigir_termos_corrompidos(hist_limpo)
        if not hist_limpo.startswith("*"):
            hist_limpo = "* " + hist_limpo
        if not hist_limpo.endswith("*"):
            hist_limpo = hist_limpo + " *"
        hist_limpo = re.sub(r"\s*\*\s*", " * ", hist_limpo)
        hist_pipe = _para_pipe(hist_limpo)
        hist_pipe_comp = normalizar_para_comparacao(hist_pipe)

        chave_direta = extrair_chave_valida_do_historico(hist_limpo, chaves_planejamento)
        if chave_direta:
            if len(debug_coletados) < debug_limite:
                debug_coletados.append(f"[OK direto] EMP={num_emp} chave={canonicalizar_chave(chave_direta)}")
            resultados.append(canonicalizar_chave(chave_direta))
            continue
        if hist_pipe in chaves_pipe_set:
            chave_star = canonicalizar_chave(hist_pipe.replace("|", "*"))
            if len(debug_coletados) < debug_limite:
                debug_coletados.append(f"[OK pipe] EMP={num_emp} chave={chave_star}")
            resultados.append(chave_star)
            continue

        caso_encontrado = None
        hist_comp = normalizar_para_comparacao(hist)
        for caso_norm, chave in casos_especificos.items():
            if caso_norm and (caso_norm in hist_comp or caso_norm in hist_pipe_comp):
                caso_encontrado = chave
                break
        if caso_encontrado:
            if len(debug_coletados) < debug_limite:
                debug_coletados.append(f"[OK caso] EMP={num_emp} chave={canonicalizar_chave(caso_encontrado)}")
            resultados.append(canonicalizar_chave(caso_encontrado))
            continue

        partes = [p.strip() for p in hist_limpo.split("*") if p.strip()]
        chave_janela = "NÃO IDENTIFICADO"
        if len(partes) >= tamanho_janela:
            for i in range(len(partes) - tamanho_janela + 1):
                janela = "* " + " * ".join(partes[i : i + tamanho_janela]) + " *"
                if janela in chaves_set:
                    chave_janela = janela
                    if len(debug_coletados) < debug_limite:
                        debug_coletados.append(f"[OK janela] EMP={num_emp} chave={janela}")
                    break
                janela_pipe = _para_pipe(janela)
                if janela_pipe in chaves_pipe_set:
                    chave_janela = canonicalizar_chave(janela_pipe.replace("|", "*"))
                    if len(debug_coletados) < debug_limite:
                        debug_coletados.append(f"[OK janela-pipe] EMP={num_emp} chave={chave_janela}")
                    break
            if chave_janela == "NÃO IDENTIFICADO" and usar_fuzzy:
                trecho_partes = partes[:tamanho_janela]
                if any(trecho_partes):
                    trecho = "* " + " * ".join(trecho_partes) + " *"
                    match = process.extractOne(trecho, chaves_planejamento, scorer=fuzz.WRatio, score_cutoff=90)
                    if match:
                        chave_janela = canonicalizar_chave(match[0])
                        if len(debug_coletados) < debug_limite:
                            debug_coletados.append(f"[OK fuzzy] EMP={num_emp} chave={chave_janela}")
        resultados.append(
            chave_janela if chave_janela == "NÃO IDENTIFICADO" else canonicalizar_chave(chave_janela)
        )

        if idx % 1000 == 0:
            print(f"   ...processados {idx}/{total}")

    df.insert(0, key_col_name, resultados)
    df[key_col_name] = df[key_col_name].apply(
        lambda x: canonicalizar_chave(x) if isinstance(x, str) and x not in ["NÃO IDENTIFICADO", "IGNORADO"] else x
    )

    return df


def carregar_forcar_chaves(json_path: Path) -> dict[str, str]:
    try:
        print(f" Lendo forcamentos de chave em: {json_path}")
        with open(json_path, "r", encoding="utf-8-sig") as file:
            bruto = json.load(file)
        normalizado: dict[str, str] = {}
        for num, chave in bruto.items():
            chave_texto = str(chave).replace("|", "*")
            chave_texto = _corrigir_termos_corrompidos(chave_texto)
            chave_limpa = canonicalizar_chave(chave_texto)
            normalizado[str(num).strip()] = chave_limpa
        print(f" Forcamentos carregados: {len(normalizado)}")
        return normalizado
    except Exception as e:
        print(f" Erro ao carregar forcamentos de chave: {e}")
        return {}


def forcar_chaves_manualmente(df: pd.DataFrame, key_col_name: str = "Chave de Planejamento") -> pd.DataFrame:
    substituicoes = carregar_forcar_chaves(JSON_FORCAR_PATH)
    if not substituicoes:
        return df

    if "Nº EMP" not in df.columns:
        print(" Coluna 'Nº EMP' não encontrada para forçar chaves.")
        return df

    df["Nº EMP"] = df["Nº EMP"].astype(str).str.strip()

    for num_emp, chave in substituicoes.items():
        cond = df["Nº EMP"] == num_emp
        df.loc[cond, key_col_name] = canonicalizar_chave(chave)

    return df


def adicionar_novas_colunas(
    df: pd.DataFrame, key_col_name: str = "Chave de Planejamento", planejamento_ativo: bool = True
) -> pd.DataFrame | None:
    print(" Iniciando a adicao de novas colunas...")

    try:
        novas_colunas_planejamento = [
            "Região",
            "Subfunção + UG",
            "ADJ",
            "Macropolítica",
            "Pilar",
            "Eixo",
            "Política_Decreto",
        ]
        novas_colunas_orcamentarias = [
            "Função",
            "Subfunção",
            "Programa de Governo",
            "PAOE",
            "Natureza de Despesa",
            "Cat.Econ",
            "Grupo",
            "Modalidade",
            "Fonte",
            "Iduso",
            "Elemento",
            "Nome do Elemento",
        ]

        if "Exercício" not in df.columns:
            raise ValueError(" ERRO: Coluna 'Exercício' não encontrada no DataFrame!")

        if planejamento_ativo:
            for col in novas_colunas_planejamento:
                if col not in df.columns:
                    df.insert(df.columns.get_loc("Exercício"), col, "NÃO INFORMADO")

        if "Dotação Orçamentária" not in df.columns:
            raise ValueError(" ERRO: Coluna 'Dotação Orçamentária' não encontrada no DataFrame!")

        posicao_insercao = df.columns.get_loc("Dotação Orçamentária") + 1
        for col in novas_colunas_orcamentarias:
            if col not in df.columns:
                df.insert(posicao_insercao, col, "NÃO INFORMADO")
                posicao_insercao += 1

        df.columns = df.columns.str.strip()

        colunas = df.columns.tolist()

        if all(col in colunas for col in ["Fonte", "Iduso", "Nome do Elemento"]):
            colunas.remove("Fonte")
            colunas.remove("Iduso")
            indice_nome_elemento = colunas.index("Nome do Elemento")
            colunas.insert(indice_nome_elemento + 1, "Fonte")
            colunas.insert(indice_nome_elemento + 2, "Iduso")

        if all(col in colunas for col in ["Histórico", "Iduso", "Credor"]):
            colunas.remove("Histórico")
            indice_iduso = colunas.index("Iduso")
            colunas.insert(indice_iduso + 1, "Histórico")

        df = df[colunas]
        return df
    except Exception as e:
        print(f" ERRO na funcao adicionar_novas_colunas: {e}")
        return None


def ajustar_largura_colunas(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str = "EMP") -> None:
    worksheet = writer.sheets[sheet_name]
    if "Histórico" in df.columns:
        col_index = df.columns.get_loc("Histórico")
        worksheet.set_column(col_index, col_index, 120)
    worksheet = writer.sheets[sheet_name]

    for i, col in enumerate(df.columns):
        if col == "Histórico":
            continue
        max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, max_length)


def preencher_novas_colunas(
    df: pd.DataFrame,
    key_col_name: str = "Chave de Planejamento",
    planejamento_ativo: bool = True,
    partes_chave: int = 7,
) -> pd.DataFrame:
    if planejamento_ativo and key_col_name in df.columns:

        def extrair_valores(chave: Any) -> list[str]:
            if not isinstance(chave, str) or chave.strip() in ["", "NÃO IDENTIFICADO", "NÃO INFORMADO"]:
                return ["NÃO INFORMADO"] * partes_chave
            if chave.strip() == "#":
                return ["#"] * partes_chave
            partes = [p.strip() for p in chave.split("*") if p.strip()]
            if len(partes) < partes_chave:
                partes.extend(["NÃO INFORMADO"] * (partes_chave - len(partes)))
            elif len(partes) > partes_chave:
                partes = partes[:partes_chave]
            return partes

        valores_extraidos = df[key_col_name].apply(lambda x: extrair_valores(x))
        valores_extraidos = pd.DataFrame(
            valores_extraidos.tolist(),
            columns=["Região", "Subfunção + UG", "ADJ", "Macropolítica", "Pilar", "Eixo", "Política_Decreto"],
            index=df.index,
        )
        df.loc[:, ["Região", "Subfunção + UG", "ADJ", "Macropolítica", "Pilar", "Eixo", "Política_Decreto"]] = (
            valores_extraidos
        )
    else:
        df = df.drop(
            columns=["Região", "Subfunção + UG", "ADJ", "Macropolítica", "Pilar", "Eixo", "Política_Decreto"],
            errors="ignore",
        )

    def extrair_dotacao(dotacao: Any) -> list[str]:
        if not isinstance(dotacao, str) or dotacao.strip() == "":
            return ["NÃO INFORMADO"] * 7

        partes = [p.strip() for p in dotacao.split(".") if p.strip()]

        if len(partes) < 11:
            partes.extend(["NÃO INFORMADO"] * (11 - len(partes)))

        return [partes[2], partes[3], partes[4], partes[5], partes[7], partes[8], partes[9]]

    valores_dotacao = df["Dotação Orçamentária"].apply(lambda x: extrair_dotacao(x))
    valores_dotacao = pd.DataFrame(
        valores_dotacao.tolist(),
        columns=["Função", "Subfunção", "Programa de Governo", "PAOE", "Natureza de Despesa", "Fonte", "Iduso"],
        index=df.index,
    )

    df.loc[:, ["Função", "Subfunção", "Programa de Governo", "PAOE", "Natureza de Despesa", "Fonte", "Iduso"]] = (
        valores_dotacao
    )

    def extrair_natureza(natureza: Any) -> list[str]:
        if not isinstance(natureza, str) or len(natureza) < 4:
            return ["NÃO INFORMADO"] * 3
        return [natureza[0], natureza[1], natureza[2:4]]

    valores_natureza = df["Natureza de Despesa"].apply(lambda x: extrair_natureza(x))
    valores_natureza = pd.DataFrame(
        valores_natureza.tolist(), columns=["Cat.Econ", "Grupo", "Modalidade"], index=df.index
    )

    df.loc[:, ["Cat.Econ", "Grupo", "Modalidade"]] = valores_natureza

    df.columns = df.columns.str.strip()
    if "Situação" in df.columns:
        df["Situação"] = df["Situação"].astype(str).str.strip()
    df.columns = df.columns.str.strip()

    colunas_remover = [
        "Nº Processo Orçamentário de Pagamento",
        "Nº NOBLIST",
        "Nº DOTLIST",
        "Nº OS",
        "Nº Emenda (EP)",
        "Autor da Emenda (EP)",
        "Nº ABJ",
        "Nº Processo do Sequestro Judicial",
        "CBA",
        "Tipo Conta Bancária",
        "Nº Licitação",
        "Ano Licitação",
        "Nome do Elemento",
        "RP",
        "Ordenador",
        "Nome do Ordenador de Despesa",
        "Finalidade de Aplicação FUNDEB (EMP)",
        "Grupo Despesa",
        "Nome do Grupo Despesa",
        "Modalidade de Aplicação",
        "Nome da Modalidade de Aplicação",
        "Usuário Responsável",
        "Número da Licitação",
        "Ano da Licitação",
        "Fundamento Legal(Amparo Legal)",
        "Justificativa para despesa sem contrato(Sim/Não)",
        "Despesa em Processamento",
        "UO Extinta",
        "Nº NEX",
        "Situação NEX",
        "Valor da NEX",
        "Nº RPV",
        "RPV Vencido",
        "Nº CAD",
        "Nº NLA",
    ]
    df = df.drop(columns=[col for col in colunas_remover if col in df.columns], errors="ignore")

    return df


def mover_colunas(
    df: pd.DataFrame, colunas_para_mover: list[str], referencia: str = "Situação", mover_para_fim: bool = True
) -> pd.DataFrame:
    df.columns = df.columns.str.strip()

    colunas_existentes = [col for col in colunas_para_mover if col in df.columns]

    if not colunas_existentes:
        print(" Nenhuma coluna encontrada para mover!")
        return df

    colunas_restantes = [col for col in df.columns if col not in colunas_existentes]

    if not mover_para_fim and referencia in colunas_restantes:
        indice_referencia = colunas_restantes.index(referencia)
        nova_ordem = colunas_restantes[: indice_referencia + 1] + colunas_existentes + colunas_restantes[
            indice_referencia + 1 :
        ]
    else:
        nova_ordem = colunas_restantes + colunas_existentes

    df = df[nova_ordem]

    print(
        f" Colunas {colunas_existentes} movidas {'para o final' if mover_para_fim else f'apos {referencia}'} com sucesso!"
    )
    return df


def atualizar_tipo_despesa(df: pd.DataFrame) -> pd.DataFrame:
    if "Histórico" in df.columns and "Tipo de Despesa" in df.columns:
        df["Tipo de Despesa"] = df.apply(
            lambda row: "Bolsa"
            if re.search(r"\bbolsas?\b", str(row["Histórico"]).lower())
            else row["Tipo de Despesa"],
            axis=1,
        )
    else:
        print(" As colunas 'Histórico' ou 'Tipo de Despesa' nao foram encontradas no DataFrame.")

    return df


def mover_colunas_para_direita(
    df: pd.DataFrame, colunas_para_mover: list[str], referencia: str = "Nº PED"
) -> pd.DataFrame:
    df.columns = df.columns.str.strip()

    colunas_existentes = [col for col in colunas_para_mover if col in df.columns]

    if not colunas_existentes:
        print(" Nenhuma coluna encontrada para mover!")
        return df

    colunas_restantes = [col for col in df.columns if col not in colunas_existentes]

    if referencia not in colunas_restantes:
        print(f" Coluna de referencia '{referencia}' nao encontrada! As colunas serao movidas para o final.")
        nova_ordem = colunas_restantes + colunas_existentes
    else:
        indice_referencia = colunas_restantes.index(referencia)
        nova_ordem = colunas_restantes[: indice_referencia + 1] + colunas_existentes + colunas_restantes[
            indice_referencia + 1 :
        ]

    df = df[nova_ordem]

    print(f" Colunas {colunas_existentes} movidas para o lado direito de '{referencia}' com sucesso!")
    return df


def calcular_valor_liquido(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()

    if "Valor EMP" in df.columns and "Devolução GCV" in df.columns:
        df["Valor EMP"] = pd.to_numeric(df["Valor EMP"], errors="coerce").fillna(0)
        df["Devolução GCV"] = pd.to_numeric(df["Devolução GCV"], errors="coerce").fillna(0)

        df["Valor EMP-Devolução GCV"] = df["Valor EMP"] - df["Devolução GCV"]
        df["Valor EMP-Devolução GCV"] = df["Valor EMP-Devolução GCV"].round(2)

        colunas = df.columns.tolist()
        indice_referencia = colunas.index("Devolução GCV")

        colunas.remove("Valor EMP-Devolução GCV")
        colunas.insert(indice_referencia + 1, "Valor EMP-Devolução GCV")

        df = df[colunas]

        print(" Coluna 'Valor EMP-Devolução GCV' corrigida e criada ao lado direito de 'Devolução GCV' com sucesso!")
    else:
        print(" ERRO: As colunas 'Valor EMP' e 'Devolução GCV' nao foram encontradas no DataFrame.")

    return df


def carregar_planilha(file_path: Path, json_file_path: Path) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    try:
        print(" Tentando carregar a planilha...")
        xls = pd.ExcelFile(file_path)
        print(" Planilha carregada com sucesso!")

        sheet_names = xls.sheet_names
        aba_correta = None
        for aba in sheet_names:
            if aba.strip().lower() == "planilha1":
                aba_correta = aba
                break
        if aba_correta is None:
            aba_correta = sheet_names[0]

        print(" Preparando a nova aba 'emp' a partir da aba original...")
        df_raw = pd.read_excel(xls, sheet_name=aba_correta, dtype=str, header=None)

        linha_cabecalho = None
        for idx, valor in df_raw.iloc[:, 0].items():
            if isinstance(valor, str) and valor.strip().lower().startswith("exerc"):
                linha_cabecalho = idx
                break

        if linha_cabecalho is None:
            raise ValueError(" ERRO: N?o foi poss?vel localizar a linha de cabe?alho (Exerc?cio).")

        cabecalho = df_raw.iloc[linha_cabecalho].fillna("").apply(lambda x: str(x).strip())
        df_emp_base = df_raw.iloc[linha_cabecalho + 1 :].copy()
        df_emp_base.columns = cabecalho
        df_emp_base = normalizar_nomes_colunas(df_emp_base)

        df_emp_base = df_emp_base.apply(lambda col: col.map(limpar_para_aba_emp))
        print(" Aba 'emp' preparada (filtros removidos, espacos normalizados).")

        df = df_emp_base.copy()

        if df.empty:
            raise ValueError(" ERRO: A aba foi carregada, mas esta vazia apos remover filtros iniciais!")

        df, removidos_df = remover_empenhos_estornados(df)
        df_emp_base, removidos_base = remover_empenhos_estornados(df_emp_base)
        print(f" Registros removidos por estorno total (df): {removidos_df}")
        print(f" Registros removidos por estorno total (aba emp): {removidos_base}")

        if "Histórico" in df.columns:
            print(" Aplicando limpeza no campo 'Histórico'...")
            df["Histórico"] = df["Histórico"].apply(limpar_historico)

        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].apply(corrigir_caracteres)

        df = converter_tipos(df)

        exercicio_val = obter_exercicio(df)
        nova_chave = exercicio_val is not None and exercicio_val >= 2026
        key_col_name = "Chave" if nova_chave else "Chave de Planejamento"
        partes_chave = 4 if nova_chave else 7
        planejamento_ativo = not nova_chave

        print(" Carregando chaves de planejamento...")
        chaves_planejamento = carregar_chaves_planejamento(json_file_path)
        print(f" {len(chaves_planejamento)} chaves carregadas!")

        df = identificar_chave_planejamento(
            df, chaves_planejamento, JSON_CASOS_PATH, key_col_name=key_col_name, partes_chave=partes_chave
        )
        df = forcar_chaves_manualmente(df, key_col_name=key_col_name)

        print(" Adicionando novas colunas...")
        df = adicionar_novas_colunas(df, key_col_name=key_col_name, planejamento_ativo=planejamento_ativo)
        if df is None:
            raise ValueError(" ERRO: Falha ao adicionar novas colunas.")
        df = preencher_novas_colunas(
            df, key_col_name=key_col_name, planejamento_ativo=planejamento_ativo, partes_chave=partes_chave
        )

        df, removidos_df_final = remover_empenhos_estornados(df)
        df_emp_base, removidos_base_final = remover_empenhos_estornados(df_emp_base)
        if removidos_df_final or removidos_base_final:
            print(f" Remocao extra por estorno total - df: {removidos_df_final}, aba emp: {removidos_base_final}")

        print(" Processamento concluido!")
        return df, df_emp_base
    except Exception as e:
        print(f" ERRO na funcao carregar_planilha: {e}")
        return None


def processar_emp(file_path: Path) -> Path:
    resultado = carregar_planilha(file_path, JSON_CHAVES_PATH)
    if resultado is None:
        raise RuntimeError("DataFrame nao carregado corretamente.")

    df, df_emp_base = resultado
    if df is None or df.empty:
        raise RuntimeError("DataFrame nao carregado corretamente.")

    print(" DataFrame carregado com sucesso! Aplicando ajustes finais...")

    df = atualizar_tipo_despesa(df)
    df = mover_colunas(
        df, ["Data emissão", "Data criação", "Nº Contrato", "Nº Convênio"], referencia="Situação", mover_para_fim=False
    )
    df = mover_colunas_para_direita(df, ["Valor EMP", "Devolução GCV"], "Nº PED")
    df = calcular_valor_liquido(df)
    df = converter_tipos(df)
    df_saida = formatar_para_saida_ptbr(df)
    df_saida = df_saida.replace("NÃO INFORMADO", "-", regex=False)

    output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{Path(file_path).stem}_Tratado.xlsx"

    writer = pd.ExcelWriter(output_file, engine="xlsxwriter")
    df_emp_base.to_excel(writer, index=False, sheet_name="emp")
    df_saida.to_excel(writer, index=False, sheet_name="emp_tratado")
    ajustar_largura_colunas(writer, df_emp_base, sheet_name="emp")
    ajustar_largura_colunas(writer, df_saida, sheet_name="emp_tratado")
    writer.close()

    print(f" Planilha salva em: {output_file}")
    return output_file


def montar_registros_para_db(
    df: pd.DataFrame, data_arquivo: datetime, user_email: str, upload_id: int
) -> list[dict[str, Any]]:
    registros: list[dict[str, Any]] = []
    rows = df.to_dict(orient="records")
    for row in rows:
        payload: dict[str, Any] = {}
        for col, val in row.items():
            key = _normalize_col(col)
            db_col = COL_MAP.get(key)
            if not db_col:
                continue
            payload[db_col] = _clean_val(val)

        ano = _parse_ano(payload.get("exercicio"))
        if ano and ano >= 2026:
            payload["chave"] = payload.get("chave") or payload.get("chave_planejamento")
            payload["chave_planejamento"] = None
        else:
            payload["chave_planejamento"] = payload.get("chave_planejamento") or payload.get("chave")
            payload["chave"] = None

        for col in ("valor_emp", "devolucao_gcv", "valor_emp_devolucao_gcv"):
            if col in payload:
                payload[col] = _parse_valor_db(payload[col])
        for col in ("data_emissao", "data_criacao"):
            if col in payload:
                payload[col] = _parse_data_db(payload[col])

        safe_row: dict[str, Any] = {}
        for k, v in row.items():
            try:
                if pd.isna(v):
                    safe_row[k] = None
                    continue
            except Exception:
                pass
            if hasattr(v, "isoformat"):
                try:
                    safe_row[k] = v.isoformat()
                    continue
                except Exception:
                    pass
            safe_row[k] = v

        payload["raw_payload"] = json.dumps(safe_row, ensure_ascii=False)
        payload["upload_id"] = upload_id
        payload["data_atualizacao"] = datetime.utcnow()
        payload["data_arquivo"] = data_arquivo
        payload["user_email"] = user_email
        payload["ativo"] = True
        registros.append(payload)
    return registros


def update_database(
    df: pd.DataFrame, data_arquivo: datetime, user_email: str, upload_id: int
) -> int:
    insert_sql = text(
        """
        INSERT INTO emp (
            upload_id, chave, chave_planejamento, regiao, subfuncao_ug, adj, macropolitica, pilar, eixo,
            politica_decreto, exercicio, situacao, historico, numero_emp, numero_ped, numero_contrato,
            numero_convenio, dotacao_orcamentaria, funcao, subfuncao, programa_governo, paoe, natureza_despesa,
            cat_econ, grupo, modalidade, fonte, iduso, elemento, uo, nome_unidade_orcamentaria, ug, nome_unidade_gestora,
            data_emissao, data_criacao, valor_emp, devolucao_gcv, valor_emp_devolucao_gcv, tipo_empenho, tipo_despesa,
            credor, nome_credor, cpf_cnpj_credor, categoria_credor, raw_payload, data_atualizacao, data_arquivo,
            user_email, ativo
        )
        VALUES (
            :upload_id, :chave, :chave_planejamento, :regiao, :subfuncao_ug, :adj, :macropolitica, :pilar, :eixo,
            :politica_decreto, :exercicio, :situacao, :historico, :numero_emp, :numero_ped, :numero_contrato,
            :numero_convenio, :dotacao_orcamentaria, :funcao, :subfuncao, :programa_governo, :paoe, :natureza_despesa,
            :cat_econ, :grupo, :modalidade, :fonte, :iduso, :elemento, :uo, :nome_unidade_orcamentaria, :ug,
            :nome_unidade_gestora, :data_emissao, :data_criacao, :valor_emp, :devolucao_gcv,
            :valor_emp_devolucao_gcv, :tipo_empenho, :tipo_despesa, :credor, :nome_credor, :cpf_cnpj_credor,
            :categoria_credor, :raw_payload, :data_atualizacao, :data_arquivo, :user_email, :ativo
        )
        """
    )

    try:
        db.session.execute(text("UPDATE emp SET ativo = 0 WHERE ativo = 1"))
        db.session.commit()
    except SQLAlchemyError:
        db.session.rollback()
        raise

    _enable_fast_executemany()
    registros = montar_registros_para_db(df, data_arquivo, user_email, upload_id)
    total_registros = len(registros)
    print(f" Gravando {total_registros} registros no banco...")
    update_status_fields(
        "emp",
        upload_id,
        progress=0,
        message=f"Gravando registros no banco (0/{total_registros}).",
    )
    total = 0
    for start in range(0, len(registros), BATCH_SIZE):
        if read_cancel_flag("emp", upload_id):
            raise RuntimeError("PROCESSAMENTO_CANCELADO")
        chunk = registros[start : start + BATCH_SIZE]
        try:
            db.session.execute(insert_sql, chunk)
            db.session.commit()
            total += len(chunk)
            print(f" Inseridos {total}/{total_registros} registros...")
            if total_registros:
                progress = min(100, int((total / total_registros) * 100))
            else:
                progress = 100
            update_status_fields(
                "emp",
                upload_id,
                progress=progress,
                message=f"Gravando registros no banco ({total}/{total_registros}).",
            )
        except SQLAlchemyError:
            db.session.rollback()
            raise
    return total


def run_emp(file_path: Path, data_arquivo: datetime, user_email: str, upload_id: int) -> tuple[int, Path]:
    ensure_dirs()
    move_existing_to_tmp(OUTPUT_DIR)
    output_path = processar_emp(file_path)

    df_tratado = pd.read_excel(output_path, sheet_name="emp_tratado", dtype=str)
    colunas_data_emp = {"data_emissao", "data_criacao", "data_atualizacao", "data_arquivo"}
    for col in df_tratado.columns:
        if _normalize_col(col) in colunas_data_emp:
            serie_str = df_tratado[col].astype(str).str.strip()
            if serie_str.str.match(r"\d{4}-\d{2}-\d{2}").all():
                df_tratado[col] = pd.to_datetime(serie_str, errors="coerce", dayfirst=False)
            else:
                df_tratado[col] = pd.to_datetime(serie_str, errors="coerce", dayfirst=True)
    total = update_database(df_tratado, data_arquivo, user_email, upload_id)
    return total, output_path
