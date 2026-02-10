# 📊 Meta Ads ETL + Banco + BI (Analytics Engineering End‑to‑End)

Pipeline **end-to-end** de **Analytics Engineering** que coleta dados da **API do Meta (Facebook/Instagram Ads)**, realiza **ETL em Python**, persiste em **banco relacional** e disponibiliza a camada de dados para **consumo por BI** (dashboards e relatórios).

✅ **Orquestração diária com Jenkins** (job agendado) para manter os dados sempre atualizados.

---

## 🎯 O que este projeto entrega (visão de Analytics Engineer)

- **Ingestão via API** (Meta Ads)
- **Transformações e padronizações** para análise
- **Carga no banco (UPSERT/MERGE)** para manter histórico e evitar duplicidade
- **Modelagem analítica** por tabelas (dimensão + fatos)
- **Camada de BI** consumindo do banco para dashboards/KPIs
- **Automação diária** com Jenkins

---

## 🧠 Arquitetura

```text
API do Meta
    ↓
ETL (Python) — extract → transform → load
    ↓
Banco Relacional (PostgreSQL recomendado)
    ↓
BI (Power BI / Looker / Tableau / Metabase)
```

---

## 🗃️ Tabelas geradas no banco

O pipeline grava (por padrão) três entidades no banco:

- **`ads_dimension`** → dimensão com mapeamentos de **IDs e nomes** (base para joins e leitura humana)
- **`ads_campaign_performance`** → **performance agregada** (nível de campanha/insights)
- **`ads_lead_insights`** → **leads em alta granularidade** (insights/demografia/geografia)

> Os nomes das tabelas estão definidos em `src/main.py` e podem ser ajustados conforme sua modelagem.

---

## 📂 Estrutura do repositório

```text
📦src
 ┣ 📂dimensao
 ┃ ┣ 📜pipeline.py
 ┃ ┗ 📜__init__.py
 ┣ 📂leads
 ┃ ┣ 📜pipeline.py
 ┃ ┗ 📜__init__.py
 ┣ 📂performance
 ┃ ┣ 📜pipeline.py
 ┃ ┗ 📜__init__.py
 ┣ 📜extract.py
 ┣ 📜load.py
 ┣ 📜main.py
 ┣ 📜transform.py
 ┗ 📜__init__.py
```

---

## ✅ Pré-requisitos

- Python 3.10+ (recomendado)
- Acesso/credenciais da **API do Meta**
- Banco de dados (PostgreSQL recomendado)
- (Produção) Jenkins para agendamento diário

---

## 🔐 Configuração (.env)

1) Crie um arquivo `.env` na raiz (ou injete variáveis no Jenkins) usando o modelo:

```bash
cp .env.example .env
```

2) Preencha os campos:

- **Meta API**: `APP_ID`, `APP_SECRET`, `ACCESS_TOKEN`, `AD_ACCOUNT_ID`
- **DB**: `DB_DIALECT`, `DB_DRIVER`, `DB_USER`, `DB_PASS`, `DB_HOST`, `DB_PORT`, `DB_NAME`

---

## ▶️ Como executar localmente

### 1) Instalar dependências
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Rodar o fluxo completo (recomendado)
```bash
python -m src.main
```

### 3) Rodar pipelines isolados (opcional)
```bash
python -m src.dimensao.pipeline
python -m src.performance.pipeline
python -m src.leads.pipeline
```

---

## 🤖 Orquestração com Jenkins (diário)

Este repositório já inclui um **`Jenkinsfile`** com agendamento diário via `cron()` e execução do comando:

```bash
python -m src.main
```

**Boas práticas sugeridas no Jenkins:**
- Guardar `.env` via **Credentials** (Secret file / Secret text)
- Persistir logs em `logs/` (se você gerar arquivos de log)
- Notificar falhas (Slack/Email) para garantir confiabilidade do pipeline

---

## 📊 Camada de BI (fechando o ciclo)

O banco alimentado por este ETL serve como **fonte única de verdade** para dashboards de BI, permitindo:
- Monitoramento de KPIs (leads, custo, cliques, conversões)
- Comparativos por período (dia/semana/mês)
- Análises por campanha/conjunto/anúncio (dependendo da granularidade coletada)
- Visão operacional + visão gerencial

> Em um projeto completo de **Analytics Engineer**, o valor final se materializa justamente aqui: **dados confiáveis no BI**.

---

## 🧩 Observações importantes

- O fluxo principal roda em sequência: **dimensão → performance → leads**
- O módulo `src/load.py` implementa carga com estratégia de **UPSERT** (mantém dados atualizados sem duplicar)
- Ajuste o período de coleta (`TOTAL_DAYS_*`) em `src/main.py` conforme sua necessidade (histórico vs. incremental)

---

## 🛣️ Próximas evoluções (ideias)

- Testes de qualidade de dados (ex.: Great Expectations)
- Monitoramento/alertas (ex.: falhas de API, volume anômalo)
- Camada semântica para BI (métricas padronizadas)
- Migração para Data Warehouse (se crescer volume/complexidade)

---

## 🧑‍💻 Autor

Projeto desenvolvido para consolidar um pipeline completo de **Analytics Engineering** (API → ETL → DB → BI + Orquestração diária).
