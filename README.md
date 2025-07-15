# 🧠 Segmentação de Clientes com Pipeline Spark-Nativo e PCA

Este projeto implementa um pipeline de **segmentação comportamental de clientes** utilizando uma abordagem **100% Spark nativa**, voltada para grandes volumes de dados e otimizada para execução em ambientes distribuídos como o **Databricks**.

---

## 🎯 Objetivo

Identificar perfis de comportamento de clientes com base no histórico de compras e categorias de produtos, aplicando técnicas robustas de:

- **Engenharia de Atributos**
- **Redução de Dimensionalidade com PCA**
- **Clusterização com KMeans**
- **Nomeação Dinâmica de Perfis**
- **Cálculo de Similaridade entre Clientes**

---

## 🏗️ Arquitetura do Pipeline

O pipeline está organizado em etapas modulares e escaláveis:

| Etapa                        | Descrição                                                                 |
|-----------------------------|---------------------------------------------------------------------------|
| 1️⃣ Pré-processamento       | Leitura de dados `.parquet`, sanitização de colunas e tratamento de datas |
| 2️⃣ Feature Engineering     | Criação dos vetores RFMT e Mix Binário de categorias                      |
| 3️⃣ Redução com PCA         | Aplicação obrigatória de PCA com proteção contra erros                    |
| 4️⃣ Clusterização           | KMeans com tuning manual e avaliação via Silhouette Score                 |
| 5️⃣ Nomeação dos Perfis     | Atribuição dinâmica com base em ticket médio, frequência e categorias     |
| 6️⃣ Similaridade com LSH    | Identificação de clientes similares via LSH e broadcast eficiente         |
| 7️⃣ Exportação              | Geração de arquivos `.csv` para análises e dashboards                     |

---

## 📦 Tecnologias Utilizadas

- Apache Spark 3.x
- PySpark MLlib
- Databricks (compatível)
- Python 3.9+
- MLflow (opcional para persistência de modelos)

---

## 📊 Métricas de Avaliação

- **Silhouette Score**: Avalia a coesão e separação dos clusters
- **Distribuição de Categorias**: Por cluster, para análise qualitativa
- **Nomeação Dinâmica**: Baseada em múltiplos critérios quantitativos e semânticos

---

## ✅ Proteções Incluídas

- Remoção de colinearidade
- PCA com fallback seguro (`min([a,b])`)
- Correção de uso incorreto de `sum()` e `min()` do Python nativo
- Proteção contra colunas ambíguas
- Alinhamento automático de colunas antes de matching
- Broadcast de vetores com segurança contra estouro
