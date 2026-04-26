# RAG Architecture & Implementation Design

## 1. High-Level Solution Architecture

The architecture seamlessly bridges structured data derived from the ETL pipeline (Gold Layer) and unstructured domain knowledge (Documentation, FAQs) to answer user queries using natural language.

### Application Layer
- **User Interface (Chatbot UI)**: A conversational frontend (e.g., Streamlit, Gradio, or a web application) where internal users interact via text.
- **Query Router Agent**: An LLM-powered chain that interprets user intent. It classifies whether a question requires statistical/aggregated data (routed to the SQL agent) or factual explanations (routed to the Vector RAG pipeline).
- **Text-to-SQL Agent**: Converts natural language into valid SQL queries based on the Gold layer schemas provided in the prompt context.
- **Document RAG Pipeline**: Standard Retrieval-Augmented Generation that fetches relevant chunks from a vector database to supply context to the LLM.

### Infrastructure Layer
- **Orchestration**: LangChain (or LlamaIndex) handles chaining the LLM calls, managing prompts, and invoking tools/retrievers.
- **Large Language Model**: Google Generative AI (Gemini) powers the router, text-to-SQL synthesis, and answering nodes.
- **Structured Data Engine (SQL)**: Delta Lake tables (Gold layer) queried directly using DuckDB. This enables extremely fast, local or cloud-object-store analytical queries.
- **Unstructured Data Engine (Vector DB)**: A vector store handling document embeddings (created via models like `sentence-transformers`).

---

## 2. Component Interactions & Data Flow

1. **User Query Input**: The user asks a question like *"What is the renewable percentage for France today?"* or *"How does the API calculate carbon intensity?"*
2. **Intent Classification**: The **Query Router Agent** evaluates the prompt.
   - If the query implies *"data extraction"*, it routes to **Flow A (Text-to-SQL)**.
   - If the query implies *"knowledge retrieval"*, it routes to **Flow B (Vector RAG)**.
3. **Flow A (Text-to-SQL)**:
   - The LLM receives the prompt alongside the schema definitions of `daily_electricity_mix`, `daily_imports`, and `daily_exports`.
   - The LLM generates a SQL query.
   - The query is executed via DuckDB against the Gold Layer Delta tables.
   - The resultant DataFrame is returned/summarized to the user.
4. **Flow B (Vector RAG)**:
   - The query is embedded using HuggingFace sentence transformers.
   - A similarity search fetches the top-K relevant document chunks from the Vector DB.
   - The LLM synthesizes an answer based strictly on the retrieved context.

---

## 3. Technology Stack & Production Considerations

### Recommended Stack
- **Data Engineering**: Polars, Delta-RS, DuckDB
- **LLM Orchestration**: LangChain
- **Embeddings**: HuggingFace `all-MiniLM-L6-v2`
- **Generative Model**: Google Gemini Pro

### Vector DB: Is Chroma DB sufficient for production?
**Chroma DB** is excellent for local development, prototyping, and small-to-medium scale production environments (especially when hosted persistently). However, for a true enterprise-grade production system, other options might be more suitable depending on requirements:

- **When to stay with Chroma DB**:
  - The document corpus is relatively small (under ~1 million embeddings).
  - High concurrency is not a major concern.
  - Deployment simplicity is prioritized over distributed scaling.
- **When to upgrade (Production Alternatives)**:
  - **Pinecone / Weaviate Cloud**: Best for managed, serverless, massive scale with high concurrency.
  - **Qdrant**: Excellent performance with a very capable open-source standalone engine.
  - **pgvector**: If you already utilize PostgreSQL in your backend infrastructure, `pgvector` prevents the need to introduce and maintain a completely new database technology to the stack.

For this specific usecase, where the primary document is `Electricity_Maps_doc.pdf` and other operational FAQs, **Chroma DB is entirely sufficient for production** due to the bounded size of the unstructured data corpus.

---

## 4. Implementation Plan

The `06_rag_chatbot.ipynb` notebook has been updated to reflect this complete architecture:
1. **Environment Initialization**: Re-imported DuckDB and `Settings` to dynamically handle database bindings.
2. **Gold Layer Integration**: Mapped `daily_electricity_mix`, `daily_imports`, and `daily_exports` into a unified schema context string.
3. **ChromaDB Initialization**: Loads and embeds the API documentation into the local Vector Store.
4. **Agent Definitions**:
   - `router_chain`: A prompt forcing the LLM to output "SQL" or "RAG" based on semantic analysis of the question.
   - `sql_chain`: A text-to-SQL prompt heavily constrained to query only the specified Delta schemas.
   - `rag_chain`: A standard contextual QA chain.
5. **Unified Interface (`ask_chatbot`)**: Wraps the routing, generation, and DuckDB execution gracefully, showing the user the decision path and the ultimate result.
