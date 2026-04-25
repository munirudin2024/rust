//! LLM Integration Module (Optional)
//! Integrasi dengan Ollama atau LLM lain untuk semantic validation

pub mod ollama_client;
pub mod prompt_templates;

pub use ollama_client::OllamaClient;
pub use prompt_templates::PromptTemplate;
