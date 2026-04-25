//! Ollama Client for Local LLM Integration
//! Komunikasi dengan Ollama untuk semantic validation

use anyhow::Result;

pub struct OllamaClient {
    pub base_url: String,
    pub model: String,
    pub enabled: bool,
}

impl OllamaClient {
    pub fn new(base_url: String, model: String) -> Self {
        Self {
            base_url,
            model,
            enabled: false, // Default disabled, aktif saat config enable
        }
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

    pub async fn validate_semantic(&self, _prompt: String) -> Result<String> {
        if !self.enabled {
            return Err(anyhow::anyhow!("Ollama client disabled"));
        }

        // Implementation akan ditambahkan di BAGIAN 2
        Ok("OK".to_string())
    }
}
