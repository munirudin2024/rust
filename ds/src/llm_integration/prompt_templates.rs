//! Prompt Templates untuk LLM Semantic Validation
//! Template untuk semantic validation queries

pub struct PromptTemplate {
    pub name: String,
    pub template: String,
}

impl PromptTemplate {
    pub fn semantic_validation() -> Self {
        Self {
            name: "SemanticValidation".to_string(),
            template: r#"
Validate the semantic correctness of the following data record:
{record_json}

Check against these business rules:
{business_rules}

Provide:
1. Is valid? (YES/NO)
2. Issues found (if any)
3. Suggested corrections
4. Confidence level (0-100%)
"#.to_string(),
        }
    }

    pub fn data_quality_assessment() -> Self {
        Self {
            name: "DataQualityAssessment".to_string(),
            template: r#"
Assess data quality dimensions for:
{dataset_summary}

Evaluate:
- Accuracy
- Completeness
- Consistency
- Timeliness
- Validity

Provide ISO/IEC 25012 compliance score.
"#.to_string(),
        }
    }

    pub fn replace_vars(&self, vars: &[(&str, &str)]) -> String {
        let mut result = self.template.clone();
        for (key, value) in vars {
            result = result.replace(&format!("{{{}}}", key), value);
        }
        result
    }
}
