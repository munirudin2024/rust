use std::env;

pub struct TerminalStyle {
    enabled: bool,
}

impl TerminalStyle {
    pub fn detect() -> Self {
        let no_color = env::var_os("NO_COLOR").is_some();
        let term_is_dumb = env::var("TERM")
            .map(|v| v.eq_ignore_ascii_case("dumb"))
            .unwrap_or(false);

        Self {
            enabled: !no_color && !term_is_dumb,
        }
    }

    pub fn header(&self, text: &str) -> String {
        self.bold(text)
    }

    pub fn stage(&self, text: &str) -> String {
        self.cyan(&self.bold(text))
    }

    pub fn stage_overview(&self, text: &str) -> String {
        self.cyan(&self.bold(text))
    }

    pub fn stage_audit(&self, text: &str) -> String {
        self.blue(&self.bold(text))
    }

    pub fn stage_cleaning(&self, text: &str) -> String {
        self.yellow(&self.bold(text))
    }

    pub fn stage_feature(&self, text: &str) -> String {
        self.green(&self.bold(text))
    }

    pub fn stage_validation(&self, text: &str) -> String {
        self.cyan(&self.bold(text))
    }

    pub fn stage_iso_gate(&self, text: &str) -> String {
        self.blue(text)
    }

    pub fn info(&self, text: &str) -> String {
        self.blue(text)
    }

    pub fn success(&self, text: &str) -> String {
        self.green(&self.bold(text))
    }

    pub fn good(&self, text: &str) -> String {
        self.green(text)
    }

    pub fn warning(&self, text: &str) -> String {
        self.yellow(&self.bold(text))
    }

    pub fn caution(&self, text: &str) -> String {
        self.yellow(text)
    }

    pub fn error(&self, text: &str) -> String {
        self.red(&self.bold(text))
    }

    pub fn critical(&self, text: &str) -> String {
        self.red(text)
    }

    pub fn neutral(&self, text: &str) -> String {
        self.cyan(text)
    }

    pub fn field_line(&self, prefix: &str, label: &str, value: &str, label_width: usize) -> String {
        let padded_label = format!("{:<width$}", label, width = label_width);

        if !self.enabled {
            return format!("{} {} : {}", prefix, padded_label, value);
        }

        format!(
            "{} {} : {}",
            self.cyan(prefix),
            self.bold(&padded_label),
            self.green(value)
        )
    }

    pub fn divider(&self, width: usize) -> String {
        if width == 0 {
            return String::new();
        }
        "─".repeat(width)
    }

    pub fn box_title(&self, title: &str, width: usize) -> String {
        if width < 4 {
            return title.to_string();
        }

        let inner_width = width - 2;
        let clean_title = title.trim();
        let title_len = clean_title.chars().count();
        let pad_total = inner_width.saturating_sub(title_len);
        let left = pad_total / 2;
        let right = pad_total - left;

        let top = format!("┌{}┐", "─".repeat(inner_width));
        let middle = format!("│{}{}{}│", " ".repeat(left), clean_title, " ".repeat(right));
        let bottom = format!("└{}┘", "─".repeat(inner_width));

        [self.bold(&top), self.bold(&middle), self.bold(&bottom)].join("\n")
    }

    fn colorize(&self, text: &str, code: &str) -> String {
        if !self.enabled {
            return text.to_string();
        }
        format!("\x1b[{}m{}\x1b[0m", code, text)
    }

    fn bold(&self, text: &str) -> String {
        self.colorize(text, "1")
    }

    fn blue(&self, text: &str) -> String {
        self.colorize(text, "34")
    }

    fn cyan(&self, text: &str) -> String {
        self.colorize(text, "36")
    }

    fn green(&self, text: &str) -> String {
        self.colorize(text, "32")
    }

    fn yellow(&self, text: &str) -> String {
        self.colorize(text, "33")
    }

    fn red(&self, text: &str) -> String {
        self.colorize(text, "31")
    }
}
