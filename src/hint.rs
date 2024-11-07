pub use colored::ColoredString;
pub use colored::Colorize;

#[derive(Debug)]
pub struct Hint {
    pub msg: Vec<ColoredString>,
}

impl Hint {
    pub fn new() -> Self {
        Hint {
            msg: Vec::with_capacity(4),
        }
    }
}

impl std::fmt::Display for Hint {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for m in &self.msg {
            write!(f, "{}", m)?;
        }
        Ok(())
    }
}

impl Hint {
    pub fn red(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.red());
        self
    }

    pub fn green(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.green());
        self
    }

    pub fn cyan(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.cyan());
        self
    }

    pub fn white(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.white());
        self
    }

    fn last(&mut self) -> Option<ColoredString> {
        self.msg.pop()
    }

    pub fn underline(&mut self) -> &mut Self {
        if let Some(s) = self.last() {
            self.msg.push(s.underline());
        }
        self
    }

    pub fn bold(&mut self) -> &mut Self {
        if let Some(s) = self.last() {
            self.msg.push(s.bold());
        }
        self
    }

    pub fn next_line(&mut self) -> &mut Self {
        self.msg.push("\n".white());
        self
    }

    pub fn clear(&mut self) -> &mut Self {
        self.msg.clear();
        self
    }

    pub fn error(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.red().bold());
        self
    }

    pub fn warning(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.yellow().bold());
        self
    }

    pub fn info(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.cyan().bold());
        self
    }

    pub fn success(&mut self, s: &str) -> &mut Self {
        self.msg.push(s.green().bold());
        self
    }

    pub fn print_and_exit(&self) -> ! {
        eprintln!("{}", self);
        std::process::exit(1);
    }

    pub fn print_and_clear(&mut self) {
        eprintln!("{}", self);
        self.clear();
    }
}
