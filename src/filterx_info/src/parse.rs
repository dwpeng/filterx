use colored::{Color, ColoredString, Colorize};
use markdown::{tokenize, Block, ListItem, Span};
use memchr::memchr2;

pub trait ToLines {
    fn to_lines(&self) -> Vec<ColoredString>;
}

struct CodeLine<'a> {
    pub line: &'a str,
    pub is_comment: bool,
    pub is_comment_start: bool,
    pub other: Option<&'a str>,
}

impl<'a> CodeLine<'a> {
    pub fn new(line: &'a str) -> Self {
        if line.starts_with("//") || line.starts_with("#") {
            return Self::start_comment(line);
        } else if line.contains("//") || line.contains("#") {
            return Self::contain_comment(line);
        } else {
            return Self::code(line);
        }
    }

    fn code(line: &'a str) -> Self {
        Self {
            line,
            is_comment: false,
            is_comment_start: false,
            other: None,
        }
    }

    fn start_comment(line: &'a str) -> Self {
        Self {
            line,
            is_comment: true,
            is_comment_start: true,
            other: None,
        }
    }

    fn contain_comment(line: &'a str) -> Self {
        // split by `//` or `#`
        if line.contains("//") {
            let offset = memchr2(b'/', b'/', line.as_bytes()).unwrap();
            let (line, other) = line.split_at(offset);
            return Self {
                line,
                is_comment: true,
                is_comment_start: false,
                other: Some(other),
            };
        } else if line.contains("#") {
            let offset = memchr2(b'#', b'#', line.as_bytes()).unwrap();
            let (line, other) = line.split_at(offset);
            return Self {
                line,
                is_comment: true,
                is_comment_start: false,
                other: Some(other),
            };
        }
        return Self {
            line,
            is_comment: false,
            is_comment_start: false,
            other: None,
        };
    }
}

impl<'a> ToLines for CodeLine<'a> {
    fn to_lines(&self) -> Vec<ColoredString> {
        if self.is_comment && self.is_comment_start {
            return vec![self.line.dimmed().italic(), "\n".into()];
        }
        if self.is_comment && !self.is_comment_start {
            return vec![
                self.line.cyan(),
                self.other.unwrap().italic().dimmed(),
                "\n".into(),
            ];
        }
        return vec![self.line.cyan(), "\n".into()];
    }
}

fn process_code<'a>(code: &'a str) -> Vec<ColoredString> {
    // split by `\n`
    let lines: Vec<&'a str> = code.split("\n").collect();
    let mut code_lines = vec![];
    for line in lines {
        let line = line.trim();
        code_lines.push(CodeLine::new(line));
    }
    code_lines.iter().map(|x| x.to_lines()).flatten().collect()
}

impl ToLines for Block {
    fn to_lines(&self) -> Vec<ColoredString> {
        let mut lines = match self {
            Block::Paragraph(paragraphs) => {
                let lines = paragraphs.iter().flat_map(|x| x.to_lines()).collect();
                lines
            }

            Block::Header(h, level) => {
                let mut lines: Vec<_> = h
                    .iter()
                    .flat_map(|x| {
                        x.to_lines()
                            .iter()
                            .map(|x| x.clone().green().bold())
                            .collect::<Vec<_>>()
                    })
                    .collect();
                let level = if *level == 1 { 0 } else { *level };
                if level > 0 {
                    lines.insert(0, "#".repeat(level).green().bold());
                    lines.insert(1, " ".into());
                }
                lines
            }

            Block::CodeBlock(language, code) => {
                let mut lines = vec![];
                let mut have_filename = 0;
                if let Some(lang) = language {
                    let pos = lang.find("title=");
                    if pos.is_some() {
                        let filename = lang.split("title=").nth(1);
                        if let Some(filename) = filename {
                            let filename = filename.trim();
                            let filename = filename.trim_matches('"');
                            let filename = filename.trim_matches('\'');
                            lines.push(filename.cyan().bold());
                            lines.push("\n".into());
                            lines.push("-".repeat(filename.len()).cyan());
                            lines.push("\n".into());
                            have_filename = filename.len();
                        }
                    }
                }
                let code_lines = process_code(code);
                lines.extend(code_lines);
                if have_filename > 0 {
                    lines.push("-".repeat(have_filename).cyan());
                }
                lines
            }

            Block::Blockquote(b) => {
                let mut lines: Vec<_> = b
                    .iter()
                    .flat_map(|x| {
                        let mut plines = x.to_lines();
                        plines.pop();
                        plines
                            .iter()
                            .map(|x| x.clone().dimmed().italic())
                            .collect::<Vec<_>>()
                    })
                    .collect();
                lines.insert(0, "> ".dimmed());
                lines
            }

            Block::Raw(r) => vec![r.color(Color::Blue)],
            Block::UnorderedList(items) => {
                let mut lines = vec![];
                for item in items {
                    lines.push("* ".blue());
                    lines.extend(item.to_lines());
                    lines.push("\n".into());
                }
                lines.pop();
                lines
            }
            Block::Hr => vec![String::from("---").color(Color::Blue)],
            Block::OrderedList(items, _order) => {
                let mut lines = vec![];

                for (index, item) in items.iter().enumerate() {
                    lines.push(format!("{:}. ", index + 1).blue());
                    lines.extend(item.to_lines());
                    lines.push("\n".into());
                }
                lines.pop();
                lines
            }
        };
        lines.push("\n\n".into());
        lines
    }
}

impl ToLines for ListItem {
    fn to_lines(&self) -> Vec<ColoredString> {
        match self {
            ListItem::Paragraph(p) => {
                let mut lines = vec![];
                for line in p {
                    lines.extend(line.to_lines());
                }
                lines
            }
            ListItem::Simple(items) => {
                let mut lines = vec![];
                for item in items {
                    lines.extend(item.to_lines());
                }
                lines
            }
        }
    }
}

impl ToLines for Span {
    fn to_lines(&self) -> Vec<ColoredString> {
        match self {
            Span::Break => vec!["\n".into()],
            Span::Text(t) => vec![t.as_str().into()],
            Span::Emphasis(s) => {
                let lines = s.iter().flat_map(|x| x.to_lines()).collect::<Vec<_>>();
                let lines = lines.iter().map(|x| x.clone().italic()).collect::<Vec<_>>();
                lines
            }
            Span::Strong(s) => {
                let lines = s.iter().flat_map(|x| x.to_lines()).collect::<Vec<_>>();
                let lines = lines.iter().map(|x| x.clone().bold()).collect::<Vec<_>>();
                lines
            }
            Span::Link(name, url, _) => {
                let mut lines = vec![];
                lines.push(name.color(Color::Blue).bold());
                lines.push(format!(" ({})", url).dimmed());
                lines
            }
            Span::Image(_, _, _) => vec![],
            Span::Code(c) => {
                vec![c.color(Color::Blue)]
            }
        }
    }
}

pub fn parse(markdown: &str) -> Vec<ColoredString> {
    let blocks = tokenize(markdown);
    let mut lines: Vec<_> = blocks.into_iter().flat_map(|b| b.to_lines()).collect();
    lines.pop();
    lines
}
