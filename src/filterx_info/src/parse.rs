use colored::{Color, ColoredString, Colorize};
// use markdown::{tokenize, Block, ListItem, Span};
use markdown::{mdast::*, to_mdast};

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

impl ToLines for Node {
    fn to_lines(&self) -> Vec<ColoredString> {
        let mut lines = match self {
            Node::Root(root) => {
                let mut lines = vec![];
                lines.extend(root.children.iter().flat_map(|x| x.to_lines()));
                lines
            }

            Node::Paragraph(paragraphs) => {
                let mut lines = vec![];

                for node in paragraphs.children.iter() {
                    if let Node::InlineCode(_) = node {
                        lines.pop();
                        lines.extend(node.to_lines());
                        lines.pop();
                    } else {
                        lines.extend(node.to_lines());
                    }
                }
                lines
            }

            Node::Heading(h) => {
                let mut lines: Vec<_> = h
                    .children
                    .iter()
                    .flat_map(|x| {
                        x.to_lines()
                            .iter()
                            .map(|x| x.clone().green().bold())
                            .collect::<Vec<_>>()
                    })
                    .collect();
                let level = h.depth as usize;
                if level > 0 {
                    lines.insert(0, "#".repeat(level).green().bold());
                    lines.insert(1, " ".into());
                }
                lines
            }

            Node::Code(code) => {
                let mut lines = vec![];
                let title = &code.meta;
                let is_example = title.is_some()
                    && title
                        .as_ref()
                        .and_then(|x| Some(x.to_lowercase().contains("example")))
                        .unwrap_or(false);

                let color = if is_example {
                    Color::BrightMagenta
                } else {
                    Color::Cyan
                };

                if let Some(lang) = title {
                    let pos = lang.find("title=");
                    if pos.is_some() {
                        let filename = lang.split("title=").nth(1);
                        if let Some(filename) = filename {
                            let filename = filename.trim();
                            let filename = filename.trim_matches('"');
                            let filename = filename.trim_matches('\'');
                            // draw a box
                            let box_top_border =
                                "+".to_string() + &"-".repeat(filename.len() + 2) + "+\n";
                            lines.push(box_top_border.color(color));
                            let content = "| ".to_string() + filename + " |\n";
                            lines.push(content.color(color).bold());
                            lines.push(box_top_border.color(color));
                        }
                    }
                }
                let code_lines = process_code(&code.value);
                lines.extend(code_lines);
                lines
            }

            Node::Blockquote(b) => {
                let mut lines: Vec<_> = b
                    .children
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
            Node::Text(t) => vec![t.value.color(Color::Blue)],
            Node::List(items) if !items.ordered => {
                let mut lines = vec![];
                for item in &items.children {
                    lines.push("* ".blue());
                    lines.extend(item.to_lines());
                    lines.push("\n".into());
                }
                lines.pop();
                lines
            }
            Node::Break(_) => vec![String::from("---").color(Color::Blue)],
            Node::List(items) if items.ordered => {
                let mut lines = vec![];

                for (index, item) in items.children.iter().enumerate() {
                    lines.push(format!("{:}. ", index + 1).blue());
                    lines.extend(item.to_lines());
                    lines.push("\n".into());
                }
                lines.pop();
                lines
            }
            Node::ListItem(item) => {
                let mut lines = vec![];
                for line in &item.children {
                    lines.extend(line.to_lines());
                }
                lines
            }
            Node::Strong(s) => {
                let lines = s
                    .children
                    .iter()
                    .flat_map(|x| x.to_lines())
                    .collect::<Vec<_>>();
                let lines = lines.iter().map(|x| x.clone().bold()).collect::<Vec<_>>();
                lines
            }
            Node::Emphasis(e) => {
                let lines = e
                    .children
                    .iter()
                    .flat_map(|x| x.to_lines())
                    .collect::<Vec<_>>();
                let lines = lines.iter().map(|x| x.clone().italic()).collect::<Vec<_>>();
                lines
            }
            Node::Link(link) => {
                let name = &link.title;
                let url = &link.url;
                let name = if name.is_none() {
                    url.clone()
                } else {
                    name.clone().unwrap()
                };
                let mut lines = vec![];
                lines.push(name.color(Color::Blue).bold());
                lines.push(format!(" ({})", url).dimmed());
                lines
            }
            Node::Image(i) => {
                let mut lines = vec![];
                lines.push(i.alt.color(Color::Blue).bold());
                lines.push(format!(" ({})", i.url).dimmed());
                lines
            }
            Node::InlineCode(c) => {
                let mut lines = vec![];
                lines.push(c.value.color(Color::Blue).bold());
                lines
            }
            _ => {
                let lines = vec![];
                lines
            }
        };
        lines.push("\n".into());
        lines
    }
}

pub fn parse(markdown: &str) -> Vec<ColoredString> {
    let options = markdown::ParseOptions::default();
    let blocks = to_mdast(markdown, &options);
    let mut lines: Vec<_> = blocks.into_iter().flat_map(|b| b.to_lines()).collect();
    lines.pop();
    lines.pop();
    lines
}
